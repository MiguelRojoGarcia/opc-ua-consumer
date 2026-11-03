package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"opc-ua-consumer/internal/domain/measurement"
	"opc-ua-consumer/internal/shared/logger"
)

func TestConfigDefaults(t *testing.T) {
	cfg := Config{Brokers: []string{" localhost:9092 ", ""}}.withDefaults()
	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("brokers: %q", cfg.Brokers)
	}
	if cfg.ClientID != "opc-ua-consumer" || cfg.Linger != 50*time.Millisecond || cfg.BatchMaxBytes != 1_000_012 ||
		cfg.Compression != CompressionZstd || cfg.MaxBufferedRecords != 500_000 || cfg.MaxBufferedBytes != 256<<20 ||
		cfg.DeliveryTimeout != 2*time.Minute || cfg.MaxRetryBackoff != 30*time.Second {
		t.Errorf("defaults: %+v", cfg)
	}
}

func TestNewConnectorValidation(t *testing.T) {
	b := []string{"localhost:9092"}
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"minimal", Config{Brokers: b}, false},
		{"no brokers", Config{}, true},
		{"blank brokers", Config{Brokers: []string{" "}}, true},
		{"lz4 uppercase", Config{Brokers: b, Compression: "LZ4"}, false},
		{"none", Config{Brokers: b, Compression: CompressionNone}, false},
		{"unknown compression", Config{Brokers: b, Compression: "brotli"}, true},
		{"buffer smaller than batch", Config{Brokers: b, BatchMaxBytes: 4 << 20, MaxBufferedBytes: 1 << 20}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConnector(tt.cfg, logger.Nop())
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewMeasurementPublisherValidation(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := NewMeasurementPublisher(nil, "measurements", logger.Nop()); err == nil {
		t.Error("expected error without client")
	}
	if _, err := NewMeasurementPublisher(client, " ", logger.Nop()); err == nil {
		t.Error("expected error without topic")
	}
}

func TestRecordUsesSiteAsKeyAndJSONValue(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	pub, err := NewMeasurementPublisher(client, "measurements", logger.Nop())
	if err != nil {
		t.Fatal(err)
	}

	v, _ := measurement.NewDecimalValue(78.5)
	m, err := measurement.New("MAD-01", "Line1.PLC1", "Temperature", v, "°C", measurement.QualityGood)
	if err != nil {
		t.Fatal(err)
	}
	r, err := pub.record(m)
	if err != nil {
		t.Fatal(err)
	}
	if r.Topic != "measurements" || string(r.Key) != "MAD-01" {
		t.Errorf("topic/key: %q %q", r.Topic, r.Key)
	}
	var got measurement.Measurement
	if err := json.Unmarshal(r.Value, &got); err != nil {
		t.Fatal(err)
	}
	if got.DeviceId != m.DeviceId || got.Name != m.Name || got.Site != m.Site || !got.Timestamp.Equal(m.Timestamp) {
		t.Errorf("value: %s", r.Value)
	}
}
