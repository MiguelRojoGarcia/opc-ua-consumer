//go:build integration

package kafka

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"opc-ua-consumer/internal/domain/measurement"
	"opc-ua-consumer/internal/shared/logger"
)

// Requiere Kafka arrancado (make up) o KAFKA_BROKERS apuntando a un broker:
//
//	go test -tags integration ./internal/infraestructure/kafka/...
func brokers() []string {
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		return []string{b}
	}
	return []string{"localhost:9094"}
}

func TestPublishIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	topic := "measurements"
	conn, err := NewConnector(Config{Brokers: brokers()}, logger.Nop())
	if err != nil {
		t.Fatal(err)
	}
	client, err := conn.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	pub, err := NewMeasurementPublisher(client, topic, logger.Nop())
	if err != nil {
		t.Fatal(err)
	}
	v, _ := measurement.NewDecimalValue(78.5)
	m, err := measurement.New(measurement.SiteCode("IT-"+time.Now().Format("150405.000000")), "Line1.PLC1", "Temperature", v, "°C", measurement.QualityGood)
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(ctx, m); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Publish es asíncrono: Flush espera a que el broker confirme.
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	for {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("measurement not consumed: %v", err)
		}
		for _, r := range fetches.Records() {
			if string(r.Key) != string(m.Site) {
				continue
			}
			var got measurement.Measurement
			if err := json.Unmarshal(r.Value, &got); err != nil {
				t.Fatal(err)
			}
			if got.Name != m.Name {
				t.Errorf("got %+v", got)
			}
			return
		}
	}
}
