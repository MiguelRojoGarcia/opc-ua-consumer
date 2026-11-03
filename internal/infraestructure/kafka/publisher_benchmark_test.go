//go:build integration

package kafka

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"opc-ua-consumer/internal/domain/measurement"
	"opc-ua-consumer/internal/shared/logger"
)

// bytesHook counts the bytes of produce requests written to the brokers.
type bytesHook struct{ produced atomic.Int64 }

func (h *bytesHook) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, n int, _, _ time.Duration, err error) {
	if key == 0 && err == nil { // 0 = Produce
		h.produced.Add(int64(n))
	}
}

// BenchmarkPublish compara configuraciones del productor contra el broker real:
//
//	docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
//	  --create --if-not-exists --topic measurements-bench --partitions 6
//	go test -tags integration -run '^$' -bench Publish -benchtime 200000x ./internal/infraestructure/kafka/
//
// Publica medidas de 18 tags repartidas entre "sites" centros.
func BenchmarkPublish(b *testing.B) {
	cases := []struct {
		name  string
		cfg   Config
		sites int
		sync  bool
	}{
		{"sync/none", Config{Compression: CompressionNone}, 1, true},
		{"async/linger0/none", Config{Linger: time.Nanosecond, Compression: CompressionNone}, 1, false},
		{"async/linger10ms/snappy", Config{Linger: 10 * time.Millisecond, Compression: CompressionSnappy}, 1, false},
		{"async/linger50ms/none", Config{Compression: CompressionNone}, 1, false},
		{"async/linger50ms/snappy", Config{Compression: CompressionSnappy}, 1, false},
		{"async/linger50ms/lz4", Config{Compression: CompressionLz4}, 1, false},
		{"async/linger50ms/zstd", Config{Compression: CompressionZstd}, 1, false},
		{"async/linger50ms/zstd/6sites", Config{Compression: CompressionZstd}, 6, false},
	}
	tags := []string{
		"Temperature", "Pressure", "MotorSpeed", "Running", "ProductionCount", "HighTempAlarm",
		"TempSetpoint", "Energy.Power_kW", "Energy.Voltage_V", "Energy.Energy_kWh", "Level_pct",
		"InletValve", "OutletValve", "Status", "Ramp1", "Sine1", "Random1", "User1",
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			hook := &bytesHook{}
			tc.cfg.Brokers = brokers()
			conn, err := NewConnector(tc.cfg, logger.Nop())
			if err != nil {
				b.Fatal(err)
			}
			conn.opts = []kgo.Opt{kgo.WithHooks(hook)}
			client, err := conn.Connect(ctx)
			if err != nil {
				b.Fatal(err)
			}
			defer client.Close()
			pub, err := NewMeasurementPublisher(client, "measurements-bench", logger.Nop())
			if err != nil {
				b.Fatal(err)
			}

			// Medidas preconstruidas para medir solo la publicación.
			ms := make([]measurement.Measurement, 1000)
			for i := range ms {
				v, _ := measurement.NewDecimalValue(20 + float64(i%500)/10)
				site := measurement.SiteCode(fmt.Sprintf("center-%02d", i%tc.sites))
				ms[i], _ = measurement.New(site, "Line1.PLC1", measurement.MetricName(tags[i%len(tags)]), v, "", measurement.QualityGood)
			}

			sizes := make([]int64, len(ms))
			for i, m := range ms {
				r, _ := pub.record(m)
				sizes[i] = int64(len(r.Value) + len(r.Key))
			}

			var payload int64
			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				m := ms[i%len(ms)]
				payload += sizes[i%len(ms)]
				if tc.sync {
					r, _ := pub.record(m)
					if err := client.ProduceSync(ctx, r).FirstErr(); err != nil {
						b.Fatal(err)
					}
					continue
				}
				if err := pub.Publish(ctx, m); err != nil {
					b.Fatal(err)
				}
			}
			if err := client.Flush(ctx); err != nil {
				b.Fatal(err)
			}
			elapsed := time.Since(start)
			b.StopTimer()

			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/s")
			b.ReportMetric(float64(payload)/float64(b.N), "payload-B/msg")
			b.ReportMetric(float64(hook.produced.Load())/float64(b.N), "wire-B/msg")
		})
	}
}
