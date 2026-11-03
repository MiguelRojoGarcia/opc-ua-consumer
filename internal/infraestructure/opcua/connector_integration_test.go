//go:build integration

package opcua

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"opc-ua-consumer/internal/domain/measurement"
	"opc-ua-consumer/internal/domain/tag"
	"opc-ua-consumer/internal/shared/logger"
)

// Requiere el simulador arrancado (make up) o OPCUA_ENDPOINT apuntando a un servidor:
//
//	go test -tags integration ./internal/infraestructure/opcua/...
func endpoint() string {
	if e := os.Getenv("OPCUA_ENDPOINT"); e != "" {
		return e
	}
	return "opc.tcp://localhost:49320"
}

func TestConnectIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := NewConnector(Config{Endpoint: endpoint()}, logger.Nop())
	if err != nil {
		t.Fatal(err)
	}
	client, err := conn.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close(ctx)

	ns, err := client.FindNamespace(ctx, "KEPServerEX")
	if err != nil {
		t.Fatalf("find namespace: %v", err)
	}
	if ns == 0 {
		t.Errorf("unexpected namespace index 0")
	}
}

func TestConnectRetriesUntilContextCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	conn, err := NewConnector(Config{Endpoint: "opc.tcp://localhost:1"}, logger.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Connect(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context deadline, got %v", err)
	}
}

func TestMeasurementSourceIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := NewConnector(Config{Endpoint: endpoint()}, logger.Nop())
	if err != nil {
		t.Fatal(err)
	}
	client, err := conn.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close(context.Background())

	source, err := NewMeasurementSource(client, SourceConfig{
		Site:               "MAD-01",
		Namespace:          "KEPServerEX",
		SamplingInterval:   100 * time.Millisecond,
		PublishingInterval: 200 * time.Millisecond,
	}, logger.Nop())
	if err != nil {
		t.Fatal(err)
	}

	// Al crear cada monitored item el servidor envía su valor actual, así que
	// basta con esperar la primera medida de cada tag.
	tags := []tag.Tag{"Line1.PLC1.Temperature", "Line1.PLC1.Running", "Line1.Tank1.Status"}
	got := map[measurement.MetricName]measurement.Measurement{}
	subCtx, stop := context.WithCancel(ctx)
	err = source.Subscribe(subCtx, tags, handlerFunc(func(_ context.Context, m measurement.Measurement) error {
		got[m.Name] = m
		if len(got) == len(tags) {
			stop()
		}
		return nil
	}))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if len(got) != len(tags) {
		t.Fatalf("got %d measurements, want %d: %+v", len(got), len(tags), got)
	}
	if m := got["Temperature"]; m.Value.Kind != measurement.KindDecimal || m.DeviceId != "Line1.PLC1" || m.Site != "MAD-01" {
		t.Errorf("Temperature: %+v", m)
	}
	if m := got["Running"]; m.Value.Kind != measurement.KindBoolean {
		t.Errorf("Running: %+v", m)
	}
}

// handlerFunc adapts a function to application.MeasurementHandler in tests.
type handlerFunc func(ctx context.Context, m measurement.Measurement) error

func (f handlerFunc) Handle(ctx context.Context, m measurement.Measurement) error { return f(ctx, m) }
