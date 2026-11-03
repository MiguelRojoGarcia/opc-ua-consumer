package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"opc-ua-consumer/internal/application"
	"opc-ua-consumer/internal/domain/measurement"
	"opc-ua-consumer/internal/shared/logger"
)

var _ application.MeasurementPublisher = (*MeasurementPublisher)(nil)

// MeasurementPublisher is the Kafka adapter of application.MeasurementPublisher.
// Each measurement is sent as JSON with the site as key, so that all the
// measurements of a site go to the same partition and keep their order.
//
// Publishing is asynchronous so that franz-go can group many measurements in
// each batch: Publish only buffers the record and returns. Delivery errors
// (after franz-go's own retries, up to DeliveryTimeout) are logged.
type MeasurementPublisher struct {
	client *kgo.Client
	topic  string
	log    logger.Logger
}

// NewMeasurementPublisher builds the adapter on a client obtained from
// Connector. The caller keeps owning the client and must release it with Close.
func NewMeasurementPublisher(client *kgo.Client, topic string, log logger.Logger) (*MeasurementPublisher, error) {
	if client == nil {
		return nil, errors.New("kafka: client is required")
	}
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return nil, errors.New("kafka: topic is required")
	}
	return &MeasurementPublisher{client: client, topic: topic, log: log}, nil
}

// Publish buffers the measurement to be sent in the next batch. It only blocks
// when the buffer is full (backpressure), until there is room or ctx is done.
func (p *MeasurementPublisher) Publish(ctx context.Context, m measurement.Measurement) error {
	record, err := p.record(m)
	if err != nil {
		return err
	}
	// El record lleva un contexto que no se cancela con el de la aplicación: al
	// parar, los mensajes ya en el buffer se siguen enviando hasta que Close los
	// vacía. ctx solo sirve para dejar de esperar si el buffer está lleno.
	record.Context = context.WithoutCancel(ctx)

	// Produce no devuelve error: los fallos (incluido ctx cancelado esperando
	// hueco en el buffer) llegan al callback, que no debe bloquear.
	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			p.log.Error(r.Context, "measurement not delivered to Kafka",
				"topic", r.Topic, "key", string(r.Key), "error", err)
		}
	})
	return nil
}

func (p *MeasurementPublisher) record(m measurement.Measurement) (*kgo.Record, error) {
	value, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("kafka: serializing measurement: %w", err)
	}
	return &kgo.Record{Topic: p.topic, Key: []byte(m.Site), Value: value}, nil
}
