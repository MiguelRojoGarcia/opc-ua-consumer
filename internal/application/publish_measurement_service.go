package application

import (
	"context"
	"errors"
	"fmt"

	"opc-ua-consumer/internal/domain/measurement"
	"opc-ua-consumer/internal/shared/logger"
)

// MeasurementPublisher is the output port that sends a measurement outside the
// service (Kafka, a queue, an HTTP endpoint...).
type MeasurementPublisher interface {
	Publish(ctx context.Context, m measurement.Measurement) error
}

// PublishMeasurementService receives measurements and publishes them through a
// MeasurementPublisher. It only depends on the port, never on an implementation.
type PublishMeasurementService struct {
	publisher MeasurementPublisher
	log       logger.Logger
}

func NewPublishMeasurementService(publisher MeasurementPublisher, log logger.Logger) (*PublishMeasurementService, error) {
	if publisher == nil {
		return nil, errors.New("application: publisher is required")
	}
	return &PublishMeasurementService{publisher: publisher, log: log}, nil
}

// Handle publishes a single measurement.
func (s *PublishMeasurementService) Handle(ctx context.Context, m measurement.Measurement) error {
	if err := s.publisher.Publish(ctx, m); err != nil {
		return fmt.Errorf("publishing measurement %s/%s: %w", m.DeviceId, m.Name, err)
	}
	s.log.Debug(ctx, "measurement published", "measurement", m)
	return nil
}

var _ MeasurementHandler = (*PublishMeasurementService)(nil)
