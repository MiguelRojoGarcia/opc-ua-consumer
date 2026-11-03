package application

import (
	"context"
	"errors"
	"testing"

	"opc-ua-consumer/internal/domain/measurement"
	"opc-ua-consumer/internal/shared/logger"
)

type fakePublisher struct {
	published []measurement.Measurement
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, m measurement.Measurement) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, m)
	return nil
}

func newMeasurement(t *testing.T) measurement.Measurement {
	t.Helper()
	v, err := measurement.NewDecimalValue(78.5)
	if err != nil {
		t.Fatal(err)
	}
	m, err := measurement.New("MAD-01", "Line1.PLC1", "Temperature", v, "°C", measurement.QualityGood)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewPublishMeasurementServiceRequiresPublisher(t *testing.T) {
	if _, err := NewPublishMeasurementService(nil, logger.Nop()); err == nil {
		t.Error("expected error without publisher")
	}
}

func TestHandlePublishes(t *testing.T) {
	pub := &fakePublisher{}
	svc, err := NewPublishMeasurementService(pub, logger.Nop())
	if err != nil {
		t.Fatal(err)
	}
	m := newMeasurement(t)
	if err := svc.Handle(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if len(pub.published) != 1 || pub.published[0] != m {
		t.Errorf("published %+v", pub.published)
	}
}

func TestHandleReturnsPublisherError(t *testing.T) {
	boom := errors.New("broker down")
	svc, err := NewPublishMeasurementService(&fakePublisher{err: boom}, logger.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Handle(context.Background(), newMeasurement(t)); !errors.Is(err, boom) {
		t.Errorf("got %v, want %v", err, boom)
	}
}
