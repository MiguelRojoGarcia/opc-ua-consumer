// Package application contains the use cases and the ports (interfaces) they
// need from the outside world. Ports only speak in domain types: the adapters
// that implement them live in the infrastructure layer.
package application

import (
	"context"

	"opc-ua-consumer/internal/domain/measurement"
	"opc-ua-consumer/internal/domain/tag"
)

// MeasurementHandler processes each measurement emitted by a MeasurementSource.
// PublishMeasurementService is the production implementation.
type MeasurementHandler interface {
	Handle(ctx context.Context, m measurement.Measurement) error
}

// MeasurementSource is the input port that delivers the readings of a set of
// tags as domain measurements.
type MeasurementSource interface {
	// Subscribe starts receiving changes of the given tags and passes each one
	// to handler. It blocks until ctx is cancelled (then it returns nil) or the
	// subscription cannot be created or kept. An error returned by the handler
	// does not stop the subscription.
	Subscribe(ctx context.Context, tags []tag.Tag, handler MeasurementHandler) error
}
