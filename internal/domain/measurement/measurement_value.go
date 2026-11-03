package measurement

import "math"

// ValueKind is the type of a MeasurementValue.
type ValueKind string

const (
	KindDecimal ValueKind = "DECIMAL"
	KindBoolean ValueKind = "BOOLEAN"
	KindText    ValueKind = "TEXT"
)

// MeasurementValue is the measured value, typed as decimal (float64), boolean
// (bool) or text (string). Build it with the New*Value constructors so Kind and
// the dynamic type of Value always match.
type MeasurementValue struct {
	Kind  ValueKind `json:"kind"`
	Value any       `json:"value"`
}

func NewDecimalValue(v float64) (MeasurementValue, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return MeasurementValue{}, ErrInvalidDecimalValue
	}
	return MeasurementValue{Kind: KindDecimal, Value: v}, nil
}

func NewBooleanValue(v bool) MeasurementValue {
	return MeasurementValue{Kind: KindBoolean, Value: v}
}

func NewTextValue(v string) MeasurementValue {
	return MeasurementValue{Kind: KindText, Value: v}
}
