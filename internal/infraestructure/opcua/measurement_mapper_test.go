package opcua

import (
	"errors"
	"math"
	"testing"

	"github.com/gopcua/opcua/ua"

	"opc-ua-consumer/internal/domain/measurement"
)

func TestParseTag(t *testing.T) {
	tests := []struct {
		tag, device, name string
		wantErr           bool
	}{
		{"Line1.PLC1.Temperature", "Line1.PLC1", "Temperature", false},
		{"Line1.PLC1.Energy.Power_kW", "Line1.PLC1", "Energy.Power_kW", false},
		{"Simulation Examples.Functions.Ramp1", "Simulation Examples.Functions", "Ramp1", false},
		{"Line1.PLC1", "", "", true},
		{"Temperature", "", "", true},
		{"Line1.PLC1. ", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			ref, err := parseTag(tt.tag)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if string(ref.deviceId) != tt.device || string(ref.name) != tt.name || ref.tag != tt.tag {
				t.Errorf("got %+v", ref)
			}
		})
	}
}

func TestToValue(t *testing.T) {
	tests := []struct {
		in   any
		kind measurement.ValueKind
		want any
	}{
		{true, measurement.KindBoolean, true},
		{"RUNNING", measurement.KindText, "RUNNING"},
		{78.5, measurement.KindDecimal, 78.5},
		{float32(1.5), measurement.KindDecimal, 1.5},
		{int32(-3), measurement.KindDecimal, -3.0},
		{uint64(42), measurement.KindDecimal, 42.0},
	}
	for _, tt := range tests {
		v, err := toValue(tt.in)
		if err != nil {
			t.Fatalf("%T: %v", tt.in, err)
		}
		if v.Kind != tt.kind || v.Value != tt.want {
			t.Errorf("%T(%v): got %+v", tt.in, tt.in, v)
		}
	}

	if _, err := toValue(nil); !errors.Is(err, measurement.ErrMissingValue) {
		t.Errorf("nil: got %v", err)
	}
	if _, err := toValue(math.NaN()); !errors.Is(err, measurement.ErrInvalidDecimalValue) {
		t.Errorf("NaN: got %v", err)
	}
	if _, err := toValue([]byte("x")); err == nil {
		t.Errorf("[]byte: expected unsupported type error")
	}
}

func TestToQuality(t *testing.T) {
	tests := []struct {
		status ua.StatusCode
		want   measurement.Quality
	}{
		{ua.StatusOK, measurement.QualityGood},
		{ua.StatusUncertain, measurement.QualityUncertain},
		{ua.StatusBad, measurement.QualityBad},
		{ua.StatusBadNodeIDUnknown, measurement.QualityBad},
	}
	for _, tt := range tests {
		if got := toQuality(tt.status); got != tt.want {
			t.Errorf("%s: got %s, want %s", statusName(tt.status), got, tt.want)
		}
	}
}

func TestToMeasurement(t *testing.T) {
	ref, err := parseTag("Line1.PLC1.Temperature")
	if err != nil {
		t.Fatal(err)
	}
	m, err := toMeasurement("MAD-01", ref, &ua.DataValue{Value: ua.MustVariant(78.5), Status: ua.StatusOK})
	if err != nil {
		t.Fatal(err)
	}
	if m.Site != "MAD-01" || m.DeviceId != "Line1.PLC1" || m.Name != "Temperature" ||
		m.Value.Value != 78.5 || m.Quality != measurement.QualityGood {
		t.Errorf("got %+v", m)
	}

	if _, err := toMeasurement("MAD-01", ref, &ua.DataValue{Status: ua.StatusBad}); !errors.Is(err, measurement.ErrMissingValue) {
		t.Errorf("value missing: got %v", err)
	}
	if _, err := toMeasurement("MAD-01", ref, nil); !errors.Is(err, measurement.ErrMissingValue) {
		t.Errorf("nil data value: got %v", err)
	}
}
