package measurement

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func mustDeviceId(t *testing.T, s string) DeviceId {
	t.Helper()
	d, err := NewDeviceId(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustMetricName(t *testing.T, s string) MetricName {
	t.Helper()
	n, err := NewMetricName(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func mustSiteCode(t *testing.T, s string) SiteCode {
	t.Helper()
	c, err := NewSiteCode(s)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustDecimal(t *testing.T, v float64) MeasurementValue {
	t.Helper()
	mv, err := NewDecimalValue(v)
	if err != nil {
		t.Fatal(err)
	}
	return mv
}

func TestNew(t *testing.T) {
	before := time.Now()
	m, err := New(
		mustSiteCode(t, "MAD-01"),
		mustDeviceId(t, "Line1.PLC1"),
		mustMetricName(t, "Temperature"),
		mustDecimal(t, 78.5),
		NewUnit("°C"),
		QualityGood,
	)
	after := time.Now()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if m.Site != "MAD-01" || m.DeviceId != "Line1.PLC1" || m.Name != "Temperature" {
		t.Errorf("unexpected identifiers: %v %v %v", m.Site, m.DeviceId, m.Name)
	}
	if m.Value != (MeasurementValue{Kind: KindDecimal, Value: 78.5}) {
		t.Errorf("unexpected value: %+v", m.Value)
	}
	if m.Unit != "°C" || m.Quality != QualityGood {
		t.Errorf("unexpected unit/quality: %v %v", m.Unit, m.Quality)
	}
	if m.Timestamp.Location() != time.UTC {
		t.Errorf("timestamp not in UTC: %v", m.Timestamp)
	}
	if m.Timestamp.Before(before) || m.Timestamp.After(after) {
		t.Errorf("timestamp %v not set at construction time [%v, %v]", m.Timestamp, before, after)
	}
}

func TestNewValidation(t *testing.T) {
	site := mustSiteCode(t, "MAD-01")
	device := mustDeviceId(t, "Line1.PLC1")
	name := mustMetricName(t, "Temperature")
	value := mustDecimal(t, 1)

	tests := []struct {
		name    string
		build   func() (Measurement, error)
		wantErr error
	}{
		{"missing site", func() (Measurement, error) {
			return New("", device, name, value, "", QualityGood)
		}, ErrEmptySiteCode},
		{"missing device", func() (Measurement, error) {
			return New(site, "", name, value, "", QualityGood)
		}, ErrEmptyDeviceId},
		{"missing name", func() (Measurement, error) {
			return New(site, device, "", value, "", QualityGood)
		}, ErrEmptyMetricName},
		{"missing value", func() (Measurement, error) {
			return New(site, device, name, MeasurementValue{}, "", QualityGood)
		}, ErrMissingValue},
		{"missing quality", func() (Measurement, error) {
			return New(site, device, name, value, "", "")
		}, ErrMissingQuality},
		{"invalid quality", func() (Measurement, error) {
			return New(site, device, name, value, "", Quality("OK"))
		}, ErrInvalidQuality},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.build(); !errors.Is(err, tt.wantErr) {
				t.Errorf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestValueObjectConstructors(t *testing.T) {
	if _, err := NewDeviceId("  "); !errors.Is(err, ErrEmptyDeviceId) {
		t.Errorf("NewDeviceId: got %v", err)
	}
	if _, err := NewMetricName(""); !errors.Is(err, ErrEmptyMetricName) {
		t.Errorf("NewMetricName: got %v", err)
	}
	if _, err := NewSiteCode(""); !errors.Is(err, ErrEmptySiteCode) {
		t.Errorf("NewSiteCode: got %v", err)
	}
	if d, _ := NewDeviceId("  PLC1 "); d != "PLC1" {
		t.Errorf("NewDeviceId does not trim: %q", d)
	}
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := NewDecimalValue(v); !errors.Is(err, ErrInvalidDecimalValue) {
			t.Errorf("NewDecimalValue(%v): got %v", v, err)
		}
	}
	for _, s := range []string{"GOOD", "BAD", "UNCERTAIN"} {
		if _, err := ParseQuality(s); err != nil {
			t.Errorf("ParseQuality(%q): %v", s, err)
		}
	}
	if _, err := ParseQuality("good"); !errors.Is(err, ErrInvalidQuality) {
		t.Errorf("ParseQuality(good): got %v", err)
	}
}

func TestMeasurementValueKinds(t *testing.T) {
	if v := NewBooleanValue(true); v.Kind != KindBoolean || v.Value != true {
		t.Errorf("boolean value: %+v", v)
	}
	if v := NewTextValue("IDLE"); v.Kind != KindText || v.Value != "IDLE" {
		t.Errorf("text value: %+v", v)
	}
	if v := mustDecimal(t, 1.25); v.Kind != KindDecimal || v.Value != 1.25 {
		t.Errorf("decimal value: %+v", v)
	}
}

func TestJSON(t *testing.T) {
	m, err := New(
		mustSiteCode(t, "MAD-01"),
		mustDeviceId(t, "Line1.PLC1"),
		mustMetricName(t, "Temperature"),
		mustDecimal(t, 78.5),
		NewUnit("°C"),
		QualityGood,
	)
	if err != nil {
		t.Fatal(err)
	}
	m.Timestamp = time.Date(2026, 10, 30, 17, 0, 0, 0, time.UTC)

	got, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"deviceId":"Line1.PLC1","name":"Temperature","value":{"kind":"DECIMAL","value":78.5},` +
		`"unit":"°C","quality":"GOOD","timestamp":"2026-10-30T17:00:00Z","site":"MAD-01"}`
	if string(got) != want {
		t.Errorf("json:\n got  %s\n want %s", got, want)
	}
}
