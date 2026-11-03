package opcua

import (
	"fmt"
	"strings"

	"github.com/gopcua/opcua/ua"

	"opc-ua-consumer/internal/domain/measurement"
)

// tagRef is a KEPServerEX tag split into the domain device and metric name.
type tagRef struct {
	tag      string
	deviceId measurement.DeviceId
	name     measurement.MetricName
}

// parseTag splits a KEPServerEX tag path "Canal.Dispositivo[.Grupo...].Tag":
// the device is "Canal.Dispositivo" and the metric name is the rest, so that
// tags inside groups keep the group ("Energy.Power_kW").
func parseTag(tag string) (tagRef, error) {
	parts := strings.SplitN(strings.TrimSpace(tag), ".", 3)
	if len(parts) < 3 {
		return tagRef{}, fmt.Errorf("opcua: tag %q must be Canal.Dispositivo.Tag", tag)
	}
	deviceId, err := measurement.NewDeviceId(parts[0] + "." + parts[1])
	if err != nil {
		return tagRef{}, fmt.Errorf("opcua: tag %q: %w", tag, err)
	}
	name, err := measurement.NewMetricName(parts[2])
	if err != nil {
		return tagRef{}, fmt.Errorf("opcua: tag %q: %w", tag, err)
	}
	return tagRef{tag: tag, deviceId: deviceId, name: name}, nil
}

// toMeasurement converts an OPC-UA DataValue into a domain measurement.
func toMeasurement(site measurement.SiteCode, ref tagRef, dv *ua.DataValue) (measurement.Measurement, error) {
	if dv == nil || dv.Value == nil {
		return measurement.Measurement{}, measurement.ErrMissingValue
	}
	value, err := toValue(dv.Value.Value())
	if err != nil {
		return measurement.Measurement{}, err
	}
	// OPC-UA no envía la unidad con el valor (está en la propiedad
	// EngineeringUnits del nodo), así que de momento va vacía.
	return measurement.New(site, ref.deviceId, ref.name, value, measurement.NewUnit(""), toQuality(dv.Status))
}

// toValue maps the Go type of an OPC-UA variant to a typed MeasurementValue.
func toValue(v any) (measurement.MeasurementValue, error) {
	switch x := v.(type) {
	case bool:
		return measurement.NewBooleanValue(x), nil
	case string:
		return measurement.NewTextValue(x), nil
	case float64:
		return measurement.NewDecimalValue(x)
	case float32:
		return measurement.NewDecimalValue(float64(x))
	case int8:
		return measurement.NewDecimalValue(float64(x))
	case int16:
		return measurement.NewDecimalValue(float64(x))
	case int32:
		return measurement.NewDecimalValue(float64(x))
	case int64:
		return measurement.NewDecimalValue(float64(x))
	case uint8:
		return measurement.NewDecimalValue(float64(x))
	case uint16:
		return measurement.NewDecimalValue(float64(x))
	case uint32:
		return measurement.NewDecimalValue(float64(x))
	case uint64:
		return measurement.NewDecimalValue(float64(x))
	case nil:
		return measurement.MeasurementValue{}, measurement.ErrMissingValue
	}
	return measurement.MeasurementValue{}, fmt.Errorf("opcua: unsupported value type %T", v)
}

// toQuality maps the severity of an OPC-UA StatusCode (its two highest bits:
// 00 Good, 01 Uncertain, 10/11 Bad) to the domain quality.
func toQuality(s ua.StatusCode) measurement.Quality {
	switch uint32(s) >> 30 {
	case 0:
		return measurement.QualityGood
	case 1:
		return measurement.QualityUncertain
	}
	return measurement.QualityBad
}

// statusName returns the symbolic name of a StatusCode (or its hex value).
func statusName(s ua.StatusCode) string {
	if d, ok := ua.StatusCodes[s]; ok {
		return d.Name
	}
	return fmt.Sprintf("0x%08X", uint32(s))
}
