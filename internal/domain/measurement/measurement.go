package measurement

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrEmptyDeviceId       = errors.New("measurement: device id is empty")
	ErrEmptyMetricName     = errors.New("measurement: metric name is empty")
	ErrEmptySiteCode       = errors.New("measurement: site code is empty")
	ErrMissingValue        = errors.New("measurement: value is missing")
	ErrMissingQuality      = errors.New("measurement: quality is missing")
	ErrInvalidQuality      = errors.New("measurement: invalid quality")
	ErrInvalidDecimalValue = errors.New("measurement: decimal value must be a finite number")
)

// Measurement is a single reading taken from a device at a site. It is a value
// object (no ID). Fields are exported so it can be serialized as is to Kafka;
// build it with New so it is validated.
type Measurement struct {
	DeviceId  DeviceId         `json:"deviceId"`
	Name      MetricName       `json:"name"`
	Value     MeasurementValue `json:"value"`
	Unit      Unit             `json:"unit"`
	Quality   Quality          `json:"quality"`
	Timestamp time.Time        `json:"timestamp"`
	Site      SiteCode         `json:"site"`
}

// New builds a validated Measurement. The timestamp is set to the current time
// in UTC at the moment of construction.
func New(
	site SiteCode,
	deviceId DeviceId,
	name MetricName,
	value MeasurementValue,
	unit Unit,
	quality Quality,
) (Measurement, error) {
	if site == "" {
		return Measurement{}, ErrEmptySiteCode
	}
	if deviceId == "" {
		return Measurement{}, ErrEmptyDeviceId
	}
	if name == "" {
		return Measurement{}, ErrEmptyMetricName
	}
	if value.Kind == "" {
		return Measurement{}, ErrMissingValue
	}
	if quality == "" {
		return Measurement{}, ErrMissingQuality
	}
	if _, err := ParseQuality(string(quality)); err != nil {
		return Measurement{}, err
	}

	return Measurement{
		DeviceId:  deviceId,
		Name:      name,
		Value:     value,
		Unit:      unit,
		Quality:   quality,
		Timestamp: time.Now().UTC(),
		Site:      site,
	}, nil
}

// DeviceId identifies the device that produced the reading.
type DeviceId string

func NewDeviceId(s string) (DeviceId, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ErrEmptyDeviceId
	}
	return DeviceId(s), nil
}

// MetricName is the name of the measured magnitude.
type MetricName string

func NewMetricName(s string) (MetricName, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ErrEmptyMetricName
	}
	return MetricName(s), nil
}

// Unit is the engineering unit of the value. It may be empty for dimensionless
// readings (booleans, text, counters).
type Unit string

func NewUnit(s string) Unit { return Unit(strings.TrimSpace(s)) }

// SiteCode is the regional code of the site ("centro") that emits the reading.
type SiteCode string

func NewSiteCode(s string) (SiteCode, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ErrEmptySiteCode
	}
	return SiteCode(s), nil
}
