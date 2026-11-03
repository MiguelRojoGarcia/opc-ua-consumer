package measurement

// Quality is the reliability of a reading: GOOD, BAD or UNCERTAIN.
type Quality string

const (
	QualityGood      Quality = "GOOD"
	QualityBad       Quality = "BAD"
	QualityUncertain Quality = "UNCERTAIN"
)

// ParseQuality validates a quality coming from outside the domain.
func ParseQuality(s string) (Quality, error) {
	switch q := Quality(s); q {
	case QualityGood, QualityBad, QualityUncertain:
		return q, nil
	}
	return "", ErrInvalidQuality
}
