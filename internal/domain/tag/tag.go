package tag

import (
	"errors"
	"strings"
)

var ErrEmptyTag = errors.New("tag: tag is empty")

// Tag is the address of a variable in the industrial server, e.g.
// "Line1.PLC1.Temperature". Its format depends on the server (KEPServerEX uses
// "Channel.Device[.Group].Tag"), so the domain only requires it to be non-empty.
type Tag string

func New(s string) (Tag, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ErrEmptyTag
	}
	return Tag(s), nil
}
