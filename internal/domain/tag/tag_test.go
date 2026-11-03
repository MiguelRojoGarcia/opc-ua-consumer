package tag

import (
	"errors"
	"testing"
)

func TestNew(t *testing.T) {
	got, err := New("  Line1.PLC1.Temperature ")
	if err != nil {
		t.Fatal(err)
	}
	if got != "Line1.PLC1.Temperature" {
		t.Errorf("got %q", got)
	}
	if _, err := New("  "); !errors.Is(err, ErrEmptyTag) {
		t.Errorf("empty: got %v", err)
	}
}
