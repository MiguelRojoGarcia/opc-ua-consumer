// Package logger is the centralized logging library of the project.
//
// Code depends only on the Logger interface; the concrete implementation is
// chosen once, at start-up (currently slog, see NewSlog). Switching to another
// backend, e.g. OpenTelemetry, means adding a new implementation of Logger
// without touching the call sites.
//
// Every method receives a context.Context so that backends that correlate logs
// with traces (OpenTelemetry) can extract the active span from it.
package logger

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

// Logger is the logging contract used across the project.
//
// Fields are passed after the message as key/value pairs, a Fields map, or a
// mix of both:
//
//	log.Info(ctx, "connected", "endpoint", endpoint, "retry_in", backoff)
//	log.Info(ctx, "connected", logger.Fields{"endpoint": endpoint, "retry_in": backoff})
//
// Keys become top-level fields in JSON output, so they can be queried directly
// in Loki or Elasticsearch. Struct values are rendered with %+v.
type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)

	// With returns a Logger that adds the given fields to every entry,
	// e.g. log.With("center_id", id).
	With(args ...any) Logger
}

// Fields is a set of fields written as a map literal: logger.Fields{"key": value}.
// Keys are emitted in alphabetical order.
type Fields map[string]any

// badKey is the key used for values without a valid string key (odd number of
// arguments or a non-string key), as log/slog does.
const badKey = "!BADKEY"

// Level is the minimum severity a Logger emits.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	}
	return fmt.Sprintf("level(%d)", int(l))
}

// ParseLevel converts "debug", "info", "warn" or "error" (case-insensitive) to a Level.
func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug, nil
	case "info":
		return LevelInfo, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "error":
		return LevelError, nil
	}
	return 0, fmt.Errorf("logger: unknown level %q (use debug, info, warn or error)", s)
}

// field is a normalized key/value pair. Every implementation builds its own
// attributes from it, so argument parsing and value formatting are the same
// regardless of the backend.
type field struct {
	key   string
	value any
}

// parseArgs turns the variadic arguments of a log call into fields.
func parseArgs(args []any) []field {
	fields := make([]field, 0, len(args)/2+1)
	for i := 0; i < len(args); {
		switch a := args[i].(type) {
		case Fields:
			keys := make([]string, 0, len(a))
			for k := range a {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fields = append(fields, field{key: k, value: formatValue(a[k])})
			}
			i++
		case string:
			if i+1 == len(args) {
				fields = append(fields, field{key: badKey, value: a})
				i++
				continue
			}
			fields = append(fields, field{key: a, value: formatValue(args[i+1])})
			i += 2
		default:
			fields = append(fields, field{key: badKey, value: formatValue(a)})
			i++
		}
	}
	return fields
}

// formatValue renders structs (and pointers to structs) with %+v so they are
// readable in a single field. time.Time and errors keep their own rendering.
func formatValue(v any) any {
	switch v.(type) {
	case nil, error, time.Time, *time.Time:
		return v
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Struct:
		return fmt.Sprintf("%+v", v)
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		if rv.Elem().Kind() == reflect.Struct {
			return fmt.Sprintf("%+v", v)
		}
	}
	return v
}
