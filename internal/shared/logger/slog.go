package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// Format is the output format of the slog implementation.
type Format string

const (
	FormatText Format = "text"
	FormatJSON Format = "json"
)

// SlogConfig configures NewSlog.
type SlogConfig struct {
	Level  Level
	Format Format    // FormatText by default
	Output io.Writer // os.Stdout by default
}

type slogLogger struct {
	l *slog.Logger
}

// NewSlog returns a Logger backed by the standard library log/slog.
func NewSlog(cfg SlogConfig) Logger {
	out := cfg.Output
	if out == nil {
		out = os.Stdout
	}
	opts := &slog.HandlerOptions{Level: toSlogLevel(cfg.Level)}

	var h slog.Handler
	if cfg.Format == FormatJSON {
		h = slog.NewJSONHandler(out, opts)
	} else {
		h = slog.NewTextHandler(out, opts)
	}
	return NewSlogWithHandler(h)
}

// NewSlogWithHandler returns a Logger over any slog.Handler. It allows plugging
// handlers from other libraries, e.g. the OpenTelemetry bridge
// (go.opentelemetry.io/contrib/bridges/otelslog), without a new implementation.
func NewSlogWithHandler(h slog.Handler) Logger {
	return &slogLogger{l: slog.New(h)}
}

func (s *slogLogger) Debug(ctx context.Context, msg string, args ...any) {
	s.log(ctx, slog.LevelDebug, msg, args)
}

func (s *slogLogger) Info(ctx context.Context, msg string, args ...any) {
	s.log(ctx, slog.LevelInfo, msg, args)
}

func (s *slogLogger) Warn(ctx context.Context, msg string, args ...any) {
	s.log(ctx, slog.LevelWarn, msg, args)
}

func (s *slogLogger) Error(ctx context.Context, msg string, args ...any) {
	s.log(ctx, slog.LevelError, msg, args)
}

func (s *slogLogger) With(args ...any) Logger {
	fields := parseArgs(args)
	attrs := make([]any, len(fields))
	for i, f := range fields {
		attrs[i] = slog.Any(f.key, f.value)
	}
	return &slogLogger{l: s.l.With(attrs...)}
}

func (s *slogLogger) log(ctx context.Context, level slog.Level, msg string, args []any) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.l.Enabled(ctx, level) {
		return
	}
	fields := parseArgs(args)
	attrs := make([]slog.Attr, len(fields))
	for i, f := range fields {
		attrs[i] = slog.Any(f.key, f.value)
	}
	s.l.LogAttrs(ctx, level, msg, attrs...)
}

func toSlogLevel(l Level) slog.Level {
	switch l {
	case LevelDebug:
		return slog.LevelDebug
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	}
	return slog.LevelInfo
}
