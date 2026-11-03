package logger

import "context"

type nopLogger struct{}

// Nop returns a Logger that discards everything. Useful in tests.
func Nop() Logger { return nopLogger{} }

func (nopLogger) Debug(context.Context, string, ...any) {}
func (nopLogger) Info(context.Context, string, ...any)  {}
func (nopLogger) Warn(context.Context, string, ...any)  {}
func (nopLogger) Error(context.Context, string, ...any) {}
func (n nopLogger) With(...any) Logger                  { return n }
