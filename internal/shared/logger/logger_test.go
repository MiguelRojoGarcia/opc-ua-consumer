package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func newJSON(buf *bytes.Buffer, level Level) Logger {
	return NewSlog(SlogConfig{Level: level, Format: FormatJSON, Output: buf})
}

func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var entries []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var e map[string]any
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("invalid json line %q: %v", line, err)
		}
		entries = append(entries, e)
	}
	return entries
}

func TestKeyValuePairs(t *testing.T) {
	var buf bytes.Buffer
	newJSON(&buf, LevelInfo).Info(context.Background(), "conectado",
		"endpoint", "opc.tcp://localhost:49320",
		"tags", 18,
		"retry_in", 2*time.Second,
		"error", errors.New("boom"),
	)

	e := decodeLines(t, &buf)[0]
	if e["level"] != "INFO" || e["msg"] != "conectado" {
		t.Errorf("unexpected level/msg: %v", e)
	}
	if e["endpoint"] != "opc.tcp://localhost:49320" || e["tags"] != float64(18) {
		t.Errorf("unexpected fields: %v", e)
	}
	if e["retry_in"] != float64(2*time.Second) {
		t.Errorf("unexpected duration: %v", e["retry_in"])
	}
	if e["error"] != "boom" {
		t.Errorf("error not rendered as its message: %v", e["error"])
	}
}

func TestFieldsMap(t *testing.T) {
	var buf bytes.Buffer
	newJSON(&buf, LevelInfo).Info(context.Background(), "mix",
		Fields{"b": 2, "a": "uno"},
		"c", true,
	)

	line := strings.TrimSpace(buf.String())
	// El mapa se emite en orden alfabético y los pares a continuación.
	if !strings.Contains(line, `"a":"uno","b":2,"c":true`) {
		t.Errorf("unexpected order or values: %s", line)
	}
}

type point struct {
	X, Y int
}

func TestStructsUsePlusV(t *testing.T) {
	var buf bytes.Buffer
	ts := time.Date(2026, 10, 30, 17, 0, 0, 0, time.UTC)
	var nilPoint *point
	newJSON(&buf, LevelInfo).Info(context.Background(), "structs",
		"value", point{X: 1, Y: 2},
		"ptr", &point{X: 3, Y: 4},
		"nil_ptr", nilPoint,
		"ts", ts,
	)

	e := decodeLines(t, &buf)[0]
	if e["value"] != "{X:1 Y:2}" {
		t.Errorf("struct not rendered with %%+v: %v", e["value"])
	}
	if e["ptr"] != "&{X:3 Y:4}" {
		t.Errorf("pointer to struct not rendered with %%+v: %v", e["ptr"])
	}
	if e["nil_ptr"] != nil {
		t.Errorf("nil pointer must be null: %v", e["nil_ptr"])
	}
	if e["ts"] != "2026-10-30T17:00:00Z" {
		t.Errorf("time.Time must keep its own format: %v", e["ts"])
	}
}

func TestBadArgs(t *testing.T) {
	var buf bytes.Buffer
	newJSON(&buf, LevelInfo).Info(context.Background(), "bad", "ok", 1, 42, "sin_valor")

	e := decodeLines(t, &buf)[0]
	if e["ok"] != float64(1) {
		t.Errorf("valid pair lost: %v", e)
	}
	// Las dos entradas inválidas comparten la clave !BADKEY; prevalece la última.
	if e[badKey] != "sin_valor" {
		t.Errorf("bad args not reported under %s: %v", badKey, e)
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	log := newJSON(&buf, LevelWarn)
	ctx := context.Background()

	log.Debug(ctx, "debug")
	log.Info(ctx, "info")
	log.Warn(ctx, "warn")
	log.Error(ctx, "error")

	entries := decodeLines(t, &buf)
	if len(entries) != 2 || entries[0]["msg"] != "warn" || entries[1]["msg"] != "error" {
		t.Errorf("expected only warn and error, got %v", entries)
	}
}

func TestWith(t *testing.T) {
	var buf bytes.Buffer
	base := newJSON(&buf, LevelInfo)
	log := base.With("center_id", "MAD-01", Fields{"service": "consumer"})
	ctx := context.Background()

	log.Info(ctx, "con centro")
	base.Info(ctx, "sin centro")

	entries := decodeLines(t, &buf)
	if entries[0]["center_id"] != "MAD-01" || entries[0]["service"] != "consumer" {
		t.Errorf("With fields missing: %v", entries[0])
	}
	if _, ok := entries[1]["center_id"]; ok {
		t.Errorf("With must not modify the parent logger: %v", entries[1])
	}
}

func TestTextDefault(t *testing.T) {
	var buf bytes.Buffer
	NewSlog(SlogConfig{Output: &buf}).Info(context.Background(), "hola", "k", "v", "p", point{X: 1, Y: 2})
	out := buf.String()
	for _, want := range []string{"level=INFO", "msg=hola", "k=v", `p="{X:1 Y:2}"`} {
		if !strings.Contains(out, want) {
			t.Errorf("text output %q does not contain %q", out, want)
		}
	}
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]Level{"debug": LevelDebug, "INFO": LevelInfo, " warn ": LevelWarn, "warning": LevelWarn, "error": LevelError} {
		got, err := ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Error("ParseLevel(verbose) must fail")
	}
}

func TestNop(t *testing.T) {
	var log Logger = Nop()
	log.With("k", "v").Info(context.Background(), "nada", "a", 1)
}
