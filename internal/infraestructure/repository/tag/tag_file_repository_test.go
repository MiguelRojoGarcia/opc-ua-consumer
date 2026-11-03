package tagrepo

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"opc-ua-consumer/internal/domain/tag"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tags.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFindAll(t *testing.T) {
	path := writeFile(t, "tags:\n  - Line1.PLC1.Temperature\n  - '  '\n  - \" Line1.PLC1.Energy.Power_kW \"\n  - Line1.PLC1.Temperature\n")
	repo, err := NewFileRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Los duplicados se mantienen: normalizarlos es cosa del TagsResolver.
	want := []tag.Tag{"Line1.PLC1.Temperature", "Line1.PLC1.Energy.Power_kW", "Line1.PLC1.Temperature"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFindAllErrors(t *testing.T) {
	if _, err := NewFileRepository(" "); err == nil {
		t.Error("expected error without path")
	}

	missing, _ := NewFileRepository(filepath.Join(t.TempDir(), "missing.yaml"))
	if _, err := missing.FindAll(context.Background()); err == nil {
		t.Error("expected error for missing file")
	}

	invalid, _ := NewFileRepository(writeFile(t, "tags: [unclosed\n"))
	if _, err := invalid.FindAll(context.Background()); err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestFindAllEmptyFile(t *testing.T) {
	repo, _ := NewFileRepository(writeFile(t, "tags: []\n"))
	got, err := repo.FindAll(context.Background())
	if err != nil || len(got) != 0 {
		t.Errorf("got %v, %v", got, err)
	}
}
