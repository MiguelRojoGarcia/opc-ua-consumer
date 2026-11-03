package application

import (
	"context"
	"errors"
	"slices"
	"testing"

	"opc-ua-consumer/internal/domain/tag"
	"opc-ua-consumer/internal/shared/logger"
)

type fakeTagRepository struct {
	tags []tag.Tag
	err  error
}

func (f fakeTagRepository) FindAll(context.Context) ([]tag.Tag, error) { return f.tags, f.err }

func TestNewTagsResolverRequiresRepository(t *testing.T) {
	if _, err := NewTagsResolver(nil, logger.Nop()); err == nil {
		t.Error("expected error without repository")
	}
}

func TestResolveRemovesDuplicatesKeepingOrder(t *testing.T) {
	r, _ := NewTagsResolver(fakeTagRepository{tags: []tag.Tag{"B", "A", "B", "C", "A"}}, logger.Nop())
	got, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []tag.Tag{"B", "A", "C"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestResolveFailsWithoutTags(t *testing.T) {
	r, _ := NewTagsResolver(fakeTagRepository{}, logger.Nop())
	if _, err := r.Resolve(context.Background()); !errors.Is(err, ErrNoTags) {
		t.Errorf("got %v, want %v", err, ErrNoTags)
	}
}

func TestResolveReturnsRepositoryError(t *testing.T) {
	boom := errors.New("db down")
	r, _ := NewTagsResolver(fakeTagRepository{err: boom}, logger.Nop())
	if _, err := r.Resolve(context.Background()); !errors.Is(err, boom) {
		t.Errorf("got %v, want %v", err, boom)
	}
}
