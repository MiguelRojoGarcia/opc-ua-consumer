package application

import (
	"context"
	"errors"
	"fmt"

	"opc-ua-consumer/internal/domain/tag"
	"opc-ua-consumer/internal/shared/logger"
)

var ErrNoTags = errors.New("application: no tags to consume")

// TagsResolver resolves the list of tags to consume. It only depends on the
// tag.Repository port, so the tags can come from a file, a database or the
// OPC-UA server without changing the service.
type TagsResolver struct {
	repo tag.Repository
	log  logger.Logger
}

func NewTagsResolver(repo tag.Repository, log logger.Logger) (*TagsResolver, error) {
	if repo == nil {
		return nil, errors.New("application: tag repository is required")
	}
	return &TagsResolver{repo: repo, log: log}, nil
}

// Resolve returns the tags without duplicates, keeping the repository order.
// It fails if there is no tag to consume.
func (r *TagsResolver) Resolve(ctx context.Context) ([]tag.Tag, error) {
	found, err := r.repo.FindAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("finding tags: %w", err)
	}

	seen := make(map[tag.Tag]bool, len(found))
	tags := make([]tag.Tag, 0, len(found))
	for _, t := range found {
		if seen[t] {
			continue
		}
		seen[t] = true
		tags = append(tags, t)
	}
	if len(tags) == 0 {
		return nil, ErrNoTags
	}
	r.log.Info(ctx, "tags resolved", "count", len(tags), "duplicates", len(found)-len(tags))
	return tags, nil
}
