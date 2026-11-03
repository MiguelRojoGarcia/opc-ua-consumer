// Package tagrepo contains the implementations of tag.Repository.
package tagrepo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"

	"opc-ua-consumer/internal/domain/tag"
)

var _ tag.Repository = (*FileRepository)(nil)

// tagsFile is the format of the tags file:
//
//	tags:
//	  - Line1.PLC1.Temperature
//	  - Line1.PLC1.Energy.Power_kW
type tagsFile struct {
	Tags []string `yaml:"tags"`
}

// FileRepository reads the tags from a YAML file. The file is read on every
// FindAll call; the consumer only calls it at startup.
type FileRepository struct {
	path string
}

func NewFileRepository(path string) (*FileRepository, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("tagrepo: file path is required")
	}
	return &FileRepository{path: path}, nil
}

// FindAll returns the tags of the file in order. Blank entries are skipped.
func (r *FileRepository) FindAll(_ context.Context) ([]tag.Tag, error) {
	data, err := os.ReadFile(r.path)
	if err != nil {
		return nil, fmt.Errorf("tagrepo: reading tags file: %w", err)
	}
	var f tagsFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("tagrepo: parsing %s: %w", r.path, err)
	}

	tags := make([]tag.Tag, 0, len(f.Tags))
	for _, s := range f.Tags {
		t, err := tag.New(s)
		if errors.Is(err, tag.ErrEmptyTag) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("tagrepo: %s: %w", r.path, err)
		}
		tags = append(tags, t)
	}
	return tags, nil
}
