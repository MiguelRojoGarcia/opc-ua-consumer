package tag

import "context"

// Repository gives access to the tags to consume. Implementations may read them
// from a file, a database or the OPC-UA server itself (Browse).
type Repository interface {
	// FindAll returns every configured tag. It may contain duplicates; the
	// caller is responsible for normalizing them.
	FindAll(ctx context.Context) ([]Tag, error)
}
