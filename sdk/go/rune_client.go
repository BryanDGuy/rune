package runesdk

import (
	"context"
	"errors"
	"io"
	"time"
)

var ErrNotFound = errors.New("key not found")

type SetOptions struct {
	// TTL is sent in whole seconds (truncated). 0 means no expiry, 0 < TTL < 1s becomes 1s, and a negative TTL is an error.
	TTL      time.Duration
	Compress bool
}

// RuneClient is the common interface satisfied by both Client (single-node) and
// ClusterClient (cluster-routed), letting callers swap between them.
type RuneClient interface {
	Set(ctx context.Context, key string, r io.Reader, opts *SetOptions) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, keys ...string) error
	Close() error
}
