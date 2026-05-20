
package storage

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound is returned by Get and TTL when the key does not exist.
var ErrNotFound = errors.New("key not found")

type StorageInfo struct {
	UsedBytes         int64
	MaxBytes          int64
	Hits              int64
	Misses            int64
	EvictionsTotal    int64
	ActiveConnections int64
}

type Storage interface {
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Set(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error
	Delete(ctx context.Context, keys ...string) (int64, error)
	Exists(ctx context.Context, keys ...string) (int64, error)
	Expire(ctx context.Context, key string, ttlSeconds int64) (bool, error)
	// TTL returns remaining seconds. -1 = no TTL. -2 = not found.
	TTL(ctx context.Context, key string) (int64, error)
	Persist(ctx context.Context, key string) (bool, error)
	Info(ctx context.Context) (StorageInfo, error)
	Close() error
}
