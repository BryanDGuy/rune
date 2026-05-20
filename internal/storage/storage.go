package storage

import (
	"errors"
	"io"
)

// ErrNotFound is returned by Get and TTL when the key does not exist.
var ErrNotFound = errors.New("key not found")

type Info struct {
	UsedBytes         int64
	MaxBytes          int64
	Hits              int64
	Misses            int64
	EvictionsTotal    int64
	ActiveConnections int64
}

type Storage interface {
	Get(key string) (io.ReadCloser, error)
	Set(key string, value []byte, ttlSeconds int64) error
	Delete(keys ...string) (int64, error)
	Exists(keys ...string) (int64, error)
	Expire(key string, ttlSeconds int64) (bool, error)
	// TTL returns remaining seconds. -1 = no TTL. -2 = not found.
	TTL(key string) (int64, error)
	Persist(key string) (bool, error)
	Info() (Info, error)
	Close() error
}
