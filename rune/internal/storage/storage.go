package storage

import "errors"

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
	Get(key string) ([]byte, error)
	Set(key string, value []byte, ttlSeconds int64) error
	Delete(keys ...string) error
	Exists(keys ...string) (int64, error)
	Info() (Info, error)
	Close() error
}
