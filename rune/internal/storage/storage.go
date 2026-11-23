package storage

import "errors"

// ReservedKeyPrefix marks storage-internal keys. GC drops keys under it, so the
// server refuses client keys that start with it.
const ReservedKeyPrefix = "\x00rune/"

var (
	ErrNotFound            = errors.New("key not found")
	ErrInsufficientStorage = errors.New("insufficient disk space")
)

// ErrOverBudget is returned by GetReserved when reserve refuses the value's size.
var ErrOverBudget = errors.New("value does not fit the memory budget")

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
	// GetReserved is Get, but calls reserve(size) before copying the value and
	// returns ErrOverBudget, without copying, if it reports false.
	GetReserved(key string, reserve func(n int64) bool) ([]byte, error)
	Set(key string, value []byte, ttlSeconds int64) error
	Delete(keys ...string) error
	Exists(keys ...string) (int64, error)
	Info() (Info, error)
	Close() error
}
