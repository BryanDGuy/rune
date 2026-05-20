// Copyright 2026 BryanDGuy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
