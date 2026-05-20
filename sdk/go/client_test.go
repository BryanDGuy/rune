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

package runesdk_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	runesdk "github.com/runicsigil/rune/sdk/go"

	runev1 "github.com/runicsigil/rune/gen/rune/v1"
	"github.com/runicsigil/rune/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSDKClient(t *testing.T) (*runesdk.Client, func()) {
	t.Helper()
	conn, cleanup := testutil.NewBufconnConn(t, 1<<20)
	return runesdk.NewFromConn(conn), cleanup
}

// newTestRawAndSDKClient returns both an SDK client and a raw gRPC client
// sharing the same server — used in TestSDKWithTTL to inspect TTL directly.
func newTestRawAndSDKClient(t *testing.T) (*runesdk.Client, runev1.RuneServiceClient, func()) {
	t.Helper()
	conn, cleanup := testutil.NewBufconnConn(t, 1<<20)
	return runesdk.NewFromConn(conn), runev1.NewRuneServiceClient(conn), cleanup
}

func TestSDKSetGet(t *testing.T) {
	client, cleanup := newTestSDKClient(t)
	defer cleanup()
	ctx := context.Background()

	data := []byte("hello rune SDK")
	err := client.Set(ctx, "sdk-key", bytes.NewReader(data))
	require.NoError(t, err)

	rc, err := client.Get(ctx, "sdk-key")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestSDKGetNotFound(t *testing.T) {
	client, cleanup := newTestSDKClient(t)
	defer cleanup()

	_, err := client.Get(context.Background(), "no-such-key")
	require.ErrorIs(t, err, runesdk.ErrNotFound)
}

func TestSDKWithTTL(t *testing.T) {
	sdkClient, rawClient, cleanup := newTestRawAndSDKClient(t)
	defer cleanup()
	ctx := context.Background()

	const ttl = 120 * time.Second
	err := sdkClient.Set(ctx, "ttl-key", bytes.NewReader([]byte("value")), runesdk.WithTTL(ttl))
	require.NoError(t, err)

	// Verify TTL was stored by querying the server directly.
	resp, err := rawClient.TTL(ctx, &runev1.TTLRequest{Key: "ttl-key"})
	require.NoError(t, err)
	assert.InDelta(t, int64(ttl.Seconds()), resp.TtlSeconds, 2,
		"expected TTL near %d seconds, got %d", int64(ttl.Seconds()), resp.TtlSeconds)
}

func TestSDKLargePayload(t *testing.T) {
	client, cleanup := newTestSDKClient(t)
	defer cleanup()
	ctx := context.Background()

	const size = 2 << 20 // 2MB
	payload := bytes.Repeat([]byte("z"), size)

	err := client.Set(ctx, "large-key", bytes.NewReader(payload))
	require.NoError(t, err)

	rc, err := client.Get(ctx, "large-key")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, size, len(got), "length mismatch")
	assert.Equal(t, payload, got, "content mismatch")
}
