package runesdk_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	runesdk "github.com/bryandguy/rune/sdk/go"

	"github.com/bryandguy/rune/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSDKClient(t *testing.T) (*runesdk.Client, func()) {
	t.Helper()
	conn, cleanup := testutil.NewBufconnConn(t, 1<<20)
	return runesdk.NewClient(conn), cleanup
}

func TestSDKSetGet(t *testing.T) {
	client, cleanup := newTestSDKClient(t)
	defer cleanup()
	ctx := context.Background()

	data := []byte("hello rune SDK")
	err := client.Set(ctx, "sdk-key", bytes.NewReader(data), nil)
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

func TestSDKLargePayload(t *testing.T) {
	client, cleanup := newTestSDKClient(t)
	defer cleanup()
	ctx := context.Background()

	const size = 2 << 20 // 2MB
	payload := bytes.Repeat([]byte("z"), size)

	err := client.Set(ctx, "large-key", bytes.NewReader(payload), nil)
	require.NoError(t, err)

	rc, err := client.Get(ctx, "large-key")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Len(t, got, size, "length mismatch")
	assert.Equal(t, payload, got, "content mismatch")
}
