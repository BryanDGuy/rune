package runesdk_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	runesdk "github.com/bryandguy/rune/sdk/go"

	"github.com/bryandguy/rune/test/testutil"
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

func TestSDKSetWithCompression(t *testing.T) {
	client, cleanup := newTestSDKClient(t)
	defer cleanup()
	ctx := context.Background()

	data := bytes.Repeat([]byte("compressible payload "), 100)
	err := client.Set(ctx, "compressed-key", bytes.NewReader(data), &runesdk.SetOptions{Compress: true})
	require.NoError(t, err)

	rc, err := client.Get(ctx, "compressed-key")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestSDKDelete(t *testing.T) {
	client, cleanup := newTestSDKClient(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, client.Set(ctx, "del-key", bytes.NewReader([]byte("v")), nil))
	require.NoError(t, client.Delete(ctx, "del-key"))

	_, err := client.Get(ctx, "del-key")
	require.ErrorIs(t, err, runesdk.ErrNotFound)
}

func TestSDKDeleteMissingKeyIsNotAnError(t *testing.T) {
	client, cleanup := newTestSDKClient(t)
	defer cleanup()

	require.NoError(t, client.Delete(context.Background(), "no-such-key"))
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
