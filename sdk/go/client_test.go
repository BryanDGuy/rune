package runesdk_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	runesdk "github.com/runicsigil/rune/sdk/go"

	runev1 "github.com/runicsigil/rune/gen/rune/v1"
	"github.com/runicsigil/rune/internal/config"
	"github.com/runicsigil/rune/internal/server"
	"github.com/runicsigil/rune/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20 // 1MB bufconn buffer

func newTestSDKClient(t *testing.T) (*runesdk.Client, func()) {
	t.Helper()
	cfg := &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    1 << 30,
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		StreamChunkSize:    1 << 20,
		GCInterval:         time.Hour,
		GCDiscardRatio:     0.5,
		TTLSweepInterval:   time.Hour,
	}
	store, err := storage.NewBadgerStore(cfg)
	require.NoError(t, err)

	lis := bufconn.Listen(bufSize)
	srv := server.New(cfg, store)
	srv.StartOnListener(lis)

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := runesdk.NewFromConn(conn)
	cleanup := func() {
		client.Close()
		srv.Stop()
		store.Close()
	}
	return client, cleanup
}

// newTestRawClient returns a low-level gRPC client sharing the same server as
// a given SDK client — used in TestSDKWithTTL to inspect TTL directly.
func newTestRawAndSDKClient(t *testing.T) (*runesdk.Client, runev1.RuneServiceClient, func()) {
	t.Helper()
	cfg := &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    1 << 30,
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		StreamChunkSize:    1 << 20,
		GCInterval:         time.Hour,
		GCDiscardRatio:     0.5,
		TTLSweepInterval:   time.Hour,
	}
	store, err := storage.NewBadgerStore(cfg)
	require.NoError(t, err)

	lis := bufconn.Listen(bufSize)
	srv := server.New(cfg, store)
	srv.StartOnListener(lis)

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())

	conn, err := grpc.NewClient("passthrough://bufnet", dialer, creds)
	require.NoError(t, err)

	sdkClient := runesdk.NewFromConn(conn)
	rawClient := runev1.NewRuneServiceClient(conn)

	cleanup := func() {
		sdkClient.Close()
		srv.Stop()
		store.Close()
	}
	return sdkClient, rawClient, cleanup
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
