package server_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	runev1 "github.com/runicsigil/rune/gen/rune/v1"
	"github.com/runicsigil/rune/internal/config"
	"github.com/runicsigil/rune/internal/server"
	"github.com/runicsigil/rune/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20 // 1MB bufconn buffer

func newTestClient(t *testing.T) (runev1.RuneServiceClient, func()) {
	t.Helper()
	cfg := &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    1 << 30,
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		StreamChunkSize:    1 << 20, // 1MB chunks
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

	client := runev1.NewRuneServiceClient(conn)
	cleanup := func() {
		conn.Close()
		srv.Stop()
		store.Close()
	}
	return client, cleanup
}

func TestPing(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	resp, err := client.Ping(context.Background(), &runev1.PingRequest{})
	require.NoError(t, err)
	assert.Equal(t, "pong", resp.Message)
}

func TestSetGet(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	data := []byte("hello rune server")
	setStream, err := client.Set(ctx)
	require.NoError(t, err)
	require.NoError(t, setStream.Send(&runev1.SetRequest{
		Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "k1"}},
	}))
	require.NoError(t, setStream.Send(&runev1.SetRequest{
		Payload: &runev1.SetRequest_Chunk{Chunk: data},
	}))
	_, err = setStream.CloseAndRecv()
	require.NoError(t, err)

	getStream, err := client.Get(ctx, &runev1.GetRequest{Key: "k1"})
	require.NoError(t, err)
	var got []byte
	for {
		resp, err := getStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, resp.Chunk...)
	}
	assert.Equal(t, data, got)
}

func TestGetNotFound(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	stream, err := client.Get(context.Background(), &runev1.GetRequest{Key: "missing"})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestDelete(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	for _, key := range []string{"a", "b"} {
		s, err := client.Set(ctx)
		require.NoError(t, err)
		require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: key}}}))
		require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("v")}}))
		_, err = s.CloseAndRecv()
		require.NoError(t, err)
	}

	resp, err := client.Delete(ctx, &runev1.DeleteRequest{Keys: []string{"a", "b", "missing"}})
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.Deleted)
}

func TestExists(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	s, err := client.Set(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "exists-key"}}}))
	require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("v")}}))
	_, err = s.CloseAndRecv()
	require.NoError(t, err)

	resp, err := client.Exists(ctx, &runev1.ExistsRequest{Keys: []string{"exists-key", "missing"}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.Count)
}

func TestExpireAndTTL(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	s, err := client.Set(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "ttl-key"}}}))
	require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("v")}}))
	_, err = s.CloseAndRecv()
	require.NoError(t, err)

	expResp, err := client.Expire(ctx, &runev1.ExpireRequest{Key: "ttl-key", TtlSeconds: 120})
	require.NoError(t, err)
	assert.True(t, expResp.Ok)

	ttlResp, err := client.TTL(ctx, &runev1.TTLRequest{Key: "ttl-key"})
	require.NoError(t, err)
	assert.InDelta(t, int64(120), ttlResp.TtlSeconds, 2)
}

func TestTTLNotFound(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	resp, err := client.TTL(context.Background(), &runev1.TTLRequest{Key: "missing"})
	require.NoError(t, err)
	assert.Equal(t, int64(-2), resp.TtlSeconds)
}

func TestPersist(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	s, err := client.Set(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "persist-key", TtlSeconds: 60}}}))
	require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("v")}}))
	_, err = s.CloseAndRecv()
	require.NoError(t, err)

	persistResp, err := client.Persist(ctx, &runev1.PersistRequest{Key: "persist-key"})
	require.NoError(t, err)
	assert.True(t, persistResp.Ok)

	ttlResp, err := client.TTL(ctx, &runev1.TTLRequest{Key: "persist-key"})
	require.NoError(t, err)
	assert.Equal(t, int64(-1), ttlResp.TtlSeconds)
}

func TestInfo(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	s, err := client.Set(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "info-key"}}}))
	require.NoError(t, s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("value")}}))
	_, err = s.CloseAndRecv()
	require.NoError(t, err)

	stream, err := client.Get(ctx, &runev1.GetRequest{Key: "info-key"})
	require.NoError(t, err)
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// trigger a miss
	missStream, err := client.Get(ctx, &runev1.GetRequest{Key: "missing"})
	require.NoError(t, err)
	_, _ = missStream.Recv()

	info, err := client.Info(ctx, &runev1.InfoRequest{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, info.CacheHits, int64(1))
	assert.GreaterOrEqual(t, info.CacheMisses, int64(1))
	assert.Greater(t, info.StorageMaxBytes, int64(0))
}

func TestLargePayload(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	const size = 5 << 20 // 5MB
	payload := bytes.Repeat([]byte("x"), size)

	setStream, err := client.Set(ctx)
	require.NoError(t, err)
	require.NoError(t, setStream.Send(&runev1.SetRequest{
		Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "large"}},
	}))

	chunkSize := 1 << 20
	for i := 0; i < len(payload); i += chunkSize {
		end := i + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		require.NoError(t, setStream.Send(&runev1.SetRequest{
			Payload: &runev1.SetRequest_Chunk{Chunk: payload[i:end]},
		}))
	}
	_, err = setStream.CloseAndRecv()
	require.NoError(t, err)

	getStream, err := client.Get(ctx, &runev1.GetRequest{Key: "large"})
	require.NoError(t, err)
	var got []byte
	for {
		resp, err := getStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, resp.Chunk...)
	}
	require.Equal(t, size, len(got))
	assert.Equal(t, payload, got)
}
