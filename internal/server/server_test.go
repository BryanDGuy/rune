package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	runev1 "github.com/bryandguy/rune/gen/rune/v1"
	"github.com/bryandguy/rune/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const bufSize = 1 << 20 // 1MB bufconn buffer

func newTestConn(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()
	return testutil.NewBufconnConn(t, bufSize)
}

func newTestClient(t *testing.T) (runev1.RuneServiceClient, func()) {
	t.Helper()
	conn, cleanup := newTestConn(t)
	return runev1.NewRuneServiceClient(conn), cleanup
}

func TestPing(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	resp, err := client.Ping(context.Background(), &runev1.PingRequest{})
	require.NoError(t, err)
	assert.Equal(t, "PONG", resp.Message)
}

func TestHealthLiveness(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()

	hc := healthpb.NewHealthClient(conn)
	resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: ""})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
}

func TestHealthReadiness(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()

	hc := healthpb.NewHealthClient(conn)
	resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "rune"})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
}

func TestSetRequiresHeaderFirst(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	stream, err := client.Set(context.Background())
	require.NoError(t, err)
	// Send a chunk without a header first — should get InvalidArgument.
	require.NoError(t, stream.Send(&runev1.SetRequest{
		Payload: &runev1.SetRequest_Chunk{Chunk: []byte("data")},
	}))
	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
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
		if errors.Is(err, io.EOF) {
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
		_, err = stream.Recv()
		if errors.Is(err, io.EOF) {
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
	assert.Positive(t, info.StorageMaxBytes)
	assert.GreaterOrEqual(t, info.ActiveConnections, int64(0))
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
		end := min(i+chunkSize, len(payload))
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
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		got = append(got, resp.Chunk...)
	}
	require.Len(t, got, size)
	assert.Equal(t, payload, got)
}
