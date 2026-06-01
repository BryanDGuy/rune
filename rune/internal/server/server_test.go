package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	runev1 "github.com/bryandguy/rune/rune/internal/gen/rune/v1"
	"github.com/bryandguy/rune/rune/internal/server"
	"github.com/bryandguy/rune/rune/internal/storage"
	"github.com/bryandguy/rune/rune/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20 // 1MB bufconn buffer

func newTestConn(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()
	return testutil.NewBufconnConn(t, bufSize, nil)
}

func newTestClient(t *testing.T) (runev1.RuneServiceClient, func()) {
	t.Helper()
	conn, cleanup := newTestConn(t)
	return runev1.NewRuneServiceClient(conn), cleanup
}

func mustSet(t *testing.T, client runev1.RuneServiceClient, key string, value []byte) {
	t.Helper()
	s, err := client.Set(context.Background())
	require.NoError(t, err)
	require.NoError(t, s.Send(&runev1.SetRequest{
		Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: key}},
	}))
	require.NoError(t, s.Send(&runev1.SetRequest{
		Payload: &runev1.SetRequest_Chunk{Chunk: value},
	}))
	_, err = s.CloseAndRecv()
	require.NoError(t, err)
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
	resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: server.ReadinessService})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
}

func TestReadinessNotServingAfterStop(t *testing.T) {
	cfg := testutil.BaseConfig(t)
	store, err := storage.NewBadgerStore(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	lis := bufconn.Listen(bufSize)
	srv := server.New(cfg, store, nil, nil, nil)
	srv.StartOnListener(lis)

	conn, err := grpc.NewClient(
		"passthrough://readiness-stop-test",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	hc := healthpb.NewHealthClient(conn)

	resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: server.ReadinessService})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)

	// Stop marks readiness NOT_SERVING before draining — existing connection still works.
	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: server.ReadinessService})
		if status.Code(err) == codes.Unavailable {
			// GracefulStop closed the connection after NOT_SERVING was set — transition succeeded.
			return
		}
		require.NoError(c, err)
		assert.Equal(c, healthpb.HealthCheckResponse_NOT_SERVING, resp.Status)
	}, 5*time.Second, 10*time.Millisecond)

	<-done
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
		mustSet(t, client, key, []byte("v"))
	}

	_, err := client.Delete(ctx, &runev1.DeleteRequest{Keys: []string{"a", "b", "missing"}})
	require.NoError(t, err)
}

func TestExists(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	mustSet(t, client, "exists-key", []byte("v"))

	resp, err := client.Exists(ctx, &runev1.ExistsRequest{Keys: []string{"exists-key", "missing"}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.Count)
}

func TestInfo(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	mustSet(t, client, "info-key", []byte("value"))

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

func TestForwardingGet(t *testing.T) {
	// Start the "owning" node (node-2) and write a value to it.
	conn2, cleanup2 := testutil.NewBufconnConn(t, bufSize, nil)
	defer cleanup2()

	rawClient2 := runev1.NewRuneServiceClient(conn2)
	stream, err := rawClient2.Set(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "fwd-key"}}}))
	require.NoError(t, stream.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("hello-from-node2")}}))
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	// Start a forwarding node (node-1) that routes all keys to node-2.
	conn1, cleanup1 := testutil.NewBufconnConn(t, bufSize, &testutil.BufconnOptions{ForwardTo: conn2})
	defer cleanup1()

	// Get via node-1 should be forwarded to node-2.
	client := runev1.NewRuneServiceClient(conn1)
	getStream, err := client.Get(context.Background(), &runev1.GetRequest{Key: "fwd-key"})
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
	assert.Equal(t, []byte("hello-from-node2"), got)
}

func TestForwardingSet(t *testing.T) {
	// The "owning" node.
	conn2, cleanup2 := testutil.NewBufconnConn(t, bufSize, nil)
	defer cleanup2()

	// The forwarding node.
	conn1, cleanup1 := testutil.NewBufconnConn(t, bufSize, &testutil.BufconnOptions{ForwardTo: conn2})
	defer cleanup1()

	// Set via node-1 — should be forwarded to node-2.
	client1 := runev1.NewRuneServiceClient(conn1)
	setStream, err := client1.Set(context.Background())
	require.NoError(t, err)
	require.NoError(t, setStream.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "set-fwd-key"}}}))
	require.NoError(t, setStream.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("written-via-node1")}}))
	_, err = setStream.CloseAndRecv()
	require.NoError(t, err)

	// Read from node-2 directly — should find the value.
	client2 := runev1.NewRuneServiceClient(conn2)
	getStream, err := client2.Get(context.Background(), &runev1.GetRequest{Key: "set-fwd-key"})
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
	assert.Equal(t, []byte("written-via-node1"), got)
}

func TestOwnerHintForwarding(t *testing.T) {
	conn2, cleanup2 := testutil.NewBufconnConn(t, bufSize, nil)
	defer cleanup2()
	mustSet(t, runev1.NewRuneServiceClient(conn2), "fwd-key", []byte("v"))

	conn1, cleanup1 := testutil.NewBufconnConn(t, bufSize, &testutil.BufconnOptions{ForwardTo: conn2})
	defer cleanup1()

	getStream, err := runev1.NewRuneServiceClient(conn1).Get(context.Background(), &runev1.GetRequest{Key: "fwd-key"})
	require.NoError(t, err)
	for {
		_, err = getStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	md, err := getStream.Header()
	require.NoError(t, err)
	assert.Equal(t, []string{conn2.Target()}, md.Get("x-rune-owner"), "client should be told the owning node's address")
}

func TestOwnerHintAbsentSingleNode(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	mustSet(t, client, "k", []byte("v"))

	getStream, err := client.Get(context.Background(), &runev1.GetRequest{Key: "k"})
	require.NoError(t, err)
	for {
		_, err = getStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	md, err := getStream.Header()
	require.NoError(t, err)
	assert.Empty(t, md.Get("x-rune-owner"), "single-node mode has no owner to hint")
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

func TestGracefulShutdown(t *testing.T) {
	cfg := testutil.BaseConfig(t)
	store, err := storage.NewBadgerStore(cfg, nil)
	require.NoError(t, err)

	lis := bufconn.Listen(1 << 20)
	srv := server.New(cfg, store, nil, nil, nil)
	srv.StartOnListener(lis)

	done := make(chan struct{})
	go func() {
		srv.Stop()
		_ = store.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("graceful shutdown timed out")
	}
}

func TestForwardingLoopPrevention(t *testing.T) {
	// conn2 is the "owning" peer. conn1 is a forwarding server that routes
	// all keys to conn2.
	conn2, cleanup2 := testutil.NewBufconnConn(t, bufSize, nil)
	defer cleanup2()
	conn1, cleanup1 := testutil.NewBufconnConn(t, bufSize, &testutil.BufconnOptions{ForwardTo: conn2})
	defer cleanup1()

	// A request with x-rune-forwarded must be served locally regardless of
	// ring ownership — this prevents infinite forwarding loops.
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-rune-forwarded", "1"))
	client := runev1.NewRuneServiceClient(conn1)
	stream, err := client.Get(ctx, &runev1.GetRequest{Key: "loop-key"})
	require.NoError(t, err)

	_, err = stream.Recv()
	// Must return NotFound (served locally) — not forwarded to conn2.
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}
