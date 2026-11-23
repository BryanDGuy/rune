package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/bryandguy/rune/rune/internal/logging"
	"github.com/bryandguy/rune/rune/internal/router"
	"github.com/bryandguy/rune/rune/internal/server"
	"github.com/bryandguy/rune/rune/internal/storage"
	"github.com/bryandguy/rune/rune/test/testutil"
	runev1 "github.com/bryandguy/rune/sdk/go/gen/rune/v1"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func startEtcd(t *testing.T) string {
	t.Helper()
	clientURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", freePort(t)))
	peerURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", freePort(t)))
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = "default=" + peerURL.String()
	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("embedded etcd not ready")
	}
	return clientURL.Host
}

type node struct {
	srv      interface{ ActiveConns() int64 }
	client   runev1.RuneServiceClient
	shutdown func()
	addr     string
}

func startNode(t *testing.T, endpoint, id string) node {
	t.Helper()
	cfg := testutil.BaseConfig(t)
	cfg.Port = freePort(t)
	cfg.NodeID = id
	cfg.NodeAddr = fmt.Sprintf("localhost:%d", cfg.Port)
	cfg.EtcdEndpoints = []string{endpoint}

	store, err := storage.NewBadgerStore(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	srv, join, shutdown := buildServer(cfg, store, logging.Discard(), nil)
	require.NoError(t, srv.Start(t.Context()))
	require.NoError(t, join(t.Context()))

	conn, err := grpc.NewClient(cfg.NodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return node{srv: srv, shutdown: shutdown, addr: cfg.NodeAddr, client: runev1.NewRuneServiceClient(conn)}
}

func set(ctx context.Context, c runev1.RuneServiceClient, key string) error {
	s, err := c.Set(ctx)
	if err != nil {
		return err
	}
	if err = s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: key}}}); err != nil {
		return err
	}
	if err = s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("v")}}); err != nil {
		return err
	}
	_, err = s.CloseAndRecv()
	return err
}

// tcpProxy forwards TCP to target until cut closes the listener and every open connection.
func tcpProxy(t *testing.T, target string) (addr string, cut func()) {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err)
	var mu sync.Mutex
	var conns []net.Conn
	closed := false
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			u, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			mu.Lock()
			if closed {
				_, _ = c.Close(), u.Close()
			}
			conns = append(conns, c, u)
			mu.Unlock()
			go func() { _, _ = io.Copy(u, c); _ = u.Close() }()
			go func() { _, _ = io.Copy(c, u); _ = c.Close() }()
		}
	}()
	cut = func() {
		_ = lis.Close()
		mu.Lock()
		defer mu.Unlock()
		closed = true
		for _, c := range conns {
			_ = c.Close()
		}
	}
	t.Cleanup(cut)
	return lis.Addr().String(), cut
}

// keyOwnedBy returns a key that the two-node ring {n1, n2} assigns to n1.
func keyOwnedBy(n1, n2 node) string {
	ring := router.New()
	ring.Reset([]router.Node{{ID: "node-1", Addr: n1.addr}, {ID: "node-2", Addr: n2.addr}})
	for i := 0; ; i++ {
		if owner, _ := ring.Lookup(fmt.Sprint("k", i)); owner.ID == "node-1" {
			return fmt.Sprint("k", i)
		}
	}
}

// TestShutdownWithoutEtcdWaitsForLeaseExpiry: if node-1 can't revoke its lease,
// node-2 keeps it in the ring until the lease expires. node-1 must keep serving
// until then, or every Set for its keys and every Delete through node-2 fails.
func TestShutdownWithoutEtcdWaitsForLeaseExpiry(t *testing.T) {
	endpoint := startEtcd(t)
	proxy, cut := tcpProxy(t, endpoint)
	n1 := startNode(t, proxy, "node-1")
	n2 := startNode(t, endpoint, "node-2")
	key := keyOwnedBy(n1, n2)
	require.Eventually(t, func() bool { return set(t.Context(), n2.client, key) == nil }, 5*time.Second, 20*time.Millisecond)

	cut() // node-1 loses etcd; node-2 does not
	done := make(chan struct{})
	go func() { n1.shutdown(); close(done) }()

	// Keep going 1s past shutdown: a node that closed too early is still in node-2's ring then.
	var sent, failures int
	var deadline time.Time
	for deadline.IsZero() || time.Now().Before(deadline) {
		select {
		case <-done:
			done, deadline = nil, time.Now().Add(time.Second) // nil: wait on the ticker from now on
		case <-time.After(10 * time.Millisecond):
		}
		sent++
		_, delErr := n2.client.Delete(t.Context(), &runev1.DeleteRequest{Keys: []string{key}})
		if set(t.Context(), n2.client, key) != nil || delErr != nil {
			failures++
		}
	}
	require.Positive(t, sent)
	require.Zero(t, failures, "%d of %d Set/Delete rounds via node-2 failed during and after node-1 shutdown", failures, sent)
	n2.shutdown()
}

// TestShutdownLeavesRingBeforeDraining: on shutdown node-1's etcd key must be gone
// while an in-flight RPC is still draining, that RPC must succeed, and forwards
// sent through node-2 during the whole shutdown must not fail.
func TestShutdownLeavesRingBeforeDraining(t *testing.T) {
	endpoint := startEtcd(t)
	n1 := startNode(t, endpoint, "node-1")
	n2 := startNode(t, endpoint, "node-2")

	watcher, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = watcher.Close() })
	registered := func() bool {
		resp, getErr := watcher.Get(t.Context(), router.NodeKeyPrefix+"node-1")
		require.NoError(t, getErr)
		return resp.Count == 1
	}
	require.True(t, registered())

	key := keyOwnedBy(n1, n2) // node-2 forwards it to node-1
	require.Eventually(t, func() bool { return set(t.Context(), n2.client, key) == nil }, 5*time.Second, 20*time.Millisecond)

	inflight, err := n1.client.Set(t.Context())
	require.NoError(t, err)
	require.NoError(t, inflight.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "k"}}}))
	require.Eventually(t, func() bool { return n1.srv.ActiveConns() >= 1 }, 5*time.Second, 10*time.Millisecond)

	done := make(chan struct{})
	go func() { n1.shutdown(); close(done) }()

	var failures, sent atomic.Int64
	forwarding := make(chan struct{})
	go func() {
		defer close(forwarding)
		for {
			select {
			case <-done:
				return
			default:
			}
			sent.Add(1)
			if set(t.Context(), n2.client, key) != nil {
				failures.Add(1)
			}
		}
	}()

	require.Eventually(t, func() bool { return !registered() }, 5*time.Second, 20*time.Millisecond,
		"node must leave the ring before the gRPC drain finishes")

	require.NoError(t, inflight.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("v")}}))
	_, err = inflight.CloseAndRecv()
	require.NoError(t, err, "in-flight RPC must complete during drain")

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	<-forwarding
	require.Positive(t, sent.Load())
	require.Zero(t, failures.Load(), "forwards via node-2 failed during node-1 shutdown (%d sent)", sent.Load())
	n2.shutdown()
}

// TestReadyOnlyAfterJoin: a node must not report Ready until it has joined the ring,
// or a hung join leaves it serving every key locally (G4 review #1).
func TestReadyOnlyAfterJoin(t *testing.T) {
	endpoint := startEtcd(t)
	cfg := testutil.BaseConfig(t)
	cfg.Port = freePort(t)
	cfg.NodeID = "node-1"
	cfg.NodeAddr = fmt.Sprintf("localhost:%d", cfg.Port)
	cfg.EtcdEndpoints = []string{endpoint}
	store, err := storage.NewBadgerStore(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	srv, join, shutdown := buildServer(cfg, store, logging.Discard(), nil)
	require.NoError(t, srv.Start(t.Context()))
	t.Cleanup(shutdown)

	conn, err := grpc.NewClient(cfg.NodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	readiness := func() healthpb.HealthCheckResponse_ServingStatus {
		resp, checkErr := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{Service: server.ReadinessService})
		require.NoError(t, checkErr)
		return resp.Status
	}

	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, readiness(), "not Ready before join")
	require.NoError(t, join(t.Context()))
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, readiness(), "Ready after join")
}
