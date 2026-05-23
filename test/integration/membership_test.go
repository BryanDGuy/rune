package integration_test

import (
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/bryandguy/rune/internal/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// startEmbeddedEtcd launches a single-node embedded etcd on random ports and
// returns the client endpoint. The server is shut down via t.Cleanup.
func startEmbeddedEtcd(t *testing.T) string {
	t.Helper()

	clientPort := freePort(t)
	peerPort := freePort(t)

	clientURL := parseURL(fmt.Sprintf("http://localhost:%d", clientPort))
	peerURL := parseURL(fmt.Sprintf("http://localhost:%d", peerPort))

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.InitialCluster = fmt.Sprintf("default=http://localhost:%d", peerPort)

	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(e.Close)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("embedded etcd did not become ready in time")
	}

	return fmt.Sprintf("localhost:%d", clientPort)
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func parseURL(s string) url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return *u
}

func newEtcdClient(t *testing.T, endpoint string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// TestEtcdMembershipJoin verifies that a node registering in etcd is visible
// to a second watcher's ring.
func TestEtcdMembershipJoin(t *testing.T) {
	endpoint := startEmbeddedEtcd(t)
	ctx := t.Context()

	// node-1 registers itself.
	m1 := cluster.New(newEtcdClient(t, endpoint), "node-1", "host1:7946", nil)
	require.NoError(t, m1.Start(ctx))
	t.Cleanup(m1.Stop)

	// node-2 is watch-only — it should see node-1 after Start.
	m2 := cluster.New(newEtcdClient(t, endpoint), "node-2", "", nil)
	require.NoError(t, m2.Start(ctx))
	t.Cleanup(m2.Stop)

	got, err := m2.Ring().Lookup("any-key")
	require.NoError(t, err)
	assert.Equal(t, "node-1", got.ID)
}

// TestEtcdMembershipTwoNodes verifies that two registered nodes each see the
// other in their local ring.
func TestEtcdMembershipTwoNodes(t *testing.T) {
	endpoint := startEmbeddedEtcd(t)
	ctx := t.Context()

	m1 := cluster.New(newEtcdClient(t, endpoint), "node-1", "host1:7946", nil)
	require.NoError(t, m1.Start(ctx))
	t.Cleanup(m1.Stop)

	m2 := cluster.New(newEtcdClient(t, endpoint), "node-2", "host2:7946", nil)
	require.NoError(t, m2.Start(ctx))
	t.Cleanup(m2.Stop)

	// Both rings should converge to 2 nodes.
	assert.Eventually(t, func() bool { return m1.Ring().Len() == 2 }, 5*time.Second, 20*time.Millisecond)
	assert.Eventually(t, func() bool { return m2.Ring().Len() == 2 }, 5*time.Second, 20*time.Millisecond)
}

// TestEtcdMembershipLeave verifies that when a node calls Stop (which revokes
// its lease), the watching node removes it from the ring.
func TestEtcdMembershipLeave(t *testing.T) {
	endpoint := startEmbeddedEtcd(t)
	ctx := t.Context()

	m1 := cluster.New(newEtcdClient(t, endpoint), "node-1", "host1:7946", nil)
	require.NoError(t, m1.Start(ctx))

	m2 := cluster.New(newEtcdClient(t, endpoint), "node-2", "host2:7946", nil)
	require.NoError(t, m2.Start(ctx))
	t.Cleanup(m2.Stop)

	// Wait for both rings to see each other.
	require.Eventually(t, func() bool { return m1.Ring().Len() == 2 }, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return m2.Ring().Len() == 2 }, 5*time.Second, 20*time.Millisecond)

	// node-1 cleanly leaves — lease revoke sends a DELETE event.
	m1.Stop()

	assert.Eventually(t, func() bool { return m2.Ring().Len() == 1 }, 5*time.Second, 20*time.Millisecond)

	got, err := m2.Ring().Lookup("any-key")
	require.NoError(t, err)
	assert.Equal(t, "node-2", got.ID)
}
