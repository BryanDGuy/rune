package testutil

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bryandguy/rune/rune/internal/cluster"
	"github.com/bryandguy/rune/rune/internal/config"
	"github.com/bryandguy/rune/rune/internal/router"
	"github.com/bryandguy/rune/rune/internal/server"
	"github.com/bryandguy/rune/rune/internal/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

var bufconnSeq atomic.Uint64

// BufconnOptions configures a NewBufconnConn call. A nil pointer starts the
// server in single-node mode.
type BufconnOptions struct {
	// ForwardTo makes the server forward all key requests to the given peer
	// connection. The ring contains only that peer, so every key routes to it.
	ForwardTo *grpc.ClientConn
}

func BaseConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    1 << 30,
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		StreamChunkSize:    1 << 20,
		GCInterval:         time.Hour,
		GCDiscardRatio:     0.5,
	}
}

// NewBufconnConn starts a Rune server over an in-process bufconn listener and
// returns a dialed gRPC connection plus a cleanup function. bufSize controls
// the in-memory buffer size. Each call gets a unique target address so that
// multiple connections within the same test can be distinguished by Target().
// Pass opts.ForwardTo to start the server in cluster-forwarding mode.
func NewBufconnConn(t *testing.T, bufSize int, opts *BufconnOptions) (*grpc.ClientConn, func()) {
	t.Helper()
	cfg := BaseConfig(t)
	store, err := storage.NewBadgerStore(cfg, nil)
	require.NoError(t, err)

	var clusterOpts *server.ClusterOptions
	var dialer *cluster.PeerDialer
	if opts != nil && opts.ForwardTo != nil {
		dialer = cluster.NewPeerDialer()
		peerAddr := opts.ForwardTo.Target()
		require.NoError(t, dialer.DialWith(peerAddr, opts.ForwardTo))
		clusterOpts = &server.ClusterOptions{
			Membership: newFakeMembership("node-self", "node-peer", peerAddr),
			Dialer:     dialer,
		}
	}

	lis := bufconn.Listen(bufSize)
	srv := server.New(cfg, store, nil, clusterOpts, nil)
	srv.StartOnListener(lis)

	addr := fmt.Sprintf("passthrough://bufnet-%d", bufconnSeq.Add(1))
	conn, err := grpc.NewClient(
		addr,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	return conn, func() {
		srv.Stop()
		_ = conn.Close()
		_ = store.Close()
		if dialer != nil {
			dialer.Close()
		}
	}
}

// NewBufconnCluster starts n Rune servers (node-0..node-n-1) sharing one ring
// that contains all of them, each able to forward to every other. It returns
// the client conns, the shared ring (to look up a key's owner), and a stop
// func per node so a test can take one peer down. Everything is cleaned up via
// t.Cleanup.
func NewBufconnCluster(t *testing.T, n int) ([]*grpc.ClientConn, *router.Router, []func()) {
	t.Helper()
	ring := router.New()
	conns := make([]*grpc.ClientConn, n)
	liss := make([]*bufconn.Listener, n)
	for i := range n {
		lis := bufconn.Listen(1 << 20)
		conn, err := grpc.NewClient(
			fmt.Sprintf("passthrough://bufnet-%d", bufconnSeq.Add(1)),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		conns[i], liss[i] = conn, lis
		ring.Add(router.Node{ID: fmt.Sprintf("node-%d", i), Addr: conn.Target()})
	}

	stops := make([]func(), n)
	for i := range n {
		cfg := BaseConfig(t)
		store, err := storage.NewBadgerStore(cfg, nil)
		require.NoError(t, err)
		dialer := cluster.NewPeerDialer()
		for j, c := range conns {
			if j != i {
				require.NoError(t, dialer.DialWith(c.Target(), c))
			}
		}
		srv := server.New(cfg, store, nil, &server.ClusterOptions{
			Membership: &fakeMembership{nodeID: fmt.Sprintf("node-%d", i), ring: ring},
			Dialer:     dialer,
		}, nil)
		srv.StartOnListener(liss[i])
		stops[i] = srv.Stop
		t.Cleanup(func() {
			srv.Stop()
			_ = store.Close()
			dialer.Close()
		})
	}
	return conns, ring, stops
}
