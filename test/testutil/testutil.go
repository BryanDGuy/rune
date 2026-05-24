package testutil

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bryandguy/rune/internal/config"
	"github.com/bryandguy/rune/internal/server"
	"github.com/bryandguy/rune/internal/storage"
	"github.com/bryandguy/rune/shared/cluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

var bufconnSeq atomic.Uint64

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
func NewBufconnConn(t *testing.T, bufSize int) (*grpc.ClientConn, func()) {
	t.Helper()
	cfg := BaseConfig(t)
	store, err := storage.NewBadgerStore(cfg)
	require.NoError(t, err)
	lis := bufconn.Listen(bufSize)
	srv := server.New(cfg, store, nil, nil)
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
	}
}

// NewBufconnConnWithForwarding starts a server that forwards all key requests to
// peerConn. The ring contains only the peer node, so every key routes to it.
func NewBufconnConnWithForwarding(t *testing.T, bufSize int, peerConn *grpc.ClientConn) (*grpc.ClientConn, func()) {
	t.Helper()
	cfg := BaseConfig(t)
	store, err := storage.NewBadgerStore(cfg)
	require.NoError(t, err)

	dialer := cluster.NewPeerDialer()
	peerAddr := peerConn.Target()
	membership := newFakeMembership("node-self", "node-peer", peerAddr)
	require.NoError(t, dialer.DialWith(peerAddr, peerConn))

	lis := bufconn.Listen(bufSize)
	srv := server.New(cfg, store, nil, &server.ClusterOptions{Membership: membership, Dialer: dialer})
	srv.StartOnListener(lis)

	conn, err := grpc.NewClient(
		fmt.Sprintf("passthrough://bufnet-%d", bufconnSeq.Add(1)),
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
		dialer.Close()
	}
}
