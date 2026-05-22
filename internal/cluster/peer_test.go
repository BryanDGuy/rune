package cluster

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func noopContextDialer(_ context.Context, _ string) (net.Conn, error) { return nil, nil }

func TestPeerDialerCachesConnection(t *testing.T) {
	d := NewPeerDialer()
	defer d.Close()

	opts := &DialOptions{ContextDialer: noopContextDialer}

	conn1, err := d.Dial("localhost:9999", opts)
	require.NoError(t, err)

	conn2, err := d.Dial("localhost:9999", opts)
	require.NoError(t, err)
	assert.Same(t, conn1, conn2, "same addr should return same connection")
}

func TestPeerDialerDifferentAddrs(t *testing.T) {
	d := NewPeerDialer()
	defer d.Close()

	opts := &DialOptions{ContextDialer: noopContextDialer}

	conn1, err := d.Dial("host1:7946", opts)
	require.NoError(t, err)
	conn2, err := d.Dial("host2:7946", opts)
	require.NoError(t, err)
	assert.NotSame(t, conn1, conn2, "different addrs should return different connections")
}

func TestPeerDialerClosePreventsFurtherDials(t *testing.T) {
	d := NewPeerDialer()
	d.Close()

	_, err := d.Dial("localhost:9999", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestPeerDialerConcurrentDial(t *testing.T) {
	d := NewPeerDialer()
	defer d.Close()
	opts := &DialOptions{ContextDialer: noopContextDialer}
	const n = 20
	conns := make([]*grpc.ClientConn, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(idx int) {
			defer wg.Done()
			conn, err := d.Dial("localhost:9999", opts)
			errs[idx] = err
			conns[idx] = conn
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	for _, c := range conns {
		assert.Same(t, conns[0], c)
	}
}
