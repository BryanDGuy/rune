package cluster

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestPeerDialerCachesConnection(t *testing.T) {
	d := NewPeerDialer()
	defer d.Close()

	dialOpts := []grpc.DialOption{
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return nil, nil
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn1, err := d.Dial("localhost:9999", dialOpts...)
	require.NoError(t, err)

	conn2, err := d.Dial("localhost:9999", dialOpts...)
	require.NoError(t, err)
	assert.Same(t, conn1, conn2, "same addr should return same connection")
}

func TestPeerDialerDifferentAddrs(t *testing.T) {
	d := NewPeerDialer()
	defer d.Close()

	dialOpts := []grpc.DialOption{
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return nil, nil
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn1, err := d.Dial("host1:7946", dialOpts...)
	require.NoError(t, err)
	conn2, err := d.Dial("host2:7946", dialOpts...)
	require.NoError(t, err)
	assert.NotSame(t, conn1, conn2, "different addrs should return different connections")
}

func TestPeerDialerClosePreventsFurtherDials(t *testing.T) {
	d := NewPeerDialer()
	d.Close()

	_, err := d.Dial("localhost:9999",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}
