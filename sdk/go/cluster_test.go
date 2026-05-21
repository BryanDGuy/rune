package runesdk_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	runesdk "github.com/bryandguy/rune/sdk/go"

	"github.com/bryandguy/rune/internal/router"
	"github.com/bryandguy/rune/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTwoNodeCluster(t *testing.T) (*runesdk.ClusterClient, func()) {
	t.Helper()

	conn1, stop1 := testutil.NewBufconnConn(t, 1<<20)
	conn2, stop2 := testutil.NewBufconnConn(t, 1<<20)

	r := router.New()
	r.Add(router.Node{ID: "node-1", Addr: conn1.Target()})
	r.Add(router.Node{ID: "node-2", Addr: conn2.Target()})

	nodeClients := map[string]*runesdk.Client{
		conn1.Target(): runesdk.NewClient(conn1),
		conn2.Target(): runesdk.NewClient(conn2),
	}
	c := runesdk.NewClusterClientFromRingAndClients(r, nodeClients)

	return c, func() {
		_ = c.Close()
		stop1()
		stop2()
	}
}

func TestClusterClientRoutesSetAndGet(t *testing.T) {
	c, cleanup := newTwoNodeCluster(t)
	defer cleanup()

	ctx := context.Background()
	data := []byte("cluster payload")
	err := c.Set(ctx, "test-key", bytes.NewReader(data), nil)
	require.NoError(t, err)

	rc, err := c.Get(ctx, "test-key")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestClusterClientGetNotFound(t *testing.T) {
	c, cleanup := newTwoNodeCluster(t)
	defer cleanup()

	_, err := c.Get(context.Background(), "missing-key")
	require.ErrorIs(t, err, runesdk.ErrNotFound)
}

func TestClusterClientEmptyRingError(t *testing.T) {
	r := router.New() // empty ring
	c := runesdk.NewClusterClientFromRingAndClients(r, nil)
	defer c.Close()

	_, err := c.Get(context.Background(), "any-key")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no nodes")
}

func TestClusterClientCloseStopsRouting(t *testing.T) {
	c, cleanup := newTwoNodeCluster(t)
	defer cleanup()

	require.NoError(t, c.Close())

	_, err := c.Get(context.Background(), "key")
	require.Error(t, err)
}
