package runesdk_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	runesdk "github.com/bryandguy/rune/sdk/go"

	"github.com/bryandguy/rune/internal/router"
	"github.com/bryandguy/rune/test/testutil"
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
	conn1, stop1 := testutil.NewBufconnConn(t, 1<<20)
	conn2, stop2 := testutil.NewBufconnConn(t, 1<<20)
	defer stop1()
	defer stop2()

	r := router.New()
	r.Add(router.Node{ID: "node-1", Addr: conn1.Target()})
	r.Add(router.Node{ID: "node-2", Addr: conn2.Target()})

	client1 := runesdk.NewClient(conn1)
	client2 := runesdk.NewClient(conn2)
	nodeClients := map[string]*runesdk.Client{
		conn1.Target(): client1,
		conn2.Target(): client2,
	}
	c := runesdk.NewClusterClientFromRingAndClients(r, nodeClients)
	defer c.Close()

	ctx := context.Background()
	data := []byte("cluster payload")
	const key = "test-routing-key"

	err := c.Set(ctx, key, bytes.NewReader(data), nil)
	require.NoError(t, err)

	// Determine which node the ring assigns this key to.
	owningNode, err := r.Lookup(key)
	require.NoError(t, err)

	// The owning node should have the value; the other should not.
	var owningClient, otherClient *runesdk.Client
	if owningNode.Addr == conn1.Target() {
		owningClient, otherClient = client1, client2
	} else {
		owningClient, otherClient = client2, client1
	}

	// Value present on owning node.
	rc, err := owningClient.Get(ctx, key)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, data, got)

	// Value absent on the other node.
	_, err = otherClient.Get(ctx, key)
	require.ErrorIs(t, err, runesdk.ErrNotFound)
}

func TestClusterClientDelete(t *testing.T) {
	c, cleanup := newTwoNodeCluster(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "del-key", bytes.NewReader([]byte("v")), nil))
	require.NoError(t, c.Delete(ctx, "del-key"))

	_, err := c.Get(ctx, "del-key")
	require.ErrorIs(t, err, runesdk.ErrNotFound)
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
	require.ErrorIs(t, err, runesdk.ErrNoNodes)
}

func TestClusterClientCloseStopsRouting(t *testing.T) {
	c, cleanup := newTwoNodeCluster(t)
	defer cleanup()

	require.NoError(t, c.Close())

	_, err := c.Get(context.Background(), "key")
	require.Error(t, err)
}
