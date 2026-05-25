package runesdk_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/bryandguy/rune/rune/test/testutil"
	runesdk "github.com/bryandguy/rune/sdk/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterClientBasicSetGet(t *testing.T) {
	conn, stop := testutil.NewBufconnConn(t, 1<<20)
	defer stop()

	c, err := runesdk.NewClusterClientFromClients(
		[]string{conn.Target()},
		map[string]*runesdk.Client{conn.Target(): runesdk.NewClient(conn)},
	)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	data := []byte("hello cluster")

	require.NoError(t, c.Set(ctx, "k", bytes.NewReader(data), nil))

	rc, err := c.Get(ctx, "k")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, data, got)
}

// TestClusterClientCachesOwnerHint verifies that the x-rune-owner response header
// is cached so that after the first Get, subsequent requests bypass the proxy and
// go directly to the owning node.
func TestClusterClientCachesOwnerHint(t *testing.T) {
	realConn, stopReal := testutil.NewBufconnConn(t, 1<<20)
	defer stopReal()

	// proxyConn forwards every key to realConn (ring contains only the peer).
	proxyConn, stopProxy := testutil.NewBufconnConnWithForwarding(t, 1<<20, realConn)

	clients := map[string]*runesdk.Client{
		proxyConn.Target(): runesdk.NewClient(proxyConn),
		realConn.Target():  runesdk.NewClient(realConn),
	}
	// Only the proxy is in the initial addr list; the real node's addr will be
	// learned from x-rune-owner and cached.
	c, err := runesdk.NewClusterClientFromClients([]string{proxyConn.Target()}, clients)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	data := []byte("owner hint payload")

	// Set goes through proxy → forwarded to real.
	require.NoError(t, c.Set(ctx, "hint-key", bytes.NewReader(data), nil))

	// First Get goes through proxy, captures x-rune-owner = realConn.Target().
	rc, err := c.Get(ctx, "hint-key")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	require.NoError(t, rc.Close())
	assert.Equal(t, data, got)

	// Take the proxy down — the cached hint must route subsequent Gets directly
	// to the real node without going through the now-dead proxy.
	stopProxy()

	rc, err = c.Get(ctx, "hint-key")
	require.NoError(t, err)
	got, _ = io.ReadAll(rc)
	require.NoError(t, rc.Close())
	assert.Equal(t, data, got)
}

func TestClusterClientDelete(t *testing.T) {
	conn, stop := testutil.NewBufconnConn(t, 1<<20)
	defer stop()

	c, err := runesdk.NewClusterClientFromClients(
		[]string{conn.Target()},
		map[string]*runesdk.Client{conn.Target(): runesdk.NewClient(conn)},
	)
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	require.NoError(t, c.Set(ctx, "del-key", bytes.NewReader([]byte("v")), nil))
	require.NoError(t, c.Delete(ctx, "del-key"))

	_, err = c.Get(ctx, "del-key")
	require.ErrorIs(t, err, runesdk.ErrNotFound)
}

func TestClusterClientGetNotFound(t *testing.T) {
	conn, stop := testutil.NewBufconnConn(t, 1<<20)
	defer stop()

	c, err := runesdk.NewClusterClientFromClients(
		[]string{conn.Target()},
		map[string]*runesdk.Client{conn.Target(): runesdk.NewClient(conn)},
	)
	require.NoError(t, err)
	defer c.Close()

	_, err = c.Get(context.Background(), "missing")
	require.ErrorIs(t, err, runesdk.ErrNotFound)
}

func TestClusterClientNoAddrsError(t *testing.T) {
	_, err := runesdk.NewClusterClient()
	require.Error(t, err)
}

func TestClusterClientCloseStopsRouting(t *testing.T) {
	conn, stop := testutil.NewBufconnConn(t, 1<<20)
	defer stop()

	c, err := runesdk.NewClusterClientFromClients(
		[]string{conn.Target()},
		map[string]*runesdk.Client{conn.Target(): runesdk.NewClient(conn)},
	)
	require.NoError(t, err)
	require.NoError(t, c.Close())

	_, err = c.Get(context.Background(), "key")
	require.Error(t, err)
}
