package runesdk_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	runesdk "github.com/bryandguy/rune/sdk/go"
	"github.com/bryandguy/rune/sdk/go/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dialFrom returns a Dial function that resolves addresses from a fixed map of
// pre-built clients. Used to inject in-process connections in tests.
func dialFrom(clients map[string]*runesdk.Client) func(string) (*runesdk.Client, error) {
	return func(addr string) (*runesdk.Client, error) {
		if c, ok := clients[addr]; ok {
			return c, nil
		}
		return nil, fmt.Errorf("no pre-dialed client for %s", addr)
	}
}

func TestClusterClientBasicSetGet(t *testing.T) {
	conn, stop := testutil.NewBufconnConn(t, 1<<20, nil)
	defer stop()

	c, err := runesdk.NewClusterClient(
		[]string{conn.Target()},
		&runesdk.ClusterOptions{Dial: dialFrom(map[string]*runesdk.Client{
			conn.Target(): runesdk.NewClient(conn),
		})},
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
	realConn, stopReal := testutil.NewBufconnConn(t, 1<<20, nil)
	defer stopReal()

	// proxyConn forwards every key to realConn (ring contains only the peer).
	proxyConn, stopProxy := testutil.NewBufconnConn(t, 1<<20, &testutil.BufconnOptions{ForwardTo: realConn})

	clients := map[string]*runesdk.Client{
		proxyConn.Target(): runesdk.NewClient(proxyConn),
		realConn.Target():  runesdk.NewClient(realConn),
	}
	// Only the proxy is in the initial addr list; the real node's addr will be
	// learned from x-rune-owner and cached.
	c, err := runesdk.NewClusterClient(
		[]string{proxyConn.Target()},
		&runesdk.ClusterOptions{Dial: dialFrom(clients)},
	)
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
	conn, stop := testutil.NewBufconnConn(t, 1<<20, nil)
	defer stop()

	c, err := runesdk.NewClusterClient(
		[]string{conn.Target()},
		&runesdk.ClusterOptions{Dial: dialFrom(map[string]*runesdk.Client{
			conn.Target(): runesdk.NewClient(conn),
		})},
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
	conn, stop := testutil.NewBufconnConn(t, 1<<20, nil)
	defer stop()

	c, err := runesdk.NewClusterClient(
		[]string{conn.Target()},
		&runesdk.ClusterOptions{Dial: dialFrom(map[string]*runesdk.Client{
			conn.Target(): runesdk.NewClient(conn),
		})},
	)
	require.NoError(t, err)

	_, err = c.Get(context.Background(), "missing")
	require.ErrorIs(t, err, runesdk.ErrNotFound)
}

func TestClusterClientNoAddrsError(t *testing.T) {
	_, err := runesdk.NewClusterClient(nil, &runesdk.ClusterOptions{Dial: func(string) (*runesdk.Client, error) { return nil, nil }})
	require.Error(t, err)
}

func TestClusterClientCloseStopsRouting(t *testing.T) {
	conn, stop := testutil.NewBufconnConn(t, 1<<20, nil)
	defer stop()

	c, err := runesdk.NewClusterClient(
		[]string{conn.Target()},
		&runesdk.ClusterOptions{Dial: dialFrom(map[string]*runesdk.Client{
			conn.Target(): runesdk.NewClient(conn),
		})},
	)
	require.NoError(t, err)
	require.NoError(t, c.Close())

	_, err = c.Get(context.Background(), "key")
	require.Error(t, err)
}
