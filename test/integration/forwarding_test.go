package integration_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/bryandguy/rune/test/testutil"
	runesdk "github.com/bryandguy/rune/sdk/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationForwardedSetGet(t *testing.T) {
	conn2, cleanup2 := testutil.NewBufconnConn(t, 1<<20)
	defer cleanup2()

	conn1, cleanup1 := testutil.NewBufconnConnWithForwarding(t, 1<<20, conn2)
	defer cleanup1()

	ctx := context.Background()
	payload := []byte("integration payload")

	client1 := runesdk.NewClient(conn1)
	err := client1.Set(ctx, "int-key", bytes.NewReader(payload), nil)
	require.NoError(t, err, "Set via forwarding node should succeed")

	client2 := runesdk.NewClient(conn2)
	rc, err := client2.Get(ctx, "int-key")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "data must round-trip through forwarding")
}

func TestIntegrationForwardedGetNotFound(t *testing.T) {
	conn2, cleanup2 := testutil.NewBufconnConn(t, 1<<20)
	defer cleanup2()
	conn1, cleanup1 := testutil.NewBufconnConnWithForwarding(t, 1<<20, conn2)
	defer cleanup1()

	client1 := runesdk.NewClient(conn1)
	_, err := client1.Get(context.Background(), "missing-key")
	require.ErrorIs(t, err, runesdk.ErrNotFound)
}
