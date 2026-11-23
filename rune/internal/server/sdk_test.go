package server_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	runesdk "github.com/bryandguy/rune/sdk/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Links the server and the SDK into one binary; both must share one registered rune.proto.
func TestSDKRoundTrip(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	client := runesdk.NewClient(conn)

	require.NoError(t, client.Set(context.Background(), "k", bytes.NewReader([]byte("v")), nil))
	r, err := client.Get(context.Background(), "k")
	require.NoError(t, err)
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "v", string(got))
}
