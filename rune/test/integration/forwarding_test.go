package integration_test

import (
	"context"
	"errors"
	"io"
	"testing"

	runev1 "github.com/bryandguy/rune/rune/internal/gen/rune/v1"
	"github.com/bryandguy/rune/rune/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIntegrationForwardedSetGet(t *testing.T) {
	conn2, cleanup2 := testutil.NewBufconnConn(t, 1<<20, nil)
	defer cleanup2()

	conn1, cleanup1 := testutil.NewBufconnConn(t, 1<<20, &testutil.BufconnOptions{ForwardTo: conn2})
	defer cleanup1()

	ctx := context.Background()
	payload := []byte("integration payload")

	// Set via node1 (which forwards to node2).
	client1 := runev1.NewRuneServiceClient(conn1)
	ps, err := client1.Set(ctx)
	require.NoError(t, err)
	require.NoError(t, ps.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "int-key"}}}))
	require.NoError(t, ps.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: payload}}))
	_, err = ps.CloseAndRecv()
	require.NoError(t, err, "Set via forwarding node should succeed")

	// Get directly from node2.
	client2 := runev1.NewRuneServiceClient(conn2)
	gs, err := client2.Get(ctx, &runev1.GetRequest{Key: "int-key"})
	require.NoError(t, err)
	var got []byte
	for {
		resp, err := gs.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		got = append(got, resp.Chunk...)
	}
	assert.Equal(t, payload, got, "data must round-trip through forwarding")
}

func TestIntegrationForwardedGetNotFound(t *testing.T) {
	conn2, cleanup2 := testutil.NewBufconnConn(t, 1<<20, nil)
	defer cleanup2()
	conn1, cleanup1 := testutil.NewBufconnConn(t, 1<<20, &testutil.BufconnOptions{ForwardTo: conn2})
	defer cleanup1()

	client1 := runev1.NewRuneServiceClient(conn1)
	gs, err := client1.Get(context.Background(), &runev1.GetRequest{Key: "missing-key"})
	require.NoError(t, err)
	_, err = gs.Recv()
	require.Equal(t, codes.NotFound, status.Code(err))
}
