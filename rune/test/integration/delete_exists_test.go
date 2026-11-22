package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	runev1 "github.com/bryandguy/rune/rune/internal/gen/rune/v1"
	"github.com/bryandguy/rune/rune/test/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setVia(t *testing.T, conn *grpc.ClientConn, key string) {
	t.Helper()
	ps, err := runev1.NewRuneServiceClient(conn).Set(t.Context())
	require.NoError(t, err)
	require.NoError(t, ps.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: key}}}))
	require.NoError(t, ps.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("v")}}))
	_, err = ps.CloseAndRecv()
	require.NoError(t, err)
}

// TestIntegrationClusterDeleteExistsRouting: Delete and Exists sent to a
// non-owner must act on the owner (G3).
func TestIntegrationClusterDeleteExistsRouting(t *testing.T) {
	conns, ring, _ := testutil.NewBufconnCluster(t, 3)
	ctx := t.Context()

	// Pick keys owned by each node so Exists must fan out to several owners.
	keys := make([]string, 0, 3)
	owner := map[string]int{}
	for i := 0; len(keys) < 3 && i < 1000; i++ {
		k := fmt.Sprintf("k-%d", i)
		n, err := ring.Lookup(k)
		require.NoError(t, err)
		var idx int
		_, _ = fmt.Sscanf(n.ID, "node-%d", &idx)
		if _, taken := owner[n.ID]; !taken {
			owner[n.ID] = idx
			keys = append(keys, k)
		}
	}
	require.Len(t, keys, 3)

	for _, k := range keys {
		n, _ := ring.Lookup(k)
		setVia(t, conns[owner[n.ID]], k)
	}

	// Exists via each node returns the true count.
	for i, c := range conns {
		resp, err := runev1.NewRuneServiceClient(c).Exists(ctx, &runev1.ExistsRequest{Keys: append(keys, "missing")})
		require.NoError(t, err)
		require.EqualValues(t, 3, resp.Count, "exists via node-%d", i)
	}

	// Delete keys[0] via a non-owner, then Get via the owner is NotFound.
	n0, _ := ring.Lookup(keys[0])
	ownerConn := conns[owner[n0.ID]]
	nonOwner := conns[(owner[n0.ID]+1)%3]
	_, err := runev1.NewRuneServiceClient(nonOwner).Delete(ctx, &runev1.DeleteRequest{Keys: []string{keys[0]}})
	require.NoError(t, err)
	gs, err := runev1.NewRuneServiceClient(ownerConn).Get(ctx, &runev1.GetRequest{Key: keys[0]})
	require.NoError(t, err)
	_, err = gs.Recv()
	require.Equal(t, codes.NotFound, status.Code(err))

	resp, err := runev1.NewRuneServiceClient(nonOwner).Exists(ctx, &runev1.ExistsRequest{Keys: keys})
	require.NoError(t, err)
	require.EqualValues(t, 2, resp.Count)
}

// TestIntegrationClusterDeletePeerDown: Delete must not report OK when a ring
// member can't be reached.
func TestIntegrationClusterDeletePeerDown(t *testing.T) {
	conns, _, stops := testutil.NewBufconnCluster(t, 3)
	stops[2]()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := runev1.NewRuneServiceClient(conns[0]).Delete(ctx, &runev1.DeleteRequest{Keys: []string{"any"}})
	require.Error(t, err)
	require.NotEqual(t, codes.DeadlineExceeded, status.Code(err), "should fail fast, not hang")
}
