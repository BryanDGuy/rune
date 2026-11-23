package integration_test

import (
	"context"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/bryandguy/rune/rune/internal/router"
	"github.com/bryandguy/rune/rune/test/testutil"
	runev1 "github.com/bryandguy/rune/sdk/go/gen/rune/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
		resp, err := runev1.NewRuneServiceClient(c).Exists(ctx, &runev1.ExistsRequest{Keys: slices.Concat(keys, []string{"missing"})})
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
	start := time.Now()
	_, err := runev1.NewRuneServiceClient(conns[0]).Delete(ctx, &runev1.DeleteRequest{Keys: []string{"any"}})
	require.Error(t, err)
	require.ErrorContains(t, err, "node-2", "error must name the failed peer")
	require.Less(t, time.Since(start), 2*time.Second, "should fail fast, not hang")

	// An empty Delete has nothing to broadcast, so the dead peer doesn't matter.
	_, err = runev1.NewRuneServiceClient(conns[0]).Delete(ctx, &runev1.DeleteRequest{})
	require.NoError(t, err)
}

// TestIntegrationClusterDeleteReachesAllLivePeers: one dead peer must not stop
// the broadcast; every reachable copy is gone after a single Delete. Peer order
// comes from map iteration and a stop-at-first-error bug shows in only ~1 round
// in 4, so 50 rounds (~0.5s) miss it about once in 2 million runs.
func TestIntegrationClusterDeleteReachesAllLivePeers(t *testing.T) {
	conns, _, stops := testutil.NewBufconnCluster(t, 4)
	stops[3]()
	fwd := metadata.AppendToOutgoingContext(t.Context(), "x-rune-forwarded", "1")

	for round := range 50 {
		for _, c := range conns[1:3] {
			setLocal(t, c, "k")
		}
		_, err := runev1.NewRuneServiceClient(conns[0]).Delete(t.Context(), &runev1.DeleteRequest{Keys: []string{"k"}})
		require.ErrorContains(t, err, "node-3", "round %d", round)
		for i, c := range conns[1:3] {
			resp, err := runev1.NewRuneServiceClient(c).Exists(fwd, &runev1.ExistsRequest{Keys: []string{"k"}})
			require.NoError(t, err)
			require.EqualValues(t, 0, resp.Count, "round %d: live peer node-%d kept its copy", round, i+1)
		}
	}
}

// setLocal stores key on the node behind conn without forwarding it to the owner.
func setLocal(t *testing.T, conn *grpc.ClientConn, key string) {
	t.Helper()
	fwd := metadata.AppendToOutgoingContext(t.Context(), "x-rune-forwarded", "1")
	ps, err := runev1.NewRuneServiceClient(conn).Set(fwd)
	require.NoError(t, err)
	require.NoError(t, ps.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: key}}}))
	require.NoError(t, ps.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("v")}}))
	_, err = ps.CloseAndRecv()
	require.NoError(t, err)
}

// TestIntegrationClusterDeleteMixedPeerFailures: when peers fail with different
// codes, Delete returns Unavailable (not whichever peer came first) and names
// every failed peer with its own code.
func TestIntegrationClusterDeleteMixedPeerFailures(t *testing.T) {
	conns, ring, stops := testutil.NewBufconnCluster(t, 3)
	stops[2]() // node-2: Unavailable
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	ring.Add(router.Node{ID: "node-hung", Addr: lis.Addr().String()}) // DeadlineExceeded

	_, err = runev1.NewRuneServiceClient(conns[0]).Delete(t.Context(), &runev1.DeleteRequest{Keys: []string{"k"}})
	require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	require.ErrorContains(t, err, "node-hung: DeadlineExceeded")
	require.ErrorContains(t, err, "node-2: Unavailable")
}

// TestIntegrationClusterPeerHangs: a ring member that accepts TCP but never
// answers must not hang Delete or Exists past the per-peer timeout.
func TestIntegrationClusterPeerHangs(t *testing.T) {
	conns, ring, _ := testutil.NewBufconnCluster(t, 2)
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	ring.Add(router.Node{ID: "node-dead", Addr: lis.Addr().String()})

	var deadKey string
	for i := 0; deadKey == "" && i < 1000; i++ {
		if n, _ := ring.Lookup(fmt.Sprintf("k-%d", i)); n.ID == "node-dead" {
			deadKey = fmt.Sprintf("k-%d", i)
		}
	}
	require.NotEmpty(t, deadKey)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	client := runev1.NewRuneServiceClient(conns[0])

	start := time.Now()
	_, err = client.Delete(ctx, &runev1.DeleteRequest{Keys: []string{"any"}})
	require.Error(t, err)
	require.Less(t, time.Since(start), 8*time.Second, "delete must give up on a hung peer")

	start = time.Now()
	_, err = client.Exists(ctx, &runev1.ExistsRequest{Keys: []string{deadKey}})
	require.ErrorContains(t, err, lis.Addr().String(), "error must name the failed peer")
	require.Less(t, time.Since(start), 8*time.Second, "exists must give up on a hung peer")
}

// TestIntegrationClusterDeleteClearsOrphan: a copy on a non-owner (left by a
// ring change) is removed by a Delete sent through a third node.
func TestIntegrationClusterDeleteClearsOrphan(t *testing.T) {
	conns, ring, _ := testutil.NewBufconnCluster(t, 3)
	n, err := ring.Lookup("orphan")
	require.NoError(t, err)
	var ownerIdx int
	_, _ = fmt.Sscanf(n.ID, "node-%d", &ownerIdx)
	orphanConn, viaConn := conns[(ownerIdx+1)%3], conns[(ownerIdx+2)%3]

	// x-rune-forwarded makes the non-owner store locally instead of forwarding.
	fwd := metadata.AppendToOutgoingContext(t.Context(), "x-rune-forwarded", "1")
	ps, err := runev1.NewRuneServiceClient(orphanConn).Set(fwd)
	require.NoError(t, err)
	require.NoError(t, ps.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "orphan"}}}))
	require.NoError(t, ps.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: []byte("v")}}))
	_, err = ps.CloseAndRecv()
	require.NoError(t, err)

	_, err = runev1.NewRuneServiceClient(viaConn).Delete(t.Context(), &runev1.DeleteRequest{Keys: []string{"orphan"}})
	require.NoError(t, err)

	resp, err := runev1.NewRuneServiceClient(orphanConn).Exists(fwd, &runev1.ExistsRequest{Keys: []string{"orphan"}})
	require.NoError(t, err)
	require.EqualValues(t, 0, resp.Count, "orphan copy must be deleted")
}
