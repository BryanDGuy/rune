package server_test

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bryandguy/rune/rune/internal/config"
	"github.com/bryandguy/rune/rune/test/testutil"
	runev1 "github.com/bryandguy/rune/sdk/go/gen/rune/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func limitedConn(t *testing.T, maxValue, inflight int64, forwardTo *grpc.ClientConn, extra ...func(*config.Config)) *grpc.ClientConn {
	t.Helper()
	conn, cleanup := testutil.NewBufconnConn(t, bufSize, &testutil.BufconnOptions{
		ForwardTo: forwardTo,
		Configure: func(c *config.Config) {
			c.MaxValueBytes = maxValue
			c.MaxInflightBytes = inflight
			c.StreamChunkSize = 64 << 10
			for _, f := range extra {
				f(c)
			}
		},
	})
	t.Cleanup(cleanup)
	return conn
}

// setChunks sends n chunks of chunk and returns the final status. Send errors
// (io.EOF once the server has rejected the stream) are ignored; the real
// status comes from CloseAndRecv.
func setChunks(ctx context.Context, client runev1.RuneServiceClient, key string, chunk []byte, n int) error {
	s, err := client.Set(ctx)
	if err != nil {
		return err
	}
	_ = s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: key}}})
	for range n {
		if s.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: chunk}}) != nil {
			break
		}
	}
	_, err = s.CloseAndRecv()
	return err
}

func TestSetRejectsOversizeValue(t *testing.T) {
	owner := limitedConn(t, 4<<10, 1<<20, nil)
	chunk := make([]byte, 1<<10)

	direct := runev1.NewRuneServiceClient(owner)
	require.NoError(t, setChunks(context.Background(), direct, "ok", chunk, 4), "exactly the cap is allowed")
	err := setChunks(context.Background(), direct, "big", chunk, 5)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "direct: %v", err)

	// Via a non-owner: the owner's ResourceExhausted must pass through, not become Unavailable.
	fwd := runev1.NewRuneServiceClient(limitedConn(t, 1<<30, 2<<30, owner))
	err = setChunks(context.Background(), fwd, "big", chunk, 5)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "forwarded: %v", err)
	// A long stream makes the forwarder's Send hit io.EOF mid-stream after the owner rejects.
	err = setChunks(context.Background(), fwd, "big", chunk, 2000)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "forwarded, long stream: %v", err)

	gs, err := direct.Get(context.Background(), &runev1.GetRequest{Key: "big"})
	require.NoError(t, err)
	_, err = gs.Recv()
	assert.Equal(t, codes.NotFound, status.Code(err), "rejected value must not be stored")
}

func TestConcurrentSetsStayWithinMemoryBudget(t *testing.T) {
	const (
		budget   = 64 << 20
		valueLen = 32 << 20
		chunkLen = 1 << 20
		writers  = 8
	)
	client := runev1.NewRuneServiceClient(limitedConn(t, valueLen, budget, nil))
	chunk := make([]byte, chunkLen)

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	var peak atomic.Uint64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var ms runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
				runtime.GC() // sample live heap, not garbage awaiting collection
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > peak.Load() {
					peak.Store(ms.HeapAlloc)
				}
			}
		}
	}()

	var wg sync.WaitGroup
	codesSeen := make([]codes.Code, writers)
	for i := range writers {
		wg.Go(func() {
			codesSeen[i] = status.Code(setChunks(context.Background(), client, "k", chunk, valueLen/chunkLen))
		})
	}
	wg.Wait()
	close(stop)
	<-sampled

	rejected := 0
	for _, c := range codesSeen {
		require.Contains(t, []codes.Code{codes.OK, codes.ResourceExhausted}, c)
		if c == codes.ResourceExhausted {
			rejected++
		}
	}
	assert.Positive(t, rejected, "8 x 32 MiB against a 64 MiB budget must reject some")
	// Slack (from a heap profile): Badger's write-path copy of each admitted value
	// (at most another budget), its 64 MiB memtable arena, and ~32 MiB of gRPC
	// buffers. Unfixed, this grew 540-740 MiB.
	grown := peak.Load() - min(base.HeapAlloc, peak.Load())
	t.Logf("live heap grew %d MiB (budget %d MiB)", grown>>20, budget>>20)
	assert.Less(t, grown, uint64(2*budget+96<<20), "heap grew %d MiB", grown>>20)
}

// getCode runs a Get, drains it, and returns its final status code.
func getCode(t *testing.T, client runev1.RuneServiceClient, key string) codes.Code {
	t.Helper()
	gs, err := client.Get(context.Background(), &runev1.GetRequest{Key: key})
	require.NoError(t, err)
	for {
		if _, err := gs.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return codes.OK
			}
			return status.Code(err)
		}
	}
}

// holdBudget opens a Set and sends n 1 MiB chunks without closing it, so the
// server keeps the grown buffer charged. Cancel releases it.
func holdBudget(t *testing.T, client runev1.RuneServiceClient, n int) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	held, err := client.Set(ctx)
	require.NoError(t, err)
	require.NoError(t, held.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: &runev1.SetHeader{Key: "held"}}}))
	for range n {
		require.NoError(t, held.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Chunk{Chunk: make([]byte, 1<<20)}}))
	}
	// Let the server absorb the chunks before anything else reserves: a concurrent
	// reservation during the held Set's growth would reject it. Every budget probe
	// reserves too, so there is nothing side-effect free to poll.
	time.Sleep(300 * time.Millisecond)
	return cancel
}

func TestGetRespectsMemoryBudget(t *testing.T) {
	const budget = 16 << 20
	client := runev1.NewRuneServiceClient(limitedConn(t, budget, budget, nil))
	require.NoError(t, setChunks(context.Background(), client, "v", make([]byte, 1<<20), 8))

	// An open Set of 9 MiB grows its buffer to 16 MiB and holds the whole budget.
	cancel := holdBudget(t, client, 9)
	assert.Eventually(t, func() bool { return getCode(t, client, "v") == codes.ResourceExhausted }, 5*time.Second, 20*time.Millisecond,
		"Get must be rejected while the budget is held")

	cancel()
	assert.Eventually(t, func() bool { return getCode(t, client, "v") == codes.OK }, 5*time.Second, 20*time.Millisecond,
		"Get must succeed once the held Set releases its bytes")
}

func TestRejectedGetsDoNotCopyValue(t *testing.T) {
	const budget = 64 << 20
	client := runev1.NewRuneServiceClient(limitedConn(t, budget, budget, nil))
	require.NoError(t, setChunks(context.Background(), client, "v", make([]byte, 1<<20), 32))

	cancel := holdBudget(t, client, 33) // grows to 64 MiB: the whole budget
	defer cancel()
	require.Eventually(t, func() bool { return getCode(t, client, "v") == codes.ResourceExhausted }, 5*time.Second, 20*time.Millisecond)

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { assert.Equal(t, codes.ResourceExhausted, getCode(t, client, "v")) })
	}
	wg.Wait()
	runtime.ReadMemStats(&after)
	grew := after.TotalAlloc - before.TotalAlloc
	t.Logf("16 rejected Gets of a 32 MiB value allocated %d MiB", grew>>20)
	assert.Less(t, grew, uint64(32<<20), "a rejected Get must not copy the value")
}

func TestStalledGetReleasesBudget(t *testing.T) {
	const valueLen = 48 << 20
	client := runev1.NewRuneServiceClient(limitedConn(t, valueLen, 2*valueLen, nil,
		func(c *config.Config) { c.GetIdleTimeout = 300 * time.Millisecond }))
	require.NoError(t, setChunks(context.Background(), client, "v", make([]byte, 1<<20), valueLen>>20))

	// Two readers take one chunk each and stop, like an SDK caller that never Closes.
	// t.Context is canceled before cleanup, so server shutdown does not wait on their streams.
	for range 2 {
		gs, err := client.Get(t.Context(), &runev1.GetRequest{Key: "v"})
		require.NoError(t, err)
		_, err = gs.Recv()
		require.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		return setChunks(context.Background(), client, "small", make([]byte, 1<<20), 1) == nil
	}, 5*time.Second, 50*time.Millisecond, "stalled Gets must release the budget after the idle timeout")
}

func TestZeroLimitsUseDefaults(t *testing.T) {
	client := runev1.NewRuneServiceClient(limitedConn(t, 0, 0, nil))
	require.NoError(t, setChunks(context.Background(), client, "k", make([]byte, 1<<10), 1))
	assert.Equal(t, codes.OK, getCode(t, client, "k"))
}

func TestForwardedSetWhenOwnerBudgetFull(t *testing.T) {
	owner := limitedConn(t, 16<<20, 16<<20, nil)
	cancel := holdBudget(t, runev1.NewRuneServiceClient(owner), 9) // owner budget full
	// A 1-byte forwarder budget: any charge on the forwarder would reject every Set.
	fwd := runev1.NewRuneServiceClient(limitedConn(t, 16<<20, 1, owner))
	set := func() codes.Code {
		return status.Code(setChunks(context.Background(), fwd, "k", make([]byte, 1<<20), 1))
	}
	assert.Eventually(t, func() bool { return set() == codes.ResourceExhausted }, 5*time.Second, 20*time.Millisecond,
		"owner's ResourceExhausted must reach the client through the forwarder")
	cancel()
	assert.Eventually(t, func() bool { return set() == codes.OK }, 5*time.Second, 20*time.Millisecond,
		"forwarder must hold no budget of its own")
}

func TestSetChargesItsBufferCapacity(t *testing.T) {
	const budget = 64 << 20
	client := runev1.NewRuneServiceClient(limitedConn(t, budget, budget, nil))
	require.NoError(t, setChunks(context.Background(), client, "v", make([]byte, 1<<20), 32))

	// 33 MiB needs a 64 MiB buffer (doubling from 32); that must be charged in full,
	// so a 32 MiB Get no longer fits.
	cancel := holdBudget(t, client, 33)
	defer cancel()
	assert.Eventually(t, func() bool { return getCode(t, client, "v") == codes.ResourceExhausted }, 5*time.Second, 20*time.Millisecond,
		"the held Set's charge must equal its 64 MiB buffer")
}
