package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

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

func runningSetHandlers() int {
	buf := make([]byte, 1<<20)
	return strings.Count(string(buf[:runtime.Stack(buf, true)]), "(*handler).Set(")
}

// A Set the SDK aborts mid-upload must store nothing and leave no server
// handler blocked in Recv (G1, checked here against the real server).
func TestSDKAbortedSetLeavesNothing(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	client := runesdk.NewClient(conn)

	t.Run("reader error", func(t *testing.T) {
		r := io.MultiReader(strings.NewReader("partial"), errReader{})
		require.Error(t, client.Set(context.Background(), "reader-err", r, nil))
		assertAborted(t, client, "reader-err")
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() { done <- client.Set(ctx, "canceled", pr, nil) }()
		_, err := pw.Write([]byte("partial"))
		require.NoError(t, err)
		require.Eventually(t, func() bool { return runningSetHandlers() > 0 }, 5*time.Second, 10*time.Millisecond)
		cancel()
		require.Eventually(t, func() bool { return runningSetHandlers() == 0 }, 5*time.Second, 10*time.Millisecond,
			"server handler must exit on cancel while the SDK is still blocked in Read")
		_ = pw.Close() // unblock the SDK's Read
		require.Error(t, <-done)
		assertAborted(t, client, "canceled")
	})
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("upstream broke") }

func assertAborted(t *testing.T, client *runesdk.Client, key string) {
	t.Helper()
	time.Sleep(500 * time.Millisecond) // give a leaked handler time to show up, so 0 is meaningful
	assert.Equal(t, 0, runningSetHandlers(), "leaked Set handler")
	_, err := client.Get(context.Background(), key)
	assert.ErrorIs(t, err, runesdk.ErrNotFound)
}
