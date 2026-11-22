package runesdk_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	runesdk "github.com/bryandguy/rune/sdk/go"
	"github.com/bryandguy/rune/sdk/go/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// cacheClient is the Set/Get surface shared by Client and ClusterClient.
type cacheClient interface {
	Set(ctx context.Context, key string, r io.Reader, opts *runesdk.SetOptions) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}

// eachClient returns a plain Client and a ClusterClient, each on its own server.
func eachClient(t *testing.T, opts *testutil.BufconnOptions) map[string]cacheClient {
	t.Helper()
	conn1, stop1 := testutil.NewBufconnConn(t, 1<<20, opts)
	t.Cleanup(stop1)
	conn2, stop2 := testutil.NewBufconnConn(t, 1<<20, opts)
	t.Cleanup(stop2)
	cc, err := runesdk.NewClusterClient([]string{conn2.Target()}, &runesdk.ClusterOptions{
		Dial: dialFrom(map[string]*runesdk.Client{conn2.Target(): runesdk.NewClient(conn2)}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return map[string]cacheClient{"Client": runesdk.NewClient(conn1), "ClusterClient": cc}
}

// A body cut short of its Content-Length returns io.ErrUnexpectedEOF from net/http.
// Set must fail and must not commit the partial value.
func TestSetTruncatedUpstreamIsNotCommitted(t *testing.T) {
	for name, c := range eachClient(t, nil) {
		t.Run(name, func(t *testing.T) {
			full := bytes.Repeat([]byte("x"), 3<<20)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", "3145728")
				_, _ = w.Write(full[:1500000])
				w.(http.Flusher).Flush()
				conn, _, _ := w.(http.Hijacker).Hijack()
				_ = conn.Close()
			}))
			defer up.Close()

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, up.URL, nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			ctx := context.Background()
			require.ErrorIs(t, c.Set(ctx, "k", resp.Body, nil), io.ErrUnexpectedEOF)
			_, err = c.Get(ctx, "k")
			require.ErrorIs(t, err, runesdk.ErrNotFound)
		})
	}
}

type failingReader struct{ sent bool }

func (r *failingReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(p, "partial"), nil
	}
	return 0, errors.New("upstream broke")
}

func blockedSetHandlers() int {
	buf := make([]byte, 1<<20)
	return strings.Count(string(buf[:runtime.Stack(buf, true)]), "(*memServer).Set(")
}

// A reader error must abort the stream so the server handler is released.
func TestSetReaderErrorReleasesServerHandler(t *testing.T) {
	for name, c := range eachClient(t, nil) {
		t.Run(name, func(t *testing.T) {
			require.Error(t, c.Set(context.Background(), "k", &failingReader{}, nil))
			time.Sleep(time.Second) // give a leaked handler time to start, so 0 is meaningful
			assert.Equal(t, 0, blockedSetHandlers())
		})
	}
}

func TestSetSubSecondTTLExpires(t *testing.T) {
	for name, c := range eachClient(t, nil) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			require.NoError(t, c.Set(ctx, "k", strings.NewReader("v"), &runesdk.SetOptions{TTL: 500 * time.Millisecond}))
			time.Sleep(2 * time.Second)
			_, err := c.Get(ctx, "k")
			require.ErrorIs(t, err, runesdk.ErrNotFound)
		})
	}
}

func TestSetNegativeTTLRejected(t *testing.T) {
	for name, c := range eachClient(t, nil) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			require.Error(t, c.Set(ctx, "k", strings.NewReader("v"), &runesdk.SetOptions{TTL: -time.Second}))
			_, err := c.Get(ctx, "k")
			require.ErrorIs(t, err, runesdk.ErrNotFound)
		})
	}
}

// When the server rejects a Set mid-stream, the caller must see the server's status, not a bare EOF.
func TestSetReturnsServerStatusOnMidStreamReject(t *testing.T) {
	for name, c := range eachClient(t, &testutil.BufconnOptions{MaxValueBytes: 1}) {
		t.Run(name, func(t *testing.T) {
			err := c.Set(context.Background(), "k", io.LimitReader(zeroReader{}, 64<<20), nil)
			require.Equal(t, codes.ResourceExhausted, status.Code(err), "got %v", err)
		})
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }
