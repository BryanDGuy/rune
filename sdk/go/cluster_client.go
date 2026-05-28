package runesdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	runev1 "github.com/bryandguy/rune/sdk/go/internal/gen/rune/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var errClusterClientClosed = errors.New("cluster: ClusterClient is closed")

// ClusterOptions configures a ClusterClient. A nil pointer uses production defaults.
type ClusterOptions struct {
	// Dial opens a connection to a node address not yet known to the client.
	// Required — NewClusterClient returns an error if nil.
	Dial func(addr string) (*Client, error)
}

// ClusterClient routes Get/Set to Rune nodes using the x-rune-owner hint returned
// in every response header. The first request for a key takes at most one
// server-side forwarding hop; the hint is then cached so subsequent requests go
// directly to the owning node. If a request fails (e.g. the owning node left the
// cluster), the cached hint is evicted so the next attempt re-routes correctly.
type ClusterClient struct {
	cache   map[string]string  // key → owner addr (populated from x-rune-owner)
	clients map[string]*Client // addr → open connection (lazy-dialed)
	dialFn  func(addr string) (*Client, error)
	addrs   []string // initial addresses for uncached keys (round-robin)
	mu      sync.RWMutex
	nextIdx atomic.Uint64
	closed  bool
}

// NewClusterClient creates a ClusterClient that distributes initial requests across
// addrs and caches owner hints for subsequent requests. At least one address is
// required. opts.Dial must be set — it is called whenever a connection to a new
// node address is needed.
func NewClusterClient(addrs []string, opts *ClusterOptions) (*ClusterClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("cluster: at least one node address required")
	}
	if opts == nil || opts.Dial == nil {
		return nil, errors.New("cluster: opts.Dial is required")
	}
	return &ClusterClient{
		addrs:   addrs,
		cache:   make(map[string]string),
		clients: make(map[string]*Client),
		dialFn:  opts.Dial,
	}, nil
}

// pickAddr returns the cached owner for key, or round-robins across addrs.
// Must be called with mu held (any level).
func (c *ClusterClient) pickAddr(key string) string {
	if addr, ok := c.cache[key]; ok {
		return addr
	}
	idx := c.nextIdx.Add(1) - 1
	return c.addrs[idx%uint64(len(c.addrs))]
}

// connFor returns (or lazy-dials) the Client for addr.
// The caller must not hold mu when calling this.
func (c *ClusterClient) connFor(addr string) (*Client, error) {
	c.mu.RLock()
	closed := c.closed
	client, ok := c.clients[addr]
	c.mu.RUnlock()

	if closed {
		return nil, errClusterClientClosed
	}
	if ok {
		return client, nil
	}

	// Slow path: acquire write lock to dial.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterClientClosed
	}
	if client, ok = c.clients[addr]; ok {
		return client, nil
	}
	client, err := c.dialFn(addr)
	if err != nil {
		return nil, err
	}
	c.clients[addr] = client
	return client, nil
}

// clientFor returns the Client that should handle key.
func (c *ClusterClient) clientFor(key string) (*Client, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, errClusterClientClosed
	}
	addr := c.pickAddr(key)
	client, ok := c.clients[addr]
	c.mu.RUnlock()
	if ok {
		return client, nil
	}
	return c.connFor(addr)
}

// cacheHint stores the x-rune-owner address from md under key.
func (c *ClusterClient) cacheHint(key string, md metadata.MD) {
	vals := md["x-rune-owner"]
	if len(vals) == 0 || vals[0] == "" {
		return
	}
	c.mu.Lock()
	if !c.closed {
		c.cache[key] = vals[0]
	}
	c.mu.Unlock()
}

// evictHint removes the cached owner for key. Called when an RPC to the cached
// node fails so the next request re-routes to a live node.
func (c *ClusterClient) evictHint(key string) {
	c.mu.Lock()
	if !c.closed {
		delete(c.cache, key)
	}
	c.mu.Unlock()
}

func (c *ClusterClient) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	client, err := c.clientFor(key)
	if err != nil {
		return nil, err
	}
	rc, md, err := clusterGet(ctx, client, key)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			c.evictHint(key)
		}
		return nil, err
	}
	c.cacheHint(key, md)
	return rc, nil
}

func (c *ClusterClient) Set(ctx context.Context, key string, r io.Reader, opts *SetOptions) error {
	client, err := c.clientFor(key)
	if err != nil {
		return err
	}
	md, err := clusterSet(ctx, client, key, r, opts)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			c.evictHint(key)
		}
		return err
	}
	c.cacheHint(key, md)
	return nil
}

// Delete removes keys from the cluster. Keys with a cached owner are routed
// directly; keys without a cached owner are sent to an arbitrary node (which only
// succeeds if that node owns the key). Perform a Get or Set before Delete for keys
// that have not been previously accessed to ensure correct routing.
func (c *ClusterClient) Delete(ctx context.Context, keys ...string) error {
	byAddr := make(map[string][]string)
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return errClusterClientClosed
	}
	for _, key := range keys {
		addr := c.pickAddr(key)
		byAddr[addr] = append(byAddr[addr], key)
	}
	c.mu.RUnlock()

	var firstErr error
	for addr, addrKeys := range byAddr {
		client, err := c.connFor(addr)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			for _, key := range addrKeys {
				c.evictHint(key)
			}
			continue
		}
		if err := client.Delete(ctx, addrKeys...); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			for _, key := range addrKeys {
				c.evictHint(key)
			}
		}
	}
	return firstErr
}

func (c *ClusterClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	for _, client := range c.clients {
		_ = client.Close()
	}
	c.clients = nil
	c.cache = nil
	return nil
}

// clusterGet streams a Get RPC and returns the x-rune-owner header for routing.
func clusterGet(ctx context.Context, c *Client, key string) (io.ReadCloser, metadata.MD, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := c.grpc.Get(ctx, &runev1.GetRequest{Key: key})
	if err != nil {
		cancel()
		if status.Code(err) == codes.NotFound {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}

	// Block until the server sends its initial metadata frame so x-rune-owner
	// is available before we return the reader to the caller.
	md, _ := stream.Header()

	resp, err := stream.Recv()
	if err != nil {
		cancel()
		if errors.Is(err, io.EOF) {
			return io.NopCloser(bytes.NewReader(nil)), md, nil
		}
		if status.Code(err) == codes.NotFound {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}

	return &streamReader{stream: stream, buf: resp.Chunk, cancel: cancel}, md, nil
}

// clusterSet streams a Set RPC and returns the x-rune-owner header for routing.
func clusterSet(ctx context.Context, c *Client, key string, r io.Reader, opts *SetOptions) (metadata.MD, error) {
	var ttl time.Duration
	var callOpts []grpc.CallOption
	if opts != nil {
		ttl = opts.TTL
		if opts.Compress {
			callOpts = append(callOpts, grpc.UseCompressor("gzip"))
		}
	}

	stream, err := c.grpc.Set(ctx, callOpts...)
	if err != nil {
		return nil, err
	}

	if err = stream.Send(&runev1.SetRequest{
		Payload: &runev1.SetRequest_Header{
			Header: &runev1.SetHeader{
				Key:        key,
				TtlSeconds: int64(ttl.Seconds()),
			},
		},
	}); err != nil {
		return nil, err
	}

	buf := make([]byte, chunkSize)
	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			if err = stream.Send(&runev1.SetRequest{
				Payload: &runev1.SetRequest_Chunk{Chunk: buf[:n]},
			}); err != nil {
				return nil, err
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}

	if _, err = stream.CloseAndRecv(); err != nil {
		return nil, err
	}
	md, _ := stream.Header()
	return md, nil
}
