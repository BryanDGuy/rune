package runesdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var errClusterClientClosed = errors.New("cluster: ClusterClient is closed")

// ClusterClient routes Get/Set to Rune nodes using the x-rune-owner hint returned
// in every response header. The first request for a key takes at most one
// server-side forwarding hop; the hint is then cached so subsequent requests go
// directly to the owning node.
type ClusterClient struct {
	cache   map[string]string  // key → owner addr (populated from x-rune-owner)
	clients map[string]*Client // addr → open connection (lazy-dialed)
	dialFn  func(addr string) (*Client, error)
	addrs   []string // initial addresses for uncached keys (round-robin)
	mu      sync.RWMutex
	nextIdx atomic.Uint64
	closed  bool
}

func defaultDial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return NewClient(conn), nil
}

// NewClusterClient creates a ClusterClient that distributes initial requests
// across addrs and caches owner hints for subsequent requests. At least one
// address is required.
func NewClusterClient(addrs ...string) (*ClusterClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("cluster: at least one node address required")
	}
	return &ClusterClient{
		addrs:   addrs,
		cache:   make(map[string]string),
		clients: make(map[string]*Client),
		dialFn:  defaultDial,
	}, nil
}

// NewClusterClientFromClients creates a ClusterClient with pre-dialed connections.
// addrs controls which addresses receive uncached-key requests; clients holds all
// connections the client may use, including those learned from x-rune-owner hints.
// Intended for testing.
func NewClusterClientFromClients(addrs []string, clients map[string]*Client) (*ClusterClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("cluster: at least one node address required")
	}
	if clients == nil {
		clients = make(map[string]*Client)
	}
	return &ClusterClient{
		addrs:   addrs,
		cache:   make(map[string]string),
		clients: clients,
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
	if c.dialFn == nil {
		return nil, fmt.Errorf("cluster: no pre-dialed client for %s", addr)
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
	c.cache[key] = vals[0]
	c.mu.Unlock()
}

func (c *ClusterClient) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	client, err := c.clientFor(key)
	if err != nil {
		return nil, err
	}
	var md metadata.MD
	rc, err := client.getWithHint(ctx, key, &md)
	if err != nil {
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
	return client.Set(ctx, key, r, opts)
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

	for addr, addrKeys := range byAddr {
		client, err := c.connFor(addr)
		if err != nil {
			return err
		}
		if err := client.Delete(ctx, addrKeys...); err != nil {
			return err
		}
	}
	return nil
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
	return nil
}
