package runesdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/bryandguy/rune/internal/cluster"
	"github.com/bryandguy/rune/internal/router"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrNoNodes             = router.ErrNoNodes
	errClusterClientClosed = errors.New("cluster: ClusterClient is closed")
)

// ClusterClient routes Get/Set to the correct Rune node using a local ring copy.
type ClusterClient struct {
	membership cluster.MembershipIface
	ring       *router.Router
	clients    map[string]*Client
	dialFn     func(addr string) (*Client, error) // nil in static-ring (test) mode
	mu         sync.RWMutex
	closed     bool
}

func defaultDial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return NewClient(conn), nil
}

// NewClusterClient creates a ClusterClient backed by a live etcd watch.
// Pass nodeID="" for pure clients (no self-registration).
func NewClusterClient(etcdClient *clientv3.Client, nodeID string) (*ClusterClient, error) {
	m := cluster.New(etcdClient, nodeID, "", nil)
	if err := m.Start(context.Background()); err != nil {
		return nil, fmt.Errorf("start membership: %w", err)
	}
	return &ClusterClient{
		membership: m,
		ring:       m.Ring(),
		clients:    make(map[string]*Client),
		dialFn:     defaultDial,
	}, nil
}

// NewClusterClientFromRingAndClients creates a ClusterClient with a static ring.
// Intended for testing.
func NewClusterClientFromRingAndClients(ring *router.Router, clients map[string]*Client) *ClusterClient {
	c := &ClusterClient{
		ring:    ring,
		clients: clients,
	}
	if c.clients == nil {
		c.clients = make(map[string]*Client)
	}
	return c
}

func (c *ClusterClient) clientFor(key string) (*Client, error) {
	node, err := c.ring.Lookup(key)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, errClusterClientClosed
	}
	client, ok := c.clients[node.Addr]
	c.mu.RUnlock()
	if ok {
		return client, nil
	}

	if c.dialFn == nil {
		return nil, fmt.Errorf("cluster: no client for node %s (%s)", node.ID, node.Addr)
	}

	// Re-check closed under write lock — Close() may have raced between the read above and here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterClientClosed
	}
	if client, ok = c.clients[node.Addr]; ok {
		return client, nil
	}
	client, err = c.dialFn(node.Addr)
	if err != nil {
		return nil, err
	}
	c.clients[node.Addr] = client
	return client, nil
}

func (c *ClusterClient) Set(ctx context.Context, key string, r io.Reader, opts *SetOptions) error {
	client, err := c.clientFor(key)
	if err != nil {
		return err
	}
	return client.Set(ctx, key, r, opts)
}

func (c *ClusterClient) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	client, err := c.clientFor(key)
	if err != nil {
		return nil, err
	}
	return client.Get(ctx, key)
}

func (c *ClusterClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.membership != nil {
		c.membership.Stop()
	}
	for _, client := range c.clients {
		_ = client.Close()
	}
	c.clients = nil
	return nil
}
