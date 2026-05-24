package router

import (
	"errors"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
)

// NodeKeyPrefix is the etcd key prefix under which nodes register themselves.
const NodeKeyPrefix = "/rune/nodes/"

var ErrNoNodes = errors.New("router: no nodes in ring")

// Node is a ring member. ID is used for placement; Addr is used for dialing.
// Keeping them separate means address changes don't shift ring position.
// The JSON tags define the etcd wire format; both server and SDK must agree on them.
type Node struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

func (n Node) String() string { return n.ID }

type xxHasher struct{}

func (h xxHasher) Sum64(data []byte) uint64 { return xxhash.Sum64(data) }

type Router struct {
	ring  *consistent.Consistent
	nodes map[string]Node
	mu    sync.RWMutex
}

// New creates a Router. Load is set high to disable bounded-load redistribution,
// preserving standard consistent hash stability: only keys owned by a removed node remap.
func New() *Router {
	cfg := consistent.Config{
		PartitionCount:    271, // prime; distributes partitions evenly across the ring
		ReplicationFactor: 20,
		Load:              10.0,
		Hasher:            xxHasher{},
	}
	return &Router{
		ring:  consistent.New(nil, cfg),
		nodes: make(map[string]Node),
	}
}

func (r *Router) Add(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ring.Add(node)
	r.nodes[node.ID] = node
}

func (r *Router) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ring.Remove(id)
	delete(r.nodes, id)
}

func (r *Router) Lookup(key string) (Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return Node{}, ErrNoNodes
	}
	m := r.ring.LocateKey([]byte(key))
	return r.nodes[m.String()], nil
}

func (r *Router) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
