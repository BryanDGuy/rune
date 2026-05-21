package router

import (
	"errors"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
)

var ErrNoNodes = errors.New("router: no nodes in ring")

// Node is a ring member. ID is used for placement; Addr is used for dialing.
// Keeping them separate means address changes don't shift ring position.
type Node struct {
	ID   string
	Addr string
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

// LookupN returns up to n nodes in ring order (primary first). Returns fewer
// than n without error if the ring has fewer members. Returns ErrNoNodes if empty.
func (r *Router) LookupN(key string, n int) ([]Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil, ErrNoNodes
	}

	count := min(n, len(r.nodes))

	// ErrInsufficientMemberCount from the library is unreachable here:
	// count was capped to len(r.nodes) which is kept in sync with the
	// library's member map under the same write lock.
	members, err := r.ring.GetClosestN([]byte(key), count)
	if err != nil {
		return nil, err
	}

	result := make([]Node, len(members))
	for i, m := range members {
		result[i] = r.nodes[m.String()]
	}
	return result, nil
}

func (r *Router) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
