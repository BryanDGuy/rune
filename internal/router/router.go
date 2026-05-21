package router

import (
	"errors"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
)

// ErrNoNodes is returned by Lookup and LookupN when the ring has no members.
var ErrNoNodes = errors.New("router: no nodes in ring")

// Node is a member of the consistent hash ring.
type Node struct {
	ID   string // stable identifier used for ring placement
	Addr string // host:port used for dialing
}

// String implements consistent.Member. The ring uses ID for placement.
func (n Node) String() string { return n.ID }

type xxHasher struct{}

func (h xxHasher) Sum64(data []byte) uint64 { return xxhash.Sum64(data) }

// Router maps keys to owning nodes using consistent hashing with virtual nodes.
// It is safe for concurrent use.
type Router struct {
	mu    sync.RWMutex
	ring  *consistent.Consistent
	nodes map[string]Node // ID → Node, for Addr lookup after ring resolution
}

// New creates a Router.
// It uses a high Load to disable bounded-load redistribution; this preserves
// standard consistent hash stability (only keys owned by a removed node remap).
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

// Add adds a node to the ring. Safe to call concurrently.
func (r *Router) Add(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ring.Add(node)
	r.nodes[node.ID] = node
}

// Remove removes a node by ID from the ring. Safe to call concurrently.
func (r *Router) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ring.Remove(id)
	delete(r.nodes, id)
}

// Lookup returns the primary owning node for key.
// Returns ErrNoNodes if the ring is empty.
func (r *Router) Lookup(key string) (Node, error) {
	nodes, err := r.LookupN(key, 1)
	if err != nil {
		return Node{}, err
	}
	return nodes[0], nil
}

// LookupN returns up to n nodes for key in ring order — primary first, then replicas.
// If fewer than n nodes exist, returns however many are available (no error).
// Returns ErrNoNodes if the ring is empty.
func (r *Router) LookupN(key string, n int) ([]Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil, ErrNoNodes
	}

	count := n
	if count > len(r.nodes) {
		count = len(r.nodes)
	}

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

// Len returns the number of nodes currently in the ring.
func (r *Router) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
