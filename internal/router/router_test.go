package router

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookupEmptyRing(t *testing.T) {
	r := New()
	_, err := r.Lookup("any-key")
	assert.ErrorIs(t, err, ErrNoNodes)
}

func TestLookupNEmptyRing(t *testing.T) {
	r := New()
	_, err := r.LookupN("any-key", 1)
	assert.ErrorIs(t, err, ErrNoNodes)
}

func TestLookupSingleNode(t *testing.T) {
	r := New()
	n := Node{ID: "node-1", Addr: "host1:7946"}
	r.Add(n)
	got, err := r.Lookup("any-key")
	require.NoError(t, err)
	assert.Equal(t, n, got)
}

func TestLookupNDistinctNodes(t *testing.T) {
	r := New()
	r.Add(Node{ID: "node-1", Addr: "host1:7946"})
	r.Add(Node{ID: "node-2", Addr: "host2:7946"})

	nodes, err := r.LookupN("my-key", 2)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	assert.NotEqual(t, nodes[0].ID, nodes[1].ID)
}

func TestLookupNPartialWhenFewer(t *testing.T) {
	r := New()
	r.Add(Node{ID: "node-1", Addr: "host1:7946"})

	nodes, err := r.LookupN("my-key", 3)
	require.NoError(t, err)
	assert.Len(t, nodes, 1)
}

func TestLen(t *testing.T) {
	r := New()
	assert.Equal(t, 0, r.Len())
	r.Add(Node{ID: "node-1", Addr: "host1:7946"})
	assert.Equal(t, 1, r.Len())
	r.Add(Node{ID: "node-2", Addr: "host2:7946"})
	assert.Equal(t, 2, r.Len())
	r.Remove("node-1")
	assert.Equal(t, 1, r.Len())
}

func TestRemoveExcludesNode(t *testing.T) {
	r := New()
	r.Add(Node{ID: "node-1", Addr: "host1:7946"})
	r.Add(Node{ID: "node-2", Addr: "host2:7946"})
	r.Remove("node-1")

	for i := 0; i < 100; i++ {
		got, err := r.Lookup(fmt.Sprintf("key-%d", i))
		require.NoError(t, err)
		assert.NotEqual(t, "node-1", got.ID, "removed node should never be returned")
	}
}

func TestStabilityOnNodeRemoval(t *testing.T) {
	r := New()
	for i := 1; i <= 5; i++ {
		r.Add(Node{ID: fmt.Sprintf("node-%d", i), Addr: fmt.Sprintf("host%d:7946", i)})
	}

	// Record initial key→node mapping for 1000 keys.
	before := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		n, err := r.Lookup(key)
		require.NoError(t, err)
		before[key] = n.ID
	}

	r.Remove("node-3")

	// Only keys previously owned by node-3 should remap.
	remapped := 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		n, err := r.Lookup(key)
		require.NoError(t, err)
		if before[key] != n.ID {
			remapped++
			assert.Equal(t, "node-3", before[key],
				"key %s remapped from %s to %s but node-3 was the only removal",
				key, before[key], n.ID)
		}
	}
	assert.Greater(t, remapped, 0, "some keys should remap after removing node-3")
}

func TestDistribution(t *testing.T) {
	r := New()
	r.Add(Node{ID: "node-1", Addr: "host1:7946"})
	r.Add(Node{ID: "node-2", Addr: "host2:7946"})
	r.Add(Node{ID: "node-3", Addr: "host3:7946"})

	counts := make(map[string]int)
	const total = 10000
	for i := 0; i < total; i++ {
		n, err := r.Lookup(fmt.Sprintf("key-%d", i))
		require.NoError(t, err)
		counts[n.ID]++
	}

	require.Len(t, counts, 3, "all 3 nodes should own at least one key")
	for id, count := range counts {
		pct := float64(count) / float64(total)
		assert.Greater(t, pct, 0.27, "node %s owns %.1f%% — less than 27%%", id, pct*100)
		assert.Less(t, pct, 0.40, "node %s owns %.1f%% — more than 40%%", id, pct*100)
	}
}

func TestAddrPreserved(t *testing.T) {
	r := New()
	n := Node{ID: "node-1", Addr: "rune-0.rune.svc.cluster.local:7946"}
	r.Add(n)
	got, err := r.Lookup("any-key")
	require.NoError(t, err)
	assert.Equal(t, n.Addr, got.Addr)
}
