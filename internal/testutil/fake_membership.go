package testutil

import (
	"github.com/bryandguy/rune/internal/cluster"
	"github.com/bryandguy/rune/internal/router"
)

type fakeMembership struct {
	ring   *router.Router
	nodeID string
}

func (f *fakeMembership) Ring() *router.Router { return f.ring }
func (f *fakeMembership) NodeID() string       { return f.nodeID }
func (f *fakeMembership) Stop()                {}

// newFakeMembership builds a MembershipIface where selfID is NOT in the ring,
// so every key lookup returns peerID → peerAddr, causing all requests to forward.
func newFakeMembership(selfID, peerID, peerAddr string) cluster.MembershipIface {
	r := router.New()
	r.Add(router.Node{ID: peerID, Addr: peerAddr})
	return &fakeMembership{nodeID: selfID, ring: r}
}
