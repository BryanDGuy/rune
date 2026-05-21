package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bryandguy/rune/internal/router"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	nodePrefix      = "/rune/nodes/"
	leaseTTLSeconds = 10
)

// NodeInfo is the value stored in etcd for each registered node.
type NodeInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// memberStore is the subset of clientv3.Client methods used by Membership.
// *clientv3.Client satisfies this interface; tests use a fake.
type memberStore interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
}

// MembershipIface is satisfied by *Membership and by test fakes.
type MembershipIface interface {
	Ring() *router.Router
	NodeID() string
	Stop()
}

// Membership watches etcd for node join/leave events and keeps a router.Router current.
// If nodeAddr is non-empty, it also registers this node in etcd (server mode).
// If nodeAddr is empty, it only watches (SDK / client mode).
type Membership struct {
	store    memberStore
	ring     *router.Router
	cancel   context.CancelFunc
	nodeID   string
	nodeAddr string
	leaseID  clientv3.LeaseID
	wg       sync.WaitGroup
}

func New(store memberStore, nodeID, nodeAddr string) *Membership {
	return &Membership{
		store:    store,
		ring:     router.New(),
		nodeID:   nodeID,
		nodeAddr: nodeAddr,
	}
}

// Start begins membership. It registers this node (if nodeAddr != ""), populates the
// ring from the current etcd state, then watches for changes in the background.
func (m *Membership) Start(ctx context.Context) error {
	ctx, m.cancel = context.WithCancel(ctx)

	if m.nodeAddr != "" {
		if err := m.register(ctx); err != nil {
			m.cancel()
			return fmt.Errorf("register: %w", err)
		}
	}

	if err := m.populate(ctx); err != nil {
		m.cancel()
		m.wg.Wait()
		return fmt.Errorf("populate ring: %w", err)
	}

	m.wg.Go(func() { m.watchLoop(ctx) })
	return nil
}

func (m *Membership) register(ctx context.Context) error {
	resp, err := m.store.Grant(ctx, leaseTTLSeconds)
	if err != nil {
		return err
	}
	m.leaseID = resp.ID

	val, err := json.Marshal(NodeInfo{ID: m.nodeID, Addr: m.nodeAddr})
	if err != nil {
		return err
	}
	if _, err = m.store.Put(ctx, nodePrefix+m.nodeID, string(val), clientv3.WithLease(m.leaseID)); err != nil {
		return err
	}

	kaCh, err := m.store.KeepAlive(ctx, m.leaseID)
	if err != nil {
		return err
	}
	m.wg.Go(func() {
		for range kaCh { //nolint:revive // drain to prevent the etcd keepalive sender from blocking
		}
	})
	return nil
}

func (m *Membership) populate(ctx context.Context) error {
	resp, err := m.store.Get(ctx, nodePrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		m.applyPut(kv.Value)
	}
	return nil
}

func (m *Membership) watchLoop(ctx context.Context) {
	for {
		wch := m.store.Watch(ctx, nodePrefix, clientv3.WithPrefix())
		for wresp := range wch {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					m.applyPut(ev.Kv.Value)
				case mvccpb.DELETE:
					nodeID := strings.TrimPrefix(string(ev.Kv.Key), nodePrefix)
					m.ring.Remove(nodeID)
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		// Watch channel closed unexpectedly — resync ring state before reconnecting.
		_ = m.populate(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (m *Membership) applyPut(val []byte) {
	var info NodeInfo
	if err := json.Unmarshal(val, &info); err == nil {
		m.ring.Add(router.Node{ID: info.ID, Addr: info.Addr})
	}
}

// Ring returns the current consistent hash ring. Safe for concurrent use.
func (m *Membership) Ring() *router.Router {
	return m.ring
}

func (m *Membership) NodeID() string {
	return m.nodeID
}

func (m *Membership) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	if m.leaseID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = m.store.Revoke(ctx, m.leaseID)
	}
}
