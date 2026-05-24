package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bryandguy/rune/shared/logging"
	"github.com/bryandguy/rune/shared/router"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	nodePrefix        = "/rune/nodes/"
	leaseTTLSeconds   = 10
	reregisterBackoff = time.Second
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
	logger   *logging.Logger
	nodeID   string
	nodeAddr string
	leaseID  clientv3.LeaseID
	wg       sync.WaitGroup
}

// New builds a Membership. A nil logger discards all log output.
func New(store memberStore, nodeID, nodeAddr string, logger *logging.Logger) *Membership {
	if logger == nil {
		logger = logging.Discard()
	}
	return &Membership{
		store:    store,
		ring:     router.New(),
		logger:   logger,
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
	if err := m.grantAndPut(ctx); err != nil {
		return err
	}
	m.logger.Info("registered in cluster", "addr", m.nodeAddr, "lease", int64(m.leaseID))
	m.wg.Go(func() { m.keepAliveLoop(ctx) })
	return nil
}

func (m *Membership) grantAndPut(ctx context.Context) error {
	resp, err := m.store.Grant(ctx, leaseTTLSeconds)
	if err != nil {
		return err
	}
	m.leaseID = resp.ID

	val, err := json.Marshal(NodeInfo{ID: m.nodeID, Addr: m.nodeAddr})
	if err != nil {
		return err
	}
	_, err = m.store.Put(ctx, nodePrefix+m.nodeID, string(val), clientv3.WithLease(m.leaseID))
	return err
}

// keepAliveLoop streams lease renewals from etcd. The renewal responses carry no
// information we act on, but the channel closing while ctx is still live means the
// lease lapsed (etcd unreachable past the TTL) and the node has dropped out of the ring.
func (m *Membership) keepAliveLoop(ctx context.Context) {
	for {
		kaCh, err := m.store.KeepAlive(ctx, m.leaseID)
		if err == nil {
			for {
				if _, ok := <-kaCh; !ok {
					break
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		m.logger.Warn("lease lost, re-registering", "lease", int64(m.leaseID))
		m.reregister(ctx)
	}
}

func (m *Membership) reregister(ctx context.Context) {
	for ctx.Err() == nil {
		if err := m.grantAndPut(ctx); err == nil {
			m.logger.Info("re-registered in cluster", "lease", int64(m.leaseID))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reregisterBackoff):
		}
	}
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
					if id, ok := m.applyPut(ev.Kv.Value); ok && id != m.nodeID {
						m.logger.Info("peer joined", "peer_id", id)
					}
				case mvccpb.DELETE:
					nodeID := strings.TrimPrefix(string(ev.Kv.Key), nodePrefix)
					m.ring.Remove(nodeID)
					if nodeID != m.nodeID {
						m.logger.Info("peer left", "peer_id", nodeID)
					}
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		// An unexpected watch drop (ctx still live) may have skipped events, leaving the ring stale.
		m.logger.Warn("watch dropped, resyncing ring")
		_ = m.populate(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (m *Membership) applyPut(val []byte) (nodeID string, ok bool) {
	var info NodeInfo
	if err := json.Unmarshal(val, &info); err != nil {
		return "", false
	}
	m.ring.Add(router.Node{ID: info.ID, Addr: info.Addr})
	return info.ID, true
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
