package cluster

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// fakeStore is an in-memory implementation of memberStore for unit tests.
type fakeStore struct {
	kvs    map[string]string
	watchC chan clientv3.WatchResponse
	leases map[clientv3.LeaseID]bool
	kaCh   chan *clientv3.LeaseKeepAliveResponse
	mu     sync.Mutex
	nextID clientv3.LeaseID
	grants int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		kvs:    make(map[string]string),
		watchC: make(chan clientv3.WatchResponse, 16),
		leases: make(map[clientv3.LeaseID]bool),
		nextID: 1,
	}
}

func (f *fakeStore) Put(_ context.Context, key, val string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kvs[key] = val
	return &clientv3.PutResponse{}, nil
}

func (f *fakeStore) Get(_ context.Context, key string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &clientv3.GetResponse{}
	for k, v := range f.kvs {
		if strings.HasPrefix(k, key) {
			resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{Key: []byte(k), Value: []byte(v)})
		}
	}
	return resp, nil
}

func (f *fakeStore) Watch(ctx context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	ch := make(chan clientv3.WatchResponse, 16)
	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-f.watchC:
				if !ok {
					return
				}
				ch <- resp
			}
		}
	}()
	return ch
}

func (f *fakeStore) Grant(_ context.Context, _ int64) (*clientv3.LeaseGrantResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	f.grants++
	f.leases[id] = true
	return &clientv3.LeaseGrantResponse{ID: id}, nil
}

func (f *fakeStore) KeepAlive(ctx context.Context, _ clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch := make(chan *clientv3.LeaseKeepAliveResponse)
	f.mu.Lock()
	f.kaCh = ch
	f.mu.Unlock()
	go func() {
		<-ctx.Done()
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.kaCh == ch {
			close(ch)
			f.kaCh = nil
		}
	}()
	return ch, nil
}

func (f *fakeStore) Revoke(_ context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.leases, id)
	return &clientv3.LeaseRevokeResponse{}, nil
}

func (f *fakeStore) grantCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.grants
}

func (f *fakeStore) hasKeepAlive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.kaCh != nil
}

// simulateLeaseLoss closes the live keepalive channel without canceling ctx,
// mimicking etcd dropping the lease while the node keeps running.
func (f *fakeStore) simulateLeaseLoss() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.kaCh != nil {
		close(f.kaCh)
		f.kaCh = nil
	}
}

func (f *fakeStore) simulateNodeLeave(nodeID string) {
	key := nodePrefix + nodeID
	f.mu.Lock()
	delete(f.kvs, key)
	f.mu.Unlock()
	f.watchC <- clientv3.WatchResponse{Events: []*clientv3.Event{{
		Type: clientv3.EventTypeDelete,
		Kv:   &mvccpb.KeyValue{Key: []byte(key)},
	}}}
}

func TestMembershipRegistersOnStart(t *testing.T) {
	store := newFakeStore()
	m := New(store, "node-1", "host1:7946", nil)
	ctx := t.Context()

	require.NoError(t, m.Start(ctx))
	defer m.Stop()

	got, err := m.Ring().Lookup("any-key")
	require.NoError(t, err)
	assert.Equal(t, "node-1", got.ID)
}

func TestMembershipWatchOnlyMode(t *testing.T) {
	store := newFakeStore()
	// Pre-populate store with one node.
	info, _ := json.Marshal(NodeInfo{ID: "node-2", Addr: "host2:7946"})
	store.kvs[nodePrefix+"node-2"] = string(info)

	// nodeAddr="" means watch-only (SDK mode).
	m := New(store, "", "", nil)
	ctx := t.Context()

	require.NoError(t, m.Start(ctx))
	defer m.Stop()

	got, err := m.Ring().Lookup("any-key")
	require.NoError(t, err)
	assert.Equal(t, "node-2", got.ID)

	// No lease should have been granted (no self-registration).
	assert.Empty(t, store.leases)
}

func TestMembershipRingUpdatesOnNodeJoin(t *testing.T) {
	store := newFakeStore()
	m := New(store, "node-1", "host1:7946", nil)
	ctx := t.Context()

	require.NoError(t, m.Start(ctx))
	defer m.Stop()

	// Simulate node-2 joining.
	info, _ := json.Marshal(NodeInfo{ID: "node-2", Addr: "host2:7946"})
	store.watchC <- clientv3.WatchResponse{Events: []*clientv3.Event{{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte(nodePrefix + "node-2"), Value: info},
	}}}

	assert.Eventually(t, func() bool {
		return m.Ring().Len() == 2
	}, time.Second, 10*time.Millisecond)
}

func TestMembershipRingUpdatesOnNodeLeave(t *testing.T) {
	store := newFakeStore()
	info, _ := json.Marshal(NodeInfo{ID: "node-2", Addr: "host2:7946"})
	store.kvs[nodePrefix+"node-2"] = string(info)

	m := New(store, "node-1", "host1:7946", nil)
	ctx := t.Context()

	require.NoError(t, m.Start(ctx))
	defer m.Stop()

	require.Equal(t, 2, m.Ring().Len())

	store.simulateNodeLeave("node-2")

	assert.Eventually(t, func() bool {
		return m.Ring().Len() == 1
	}, time.Second, 10*time.Millisecond)

	got, err := m.Ring().Lookup("any-key")
	require.NoError(t, err)
	assert.Equal(t, "node-1", got.ID)
}

func TestMembershipReregistersOnLeaseLoss(t *testing.T) {
	store := newFakeStore()
	m := New(store, "node-1", "host1:7946", nil)
	ctx := t.Context()

	require.NoError(t, m.Start(ctx))
	defer m.Stop()

	require.Equal(t, 1, store.grantCount())
	require.Eventually(t, store.hasKeepAlive, time.Second, 5*time.Millisecond)

	store.simulateLeaseLoss()

	assert.Eventually(t, func() bool {
		return store.grantCount() >= 2
	}, time.Second, 10*time.Millisecond, "node should re-grant a lease after losing it")

	got, err := m.Ring().Lookup("any-key")
	require.NoError(t, err)
	assert.Equal(t, "node-1", got.ID)
}

func TestMembershipStopRevokesLease(t *testing.T) {
	store := newFakeStore()
	m := New(store, "node-1", "host1:7946", nil)
	ctx := t.Context()

	require.NoError(t, m.Start(ctx))
	assert.Len(t, store.leases, 1)

	m.Stop()
	assert.Empty(t, store.leases, "lease should be revoked on Stop")
}
