package runesdk

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/bryandguy/rune/shared/router"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcdReader is the subset of clientv3.Client used by discovery.
type etcdReader interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

// discovery watches etcd for node join/leave events and keeps a *router.Router
// current. It only observes — it never registers the client as a node.
type discovery struct {
	store  etcdReader
	ring   *router.Router
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newDiscovery(store etcdReader) *discovery {
	return &discovery{store: store, ring: router.New()}
}

func (d *discovery) start(ctx context.Context) error {
	ctx, d.cancel = context.WithCancel(ctx)
	if err := d.populate(ctx); err != nil {
		d.cancel()
		return err
	}
	d.wg.Go(func() { d.watchLoop(ctx) })
	return nil
}

func (d *discovery) stop() {
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
}

func (d *discovery) populate(ctx context.Context) error {
	resp, err := d.store.Get(ctx, router.NodeKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		d.applyPut(kv.Value)
	}
	return nil
}

func (d *discovery) watchLoop(ctx context.Context) {
	for {
		wch := d.store.Watch(ctx, router.NodeKeyPrefix, clientv3.WithPrefix())
		for wresp := range wch {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					d.applyPut(ev.Kv.Value)
				case mvccpb.DELETE:
					nodeID := strings.TrimPrefix(string(ev.Kv.Key), router.NodeKeyPrefix)
					d.ring.Remove(nodeID)
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		// Watch dropped while ctx is still live — ring may be stale; resync.
		_ = d.populate(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (d *discovery) applyPut(val []byte) {
	var node router.Node
	if err := json.Unmarshal(val, &node); err != nil {
		return
	}
	d.ring.Add(node)
}
