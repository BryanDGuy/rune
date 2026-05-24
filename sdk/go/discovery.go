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
	rev, err := d.populate(ctx)
	if err != nil {
		d.cancel()
		return err
	}
	d.wg.Go(func() { d.watchLoop(ctx, rev) })
	return nil
}

func (d *discovery) stop() {
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
}

func (d *discovery) populate(ctx context.Context) (int64, error) {
	resp, err := d.store.Get(ctx, router.NodeKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	nodes := make([]router.Node, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var node router.Node
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			continue
		}
		nodes = append(nodes, node)
	}
	d.ring.Reset(nodes)
	return resp.Header.GetRevision(), nil
}

func (d *discovery) watchLoop(ctx context.Context, rev int64) {
	for {
		opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(rev + 1)}
		wch := d.store.Watch(ctx, router.NodeKeyPrefix, opts...)
		for wresp := range wch {
			if wresp.Err() != nil {
				break
			}
			for _, ev := range wresp.Events {
				rev = ev.Kv.ModRevision
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
		// Watch dropped while ctx is still live — backoff then resync to recover any missed events.
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		if newRev, err := d.populate(ctx); err == nil {
			rev = newRev
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
