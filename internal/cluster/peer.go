package cluster

import (
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var errDialerClosed = errors.New("cluster: PeerDialer is closed")

// PeerDialer maintains a pool of gRPC connections to peer Rune nodes.
// Connections are created lazily on first Dial and reused on subsequent calls.
type PeerDialer struct {
	conns  map[string]*grpc.ClientConn
	mu     sync.RWMutex
	closed bool
}

func NewPeerDialer() *PeerDialer {
	return &PeerDialer{conns: make(map[string]*grpc.ClientConn)}
}

// Dial returns a cached connection to addr, or dials a new one.
// Extra opts are passed to grpc.NewClient only on the first dial; use for testing overrides.
func (d *PeerDialer) Dial(addr string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return nil, errDialerClosed
	}
	if conn, ok := d.conns[addr]; ok {
		d.mu.RUnlock()
		return conn, nil
	}
	d.mu.RUnlock()

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, errDialerClosed
	}
	if conn, ok := d.conns[addr]; ok {
		return conn, nil
	}

	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, extraOpts...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	d.conns[addr] = conn
	return conn, nil
}

// Close closes all pooled connections. Subsequent Dial calls return an error.
func (d *PeerDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for _, conn := range d.conns {
		_ = conn.Close()
	}
	d.conns = nil
}
