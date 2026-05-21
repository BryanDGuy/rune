package cluster

import (
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var errDialerClosed = errors.New("cluster: PeerDialer is closed")

// PeerDialer maintains a pool of gRPC connections to peer Rune nodes.
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

	opts := make([]grpc.DialOption, 0, 1+len(extraOpts))
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, extraOpts...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	d.conns[addr] = conn
	return conn, nil
}

// DialWith pre-populates the connection cache for addr without dialing.
// Use this to inject test connections or pre-built clients.
func (d *PeerDialer) DialWith(addr string, conn *grpc.ClientConn) error {
	if conn == nil {
		return errors.New("cluster: DialWith: conn must not be nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return errDialerClosed
	}
	d.conns[addr] = conn
	return nil
}

// Close closes all pooled connections.
func (d *PeerDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for _, conn := range d.conns {
		_ = conn.Close()
	}
	d.conns = nil
}
