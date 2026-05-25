package cluster

import (
	"context"
	"errors"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var errDialerClosed = errors.New("cluster: PeerDialer is closed")

// DialOptions configures a Dial call. A nil pointer means production defaults.
type DialOptions struct {
	ContextDialer func(context.Context, string) (net.Conn, error)
}

// PeerDialer maintains a pool of gRPC connections to peer Rune nodes.
type PeerDialer struct {
	conns    map[string]*grpc.ClientConn
	injected map[string]bool // true = externally owned, must not be closed by Close
	mu       sync.RWMutex
	closed   bool
}

func NewPeerDialer() *PeerDialer {
	return &PeerDialer{conns: make(map[string]*grpc.ClientConn), injected: make(map[string]bool)}
}

func (d *PeerDialer) Dial(addr string, opts *DialOptions) (*grpc.ClientConn, error) {
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

	grpcOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if opts != nil && opts.ContextDialer != nil {
		grpcOpts = append(grpcOpts, grpc.WithContextDialer(opts.ContextDialer))
	}
	conn, err := grpc.NewClient(addr, grpcOpts...)
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
	d.injected[addr] = true
	return nil
}

func (d *PeerDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for addr, conn := range d.conns {
		if !d.injected[addr] {
			_ = conn.Close()
		}
	}
	d.conns = nil
	d.injected = nil
}
