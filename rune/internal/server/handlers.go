package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bryandguy/rune/rune/internal/storage"
	runev1 "github.com/bryandguy/rune/sdk/go/gen/rune/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	metaForwarded = "x-rune-forwarded"
	metaOwner     = "x-rune-owner"

	// peerTimeout bounds each Delete/Exists call to a peer, so a silently dead
	// peer can't hang the request until the TCP timeout.
	peerTimeout = 3 * time.Second
)

type handler struct {
	runev1.UnimplementedRuneServiceServer
	srv *Server
}

// owner reports the advertised address of the node that owns key (the client's
// routing hint) and whether that owner is a remote peer, in which case the
// request must be forwarded. addr is empty when ownership can't be resolved
// here: single-node mode, an already-forwarded request (we are not the
// client-facing node), or an empty ring.
func (h *handler) owner(ctx context.Context, key string) (addr string, remote bool) {
	if !h.srv.clusterMode() {
		return "", false
	}
	if md, found := metadata.FromIncomingContext(ctx); found && len(md[metaForwarded]) > 0 {
		return "", false
	}
	node, err := h.srv.membership.Ring().Lookup(key)
	if err != nil {
		return "", false
	}
	return node.Addr, node.ID != h.srv.membership.NodeID()
}

// setOwnerHint advertises the owning node's address so a direct gRPC client can
// cache key→node and route there itself, skipping the forwarding hop next time.
func setOwnerHint(stream grpc.ServerStream, addr string) {
	if addr != "" {
		_ = stream.SetHeader(metadata.Pairs(metaOwner, addr))
	}
}

func forwardCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metaForwarded, "1")
}

func (h *handler) Ping(_ context.Context, _ *runev1.PingRequest) (*runev1.PingResponse, error) {
	return &runev1.PingResponse{Message: "PONG"}, nil
}

func (h *handler) Get(req *runev1.GetRequest, stream grpc.ServerStreamingServer[runev1.GetResponse]) error {
	addr, remote := h.owner(stream.Context(), req.Key)
	setOwnerHint(stream, addr)
	if remote {
		h.srv.logger.Debug("forwarding get", "key", req.Key, "owner", addr)
		return h.forwardGet(stream.Context(), addr, req, stream)
	}
	var charged int64
	defer func() { h.srv.release(charged) }()
	value, err := h.srv.store.GetReserved(req.Key, func(n int64) bool {
		if !h.srv.reserve(n) {
			return false
		}
		charged = n
		return true
	})
	if errors.Is(err, storage.ErrNotFound) {
		return status.Error(codes.NotFound, "key not found")
	}
	if errors.Is(err, storage.ErrOverBudget) {
		return status.Error(codes.ResourceExhausted, "server memory budget exhausted")
	}
	if err != nil {
		return status.Errorf(codes.Internal, "get: %v", err)
	}

	// Send from a goroutine so a client that stops reading cannot hold the budget
	// forever: when no chunk goes out for getIdleTimeout the handler returns, gRPC
	// cancels the stream, and the blocked Send unblocks with an error.
	chunkSize := h.srv.cfg.StreamChunkSize
	done := make(chan error, 1)
	progress := make(chan struct{}, 1)
	go func() {
		for len(value) > 0 {
			n := min(chunkSize, len(value))
			if err := stream.Send(&runev1.GetResponse{Chunk: value[:n]}); err != nil {
				done <- err
				return
			}
			value = value[n:]
			select {
			case progress <- struct{}{}:
			default:
			}
		}
		done <- nil
	}()
	idle := time.NewTimer(h.srv.getIdleTimeout)
	defer idle.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-progress:
			idle.Reset(h.srv.getIdleTimeout)
		case <-idle.C:
			return status.Errorf(codes.DeadlineExceeded, "client read nothing for %s", h.srv.getIdleTimeout)
		}
	}
}

func (h *handler) peerClient(peerAddr string) (runev1.RuneServiceClient, error) {
	conn, err := h.srv.dialer.Dial(peerAddr, nil)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial peer: %v", err)
	}
	return runev1.NewRuneServiceClient(conn), nil
}

func (h *handler) forwardGet(ctx context.Context, peerAddr string, req *runev1.GetRequest, stream grpc.ServerStreamingServer[runev1.GetResponse]) error {
	client, err := h.peerClient(peerAddr)
	if err != nil {
		return err
	}
	peerStream, err := client.Get(forwardCtx(ctx), req)
	if err != nil {
		return status.Errorf(codes.Unavailable, "peer get: %v", err)
	}
	for {
		resp, err := peerStream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (h *handler) Set(stream grpc.ClientStreamingServer[runev1.SetRequest, runev1.SetResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Error(codes.InvalidArgument, "first message must contain SetHeader")
	}

	addr, remote := h.owner(stream.Context(), hdr.Key)
	setOwnerHint(stream, addr)
	if remote {
		h.srv.logger.Debug("forwarding set", "key", hdr.Key, "owner", addr)
		return h.forwardSet(stream.Context(), addr, hdr, stream)
	}

	key := hdr.Key
	ttl := hdr.TtlSeconds
	var value []byte
	var charged int64
	defer func() { h.srv.release(charged) }()
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		chunk, ok := msg.Payload.(*runev1.SetRequest_Chunk)
		if !ok || chunk == nil {
			return status.Error(codes.InvalidArgument, "expected chunk payload after header")
		}
		need := int64(len(value) + len(chunk.Chunk))
		if need > h.srv.maxValueBytes {
			return status.Errorf(codes.ResourceExhausted, "value exceeds max size of %d bytes", h.srv.maxValueBytes)
		}
		if need > int64(cap(value)) {
			// Grow by doubling (capped at the max) and charge the budget before allocating.
			// make, not append/slices.Grow, so the capacity is exactly what was charged.
			newCap := min(max(2*int64(cap(value)), need), h.srv.maxValueBytes)
			if !h.srv.reserve(newCap - charged) {
				return status.Error(codes.ResourceExhausted, "server memory budget exhausted")
			}
			charged = newCap
			grown := make([]byte, len(value), newCap)
			copy(grown, value)
			value = grown
		}
		value = append(value, chunk.Chunk...)
	}

	if err := h.srv.store.Set(key, value, ttl); err != nil {
		return status.Errorf(codes.Internal, "set: %v", err)
	}
	return stream.SendAndClose(&runev1.SetResponse{})
}

func (h *handler) forwardSet(ctx context.Context, peerAddr string, hdr *runev1.SetHeader, stream grpc.ClientStreamingServer[runev1.SetRequest, runev1.SetResponse]) error {
	client, err := h.peerClient(peerAddr)
	if err != nil {
		return err
	}
	peerStream, err := client.Set(forwardCtx(ctx))
	if err != nil {
		return status.Errorf(codes.Unavailable, "peer set: %v", err)
	}
	if err := peerStream.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: hdr}}); err != nil {
		return status.Errorf(codes.Unavailable, "peer set header: %v", err)
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := peerStream.Send(msg); err != nil {
			if errors.Is(err, io.EOF) {
				// The peer ended the stream; its real status only arrives via CloseAndRecv.
				_, err = peerStream.CloseAndRecv()
			}
			return err
		}
	}
	if _, err := peerStream.CloseAndRecv(); err != nil {
		return err
	}
	return stream.SendAndClose(&runev1.SetResponse{})
}

// Delete removes keys locally and, on the client-facing node in cluster mode,
// broadcasts to every other ring member so the owner and any orphan copies
// (left by ring changes) are cleared. Every peer is tried even if one fails; it
// returns OK only if every node succeeds.
func (h *handler) Delete(ctx context.Context, req *runev1.DeleteRequest) (*runev1.DeleteResponse, error) {
	if len(req.Keys) == 0 {
		return &runev1.DeleteResponse{}, nil
	}
	if err := h.srv.store.Delete(req.Keys...); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	// owner() resolves no address unless we are the client-facing node in cluster mode.
	if addr, _ := h.owner(ctx, ""); addr == "" {
		return &runev1.DeleteResponse{}, nil
	}
	// shortcut: peers are contacted sequentially; parallelise if ring size makes latency matter.
	var errs []error
	code := codes.OK // the first peer's status code, passed through
	for _, node := range h.srv.membership.Ring().Nodes() {
		if node.ID == h.srv.membership.NodeID() {
			continue
		}
		client, err := h.peerClient(node.Addr)
		if err == nil {
			peerCtx, cancel := context.WithTimeout(forwardCtx(ctx), peerTimeout)
			_, err = client.Delete(peerCtx, req)
			cancel()
		}
		if err != nil {
			if code == codes.OK {
				code = status.Code(err)
			}
			errs = append(errs, fmt.Errorf("%s: %s", node.ID, status.Convert(err).Message()))
		}
	}
	if len(errs) > 0 {
		return nil, status.Errorf(code, "peer delete: %v", errors.Join(errs...))
	}
	return &runev1.DeleteResponse{}, nil
}

// Exists counts keys on their owners: local keys here, the rest forwarded to
// each owner in one request per owner.
func (h *handler) Exists(ctx context.Context, req *runev1.ExistsRequest) (*runev1.ExistsResponse, error) {
	var local []string
	remote := map[string][]string{}
	for _, key := range req.Keys {
		if addr, isRemote := h.owner(ctx, key); isRemote {
			remote[addr] = append(remote[addr], key)
		} else {
			local = append(local, key)
		}
	}
	n, err := h.srv.store.Exists(local...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "exists: %v", err)
	}
	// shortcut: owners are contacted sequentially; parallelise if ring size makes latency matter.
	for addr, keys := range remote {
		client, err := h.peerClient(addr)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "peer exists %s: %v", addr, err)
		}
		peerCtx, cancel := context.WithTimeout(forwardCtx(ctx), peerTimeout)
		resp, err := client.Exists(peerCtx, &runev1.ExistsRequest{Keys: keys})
		cancel()
		if err != nil {
			return nil, status.Errorf(status.Code(err), "peer exists %s: %s", addr, status.Convert(err).Message())
		}
		n += resp.Count
	}
	return &runev1.ExistsResponse{Count: n}, nil
}

func (h *handler) Info(_ context.Context, _ *runev1.InfoRequest) (*runev1.InfoResponse, error) {
	info, err := h.srv.store.Info()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "info: %v", err)
	}
	return &runev1.InfoResponse{
		StorageUsedBytes:  info.UsedBytes,
		StorageMaxBytes:   info.MaxBytes,
		CacheHits:         info.Hits,
		CacheMisses:       info.Misses,
		ActiveConnections: h.srv.ActiveConns(),
		EvictionsTotal:    info.EvictionsTotal,
	}, nil
}
