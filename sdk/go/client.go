package runesdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	runev1 "github.com/bryandguy/rune/gen/rune/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // registers gzip compressor for SetOptions.Compress
	"google.golang.org/grpc/status"
)

const chunkSize = 1 << 20 // 1MB

type Client struct {
	conn *grpc.ClientConn
	grpc runev1.RuneServiceClient
}

func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{conn: conn, grpc: runev1.NewRuneServiceClient(conn)}
}

// Chunked via client-streaming gRPC in 1MB pieces.
func (c *Client) Set(ctx context.Context, key string, r io.Reader, opts *SetOptions) error {
	var ttl time.Duration
	var callOpts []grpc.CallOption
	if opts != nil {
		ttl = opts.TTL
		if opts.Compress {
			callOpts = append(callOpts, grpc.UseCompressor("gzip"))
		}
	}

	stream, err := c.grpc.Set(ctx, callOpts...)
	if err != nil {
		return err
	}

	if err = stream.Send(&runev1.SetRequest{
		Payload: &runev1.SetRequest_Header{
			Header: &runev1.SetHeader{
				Key:        key,
				TtlSeconds: int64(ttl.Seconds()),
			},
		},
	}); err != nil {
		return err
	}

	buf := make([]byte, chunkSize)
	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			if err = stream.Send(&runev1.SetRequest{
				Payload: &runev1.SetRequest_Chunk{Chunk: buf[:n]},
			}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	_, err = stream.CloseAndRecv()
	return err
}

// Caller must Close() the reader when done. Returns ErrNotFound if key is missing.
func (c *Client) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := c.grpc.Get(ctx, &runev1.GetRequest{Key: key})
	if err != nil {
		cancel()
		if status.Code(err) == codes.NotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}

	// Eagerly probe the first message so we can surface NotFound immediately.
	resp, err := stream.Recv()
	if err != nil {
		cancel()
		if errors.Is(err, io.EOF) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		if status.Code(err) == codes.NotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &streamReader{stream: stream, buf: resp.Chunk, cancel: cancel}, nil
}

func (c *Client) Delete(ctx context.Context, keys ...string) error {
	_, err := c.grpc.Delete(ctx, &runev1.DeleteRequest{Keys: keys})
	return err
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// streamReader implements io.ReadCloser over a server-streaming gRPC call.
type streamReader struct {
	stream grpc.ServerStreamingClient[runev1.GetResponse]
	cancel context.CancelFunc
	buf    []byte
}

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		resp, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		r.buf = resp.Chunk
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *streamReader) Close() error {
	r.cancel()
	return nil
}
