package runesdk

import (
	"context"
	"errors"
	"io"
	"time"

	runev1 "github.com/bryandguy/rune/gen/rune/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const chunkSize = 1 << 20 // 1MB

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("key not found")

// SetOptions configures a Set call. Pass nil for defaults.
type SetOptions struct {
	TTL time.Duration
}

// Client is a Rune cache client.
type Client struct {
	conn *grpc.ClientConn
	grpc runev1.RuneServiceClient
}

// NewClient creates a Client from an existing gRPC connection.
func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{conn: conn, grpc: runev1.NewRuneServiceClient(conn)}
}

// Set writes the value from r to the cache at key.
// The value is chunked and sent via client-streaming gRPC in 1MB pieces.
func (c *Client) Set(ctx context.Context, key string, r io.Reader, opts *SetOptions) error {
	var ttl time.Duration
	if opts != nil {
		ttl = opts.TTL
	}

	stream, err := c.grpc.Set(ctx)
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

// Get retrieves a key from the cache and returns a streaming io.ReadCloser.
// The caller must Close() the reader when done.
// Returns ErrNotFound if the key does not exist.
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
			return io.NopCloser(emptyReader{}), nil
		}
		if status.Code(err) == codes.NotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &streamReader{stream: stream, buf: resp.Chunk, cancel: cancel}, nil
}

// Close closes the underlying gRPC connection.
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

// emptyReader is an io.Reader that always returns EOF.
type emptyReader struct{}

func (emptyReader) Read(_ []byte) (int, error) { return 0, io.EOF }
