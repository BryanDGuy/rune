// Copyright 2026 BryanDGuy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runesdk

import (
	"context"
	"errors"
	"io"

	runev1 "github.com/runicsigil/rune/gen/rune/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const chunkSize = 1 << 20 // 1MB

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("key not found")

// Client is a Rune cache client.
type Client struct {
	conn *grpc.ClientConn
	grpc runev1.RuneServiceClient
}

// New creates a Client connected to addr (e.g. "localhost:7946").
// Uses insecure credentials; TLS is a future concern.
func New(addr string, opts ...grpc.DialOption) (*Client, error) {
	defaults := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient(addr, append(defaults, opts...)...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, grpc: runev1.NewRuneServiceClient(conn)}, nil
}

// NewFromConn creates a Client from an existing gRPC connection.
// Useful for testing (e.g. with bufconn).
func NewFromConn(conn *grpc.ClientConn) *Client {
	return &Client{conn: conn, grpc: runev1.NewRuneServiceClient(conn)}
}

// Set writes the value from r to the cache at key.
// The value is chunked and sent via client-streaming gRPC in 1MB pieces.
func (c *Client) Set(ctx context.Context, key string, r io.Reader, opts ...SetOption) error {
	o := &setOptions{}
	for _, opt := range opts {
		opt(o)
	}

	stream, err := c.grpc.Set(ctx)
	if err != nil {
		return err
	}

	// Send header first.
	if err := stream.Send(&runev1.SetRequest{
		Payload: &runev1.SetRequest_Header{
			Header: &runev1.SetHeader{
				Key:        key,
				TtlSeconds: int64(o.ttl.Seconds()),
			},
		},
	}); err != nil {
		return err
	}

	// Stream chunks.
	buf := make([]byte, chunkSize)
	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			if err := stream.Send(&runev1.SetRequest{
				Payload: &runev1.SetRequest_Chunk{Chunk: buf[:n]},
			}); err != nil {
				return err
			}
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
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
	stream, err := c.grpc.Get(ctx, &runev1.GetRequest{Key: key})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}

	// Eagerly probe the first message so we can surface NotFound immediately.
	resp, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			// Empty value stored — return an empty reader.
			return io.NopCloser(io.Reader(emptyReader{})), nil
		}
		if status.Code(err) == codes.NotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &streamReader{stream: stream, buf: resp.Chunk}, nil
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
	buf    []byte
}

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		resp, err := r.stream.Recv()
		if err == io.EOF {
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

// Close drains remaining messages to let the server complete cleanly.
func (r *streamReader) Close() error {
	for {
		_, err := r.stream.Recv()
		if err != nil {
			return nil
		}
	}
}

// emptyReader is an io.Reader that always returns EOF.
type emptyReader struct{}

func (emptyReader) Read(_ []byte) (int, error) { return 0, io.EOF }
