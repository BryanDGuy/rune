# Code Style Guidelines

## API Design

### Optional Configuration

For optional configuration on our own functions and methods, use a `*OptionsStruct` parameter instead of `...OptionType` variadic. A nil pointer means production defaults. This avoids ordering hazards and makes the zero value (nil) clearly express "no overrides."

```go
// Prefer this:
func (d *PeerDialer) Dial(addr string, opts *DialOptions) (*grpc.ClientConn, error)

// Over this:
func (d *PeerDialer) Dial(addr string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error)
```

This applies to our own APIs. It does not apply to methods that mirror external interfaces (e.g., `memberStore` methods that mirror `clientv3.Client`).
