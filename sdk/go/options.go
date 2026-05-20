
package runesdk

import "time"

// SetOption configures a Set call.
type SetOption func(*setOptions)

type setOptions struct {
	ttl time.Duration
}

// WithTTL sets a time-to-live on the cached value.
func WithTTL(d time.Duration) SetOption {
	return func(o *setOptions) { o.ttl = d }
}
