package runesdk

import "time"

// SetOptions configures a Set call. Pass nil for defaults.
type SetOptions struct {
	TTL time.Duration
}
