package tollgate

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrInvalidRate is returned when a Rate fails validation. Use errors.Is
// to check for it, since implementations may wrap it with additional detail.
var ErrInvalidRate = errors.New("tollgate: invalid rate")

// ErrEmptyKey is returned when a Limiter is called with an empty key.
var ErrEmptyKey = errors.New("tollgate: empty key")

// Limiter decides whether a request identified by key may proceed.
//
// Implementations are expected to be safe for concurrent use by multiple
// goroutines.
type Limiter interface {
	// Allow reports whether the request identified by key is permitted
	// under the limiter's configured rate. It returns a non-nil error if
	// the decision could not be made, for example due to a canceled
	// context or a backing store failure.
	Allow(ctx context.Context, key string) (Result, error)
}

// Result describes the outcome of a Limiter decision.
type Result struct {
	// Allowed reports whether the request may proceed.
	Allowed bool

	// Limit is the configured limit that produced this result.
	Limit int

	// Remaining is the number of requests left in the current
	// window or bucket after this decision.
	Remaining int

	// RetryAfter is how long the caller should wait before retrying.
	// It is zero when Allowed is true.
	RetryAfter time.Duration

	// ResetAfter is how long until the limiter's capacity fully resets.
	ResetAfter time.Duration
}

// Rate describes how many requests are permitted over a given period.
type Rate struct {
	// Limit is the number of requests allowed per Period.
	Limit int

	// Period is the duration over which Limit applies.
	Period time.Duration

	// Burst is the maximum number of requests that may be served in a
	// single burst. It is meaningful only to token-bucket-style
	// implementations and defaults to Limit when zero.
	Burst int
}

// Validate reports whether the Rate is well-formed. It returns an error
// wrapping ErrInvalidRate when Limit is not positive, Period is not
// positive, or Burst is negative.
func (r Rate) Validate() error {
	if r.Limit <= 0 {
		return fmt.Errorf("%w: limit must be positive, got %d", ErrInvalidRate, r.Limit)
	}
	if r.Period <= 0 {
		return fmt.Errorf("%w: period must be positive, got %s", ErrInvalidRate, r.Period)
	}
	if r.Burst < 0 {
		return fmt.Errorf("%w: burst must not be negative, got %d", ErrInvalidRate, r.Burst)
	}
	return nil
}

// PerSecond returns a Rate that allows n requests per second.
func PerSecond(n int) Rate {
	return Rate{Limit: n, Period: time.Second}
}

// PerMinute returns a Rate that allows n requests per minute.
func PerMinute(n int) Rate {
	return Rate{Limit: n, Period: time.Minute}
}

// PerHour returns a Rate that allows n requests per hour.
func PerHour(n int) Rate {
	return Rate{Limit: n, Period: time.Hour}
}
