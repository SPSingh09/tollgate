package tollgate

import (
	"context"
	"fmt"
	"time"
)

// windowCount holds the fixed window state for one key.
type windowCount struct {
	window int64 // window index: Unix nanoseconds / period
	count  int   // requests allowed in window
}

// FixedWindow is an in-memory fixed window Limiter. Time is divided into
// consecutive windows of length Rate.Period, aligned to the Unix epoch, and
// each key may make Rate.Limit requests per window. The count resets at each
// window boundary.
//
// Because the count resets all at once, a key can make Limit requests at the
// end of one window and Limit more at the start of the next: up to 2×Limit
// requests in a span much shorter than Period. Use SlidingWindow or
// TokenBucket when that matters.
//
// Rate.Burst is ignored; it applies only to TokenBucket.
// A FixedWindow is safe for concurrent use.
type FixedWindow struct {
	monoClock
	limit  int
	period int64 // window length in nanoseconds
	store  shardedStore[windowCount]
}

// Compile-time check that FixedWindow implements Limiter.
var _ Limiter = (*FixedWindow)(nil)

// NewFixedWindow returns a FixedWindow that allows rate.Limit requests per
// key in each window of length rate.Period. rate.Burst is ignored.
//
// It returns an error wrapping ErrInvalidRate if rate is invalid, or
// ErrInvalidOption if an Option is given an invalid value.
func NewFixedWindow(rate Rate, opts ...Option) (*FixedWindow, error) {
	cfg, err := newLimiterConfig(rate, opts)
	if err != nil {
		return nil, fmt.Errorf("tollgate: new fixed window: %w", err)
	}
	return &FixedWindow{
		monoClock: newMonoClock(cfg.clock),
		limit:     rate.Limit,
		period:    int64(rate.Period),
		store:     newShardedStore[windowCount](cfg.shards),
	}, nil
}

// Allow reports whether the request identified by key may proceed, counting
// it against key's current window if so. It returns ErrEmptyKey if key is
// empty and the context's error if ctx is already done.
func (fw *FixedWindow) Allow(ctx context.Context, key string) (Result, error) {
	if key == "" {
		return Result{}, ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	now := fw.unixNanos()
	w := now / fw.period
	s := fw.store.shardFor(key)

	s.mu.Lock()
	c, ok := s.m[key]
	// A window index lower than the stored one means the clock moved
	// backwards; keep counting against the stored window rather than
	// resetting.
	if !ok || w > c.window {
		c = windowCount{window: w}
	}
	allowed := c.count < fw.limit
	if allowed {
		c.count++
	}
	s.m[key] = c
	s.mu.Unlock()

	reset := time.Duration((c.window+1)*fw.period - now)
	res := Result{
		Allowed:    allowed,
		Limit:      fw.limit,
		Remaining:  fw.limit - c.count,
		ResetAfter: reset,
	}
	if !allowed {
		res.RetryAfter = reset
	}
	return res, nil
}
