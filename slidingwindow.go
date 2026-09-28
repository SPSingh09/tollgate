package tollgate

import (
	"context"
	"fmt"
	"math/bits"
	"time"
)

// slidingCount holds the sliding window state for one key.
type slidingCount struct {
	window int64 // index of the current window: Unix nanoseconds / period
	prev   int   // requests allowed in window-1
	curr   int   // requests allowed in window
}

// SlidingWindow is an in-memory sliding window Limiter using the weighted
// approximation algorithm. Time is divided into windows of length
// Rate.Period, aligned to the Unix epoch. For each key it keeps only the
// counts for the current and previous windows, and estimates the number of
// requests in the trailing Period as
//
//	previous × overlap + current
//
// where overlap is the fraction of the previous window that still falls
// inside the trailing Period. A request is allowed if it would not take the
// estimate above Rate.Limit. This avoids FixedWindow's 2×Limit burst at
// window boundaries while using constant memory per key.
//
// The estimate assumes requests in the previous window were evenly spread,
// so it is an approximation of a true sliding log.
//
// Rate.Burst is ignored; it applies only to TokenBucket.
// A SlidingWindow is safe for concurrent use.
type SlidingWindow struct {
	monoClock
	limit  int
	period int64 // window length in nanoseconds
	store  shardedStore[slidingCount]
}

// Compile-time check that SlidingWindow implements Limiter.
var _ Limiter = (*SlidingWindow)(nil)

// NewSlidingWindow returns a SlidingWindow that allows an estimated
// rate.Limit requests per key in any trailing rate.Period. rate.Burst is
// ignored.
//
// It returns an error wrapping ErrInvalidRate if rate is invalid, or
// ErrInvalidOption if an Option is given an invalid value.
func NewSlidingWindow(rate Rate, opts ...Option) (*SlidingWindow, error) {
	cfg, err := newLimiterConfig(rate, opts)
	if err != nil {
		return nil, fmt.Errorf("tollgate: new sliding window: %w", err)
	}
	return &SlidingWindow{
		monoClock: newMonoClock(cfg.clock),
		limit:     rate.Limit,
		period:    int64(rate.Period),
		store:     newShardedStore[slidingCount](cfg.shards),
	}, nil
}

// Allow reports whether the request identified by key may proceed, counting
// it against key's current window if so. It returns ErrEmptyKey if key is
// empty and the context's error if ctx is already done.
//
// The arithmetic is done in integers scaled by the period, so decisions and
// RetryAfter are exact to the nanosecond.
func (sw *SlidingWindow) Allow(ctx context.Context, key string) (Result, error) {
	if key == "" {
		return Result{}, ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	now := sw.unixNanos()
	w := now / sw.period
	s := sw.store.shardFor(key)

	s.mu.Lock()
	c, ok := s.m[key]
	switch {
	case !ok || w > c.window+1:
		// New key, or idle for at least a whole window.
		c = slidingCount{window: w}
	case w == c.window+1:
		c = slidingCount{window: w, prev: c.curr}
	}
	// If w < c.window the clock moved backwards; keep the stored windows
	// and clamp elapsed below.
	elapsed := min(max(now-c.window*sw.period, 0), sw.period-1)
	allowed := sw.fits(c, elapsed)
	if allowed {
		c.curr++
	}
	s.m[key] = c
	s.mu.Unlock()

	res := Result{
		Allowed:    allowed,
		Limit:      sw.limit,
		Remaining:  sw.remaining(c, elapsed),
		ResetAfter: sw.resetAfter(c, elapsed),
	}
	if !allowed {
		res.RetryAfter = sw.retryAfter(c, elapsed)
	}
	return res, nil
}

// fits reports whether one more request fits under the limit, elapsed
// nanoseconds into c's current window. With P the period and remaining the
// part of the previous window still inside the trailing period, the check
// prev×remaining/P + curr + 1 <= limit is rearranged to
// prev×remaining <= (limit-curr-1)×P to stay in integers.
func (sw *SlidingWindow) fits(c slidingCount, elapsed int64) bool {
	free := sw.limit - c.curr - 1
	if free < 0 {
		return false
	}
	return mulLE(uint64(c.prev), uint64(sw.period-elapsed), uint64(free), uint64(sw.period))
}

// remaining returns how many more requests would be allowed right now:
// floor(limit - estimate), which is limit - curr - ceil(prev×overlap).
func (sw *SlidingWindow) remaining(c slidingCount, elapsed int64) int {
	weighted := mulDivCeil(uint64(c.prev), uint64(sw.period-elapsed), uint64(sw.period))
	return max(sw.limit-c.curr-int(weighted), 0)
}

// retryAfter returns how long until one more request would fit, given that
// it does not fit now.
func (sw *SlidingWindow) retryAfter(c slidingCount, elapsed int64) time.Duration {
	if free := sw.limit - c.curr - 1; free >= 0 {
		// The current window has room, so wait for the previous window's
		// share to decay: the earliest point e in this window at which
		// prev×(P-e) <= free×P.
		return time.Duration(sw.decayPoint(c.prev, free) - elapsed)
	}
	// The current window alone is full. In the next window it becomes the
	// previous one and the new current count starts at zero.
	return time.Duration(sw.period - elapsed + sw.decayPoint(c.curr, sw.limit-1))
}

// decayPoint returns the earliest offset e into a window at which
// prev×(P-e) <= free×P, that is, P - floor(free×P/prev). prev must be
// positive and free non-negative.
func (sw *SlidingWindow) decayPoint(prev, free int) int64 {
	q, _ := mulDiv(uint64(free), uint64(sw.period), uint64(prev))
	return sw.period - int64(min(q, uint64(sw.period)))
}

// resetAfter returns how long until the estimate decays to zero: the end of
// the current window if only the previous window has requests, or the end
// of the next window if the current one does.
func (sw *SlidingWindow) resetAfter(c slidingCount, elapsed int64) time.Duration {
	switch {
	case c.curr > 0:
		return time.Duration(2*sw.period - elapsed)
	case c.prev > 0:
		return time.Duration(sw.period - elapsed)
	default:
		return 0
	}
}

// mulLE reports whether a×b <= c×d, using 128-bit products so that large
// limits and periods cannot overflow.
func mulLE(a, b, c, d uint64) bool {
	h1, l1 := bits.Mul64(a, b)
	h2, l2 := bits.Mul64(c, d)
	return h1 < h2 || (h1 == h2 && l1 <= l2)
}

// mulDiv returns floor(a×b/c) using a 128-bit intermediate product, and
// whether the division left a remainder. c must be positive and the quotient
// must fit in a uint64.
func mulDiv(a, b, c uint64) (q uint64, inexact bool) {
	hi, lo := bits.Mul64(a, b)
	q, r := bits.Div64(hi, lo, c)
	return q, r != 0
}

// mulDivCeil returns ceil(a×b/c), with the same constraints as mulDiv.
func mulDivCeil(a, b, c uint64) uint64 {
	q, inexact := mulDiv(a, b, c)
	if inexact {
		q++
	}
	return q
}
