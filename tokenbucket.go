package tollgate

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// bucket holds the token bucket state for one key. last is stored as an
// int64 offset rather than time.Time to keep per-key memory small.
type bucket struct {
	tokens float64 // tokens currently available
	last   int64   // nanoseconds since TokenBucket.base of the last refill
}

// shard is one partition of the per-key state, guarded by its own lock.
type shard struct {
	mu      sync.Mutex
	buckets map[string]bucket

	// Pad to a cache line so neighbouring shards' locks in the shard slice
	// do not contend through false sharing.
	_ [48]byte
}

// TokenBucket is an in-memory token bucket Limiter. Each key has its own
// bucket holding up to Burst tokens, refilled continuously at
// Limit/Period tokens per second. Each allowed request consumes one token.
//
// Refill is computed lazily on each call, so there are no background
// goroutines and the cost per call is O(1) regardless of the number of keys.
// A TokenBucket is safe for concurrent use.
type TokenBucket struct {
	burst      float64 // bucket capacity
	refillRate float64 // tokens added per second
	limit      int     // burst as reported in Result.Limit
	clock      Clock
	base       time.Time // reference point for nowNanos, captured at construction
	shards     []shard
	mask       uint32 // len(shards)-1; len(shards) is a power of two
}

// Compile-time check that TokenBucket implements Limiter.
var _ Limiter = (*TokenBucket)(nil)

// NewTokenBucket returns a TokenBucket that refills at rate.Limit tokens per
// rate.Period and holds at most rate.Burst tokens per key, defaulting to
// rate.Limit when Burst is zero. New keys start with a full bucket.
//
// It returns an error wrapping ErrInvalidRate if rate is invalid, or
// ErrInvalidOption if an Option is given an invalid value.
func NewTokenBucket(rate Rate, opts ...Option) (*TokenBucket, error) {
	if err := rate.Validate(); err != nil {
		return nil, fmt.Errorf("tollgate: new token bucket: %w", err)
	}
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("tollgate: new token bucket: %w", err)
	}

	burst := rate.Burst
	if burst == 0 {
		burst = rate.Limit
	}

	shards := make([]shard, cfg.shards)
	for i := range shards {
		shards[i].buckets = make(map[string]bucket)
	}

	return &TokenBucket{
		burst:      float64(burst),
		refillRate: float64(rate.Limit) / rate.Period.Seconds(),
		limit:      burst,
		clock:      cfg.clock,
		base:       cfg.clock.Now(),
		shards:     shards,
		mask:       uint32(cfg.shards - 1),
	}, nil
}

// Allow reports whether the request identified by key may proceed, consuming
// one token from key's bucket if so. It returns ErrEmptyKey if key is empty
// and the context's error if ctx is already done.
func (tb *TokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	if key == "" {
		return Result{}, ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	now := tb.nowNanos()
	s := &tb.shards[fnv1a(key)&tb.mask]

	// Read, refill, check and decrement must all happen under one lock
	// acquisition; otherwise two goroutines can both see the same last token.
	s.mu.Lock()
	b, ok := s.buckets[key]
	if !ok {
		b = bucket{tokens: tb.burst, last: now}
	}
	// Ignore a clock that moves backwards rather than draining the bucket.
	if now > b.last {
		elapsed := time.Duration(now - b.last).Seconds()
		b.tokens = min(tb.burst, b.tokens+elapsed*tb.refillRate)
		b.last = now
	}
	allowed := b.tokens >= 1
	if allowed {
		b.tokens--
	}
	s.buckets[key] = b
	tokens := b.tokens
	s.mu.Unlock()

	res := Result{
		Allowed:    allowed,
		Limit:      tb.limit,
		Remaining:  int(math.Floor(tokens)),
		ResetAfter: tb.timeToFill(tb.burst - tokens),
	}
	if !allowed {
		res.RetryAfter = tb.timeToFill(1 - tokens)
	}
	return res, nil
}

// nowNanos returns the current time as nanoseconds since tb.base.
//
// It deliberately avoids UnixNano: that discards the monotonic clock reading,
// so a wall-clock jump (NTP correction, VM migration) would corrupt refill by
// adding or removing tokens. Sub between two Times that both carry a
// monotonic reading uses only the monotonic clock and is immune to such jumps.
func (tb *TokenBucket) nowNanos() int64 {
	return int64(tb.clock.Now().Sub(tb.base))
}

// timeToFill returns how long it takes to refill n tokens, rounded up to the
// next nanosecond so that waiting that long is always enough.
func (tb *TokenBucket) timeToFill(n float64) time.Duration {
	if n <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(n / tb.refillRate * float64(time.Second)))
}

// fnv1a returns the 32-bit FNV-1a hash of s. It is inlined here rather than
// using hash/fnv to avoid allocating a hasher and copying key to a []byte on
// every call.
func fnv1a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}
