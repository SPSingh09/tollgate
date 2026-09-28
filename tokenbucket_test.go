package tollgate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestBucket returns a TokenBucket driven by a fresh fakeClock.
func newTestBucket(t *testing.T, rate Rate, opts ...Option) (*TokenBucket, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	tb, err := NewTokenBucket(rate, append([]Option{WithClock(clock)}, opts...)...)
	if err != nil {
		t.Fatalf("NewTokenBucket(%+v) = %v, want nil error", rate, err)
	}
	return tb, clock
}

// allowN calls Allow n times for key and returns how many were allowed.
func allowN(t *testing.T, tb *TokenBucket, key string, n int) int {
	t.Helper()
	allowed := 0
	for range n {
		res, err := tb.Allow(context.Background(), key)
		if err != nil {
			t.Fatalf("Allow(%q) = %v, want nil error", key, err)
		}
		if res.Allowed {
			allowed++
		}
	}
	return allowed
}

func TestNewTokenBucketErrors(t *testing.T) {
	tests := []struct {
		name    string
		rate    Rate
		opts    []Option
		wantErr error
	}{
		{"zero limit", Rate{Limit: 0, Period: time.Second}, nil, ErrInvalidRate},
		{"zero period", Rate{Limit: 10, Period: 0}, nil, ErrInvalidRate},
		{"negative burst", Rate{Limit: 10, Period: time.Second, Burst: -1}, nil, ErrInvalidRate},
		{"nil clock", PerSecond(10), []Option{WithClock(nil)}, ErrInvalidOption},
		{"zero shards", PerSecond(10), []Option{WithShards(0)}, ErrInvalidOption},
		{"negative shards", PerSecond(10), []Option{WithShards(-4)}, ErrInvalidOption},
		{"too many shards", PerSecond(10), []Option{WithShards(maxShards + 1)}, ErrInvalidOption},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb, err := NewTokenBucket(tt.rate, tt.opts...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewTokenBucket() error = %v, want error wrapping %v", err, tt.wantErr)
			}
			if tb != nil {
				t.Fatalf("NewTokenBucket() = %v, want nil limiter on error", tb)
			}
		})
	}
}

func TestWithShardsRoundsToPowerOfTwo(t *testing.T) {
	tests := []struct {
		shards int
		want   int
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{64, 64},
		{65, 128},
		{1000, 1024},
		{maxShards, maxShards},
	}

	for _, tt := range tests {
		tb, err := NewTokenBucket(PerSecond(10), WithShards(tt.shards))
		if err != nil {
			t.Fatalf("WithShards(%d): NewTokenBucket() = %v", tt.shards, err)
		}
		if got := len(tb.shards); got != tt.want {
			t.Errorf("WithShards(%d): got %d shards, want %d", tt.shards, got, tt.want)
		}
		if got := int(tb.mask) + 1; got != tt.want {
			t.Errorf("WithShards(%d): mask+1 = %d, want %d", tt.shards, got, tt.want)
		}
	}

	tb, err := NewTokenBucket(PerSecond(10))
	if err != nil {
		t.Fatalf("NewTokenBucket() = %v", err)
	}
	if got := len(tb.shards); got != defaultShards {
		t.Errorf("default shards = %d, want %d", got, defaultShards)
	}
}

func TestTokenBucketAllow(t *testing.T) {
	tests := []struct {
		name string
		rate Rate
		// advance, if non-zero, is applied after the first drain calls.
		drain   int
		advance time.Duration
		// then is how many further calls are made after advancing.
		then        int
		wantDrained int
		wantThen    int
	}{
		{
			name:        "new key starts with a full bucket",
			rate:        PerSecond(10),
			drain:       10,
			wantDrained: 10,
		},
		{
			name:        "burst defaults to limit, then denied",
			rate:        PerSecond(10),
			drain:       15,
			wantDrained: 10,
		},
		{
			name:        "explicit burst above limit",
			rate:        Rate{Limit: 10, Period: time.Second, Burst: 25},
			drain:       30,
			wantDrained: 25,
		},
		{
			name:        "partial refill after 500ms at 10/sec",
			rate:        PerSecond(10),
			drain:       10,
			advance:     500 * time.Millisecond,
			then:        10,
			wantDrained: 10,
			wantThen:    5,
		},
		{
			name:        "tokens cap at burst after long idle",
			rate:        PerSecond(10),
			drain:       10,
			advance:     time.Hour,
			then:        100,
			wantDrained: 10,
			wantThen:    10,
		},
		{
			name:        "per-minute rate refills fractionally",
			rate:        PerMinute(60),
			drain:       60,
			advance:     3 * time.Second,
			then:        10,
			wantDrained: 60,
			wantThen:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb, clock := newTestBucket(t, tt.rate)

			if got := allowN(t, tb, "k", tt.drain); got != tt.wantDrained {
				t.Fatalf("first %d calls: allowed %d, want %d", tt.drain, got, tt.wantDrained)
			}
			if tt.advance == 0 && tt.then == 0 {
				return
			}
			clock.Advance(tt.advance)
			if got := allowN(t, tb, "k", tt.then); got != tt.wantThen {
				t.Fatalf("after advancing %s, %d calls: allowed %d, want %d", tt.advance, tt.then, got, tt.wantThen)
			}
		})
	}
}

func TestTokenBucketSeparateKeys(t *testing.T) {
	tb, _ := newTestBucket(t, PerSecond(3))

	if got := allowN(t, tb, "alice", 5); got != 3 {
		t.Fatalf("alice: allowed %d, want 3", got)
	}
	if got := allowN(t, tb, "bob", 5); got != 3 {
		t.Fatalf("bob after alice exhausted: allowed %d, want 3", got)
	}
}

func TestTokenBucketResult(t *testing.T) {
	// 10 tokens per second: one token every 100ms, full refill in 1s.
	tb, clock := newTestBucket(t, PerSecond(10))
	ctx := context.Background()

	type step struct {
		name    string
		advance time.Duration
		want    Result
	}

	var steps []step
	for remaining := 9; remaining >= 0; remaining-- {
		steps = append(steps, step{
			name: "drain",
			want: Result{Allowed: true, Limit: 10, Remaining: remaining, ResetAfter: time.Duration(10-remaining) * 100 * time.Millisecond},
		})
	}
	steps = append(steps, []step{
		{
			name: "empty bucket is denied",
			want: Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 100 * time.Millisecond, ResetAfter: time.Second},
		},
		{
			name:    "half a token is still denied",
			advance: 50 * time.Millisecond,
			want:    Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 50 * time.Millisecond, ResetAfter: 950 * time.Millisecond},
		},
		{
			name:    "waiting RetryAfter is enough",
			advance: 50 * time.Millisecond,
			want:    Result{Allowed: true, Limit: 10, Remaining: 0, ResetAfter: time.Second},
		},
		{
			name:    "fractional remaining is floored",
			advance: 250 * time.Millisecond,
			want:    Result{Allowed: true, Limit: 10, Remaining: 1, ResetAfter: 850 * time.Millisecond},
		},
	}...)

	for i, step := range steps {
		clock.Advance(step.advance)
		got, err := tb.Allow(ctx, "k")
		if err != nil {
			t.Fatalf("step %d (%s): Allow() = %v", i, step.name, err)
		}
		if got != step.want {
			t.Fatalf("step %d (%s): Allow() = %+v, want %+v", i, step.name, got, step.want)
		}
	}
}

func TestTokenBucketErrors(t *testing.T) {
	tb, _ := newTestBucket(t, PerSecond(10))

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name    string
		ctx     context.Context
		key     string
		wantErr error
	}{
		{"empty key", context.Background(), "", ErrEmptyKey},
		{"canceled context", canceled, "k", context.Canceled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tb.Allow(tt.ctx, tt.key)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Allow() error = %v, want %v", err, tt.wantErr)
			}
			if res != (Result{}) {
				t.Fatalf("Allow() result = %+v, want zero Result on error", res)
			}
		})
	}

	// A rejected call must not have consumed a token.
	if got := allowN(t, tb, "k", 20); got != 10 {
		t.Fatalf("after canceled call: allowed %d, want 10", got)
	}
}

func TestTokenBucketClockBackwards(t *testing.T) {
	clock := newFakeClock()
	tb, err := NewTokenBucket(PerSecond(10), WithClock(clock))
	if err != nil {
		t.Fatalf("NewTokenBucket() = %v", err)
	}

	allowN(t, tb, "k", 5)
	clock.Advance(-time.Hour)
	if got := allowN(t, tb, "k", 10); got != 5 {
		t.Fatalf("after clock moved backwards: allowed %d, want 5", got)
	}
}

func TestTokenBucketConcurrentSingleKey(t *testing.T) {
	const (
		burst      = 50
		goroutines = 100
		perG       = 10
	)
	// The fake clock is never advanced, so no refill happens: exactly burst
	// requests may succeed no matter how the goroutines interleave.
	tb, _ := newTestBucket(t, Rate{Limit: 1, Period: time.Hour, Burst: burst})

	var (
		allowed atomic.Int64
		start   = make(chan struct{})
		wg      sync.WaitGroup
	)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range perG {
				res, err := tb.Allow(context.Background(), "hot")
				if err != nil {
					t.Errorf("Allow() = %v", err)
					return
				}
				if res.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := allowed.Load(); got != burst {
		t.Fatalf("allowed %d requests across %d goroutines, want exactly %d", got, goroutines, burst)
	}
}

func TestFNV1a(t *testing.T) {
	// Reference values for 32-bit FNV-1a.
	tests := []struct {
		in   string
		want uint32
	}{
		{"", 0x811c9dc5},
		{"a", 0xe40c292c},
		{"foobar", 0xbf9cf968},
	}
	for _, tt := range tests {
		if got := fnv1a(tt.in); got != tt.want {
			t.Errorf("fnv1a(%q) = %#x, want %#x", tt.in, got, tt.want)
		}
	}
}
