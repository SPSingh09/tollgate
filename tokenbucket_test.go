package tollgate

import (
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

func TestTokenBucketAllow(t *testing.T) {
	tests := []struct {
		name string
		rate Rate
		// drain calls are made first, then the clock is advanced by advance
		// and then further calls are made.
		drain       int
		advance     time.Duration
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

func TestTokenBucketResult(t *testing.T) {
	// 10 tokens per second: one token every 100ms, full refill in 1s.
	tb, clock := newTestBucket(t, PerSecond(10))

	var steps []resultStep
	for remaining := 9; remaining >= 0; remaining-- {
		steps = append(steps, resultStep{
			name: "drain",
			want: Result{Allowed: true, Limit: 10, Remaining: remaining, ResetAfter: time.Duration(10-remaining) * 100 * time.Millisecond},
		})
	}
	steps = append(steps, []resultStep{
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

	runResultSteps(t, tb, clock, steps)
}

func TestTokenBucketClockBackwards(t *testing.T) {
	tb, clock := newTestBucket(t, PerSecond(10))

	allowN(t, tb, "k", 5)
	clock.Advance(-time.Hour)
	if got := allowN(t, tb, "k", 10); got != 5 {
		t.Fatalf("after clock moved backwards: allowed %d, want 5", got)
	}
}
