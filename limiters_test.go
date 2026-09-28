package tollgate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConstructorErrors(t *testing.T) {
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

	for _, ctor := range limiterCtors {
		for _, tt := range tests {
			t.Run(ctor.name+"/"+tt.name, func(t *testing.T) {
				l, err := ctor.new(tt.rate, tt.opts...)
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("constructor error = %v, want error wrapping %v", err, tt.wantErr)
				}
				if l != nil {
					t.Fatalf("constructor returned %v, want nil limiter on error", l)
				}
			})
		}
	}
}

func TestLimitersSeparateKeys(t *testing.T) {
	for _, ctor := range limiterCtors {
		t.Run(ctor.name, func(t *testing.T) {
			l, err := ctor.new(PerSecond(3), WithClock(newFakeClock()))
			if err != nil {
				t.Fatalf("constructor = %v", err)
			}
			if got := allowN(t, l, "alice", 5); got != 3 {
				t.Fatalf("alice: allowed %d, want 3", got)
			}
			if got := allowN(t, l, "bob", 5); got != 3 {
				t.Fatalf("bob after alice exhausted: allowed %d, want 3", got)
			}
		})
	}
}

func TestLimitersAllowErrors(t *testing.T) {
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

	for _, ctor := range limiterCtors {
		t.Run(ctor.name, func(t *testing.T) {
			l, err := ctor.new(PerSecond(10), WithClock(newFakeClock()))
			if err != nil {
				t.Fatalf("constructor = %v", err)
			}
			for _, tt := range tests {
				res, err := l.Allow(tt.ctx, tt.key)
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("%s: Allow() error = %v, want %v", tt.name, err, tt.wantErr)
				}
				if res != (Result{}) {
					t.Fatalf("%s: Allow() result = %+v, want zero Result on error", tt.name, res)
				}
			}
			// A rejected call must not have counted against the key.
			if got := allowN(t, l, "k", 20); got != 10 {
				t.Fatalf("after rejected calls: allowed %d, want 10", got)
			}
		})
	}
}

func TestLimitersConcurrentSingleKey(t *testing.T) {
	const (
		limit      = 50
		goroutines = 100
		perG       = 10
	)

	for _, ctor := range limiterCtors {
		t.Run(ctor.name, func(t *testing.T) {
			// The fake clock is never advanced, so there is no refill and no
			// window boundary: exactly limit requests may succeed no matter
			// how the goroutines interleave.
			l, err := ctor.new(Rate{Limit: limit, Period: time.Hour}, WithClock(newFakeClock()))
			if err != nil {
				t.Fatalf("constructor = %v", err)
			}

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
						res, err := l.Allow(context.Background(), "hot")
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

			if got := allowed.Load(); got != limit {
				t.Fatalf("allowed %d requests across %d goroutines, want exactly %d", got, goroutines, limit)
			}
		})
	}
}

// TestWindowLimitersFirstWindowIsPartial documents a consequence of aligning
// windows to the wall clock: a limiter built part-way through a window gets a
// shorter first window. Built at 10:00:30, a per-minute limiter's first
// window is 10:00:00–10:01:00, so only 30 seconds of it remain.
func TestWindowLimitersFirstWindowIsPartial(t *testing.T) {
	tests := []struct {
		ctor limiterCtor
		// wantFirstReset is ResetAfter on the first request, at 10:00:30.
		wantFirstReset time.Duration
		// wantAtBoundary is how many of 5 requests pass at 10:01:00, after
		// the limit of 5 was used up at 10:00:30.
		wantAtBoundary int
	}{
		{
			// The count resets at 10:01:00, 30 seconds after construction.
			ctor:           limiterCtors[1],
			wantFirstReset: 30 * time.Second,
			wantAtBoundary: 5,
		},
		{
			// Requests in the partial window are weighted as if spread over
			// the whole of it, so at 10:01:00 they still count in full and
			// decay over the following minute: the estimate reaches zero at
			// 10:02:00, 90 seconds after construction.
			ctor:           limiterCtors[2],
			wantFirstReset: 90 * time.Second,
			wantAtBoundary: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.ctor.name, func(t *testing.T) {
			clock := newFakeClock()
			clock.Advance(10*time.Hour + 30*time.Second)
			l, err := tt.ctor.new(PerMinute(5), WithClock(clock))
			if err != nil {
				t.Fatalf("constructor = %v", err)
			}

			res, err := l.Allow(context.Background(), "k")
			if err != nil {
				t.Fatalf("Allow() = %v", err)
			}
			if res.ResetAfter != tt.wantFirstReset {
				t.Fatalf("first ResetAfter = %s, want %s", res.ResetAfter, tt.wantFirstReset)
			}
			if got := allowN(t, l, "k", 10); got != 4 {
				t.Fatalf("rest of first window: allowed %d, want 4", got)
			}

			clock.Advance(30 * time.Second)
			if got := allowN(t, l, "k", 5); got != tt.wantAtBoundary {
				t.Fatalf("at 10:01:00: allowed %d, want %d", got, tt.wantAtBoundary)
			}
		})
	}
}

func TestWindowLimitersIgnoreBurst(t *testing.T) {
	rate := Rate{Limit: 5, Period: time.Second, Burst: 20}

	for _, ctor := range limiterCtors[1:] {
		t.Run(ctor.name, func(t *testing.T) {
			l, err := ctor.new(rate, WithClock(newFakeClock()))
			if err != nil {
				t.Fatalf("constructor = %v", err)
			}
			res, err := l.Allow(context.Background(), "k")
			if err != nil {
				t.Fatalf("Allow() = %v", err)
			}
			if res.Limit != rate.Limit {
				t.Fatalf("Result.Limit = %d, want %d (Limit, not Burst)", res.Limit, rate.Limit)
			}
			if got := allowN(t, l, "k", 30); got != rate.Limit-1 {
				t.Fatalf("allowed %d more, want %d", got, rate.Limit-1)
			}
		})
	}
}
