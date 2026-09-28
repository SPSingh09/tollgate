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
