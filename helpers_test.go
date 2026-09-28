package tollgate

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// limiterCtor is a limiter constructor adapted to return a Limiter.
type limiterCtor struct {
	name string
	new  func(Rate, ...Option) (Limiter, error)
}

// limiterCtors lists every Limiter implementation, for tests of behaviour
// they must all share.
var limiterCtors = []limiterCtor{
	{"TokenBucket", asLimiter(NewTokenBucket)},
	{"FixedWindow", asLimiter(NewFixedWindow)},
	{"SlidingWindow", asLimiter(NewSlidingWindow)},
}

// asLimiter adapts a concrete constructor to return a Limiter. On error it
// returns a nil Limiter, and reports a constructor that returned a non-nil
// value alongside its error rather than hiding it inside a non-nil interface.
func asLimiter[L Limiter](newL func(Rate, ...Option) (L, error)) func(Rate, ...Option) (Limiter, error) {
	return func(rate Rate, opts ...Option) (Limiter, error) {
		l, err := newL(rate, opts...)
		if err != nil {
			var zero L
			if any(l) != any(zero) {
				return l, fmt.Errorf("constructor returned a non-nil limiter with its error: %w", err)
			}
			return nil, err
		}
		return l, nil
	}
}

// allowN calls Allow n times for key and returns how many were allowed.
func allowN(t *testing.T, l Limiter, key string, n int) int {
	t.Helper()
	allowed := 0
	for range n {
		res, err := l.Allow(context.Background(), key)
		if err != nil {
			t.Fatalf("Allow(%q) = %v, want nil error", key, err)
		}
		if res.Allowed {
			allowed++
		}
	}
	return allowed
}

// resultStep is one call in a sequence that checks the full Result: the
// clock is advanced, then Allow is called once for key "k".
type resultStep struct {
	name    string
	advance time.Duration
	want    Result
}

// runResultSteps runs steps against l, advancing clock before each call.
func runResultSteps(t *testing.T, l Limiter, clock *fakeClock, steps []resultStep) {
	t.Helper()
	for i, step := range steps {
		clock.Advance(step.advance)
		got, err := l.Allow(context.Background(), "k")
		if err != nil {
			t.Fatalf("step %d (%s): Allow() = %v", i, step.name, err)
		}
		if got != step.want {
			t.Fatalf("step %d (%s): Allow() = %+v, want %+v", i, step.name, got, step.want)
		}
	}
}
