package tollgate

import (
	"context"
	"testing"
	"time"
)

// newTestSlidingWindow returns a SlidingWindow driven by a fresh fakeClock,
// which starts exactly on a window boundary.
func newTestSlidingWindow(t *testing.T, rate Rate) (*SlidingWindow, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	sw, err := NewSlidingWindow(rate, WithClock(clock))
	if err != nil {
		t.Fatalf("NewSlidingWindow(%+v) = %v, want nil error", rate, err)
	}
	return sw, clock
}

func TestSlidingWindowLimitInFreshWindow(t *testing.T) {
	sw, _ := newTestSlidingWindow(t, PerSecond(10))

	if got := allowN(t, sw, "k", 15); got != 10 {
		t.Fatalf("allowed %d, want 10", got)
	}
}

// TestSlidingWindowBoundaryBurst makes the same attempt as
// TestFixedWindowBoundaryBurst. The previous window's requests still count
// almost fully just after the boundary, so the second burst is denied.
func TestSlidingWindowBoundaryBurst(t *testing.T) {
	sw, clock := newTestSlidingWindow(t, PerSecond(10))

	clock.Advance(999 * time.Millisecond)
	before := allowN(t, sw, "k", 10)
	clock.Advance(2 * time.Millisecond)
	after := allowN(t, sw, "k", 10)

	if before != 10 || after != 0 {
		t.Fatalf("allowed %d before and %d after the boundary, want 10 and 0", before, after)
	}
}

func TestSlidingWindowWeighting(t *testing.T) {
	// The previous window is filled with 10 requests. At a fraction f through
	// the next window, those count as 10×(1-f), and a request is allowed only
	// if it keeps the estimate at or below 10.
	tests := []struct {
		name    string
		advance time.Duration // from the start of the filled window
		want    int
	}{
		{"25% through: previous counts 7.5", 1250 * time.Millisecond, 2},
		{"50% through: previous counts 5", 1500 * time.Millisecond, 5},
		{"75% through: previous counts 2.5", 1750 * time.Millisecond, 7},
		{"a whole window later: previous has expired", 2 * time.Second, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sw, clock := newTestSlidingWindow(t, PerSecond(10))
			if got := allowN(t, sw, "k", 10); got != 10 {
				t.Fatalf("filling previous window: allowed %d, want 10", got)
			}
			clock.Advance(tt.advance)
			if got := allowN(t, sw, "k", 20); got != tt.want {
				t.Fatalf("allowed %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSlidingWindowResult(t *testing.T) {
	t.Run("waiting for the previous window to decay", func(t *testing.T) {
		sw, clock := newTestSlidingWindow(t, PerSecond(10))
		allowN(t, sw, "k", 10)

		// 25% into the next window the previous 10 count as 7.5. Two more
		// requests take the estimate to 9.5. A third needs the previous
		// share down to 7, which happens 30% into the window.
		runResultSteps(t, sw, clock, []resultStep{
			{
				name:    "estimate 8.5",
				advance: 1250 * time.Millisecond,
				want:    Result{Allowed: true, Limit: 10, Remaining: 1, ResetAfter: 1750 * time.Millisecond},
			},
			{
				name: "estimate 9.5",
				want: Result{Allowed: true, Limit: 10, Remaining: 0, ResetAfter: 1750 * time.Millisecond},
			},
			{
				name: "denied until 30% through",
				want: Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 50 * time.Millisecond, ResetAfter: 1750 * time.Millisecond},
			},
			{
				name:    "one nanosecond early is still denied",
				advance: 50*time.Millisecond - 1,
				want:    Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 1, ResetAfter: 1700*time.Millisecond + 1},
			},
			{
				name:    "waiting RetryAfter is enough",
				advance: 1,
				want:    Result{Allowed: true, Limit: 10, Remaining: 0, ResetAfter: 1700 * time.Millisecond},
			},
		})
	})

	t.Run("current window full", func(t *testing.T) {
		sw, clock := newTestSlidingWindow(t, PerSecond(10))
		allowN(t, sw, "k", 10)

		// The current window alone holds the limit, so the caller must wait
		// for the next window and then for those 10 to decay to 9, which is
		// 10% into it.
		runResultSteps(t, sw, clock, []resultStep{
			{
				name: "denied",
				want: Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 1100 * time.Millisecond, ResetAfter: 2 * time.Second},
			},
			{
				name:    "one nanosecond early is still denied",
				advance: 1100*time.Millisecond - 1,
				want:    Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 1, ResetAfter: 900*time.Millisecond + 1},
			},
			{
				name:    "waiting RetryAfter is enough",
				advance: 1,
				want:    Result{Allowed: true, Limit: 10, Remaining: 0, ResetAfter: 1900 * time.Millisecond},
			},
		})
	})
}

func TestSlidingWindowClockBackwards(t *testing.T) {
	sw, clock := newTestSlidingWindow(t, PerSecond(10))

	allowN(t, sw, "k", 10)
	clock.Advance(-time.Hour)
	if got := allowN(t, sw, "k", 10); got != 0 {
		t.Fatalf("after clock moved backwards: allowed %d, want 0 (no reset)", got)
	}
}

func TestSlidingWindowLargeValuesDoNotOverflow(t *testing.T) {
	// limit × period in nanoseconds is about 8.6e19, beyond int64's 9.2e18.
	sw, clock := newTestSlidingWindow(t, Rate{Limit: 1_000_000, Period: 24 * time.Hour})

	if got := allowN(t, sw, "k", 10); got != 10 {
		t.Fatalf("allowed %d, want 10", got)
	}
	clock.Advance(36 * time.Hour) // halfway through the next window
	res, err := sw.Allow(context.Background(), "k")
	if err != nil {
		t.Fatalf("Allow() = %v", err)
	}
	// Previous 10 count as 5, plus this request: 1,000,000 - 6.
	if !res.Allowed || res.Remaining != 999_994 {
		t.Fatalf("Allow() = %+v, want allowed with Remaining 999994", res)
	}
}
