package tollgate

import (
	"testing"
	"time"
)

// newTestFixedWindow returns a FixedWindow driven by a fresh fakeClock, which
// starts exactly on a window boundary.
func newTestFixedWindow(t *testing.T, rate Rate) (*FixedWindow, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	fw, err := NewFixedWindow(rate, WithClock(clock))
	if err != nil {
		t.Fatalf("NewFixedWindow(%+v) = %v, want nil error", rate, err)
	}
	return fw, clock
}

func TestFixedWindowLimitPerWindow(t *testing.T) {
	fw, clock := newTestFixedWindow(t, PerSecond(10))

	if got := allowN(t, fw, "k", 15); got != 10 {
		t.Fatalf("start of window: allowed %d, want 10", got)
	}
	clock.Advance(999 * time.Millisecond)
	if got := allowN(t, fw, "k", 5); got != 0 {
		t.Fatalf("end of same window: allowed %d, want 0", got)
	}
	clock.Advance(time.Millisecond)
	if got := allowN(t, fw, "k", 15); got != 10 {
		t.Fatalf("after boundary: allowed %d, want 10 (count reset)", got)
	}
}

// TestFixedWindowBoundaryBurst documents the algorithm's known weakness:
// because the count resets all at once, 2×Limit requests pass in a span far
// shorter than Period.
func TestFixedWindowBoundaryBurst(t *testing.T) {
	fw, clock := newTestFixedWindow(t, PerSecond(10))

	clock.Advance(999 * time.Millisecond)
	before := allowN(t, fw, "k", 10)
	clock.Advance(2 * time.Millisecond)
	after := allowN(t, fw, "k", 10)

	if before != 10 || after != 10 {
		t.Fatalf("allowed %d before and %d after the boundary, want 10 and 10", before, after)
	}
	// 20 requests passed within 2ms under a 10/sec limit.
}

func TestFixedWindowResult(t *testing.T) {
	fw, clock := newTestFixedWindow(t, PerSecond(10))

	steps := []resultStep{
		{
			name: "first request",
			want: Result{Allowed: true, Limit: 10, Remaining: 9, ResetAfter: time.Second},
		},
	}
	// Request k arrives at k×50ms, so the window ends 1s - k×50ms later.
	for k := 1; k <= 9; k++ {
		steps = append(steps, resultStep{
			name:    "drain",
			advance: 50 * time.Millisecond,
			want:    Result{Allowed: true, Limit: 10, Remaining: 9 - k, ResetAfter: time.Second - time.Duration(k)*50*time.Millisecond},
		})
	}
	steps = append(steps, []resultStep{
		{
			name:    "exhausted window is denied until the boundary",
			advance: 100 * time.Millisecond,
			want:    Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 450 * time.Millisecond, ResetAfter: 450 * time.Millisecond},
		},
		{
			name:    "one nanosecond before the boundary is still denied",
			advance: 450*time.Millisecond - 1,
			want:    Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 1, ResetAfter: 1},
		},
		{
			name:    "waiting RetryAfter is enough",
			advance: 1,
			want:    Result{Allowed: true, Limit: 10, Remaining: 9, ResetAfter: time.Second},
		},
	}...)

	runResultSteps(t, fw, clock, steps)
}

func TestFixedWindowAlignsToWallClock(t *testing.T) {
	// Construct 300ms into a wall-clock second: the first window still ends
	// on the second, not 1s after construction.
	clock := newFakeClock()
	clock.Advance(300 * time.Millisecond)
	fw, err := NewFixedWindow(PerSecond(10), WithClock(clock))
	if err != nil {
		t.Fatalf("NewFixedWindow() = %v", err)
	}

	runResultSteps(t, fw, clock, []resultStep{
		{
			name: "first request",
			want: Result{Allowed: true, Limit: 10, Remaining: 9, ResetAfter: 700 * time.Millisecond},
		},
	})
}

func TestFixedWindowClockBackwards(t *testing.T) {
	fw, clock := newTestFixedWindow(t, PerSecond(10))

	allowN(t, fw, "k", 10)
	clock.Advance(-time.Hour)
	if got := allowN(t, fw, "k", 10); got != 0 {
		t.Fatalf("after clock moved backwards: allowed %d, want 0 (no reset)", got)
	}
}
