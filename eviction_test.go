package tollgate

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// All tests here use a single shard so that traffic on one key drives sweeps
// over every other key.

func TestEvictionReclaimsIdleEntries(t *testing.T) {
	const idleKeys = 1000

	for _, ctor := range limiterCtors {
		t.Run(ctor.name, func(t *testing.T) {
			clock := newFakeClock()
			l, err := ctor.new(PerSecond(10), WithClock(clock), WithShards(1))
			if err != nil {
				t.Fatalf("constructor = %v", err)
			}

			for i := range idleKeys {
				allowN(t, l, fmt.Sprintf("idle-%d", i), 1)
			}
			if got := len(storeKeys(t, l)); got != idleKeys {
				t.Fatalf("after one request per key: %d entries, want %d (none idle yet)", got, idleKeys)
			}

			// Two periods is long enough for every algorithm's state to lapse:
			// a token bucket refills, and both windows of a sliding window end.
			clock.Advance(2 * time.Second)

			// Drain "busy", then keep calling it until the shard has seen
			// enough operations to sweep.
			allowN(t, l, "busy", idleKeys+100)

			keys := storeKeys(t, l)
			if len(keys) != 1 || !keys["busy"] {
				t.Fatalf("after sweep: entries %d (busy present: %v), want only busy", len(keys), keys["busy"])
			}
			if got := allowN(t, l, "busy", 1); got != 0 {
				t.Fatalf("busy after sweep: allowed %d, want 0 (its state must survive)", got)
			}
		})
	}
}

func TestEvictionPreservesBehaviour(t *testing.T) {
	for _, ctor := range limiterCtors {
		t.Run(ctor.name, func(t *testing.T) {
			clock := newFakeClock()
			l, err := ctor.new(PerSecond(10), WithClock(clock), WithShards(1))
			if err != nil {
				t.Fatalf("constructor = %v", err)
			}

			allowN(t, l, "old", 7)
			clock.Advance(2 * time.Second)
			allowN(t, l, "driver", minSweepInterval)
			if storeKeys(t, l)["old"] {
				t.Fatalf("old was not evicted")
			}

			// From here, the evicted key must behave exactly like one never
			// seen, call for call.
			ctx := context.Background()
			schedule := []time.Duration{0, 250 * time.Millisecond, time.Second, 1500 * time.Millisecond}
			for _, advance := range schedule {
				clock.Advance(advance)
				for i := range 12 {
					old, err := l.Allow(ctx, "old")
					if err != nil {
						t.Fatalf("Allow(old) = %v", err)
					}
					fresh, err := l.Allow(ctx, "fresh")
					if err != nil {
						t.Fatalf("Allow(fresh) = %v", err)
					}
					if old != fresh {
						t.Fatalf("after advancing %s, call %d: evicted key got %+v, fresh key got %+v", advance, i, old, fresh)
					}
				}
			}
		})
	}
}

func TestTokenBucketEvictionGracePeriod(t *testing.T) {
	// At 10/sec with a burst of 10, one token refills in 100ms and the whole
	// bucket in 1s. A bucket that is full again but was touched within the
	// last second must be kept, so that keys returning at short intervals
	// are not deleted and re-created on every visit.
	tests := []struct {
		advance  time.Duration
		wantKept bool
	}{
		{500 * time.Millisecond, true}, // full since 100ms, but touched recently
		{time.Second - 1, true},        // one nanosecond short of the grace period
		{time.Second, false},           // untouched for a full refill
		{10 * time.Second, false},      // long idle
	}

	for _, tt := range tests {
		t.Run(tt.advance.String(), func(t *testing.T) {
			clock := newFakeClock()
			tb, err := NewTokenBucket(PerSecond(10), WithClock(clock), WithShards(1))
			if err != nil {
				t.Fatalf("NewTokenBucket() = %v", err)
			}
			allowN(t, tb, "k", 1)
			clock.Advance(tt.advance)
			allowN(t, tb, "other", minSweepInterval)

			if got := storeKeys(t, tb)["k"]; got != tt.wantKept {
				t.Fatalf("k kept = %v, want %v", got, tt.wantKept)
			}
		})
	}
}

func TestEvictionKeepsEntriesWithState(t *testing.T) {
	// A key is drained, the clock advanced by less than it takes for its
	// state to lapse, and sweeps are forced. The state must survive: wantAfter
	// is what the key allows afterwards, not the 10 of a fresh key.
	tests := []struct {
		ctor      limiterCtor
		advance   time.Duration
		wantAfter int
	}{
		{limiterCtors[0], 0, 0},
		{limiterCtors[0], 500 * time.Millisecond, 5}, // half refilled
		{limiterCtors[1], 0, 0},
		{limiterCtors[1], 999 * time.Millisecond, 0}, // window not yet ended
		{limiterCtors[2], 0, 0},
		{limiterCtors[2], 1500 * time.Millisecond, 5}, // previous window counts half
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/after %s", tt.ctor.name, tt.advance), func(t *testing.T) {
			clock := newFakeClock()
			l, err := tt.ctor.new(PerSecond(10), WithClock(clock), WithShards(1))
			if err != nil {
				t.Fatalf("constructor = %v", err)
			}
			if got := allowN(t, l, "k", 10); got != 10 {
				t.Fatalf("allowed %d, want 10", got)
			}
			clock.Advance(tt.advance)
			allowN(t, l, "other", 10*minSweepInterval)
			if !storeKeys(t, l)["k"] {
				t.Fatalf("k was evicted while it still had state")
			}
			if got := allowN(t, l, "k", 10); got != tt.wantAfter {
				t.Fatalf("k after sweeps: allowed %d, want %d", got, tt.wantAfter)
			}
		})
	}
}
