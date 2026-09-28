package tollgate

import (
	"sync"
	"time"
)

// fakeClock is a Clock whose time only moves when Advance is called.
// It is safe for concurrent use.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// newFakeClock returns a fakeClock set to a fixed, arbitrary instant.
func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// Now returns the fake current time.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the fake clock forward by d.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
