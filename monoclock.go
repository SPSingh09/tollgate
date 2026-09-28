package tollgate

import "time"

// monoClock reads a Clock as time elapsed since a base captured at
// construction.
//
// It deliberately avoids calling UnixNano on each reading: that discards the
// monotonic clock reading, so a wall-clock jump (NTP correction, VM
// migration) would corrupt refill and window arithmetic. Sub between two
// Times that both carry a monotonic reading uses only the monotonic clock and
// is immune to such jumps.
type monoClock struct {
	clock    Clock
	base     time.Time
	baseUnix int64 // base as Unix nanoseconds
}

// newMonoClock returns a monoClock whose base is c.Now().
func newMonoClock(c Clock) monoClock {
	base := c.Now()
	return monoClock{clock: c, base: base, baseUnix: base.UnixNano()}
}

// nowNanos returns the current time as nanoseconds since base.
func (m monoClock) nowNanos() int64 {
	return int64(m.clock.Now().Sub(m.base))
}

// unixNanos returns the current time as Unix nanoseconds, computed as base's
// wall-clock reading plus the monotonic time elapsed since. Window limiters
// use it so that windows align to wall-clock boundaries (a per-minute window
// starts on the minute) while later wall-clock jumps cannot move them.
func (m monoClock) unixNanos() int64 {
	return m.baseUnix + m.nowNanos()
}
