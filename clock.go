package tollgate

import "time"

// Clock provides the current time. It exists so that Limiter implementations
// can be tested with a fake clock instead of relying on wall-clock time.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
}

// realClock is a Clock backed by the standard library's time.Now.
type realClock struct{}

// Now returns time.Now().
func (realClock) Now() time.Time {
	return time.Now()
}

// SystemClock is the default Clock, backed by time.Now.
var SystemClock Clock = realClock{}
