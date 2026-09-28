// Package tollgate provides rate limiters for Go.
//
// The Limiter interface is satisfied by every algorithm implementation, along
// with the supporting types (Rate, Result) and a Clock abstraction so that
// implementations can be tested without relying on wall-clock time.
//
// In-memory algorithms live in this package, so callers write, for example:
//
//	limiter, err := tollgate.NewTokenBucket(tollgate.PerSecond(10))
//
// Subpackages are reserved for code that pulls in separate dependencies, such
// as a Redis-backed store or net/http middleware, so that importing tollgate
// alone stays dependency-free.
package tollgate
