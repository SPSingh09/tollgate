// Package tollgate provides interfaces and shared types for building
// rate limiters in Go.
//
// tollgate itself does not implement a rate-limiting algorithm. It defines
// the Limiter interface that algorithm implementations satisfy, along with
// the supporting types (Rate, Result) and a Clock abstraction so that
// implementations can be tested without relying on wall-clock time.
//
// This package is a work in progress; algorithm implementations (token
// bucket, sliding window, etc.) will live in subpackages.
package tollgate
