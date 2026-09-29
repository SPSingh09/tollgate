// Package middleware provides net/http middleware that applies a
// tollgate.Limiter to incoming requests.
package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/SPSingh09/tollgate"
)

// config holds the settings that Options modify.
type config struct {
	failOpen bool
}

// Option configures RateLimit.
type Option func(*config)

// WithFailOpen makes RateLimit let a request through when the limiter
// returns an error, instead of rejecting it with 503 Service Unavailable.
//
// Neither choice is safe in every deployment:
//
//   - Fail-closed (the default) means a broken limiter, such as an
//     unreachable backing store, rejects every request. Your own safety
//     mechanism then causes the outage it was meant to prevent.
//   - Fail-open keeps serving while the limiter is broken, but with no limit
//     at all, so the traffic the limiter was protecting against can overload
//     the service behind it.
//
// Fail-open suits limiters that protect fairness or cost; fail-closed suits
// ones that protect a resource that cannot survive unlimited load.
//
// WithFailOpen does not apply to an empty key, which is always rejected; see
// RateLimit.
func WithFailOpen() Option {
	return func(c *config) {
		c.failOpen = true
	}
}

// RateLimit returns middleware that calls lim.Allow with the request's
// context and the key keyFunc returns for the request.
//
// Allowed requests are passed to the next handler, with the RateLimit-Limit,
// RateLimit-Remaining and RateLimit-Reset headers set from the Result.
// Denied requests get 429 Too Many Requests, the same headers and a
// Retry-After header, and the next handler is not called. Durations are
// sent as whole seconds, rounded up, so a client that waits that long is
// not denied again for having retried early.
//
// If lim.Allow returns an error, the request is rejected with 503 Service
// Unavailable, or passed through without rate limit headers under
// WithFailOpen.
//
// If keyFunc returns an empty key, the request is rejected with 400 Bad
// Request whatever the failure mode, and Allow is not called. The key is
// usually derived from the request, so failing open here would let any
// client bypass the limit by, for example, omitting a header.
//
// RateLimit panics if lim or keyFunc is nil.
func RateLimit(lim tollgate.Limiter, keyFunc func(*http.Request) string, opts ...Option) func(http.Handler) http.Handler {
	if lim == nil {
		panic("middleware: RateLimit called with a nil Limiter")
	}
	if keyFunc == nil {
		panic("middleware: RateLimit called with a nil keyFunc")
	}
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)
			if key == "" {
				http.Error(w, "rate limit key missing", http.StatusBadRequest)
				return
			}

			res, err := lim.Allow(r.Context(), key)
			if err != nil {
				if cfg.failOpen {
					next.ServeHTTP(w, r)
					return
				}
				http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
				return
			}

			setHeaders(w.Header(), res)
			if !res.Allowed {
				w.Header().Set("Retry-After", strconv.FormatInt(max(ceilSeconds(res.RetryAfter), 1), 10))
				http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// setHeaders sets the RateLimit-* headers from res.
func setHeaders(h http.Header, res tollgate.Result) {
	h.Set("RateLimit-Limit", strconv.Itoa(res.Limit))
	h.Set("RateLimit-Remaining", strconv.Itoa(max(res.Remaining, 0)))
	h.Set("RateLimit-Reset", strconv.FormatInt(ceilSeconds(res.ResetAfter), 10))
}

// ceilSeconds returns d in whole seconds, rounded up. Negative durations
// count as zero.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}
