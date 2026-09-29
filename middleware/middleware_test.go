package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/SPSingh09/tollgate"
)

// stubLimiter returns a fixed Result and error, and records its calls.
type stubLimiter struct {
	res tollgate.Result
	err error

	mu     sync.Mutex
	calls  int
	gotCtx context.Context
	gotKey string
}

func (s *stubLimiter) Allow(ctx context.Context, key string) (tollgate.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.gotCtx = ctx
	s.gotKey = key
	return s.res, s.err
}

// recordingHandler records whether it was called and responds 200 OK.
type recordingHandler struct {
	called bool
}

func (h *recordingHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	h.called = true
	w.WriteHeader(http.StatusOK)
}

// serve runs one request through RateLimit(lim, keyFunc, opts...) in front
// of a recordingHandler.
func serve(t *testing.T, lim tollgate.Limiter, keyFunc func(*http.Request) string, req *http.Request, opts ...Option) (*httptest.ResponseRecorder, *recordingHandler) {
	t.Helper()
	next := &recordingHandler{}
	rec := httptest.NewRecorder()
	RateLimit(lim, keyFunc, opts...)(next).ServeHTTP(rec, req)
	return rec, next
}

func constKey(key string) func(*http.Request) string {
	return func(*http.Request) string { return key }
}

// assertHeaders checks the rate limit headers on rec. An empty want value
// asserts that the header is absent.
func assertHeaders(t *testing.T, rec *httptest.ResponseRecorder, want map[string]string) {
	t.Helper()
	for name, v := range want {
		if got := rec.Header().Get(name); got != v {
			t.Errorf("header %s = %q, want %q", name, got, v)
		}
	}
}

func TestRateLimitAllowed(t *testing.T) {
	lim := &stubLimiter{res: tollgate.Result{
		Allowed:    true,
		Limit:      10,
		Remaining:  7,
		ResetAfter: 1500 * time.Millisecond,
	}}
	rec, next := serve(t, lim, constKey("k"), httptest.NewRequest(http.MethodGet, "/", nil))

	if !next.called {
		t.Fatal("next handler was not called for an allowed request")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	assertHeaders(t, rec, map[string]string{
		"RateLimit-Limit":     "10",
		"RateLimit-Remaining": "7",
		"RateLimit-Reset":     "2",
		"Retry-After":         "",
	})
}

func TestRateLimitDenied(t *testing.T) {
	lim := &stubLimiter{res: tollgate.Result{
		Allowed:    false,
		Limit:      10,
		Remaining:  0,
		RetryAfter: 1200 * time.Millisecond,
		ResetAfter: 30 * time.Second,
	}}
	rec, next := serve(t, lim, constKey("k"), httptest.NewRequest(http.MethodGet, "/", nil))

	if next.called {
		t.Fatal("next handler was called for a denied request")
	}
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	assertHeaders(t, rec, map[string]string{
		"Retry-After":         "2",
		"RateLimit-Limit":     "10",
		"RateLimit-Remaining": "0",
		"RateLimit-Reset":     "30",
	})
}

func TestRateLimitLimiterError(t *testing.T) {
	tests := []struct {
		name       string
		opts       []Option
		wantStatus int
		wantNext   bool
	}{
		{"fail-closed by default", nil, http.StatusServiceUnavailable, false},
		{"fail-open", []Option{WithFailOpen()}, http.StatusOK, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lim := &stubLimiter{err: errors.New("store unreachable")}
			rec, next := serve(t, lim, constKey("k"), httptest.NewRequest(http.MethodGet, "/", nil), tt.opts...)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if next.called != tt.wantNext {
				t.Fatalf("next called = %v, want %v", next.called, tt.wantNext)
			}
			// With no Result there is nothing to report.
			assertHeaders(t, rec, map[string]string{
				"RateLimit-Limit": "",
				"Retry-After":     "",
			})
		})
	}
}

func TestRateLimitEmptyKeyAlwaysRejected(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
	}{
		{"fail-closed", nil},
		{"fail-open", []Option{WithFailOpen()}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lim := &stubLimiter{res: tollgate.Result{Allowed: true}}
			rec, next := serve(t, lim, constKey(""), httptest.NewRequest(http.MethodGet, "/", nil), tt.opts...)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
			if next.called {
				t.Fatal("next handler was called for an empty key")
			}
			if lim.calls != 0 {
				t.Fatalf("Allow called %d times, want 0", lim.calls)
			}
		})
	}
}

func TestRateLimitPassesRequestContextAndKey(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "request-scoped")
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)

	lim := &stubLimiter{res: tollgate.Result{Allowed: true}}
	serve(t, lim, KeyByHeader("X-API-Key"), withHeader(req, "X-API-Key", "abc"))

	if lim.gotCtx == nil || lim.gotCtx.Value(ctxKey{}) != "request-scoped" {
		t.Fatalf("Allow did not receive the request's context")
	}
	if lim.gotKey != "abc" {
		t.Fatalf("Allow key = %q, want %q", lim.gotKey, "abc")
	}
}

func TestRateLimitNilArgumentsPanic(t *testing.T) {
	tests := []struct {
		name    string
		lim     tollgate.Limiter
		keyFunc func(*http.Request) string
	}{
		{"nil limiter", nil, constKey("k")},
		{"nil keyFunc", &stubLimiter{}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("RateLimit did not panic")
				}
			}()
			RateLimit(tt.lim, tt.keyFunc)
		})
	}
}

func TestCeilSeconds(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want int64
	}{
		{-time.Second, 0},
		{0, 0},
		{1, 1},
		{time.Second, 1},
		{time.Second + 1, 2},
		{90 * time.Second, 90},
	}
	for _, tt := range tests {
		if got := ceilSeconds(tt.d); got != tt.want {
			t.Errorf("ceilSeconds(%s) = %d, want %d", tt.d, got, tt.want)
		}
	}
}

// fakeClock is a tollgate.Clock that only moves when Advance is called.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestRateLimitWithRealLimiter(t *testing.T) {
	// 10 seconds into a minute, so the window resets 50 seconds later.
	clock := &fakeClock{now: time.Date(2026, 1, 1, 10, 0, 10, 0, time.UTC)}
	lim, err := tollgate.NewFixedWindow(tollgate.PerMinute(2), tollgate.WithClock(clock))
	if err != nil {
		t.Fatalf("NewFixedWindow() = %v", err)
	}

	request := func(remoteAddr string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = remoteAddr
		rec, _ := serve(t, lim, KeyByRemoteAddr, req)
		return rec
	}

	for i, want := range []int{http.StatusOK, http.StatusOK, http.StatusTooManyRequests} {
		if got := request("192.0.2.1:1234").Code; got != want {
			t.Fatalf("request %d from 192.0.2.1: status = %d, want %d", i+1, got, want)
		}
	}

	rec := request("192.0.2.1:5678") // same client, new source port
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("same IP, different port: status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	assertHeaders(t, rec, map[string]string{
		"Retry-After":         "50",
		"RateLimit-Limit":     "2",
		"RateLimit-Remaining": "0",
		"RateLimit-Reset":     "50",
	})

	if got := request("192.0.2.2:1234").Code; got != http.StatusOK {
		t.Fatalf("other client: status = %d, want %d", got, http.StatusOK)
	}

	clock.Advance(50 * time.Second)
	if got := request("192.0.2.1:1234").Code; got != http.StatusOK {
		t.Fatalf("after the window reset: status = %d, want %d", got, http.StatusOK)
	}
}

func withHeader(r *http.Request, name, value string) *http.Request {
	r.Header.Set(name, value)
	return r
}
