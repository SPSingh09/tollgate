package middleware

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// KeyByIP returns the client IP address for r, taken from the last entry of
// the X-Forwarded-For header if it holds a valid address, and otherwise from
// r.RemoteAddr. Addresses are normalised, so equivalent spellings of one
// address share a key.
//
// Only use KeyByIP behind a trusted reverse proxy that appends the address it
// received the request from to X-Forwarded-For. Without one, the header comes
// straight from the client: anyone can send a different made-up address on
// every request, get a fresh limit each time, and bypass the limiter
// entirely. Servers that clients reach directly should use KeyByRemoteAddr.
//
// KeyByIP reads the last entry, not the first, because a proxy appends to
// whatever X-Forwarded-For the client sent rather than replacing it. The
// first entry is therefore client-controlled even behind a trusted proxy;
// the last is the one the proxy wrote. That assumes exactly one proxy. With
// several (a CDN in front of a load balancer, say), the last entry is the
// address of the outer proxy, so every client would share one key; in that
// case use KeyByHeader with a header your outermost proxy sets to the client
// address and strips from incoming requests.
//
// Each IPv6 address gets its own key, so a client holding a whole IPv6
// prefix can rotate addresses within it.
func KeyByIP(r *http.Request) string {
	if values := r.Header.Values("X-Forwarded-For"); len(values) > 0 {
		entries := strings.Split(values[len(values)-1], ",")
		if addr, ok := parseAddr(strings.TrimSpace(entries[len(entries)-1])); ok {
			return addr
		}
	}
	return KeyByRemoteAddr(r)
}

// KeyByRemoteAddr returns the IP address of the connection r arrived on,
// ignoring any forwarding headers. Behind a reverse proxy that is the
// proxy's address, so every client would share one key; use KeyByIP there.
func KeyByRemoteAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if addr, ok := parseAddr(host); ok {
		return addr
	}
	return host
}

// KeyByHeader returns a key function that uses the value of the named
// request header, with surrounding whitespace removed. A request without the
// header gets an empty key, which RateLimit rejects with 400 Bad Request.
//
// The header's value is whatever the client sent. If it is an API key or
// similar credential, authenticate it before the limiter runs; otherwise a
// client can send a new made-up value on every request and bypass the limit.
func KeyByHeader(name string) func(*http.Request) string {
	return func(r *http.Request) string {
		return strings.TrimSpace(r.Header.Get(name))
	}
}

// parseAddr parses s as an IP address, with or without a port, and returns
// it in canonical form. IPv4-mapped IPv6 addresses are returned as IPv4.
func parseAddr(s string) (string, bool) {
	if addr, err := netip.ParseAddr(s); err == nil {
		return addr.Unmap().String(), true
	}
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return ap.Addr().Unmap().String(), true
	}
	return "", false
}
