package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestKeyByIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        []string // one element per X-Forwarded-For header line
		want       string
	}{
		{
			name:       "no header uses RemoteAddr",
			remoteAddr: "192.0.2.1:1234",
			want:       "192.0.2.1",
		},
		{
			name:       "single entry",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"203.0.113.7"},
			want:       "203.0.113.7",
		},
		{
			name:       "client-supplied entries are ignored in favour of the last",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"198.51.100.99, 203.0.113.7"},
			want:       "203.0.113.7",
		},
		{
			name:       "last entry of the last header line",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"198.51.100.99", "203.0.113.7"},
			want:       "203.0.113.7",
		},
		{
			name:       "entry with port",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"203.0.113.7:4711"},
			want:       "203.0.113.7",
		},
		{
			name:       "IPv6 is normalised",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"2001:DB8:0:0::1"},
			want:       "2001:db8::1",
		},
		{
			name:       "IPv4-mapped IPv6 becomes IPv4",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"::ffff:203.0.113.7"},
			want:       "203.0.113.7",
		},
		{
			// An invalid last entry is not skipped in favour of an earlier
			// one, since earlier entries are client-controlled.
			name:       "invalid last entry falls back to RemoteAddr",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"203.0.113.7, garbage"},
			want:       "10.0.0.1",
		},
		{
			name:       "empty header falls back to RemoteAddr",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{""},
			want:       "10.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			for _, v := range tt.xff {
				req.Header.Add("X-Forwarded-For", v)
			}
			if got := KeyByIP(req); got != tt.want {
				t.Fatalf("KeyByIP() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestKeyByRemoteAddr(t *testing.T) {
	tests := []struct {
		remoteAddr string
		want       string
	}{
		{"192.0.2.1:1234", "192.0.2.1"},
		{"[2001:db8::1]:443", "2001:db8::1"},
		{"192.0.2.1", "192.0.2.1"},
		{"", ""},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = tt.remoteAddr
		req.Header.Set("X-Forwarded-For", "203.0.113.7")
		if got := KeyByRemoteAddr(req); got != tt.want {
			t.Errorf("KeyByRemoteAddr(%q) = %q, want %q (X-Forwarded-For must be ignored)", tt.remoteAddr, got, tt.want)
		}
	}
}

func TestKeyByHeader(t *testing.T) {
	keyFunc := KeyByHeader("X-API-Key")
	tests := []struct {
		name  string
		value *string
		want  string
	}{
		{"present", ptr("abc"), "abc"},
		{"whitespace trimmed", ptr("  abc \t"), "abc"},
		{"missing", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.value != nil {
				req.Header.Set("X-API-Key", *tt.value)
			}
			if got := keyFunc(req); got != tt.want {
				t.Fatalf("key = %q, want %q", got, tt.want)
			}
		})
	}
}

func ptr(s string) *string { return &s }
