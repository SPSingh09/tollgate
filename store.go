package tollgate

import "sync"

// shard is one partition of a shardedStore, guarded by its own lock.
type shard[V any] struct {
	mu sync.Mutex
	m  map[string]V

	// Pad to a cache line so neighbouring shards' locks in the shard slice
	// do not contend through false sharing.
	_ [48]byte
}

// shardedStore holds per-key limiter state split across a power-of-two
// number of shards, each with its own lock, so that requests for unrelated
// keys rarely contend. A key's shard is selected by its FNV-1a hash.
type shardedStore[V any] struct {
	shards []shard[V]
	mask   uint32 // len(shards)-1
}

// newShardedStore returns a store with n shards. n must be a power of two.
func newShardedStore[V any](n int) shardedStore[V] {
	shards := make([]shard[V], n)
	for i := range shards {
		shards[i].m = make(map[string]V)
	}
	return shardedStore[V]{shards: shards, mask: uint32(n - 1)}
}

// shardFor returns the shard holding key. Callers must hold its lock for the
// whole read-modify-write of the key's state; splitting that sequence lets
// two goroutines act on the same stale state.
func (s *shardedStore[V]) shardFor(key string) *shard[V] {
	return &s.shards[fnv1a(key)&s.mask]
}

// fnv1a returns the 32-bit FNV-1a hash of s. It is inlined here rather than
// using hash/fnv to avoid allocating a hasher and copying key to a []byte on
// every call.
func fnv1a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}
