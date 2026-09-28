package tollgate

import "sync"

// shard is one partition of a shardedStore, guarded by its own lock.
type shard[V any] struct {
	mu  sync.Mutex
	m   map[string]V
	ops int // operations since the last sweep

	// Pad to a cache line so neighbouring shards' locks in the shard slice
	// do not contend through false sharing.
	_ [40]byte
}

// minSweepInterval is the fewest operations between sweeps of a shard, so
// that tiny shards are not rescanned on every call.
const minSweepInterval = 64

// sweepIfDue records one operation on the shard and, once the shard has seen
// at least as many operations since its last sweep as it holds entries,
// deletes every entry for which idle reports true. The caller must hold s.mu.
//
// idle must report true only for entries that carry no state: ones for
// which the next Allow would behave exactly as for a key never seen. Deleting
// them is then invisible to callers.
//
// Sweeping here, on the request path, rather than in a background goroutine
// is a deliberate tradeoff:
//
//   - There is no goroutine to start or stop, so limiters need no Close
//     method and cannot leak one.
//   - The cost is amortized O(1) per call: a sweep over n entries runs only
//     after at least n operations on that shard.
//   - But the call that triggers a sweep holds the shard lock for the whole
//     scan, a latency spike proportional to the shard's size (keys divided by
//     shards). Sharding keeps that bounded; more shards shrink it further.
//   - And a shard is swept only when it receives traffic, so a limiter that
//     goes completely quiet keeps its memory until it is used again.
func (s *shard[V]) sweepIfDue(idle func(V) bool) {
	s.ops++
	if s.ops < max(len(s.m), minSweepInterval) {
		return
	}
	s.ops = 0
	for k, v := range s.m {
		if idle(v) {
			delete(s.m, k)
		}
	}
}

// shardedStore holds per-key limiter state split across a power-of-two
// number of shards, each with its own lock, so that requests for unrelated
// keys rarely contend. A key's shard is selected by its FNV-1a hash.
//
// State is stored in the map by value rather than by pointer. That is a
// workload tradeoff, not a clear win, measured on TokenBucket: a pointer lets
// Allow update state in place without writing it back to the map, which made
// a single contended key about 11% faster; values made 100k keys 8-13%
// faster and cost no allocation when an evicted key is re-created. Per-key
// memory was within 3% either way. Values favour the many-keys workload a
// sharded store exists for.
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

// fnv1a returns the 32-bit FNV-1a hash of s. It is written out rather than
// using hash/fnv for a small, measured gain: hash/fnv does not allocate here
// either, since the compiler keeps the hasher on the stack, but it is about
// 0.3ns slower per call.
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
