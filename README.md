# tollgate

In-memory rate limiters for Go: token bucket, fixed window and sliding window,
behind one `Limiter` interface.

```go
limiter, err := tollgate.NewTokenBucket(tollgate.PerSecond(10))
if err != nil {
	return err
}

res, err := limiter.Allow(ctx, clientIP)
if err != nil {
	return err
}
if !res.Allowed {
	w.Header().Set("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())+1))
	http.Error(w, "rate limited", http.StatusTooManyRequests)
	return
}
```

| Constructor | Algorithm | Notes |
|---|---|---|
| `NewTokenBucket` | Token bucket with lazy refill | Supports `Rate.Burst`. |
| `NewFixedWindow` | Counter per wall-clock-aligned window | Allows up to 2×Limit across a window boundary. |
| `NewSlidingWindow` | Weighted previous + current window | Prevents the boundary burst, using constant memory per key. |

All three are safe for concurrent use, run no background goroutines, and
delete a key's state once it is indistinguishable from a key never seen.

## Performance

`go test -bench Allow -count=10`, compared with
[benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat). Measured on a
12th Gen Intel i5-1235U with 4 vCPUs under WSL2, so run-to-run noise is
±5–15%: treat differences smaller than that as no difference. The real clock is
used, so `time.Now` is included in every call.

| ns/op | 1 hot key, serial | 1 hot key, 4 goroutines | 100k keys, serial | 100k keys, 4 goroutines |
|---|---|---|---|---|
| TokenBucket | 103 | 162 | 171 | 105 |
| FixedWindow | 102 | 157 | 159 | 104 |
| SlidingWindow | 104 | 165 | 176 | 109 |

The main table reproduces with `go test -bench=. -count=10`. The eviction,
padding and value-vs-pointer comparisons were run against one-off variant
builds not kept in the repo.

Every `Allow` makes zero allocations. Live heap per key at 100k keys is about
53 B for TokenBucket and FixedWindow and 63 B for SlidingWindow.

**Sharding.** With 4 goroutines, 100k keys run about 35% faster per call than
one hot key (FixedWindow: 104 ns vs 157 ns). Many keys spread across 64
independently locked shards and stop contending; one hot key serialises on a
single lock. That is the gap sharding exists to create.

## Design decisions, measured

Each of these was benchmarked rather than assumed. Two initial justifications
did not survive measurement, and are recorded here as such.

- **Eviction waits for a full refill.** A token bucket was first deleted as
  soon as it was full. At 1000/sec with 100k keys, each key was full again
  by the time it was revisited, so almost every call deleted and re-created
  its entry. Deleting only buckets left untouched for a whole refill from
  empty cut 100k-key TokenBucket from 253 ns to 171 ns serial (−33%) and
  from 181 ns to 105 ns with 4 goroutines (−42%). Idle keys are still
  reclaimed within one refill period.

- **State is stored by value, not pointer: a workload tradeoff.** This was
  first justified as saving memory; measured, per-key memory differed by
  under 3%, slightly in the pointer's favour. What values do buy is speed
  with many keys (8–13% faster at 100k keys) and no allocation when an
  evicted key is re-created. Pointers are about 11% faster on one contended
  key, because state is updated in place without a map write-back. Values
  fit the many-keys workload the sharded store is built for.

- **Shards are padded to a cache line.** No effect in 10 of 12 benchmarks;
  removing padding cost 6–8% in the two 100k-key parallel cases, the only
  ones where goroutines hit different shards and so the only ones it
  targets. It costs 2.5 KB per limiter.

- **FNV-1a is written out instead of using `hash/fnv`: a marginal choice.**
  This was first justified as avoiding an allocation per call; measured,
  `hash/fnv` does not allocate either, and the difference is 3.4 ns vs
  3.6 ns.

- **Sliding window arithmetic is exact.** Decisions and `RetryAfter` use
  128-bit integer products instead of floats, so waiting exactly
  `RetryAfter` always succeeds, and large limits × long periods (for example
  1M per day) cannot overflow.

- **Windows align to the wall clock.** A per-minute window starts on the
  minute, so a limiter built at 10:00:30 has a 30-second first window. Time
  is read as the wall clock at construction plus monotonic time since, so
  later wall-clock jumps (NTP, VM migration) cannot move window boundaries or
  corrupt refill.
