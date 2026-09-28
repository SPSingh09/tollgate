package tollgate

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

const benchKeyCount = 100_000

// benchRate is high enough that, with 100k keys, most requests are allowed,
// and low enough that a single hot key is mostly denied. Both paths do the
// same work under the lock.
var benchRate = Rate{Limit: 1000, Period: time.Second}

// benchKeys returns n distinct keys, built before timing starts so that key
// construction is not measured.
func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("client-%d", i)
	}
	return keys
}

// BenchmarkAllow measures Allow for each limiter with one hot key and with
// 100k keys, from one goroutine and from GOMAXPROCS goroutines. It uses the
// real clock, so time.Now is part of the cost.
func BenchmarkAllow(b *testing.B) {
	keySets := []struct {
		name string
		keys []string
	}{
		{"hot", []string{"hot"}},
		{"100k", benchKeys(benchKeyCount)},
	}

	for _, ctor := range limiterCtors {
		for _, ks := range keySets {
			b.Run(ctor.name+"/"+ks.name+"/serial", func(b *testing.B) {
				l := newBenchLimiter(b, ctor, ks.keys)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := l.Allow(ctx, ks.keys[i%len(ks.keys)]); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run(ctor.name+"/"+ks.name+"/parallel", func(b *testing.B) {
				l := newBenchLimiter(b, ctor, ks.keys)
				ctx := context.Background()
				var worker atomic.Int64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					// Start each goroutine at a different point in the key set.
					i := int(worker.Add(1)*7919) % len(ks.keys)
					for pb.Next() {
						if _, err := l.Allow(ctx, ks.keys[i]); err != nil {
							b.Error(err)
							return
						}
						if i++; i == len(ks.keys) {
							i = 0
						}
					}
				})
			})
		}
	}
}

// newBenchLimiter returns a limiter with an entry for every key, so that the
// timed loop measures steady state rather than map growth.
func newBenchLimiter(b *testing.B, ctor limiterCtor, keys []string) Limiter {
	b.Helper()
	l, err := ctor.new(benchRate)
	if err != nil {
		b.Fatal(err)
	}
	for _, k := range keys {
		if _, err := l.Allow(context.Background(), k); err != nil {
			b.Fatal(err)
		}
	}
	return l
}

// BenchmarkMemoryPerKey reports the live heap each limiter holds per key,
// as the custom B/key metric, after inserting 100k keys. Key strings are
// built beforehand and excluded. ns/op is the time to insert all 100k keys.
func BenchmarkMemoryPerKey(b *testing.B) {
	keys := benchKeys(benchKeyCount)

	for _, ctor := range limiterCtors {
		b.Run(ctor.name, func(b *testing.B) {
			ctx := context.Background()
			var total int64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				before := liveHeap()
				b.StartTimer()

				// A fake clock that never advances keeps every entry live.
				l, err := ctor.new(benchRate, WithClock(newFakeClock()))
				if err != nil {
					b.Fatal(err)
				}
				for _, k := range keys {
					if _, err := l.Allow(ctx, k); err != nil {
						b.Fatal(err)
					}
				}

				b.StopTimer()
				total += int64(liveHeap() - before)
				runtime.KeepAlive(l)
				b.StartTimer()
			}
			b.ReportMetric(float64(total)/float64(b.N)/benchKeyCount, "B/key")
		})
	}
}

// liveHeap returns the bytes of live heap objects after a full GC.
func liveHeap() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
