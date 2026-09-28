package tollgate

import (
	"errors"
	"fmt"
	"math/bits"
)

// ErrInvalidOption is returned by a constructor when an Option is given an
// invalid value. Use errors.Is to check for it, since it is wrapped with
// additional detail.
var ErrInvalidOption = errors.New("tollgate: invalid option")

const (
	// defaultShards is the number of shards used when WithShards is not given.
	defaultShards = 64

	// maxShards bounds WithShards so that rounding up to a power of two
	// cannot overflow and a typo cannot allocate an absurd shard table.
	maxShards = 1 << 16
)

// config holds the settings that Options modify.
type config struct {
	clock  Clock
	shards int
}

// Option configures a limiter constructor such as NewTokenBucket.
type Option func(*config)

// WithClock sets the Clock used to read the current time. It defaults to
// SystemClock and is mainly useful for tests.
func WithClock(c Clock) Option {
	return func(cfg *config) {
		cfg.clock = c
	}
}

// WithShards sets the number of shards that per-key state is split across.
// Each shard has its own lock, so more shards means less contention between
// unrelated keys. n is rounded up to the next power of two and must be
// between 1 and 65536. It defaults to 64.
func WithShards(n int) Option {
	return func(cfg *config) {
		cfg.shards = n
	}
}

// newConfig applies opts over the defaults and validates the result.
func newConfig(opts []Option) (config, error) {
	cfg := config{
		clock:  SystemClock,
		shards: defaultShards,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.clock == nil {
		return config{}, fmt.Errorf("%w: clock must not be nil", ErrInvalidOption)
	}
	if cfg.shards < 1 || cfg.shards > maxShards {
		return config{}, fmt.Errorf("%w: shards must be between 1 and %d, got %d", ErrInvalidOption, maxShards, cfg.shards)
	}
	cfg.shards = nextPowerOfTwo(cfg.shards)
	return cfg, nil
}

// nextPowerOfTwo returns the smallest power of two >= n, for n >= 1.
func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}
