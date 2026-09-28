package tollgate

import "testing"

func TestShardCount(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want int
	}{
		{"default", nil, defaultShards},
		{"1", []Option{WithShards(1)}, 1},
		{"2", []Option{WithShards(2)}, 2},
		{"3 rounds up", []Option{WithShards(3)}, 4},
		{"64", []Option{WithShards(64)}, 64},
		{"65 rounds up", []Option{WithShards(65)}, 128},
		{"1000 rounds up", []Option{WithShards(1000)}, 1024},
		{"max", []Option{WithShards(maxShards)}, maxShards},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := newConfig(tt.opts)
			if err != nil {
				t.Fatalf("newConfig() = %v", err)
			}
			store := newShardedStore[int](cfg.shards)
			if got := len(store.shards); got != tt.want {
				t.Errorf("got %d shards, want %d", got, tt.want)
			}
			if got := int(store.mask) + 1; got != tt.want {
				t.Errorf("mask+1 = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFNV1a(t *testing.T) {
	// Reference values for 32-bit FNV-1a.
	tests := []struct {
		in   string
		want uint32
	}{
		{"", 0x811c9dc5},
		{"a", 0xe40c292c},
		{"foobar", 0xbf9cf968},
	}
	for _, tt := range tests {
		if got := fnv1a(tt.in); got != tt.want {
			t.Errorf("fnv1a(%q) = %#x, want %#x", tt.in, got, tt.want)
		}
	}
}
