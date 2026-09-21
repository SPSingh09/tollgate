package tollgate

import (
	"errors"
	"testing"
	"time"
)

func TestRateValidate(t *testing.T) {
	tests := []struct {
		name    string
		rate    Rate
		wantErr bool
	}{
		{
			name:    "valid rate",
			rate:    Rate{Limit: 10, Period: time.Second},
			wantErr: false,
		},
		{
			name:    "valid rate with burst",
			rate:    Rate{Limit: 10, Period: time.Second, Burst: 20},
			wantErr: false,
		},
		{
			name:    "zero limit",
			rate:    Rate{Limit: 0, Period: time.Second},
			wantErr: true,
		},
		{
			name:    "negative limit",
			rate:    Rate{Limit: -1, Period: time.Second},
			wantErr: true,
		},
		{
			name:    "zero period",
			rate:    Rate{Limit: 10, Period: 0},
			wantErr: true,
		},
		{
			name:    "negative period",
			rate:    Rate{Limit: 10, Period: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative burst",
			rate:    Rate{Limit: 10, Period: time.Second, Burst: -1},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.rate.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Validate() = nil, want error")
				}
				if !errors.Is(err, ErrInvalidRate) {
					t.Fatalf("Validate() = %v, want error wrapping ErrInvalidRate", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestHelperConstructors(t *testing.T) {
	tests := []struct {
		name       string
		rate       Rate
		wantLimit  int
		wantPeriod time.Duration
	}{
		{"PerSecond", PerSecond(5), 5, time.Second},
		{"PerMinute", PerMinute(5), 5, time.Minute},
		{"PerHour", PerHour(5), 5, time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.rate.Limit != tt.wantLimit {
				t.Errorf("Limit = %d, want %d", tt.rate.Limit, tt.wantLimit)
			}
			if tt.rate.Period != tt.wantPeriod {
				t.Errorf("Period = %s, want %s", tt.rate.Period, tt.wantPeriod)
			}
			if err := tt.rate.Validate(); err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
		})
	}
}
