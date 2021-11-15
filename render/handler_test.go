package render

import (
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
)

func Test_getCacheTimeout(t *testing.T) {
	cacheConfig := config.CacheConfig{
		ShortTimeoutSec:   60,
		DefaultTimeoutSec: 300,
	}

	now := int64(1636985018)

	tests := []struct {
		name  string
		now   time.Time
		from  int64
		until int64
		want  int32
	}{
		{
			name:  "short: from = now - 10800",
			now:   time.Unix(now, 0),
			from:  now - 10800,
			until: now,
			want:  60,
		},
		{
			name:  "short: from = now - 10810, until = now - 60",
			now:   time.Unix(now, 0),
			from:  now - 10800,
			until: now - 60,
			want:  60,
		},
		{
			name:  "default: from = now - 10810",
			now:   time.Unix(now, 0),
			from:  now - 10810,
			until: now,
			want:  300,
		},
		{
			name:  "default: from = now - 7200, until = now - 62",
			now:   time.Unix(now, 0),
			from:  now - 7200,
			until: now - 62,
			want:  300,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getCacheTimeout(tt.now, tt.from, tt.until, &cacheConfig); got != tt.want {
				t.Errorf("getCacheTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}
