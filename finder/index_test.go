package finder

import (
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
)

func TestIndexFinder_whereFilter(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		from          int64
		until         int64
		dailyEnabled  bool
		indexReverse  config.IndexDirection
		indexReverses config.IndexReverses
		want          string
		wantReverse   bool
	}{
		{
			name:         "nodaily (direct)",
			query:        "test.metric*",
			from:         1668106860,
			until:        1668106870,
			dailyEnabled: false,
			want:         "((Level=20002) AND (Path LIKE 'test.metric%')) AND (Date='1970-02-12')",
		},
		{
			name:         "nodaily (reverse)",
			query:        "*test.metric",
			from:         1668106860,
			until:        1668106870,
			dailyEnabled: false,
			want:         "((Level=30002) AND (Path LIKE 'metric.%' AND match(Path, '^metric[.]([^.]*?)test[.]?$'))) AND (Date='1970-02-12')",
		},
		{
			name:         "midnight at utc (direct)",
			query:        "test.metric*",
			from:         1668124800, // 2022-11-11 00:00:00 UTC
			until:        1668124810, // 2022-11-11 00:00:10 UTC
			dailyEnabled: true,
			want: "((Level=2) AND (Path LIKE 'test.metric%')) AND (Date >='" +
				date.FromTimestampToDaysFormat(1668124800) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(1668124810) + "')",
		},
		{
			name:         "midnight at utc (reverse)",
			query:        "*test.metric",
			from:         1668124800, // 2022-11-11 00:00:00 UTC
			until:        1668124810, // 2022-11-11 00:00:10 UTC
			dailyEnabled: true,
			want: "((Level=10002) AND (Path LIKE 'metric.%' AND match(Path, '^metric[.]([^.]*?)test[.]?$'))) AND (Date >='" +
				date.FromTimestampToDaysFormat(1668124800) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(1668124810) + "')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+" "+time.Unix(tt.from, 0).Format(time.RFC3339), func(t *testing.T) {
			idx := NewIndex("http://localhost:8123/", "graphite_index", tt.dailyEnabled, tt.indexReverse, tt.indexReverses, clickhouse.Options{}, false).(*IndexFinder)
			if got, gotReverse := idx.whereFilter(tt.query, tt.from, tt.until); got.String() != tt.want {
				t.Errorf("IndexFinder.whereFilter() =\n%v\nwant\n%v", got, tt.want)
				if gotReverse != tt.wantReverse {
					t.Errorf("Glob() reverse = %v, want %v", gotReverse, tt.wantReverse)
				}
			}
		})
	}
}
