package finder

import (
	"bytes"
	"context"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

const ReverseLevelOffset = 10000
const TreeLevelOffset = 20000
const ReverseTreeLevelOffset = 30000

const DefaultTreeDate = "1970-02-12"

type IndexFinder struct {
	url          string             // clickhouse dsn
	table        string             // graphite_tree table
	opts         clickhouse.Options // timeout, connectTimeout
	dailyEnabled bool
	confReverses config.IndexReverses
	confReverse  config.IndexDirection // calculated in IndexFinder.useReverse only once
	body         []byte                // clickhouse response body
	rows         [][]byte
	useCache     bool // rotate body if needed (for store in cache)
	useDaily     bool
}

func NewCachedIndex(body []byte) Finder {
	idx := &IndexFinder{
		body:        body,
		confReverse: config.IndexDirect,
	}
	idx.bodySplit(false)

	return idx
}

func NewIndex(url string, table string, dailyEnabled bool, reverse config.IndexDirection, reverses config.IndexReverses, opts clickhouse.Options, useCache bool) Finder {
	return &IndexFinder{
		url:          url,
		table:        table,
		opts:         opts,
		dailyEnabled: dailyEnabled,
		confReverse:  reverse,
		confReverses: reverses,
		useCache:     useCache,
	}
}

func (idx *IndexFinder) whereFilter(query string, from int64, until int64) (*where.Where, bool) {
	if idx.dailyEnabled && from > 0 && until > 0 {
		idx.useDaily = true
	} else {
		idx.useDaily = false
	}

	q, reverse := where.TreeGlob("Path", query, config.ExpandMax, config.ExpandDepth, idx.confReverse, idx.confReverses)

	var levelOffset int
	if idx.useDaily {
		if reverse {
			levelOffset = ReverseLevelOffset
		}
	} else if reverse {
		levelOffset = ReverseTreeLevelOffset
	} else {
		levelOffset = TreeLevelOffset
	}

	level := strings.Count(query, ".") + 1

	w := where.New()

	w.And(where.Eq("Level", level+levelOffset))
	w.And(q)

	if idx.useDaily {
		w.Andf(
			"Date >='%s' AND Date <= '%s'",
			date.FromTimestampToDaysFormat(from),
			date.UntilTimestampToDaysFormat(until),
		)
	} else {
		w.And(where.Eq("Date", DefaultTreeDate))
	}
	return w, reverse
}

func (idx *IndexFinder) Execute(ctx context.Context, cfg *config.Config, query string, from int64, until int64, stat *FinderStat) (err error) {
	var (
		q            string
		ok, reversed bool
		w            *where.Where
	)
	if config.IndexFinderQueryCache == nil {
		w, reversed = idx.whereFilter(query, from, until)
		q = w.String()
	} else {
		q, ok = config.IndexFinderQueryCache.Get(query)
		if !ok {
			w, reversed = idx.whereFilter(query, from, until)
			q = w.String()
			config.IndexFinderQueryCache.Set(query, q, 1, config.ExpandTTL)
		}
	}

	// TODO: consider consistent query generator
	q = "SELECT Path FROM " + idx.table + " WHERE " + q + " GROUP BY Path FORMAT TabSeparatedRaw"
	idx.body, stat.ChReadRows, stat.ChReadBytes, err = clickhouse.Query(
		scope.WithTable(ctx, idx.table),
		idx.url,
		q,
		idx.opts,
		nil,
	)
	stat.Table = idx.table
	if err == nil {
		stat.ReadBytes = int64(len(idx.body))
		idx.bodySplit(reversed)
	}

	return
}

func (idx *IndexFinder) Abs(v []byte) []byte {
	return v
}

func (idx *IndexFinder) bodySplit(reversed bool) {
	idx.rows = bytes.Split(idx.body, []byte{'\n'})

	if reversed {
		// rotate names for reduce
		var buf bytes.Buffer
		if idx.useCache {
			buf.Grow(len(idx.body))
			for i := 0; i < len(idx.rows); i++ {
				idx.rows[i] = reverse.BytesNoTag(idx.rows[i])
				if idx.useCache {
					buf.Write(idx.rows[i])
					buf.WriteByte('\n')
				}
			}
			idx.body = buf.Bytes()
			idx.confReverse = config.IndexDirect
		} else {
			for i := 0; i < len(idx.rows); i++ {
				idx.rows[i] = reverse.BytesNoTag(idx.rows[i])
			}
		}
	}
}

func (idx *IndexFinder) makeList(onlySeries bool) [][]byte {
	if len(idx.rows) == 0 {
		return [][]byte{}
	}

	rows := make([][]byte, len(idx.rows))
	copy(rows, idx.rows)

	return rows
}

func (idx *IndexFinder) List() [][]byte {
	return idx.makeList(false)
}

func (idx *IndexFinder) Series() [][]byte {
	return idx.makeList(true)
}

func (idx *IndexFinder) Bytes() ([]byte, error) {
	return idx.body, nil
}
