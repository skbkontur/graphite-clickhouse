package finder

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

var (
	ErrNotImplemented = errors.New("not implemented")
)

type BaseFinder struct {
	url   string             // clickhouse dsn
	table string             // graphite_tree table
	opts  clickhouse.Options // timeout, connectTimeout
	body  []byte             // clickhouse response body
}

func NewBase(url string, table string, opts clickhouse.Options) Finder {
	return &BaseFinder{
		url:   url,
		table: table,
		opts:  opts,
	}
}

func (b *BaseFinder) where(query string) *where.Where {
	level := strings.Count(query, ".") + 1

	w := where.New()
	w.And(where.Eq("Level", level))

	q, _ := where.TreeGlob("Path", query, config.ExpandMax, config.ExpandDepth, config.IndexDirect, nil)
	w.And(q)

	return w
}

func (b *BaseFinder) Execute(ctx context.Context, cfg *config.Config, query string, from int64, until int64, stat *FinderStat) (err error) {
	var (
		q  string
		ok bool
	)
	if config.BaseFinderQueryCache == nil {
		w := b.where(query)
		q = w.String()
	} else {
		q, ok = config.BaseFinderQueryCache.Get(query)
		if !ok {
			w := b.where(query)
			q = w.String()
			config.BaseFinderQueryCache.Set(query, q, 1, config.ExpandTTL)
		}
	}

	// TODO: consider consistent query generator
	q = "SELECT Path FROM " + b.table + " WHERE " + q + " GROUP BY Path FORMAT TabSeparatedRaw"

	b.body, stat.ChReadRows, stat.ChReadBytes, err = clickhouse.Query(
		scope.WithTable(ctx, b.table),
		b.url,
		q,
		b.opts,
		nil,
	)
	stat.Table = b.table
	stat.ReadBytes = int64(len(b.body))
	return
}

func (b *BaseFinder) makeList(onlySeries bool) [][]byte {
	if b.body == nil {
		return [][]byte{}
	}

	rows := bytes.Split(b.body, []byte{'\n'})

	skip := 0
	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			skip++
			continue
		}
		if onlySeries && rows[i][len(rows[i])-1] == '.' {
			skip++
			continue
		}
		if skip > 0 {
			rows[i-skip] = rows[i]
		}
	}

	rows = rows[:len(rows)-skip]

	return rows
}

func (b *BaseFinder) List() [][]byte {
	return b.makeList(false)
}

func (b *BaseFinder) Series() [][]byte {
	return b.makeList(true)
}

func (b *BaseFinder) Abs(v []byte) []byte {
	return v
}

func (b *BaseFinder) Bytes() ([]byte, error) {
	return b.body, nil
}
