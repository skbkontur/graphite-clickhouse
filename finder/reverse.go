package finder

import (
	"context"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

type ReverseFinder struct {
	wrapped    Finder
	baseFinder Finder
	url        string // clickhouse dsn
	table      string // graphite_reverse_tree table
	isUsed     bool   // use reverse table
}

func WrapReverse(f Finder, url string, table string, opts clickhouse.Options) *ReverseFinder {
	return &ReverseFinder{
		wrapped:    f,
		baseFinder: NewBase(url, table, opts),
		url:        url,
		table:      table,
	}
}

func (r *ReverseFinder) Execute(ctx context.Context, config *config.Config, query string, from int64, until int64, stat *FinderStat) (err error) {
	p := strings.LastIndexByte(query, '.')
	if p < 0 || p >= len(query)-1 {
		return r.wrapped.Execute(ctx, config, query, from, until, stat)
	}

	if where.HasWildcard(query[p+1:]) {
		return r.wrapped.Execute(ctx, config, query, from, until, stat)
	}

	r.isUsed = true
	return r.baseFinder.Execute(ctx, config, reverse.StringNoTag(query), from, until, stat)
}

func (r *ReverseFinder) List() [][]byte {
	if !r.isUsed {
		return r.wrapped.List()
	}

	list := r.baseFinder.List()
	for i := 0; i < len(list); i++ {
		list[i] = reverse.BytesNoTag(list[i])
	}

	return list
}

func (r *ReverseFinder) Series() [][]byte {
	if !r.isUsed {
		return r.wrapped.Series()
	}

	list := r.baseFinder.Series()
	for i := 0; i < len(list); i++ {
		list[i] = reverse.BytesNoTag(list[i])
	}

	return list
}

func (r *ReverseFinder) Abs(v []byte) []byte {
	return v
}

func (f *ReverseFinder) Bytes() ([]byte, error) {
	return f.wrapped.Bytes()
}
