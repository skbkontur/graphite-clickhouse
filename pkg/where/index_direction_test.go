package where

import (
	"fmt"
	"testing"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/stretchr/testify/assert"
)

func Test_useReverse(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		query  string
		result bool
	}{
		{"a.b.c.d.e", false},
		{"a.b*", false},
		{"a.b.c.d.e*", false},
		{"a.b.c.d*.e", false},
		{"a.b*.c*.d.e", true},
		{"a.b*.c.d.e", true},
	}

	for _, tt := range table {
		assert.Equal(tt.result, useReverse(tt.query, config.IndexAuto, nil), tt.query)
	}
}

func Test_useReverseWithSetConfig(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		query   string
		reverse config.IndexDirection
		result  bool
	}{
		{"a.b.c.d.e", config.IndexReversed, true},
		{"a.b.c.d.e", config.IndexAuto, false},
		{"a.b.c.d.e", config.IndexDirect, false},
		{"a.b.c.d.e", config.IndexDirect, false},
		{"a.b.c.d.e*", config.IndexDirect, false},
		{"a.b.c.d*.e", config.IndexDirect, false},
		{"a.b.c.d*.e", config.IndexReversed, true},
		{"a*.b.c.d*.e", config.IndexReversed, true}, // Wildcard at first level, use reverse if possible
		{"a.b*.c.d*.e", config.IndexReversed, true},
		{"a.*.c.*.e.*.j", config.IndexReversed, true},
		{"a.*.c.*.e.*.j", config.IndexDirect, false},
		{"a.b*.c.*d.e", config.IndexReversed, true},
	}

	for _, tt := range table {
		assert.Equal(tt.result, useReverse(tt.query, tt.reverse, nil), fmt.Sprintf("%s with iota %d", tt.query, tt.reverse))
	}
}

func Test_checkReverses(t *testing.T) {
	assert := assert.New(t)

	reverses := config.IndexReverses{
		{Suffix: ".sum", ReverseStr: "direct"},
		{Prefix: "test.", Suffix: ".alloc", ReverseStr: "direct"},
		{Prefix: "test2.", ReverseStr: "reversed"},
		{RegexStr: `^a\..*\.max$`, ReverseStr: "reversed"},
	}

	table := []struct {
		query   string
		reverse config.IndexDirection
		result  config.IndexDirection
	}{
		{"a.b.c.d*.sum", config.IndexAuto, config.IndexDirect},
		{"a*.b.c.d.sum", config.IndexAuto, config.IndexDirect},
		{"test.b.c*.d*.alloc", config.IndexAuto, config.IndexDirect},
		{"test.b.c*.d.alloc", config.IndexAuto, config.IndexDirect},
		{"test2.b.c*.d*.e", config.IndexAuto, config.IndexReversed},
		{"test2.b.c*.d.e", config.IndexAuto, config.IndexReversed},
		{"a.b.c.d*.max", config.IndexAuto, config.IndexReversed}, // regex test
		{"a.b.c*.d.max", config.IndexAuto, config.IndexReversed}, // regex test
	}

	assert.NoError(reverses.Compile())

	for _, tt := range table {
		assert.Equal(tt.result, checkReverses(tt.query, tt.reverse, reverses), fmt.Sprintf("%s with iota %d", tt.query, tt.reverse))
	}
}

func Benchmark_useReverseDepth(b *testing.B) {
	reverses := config.IndexReverses{
		{Prefix: "test2.", Reverse: config.IndexReversed},
	}
	if err := reverses.Compile(); err != nil {
		b.Fatal("failed to compile reverses")
	}

	for i := 0; i < b.N; i++ {
		_ = checkReverses("test2.b.c*.d.e", config.IndexAuto, reverses)
	}
}

func Benchmark_useReverseDepthPrefixSuffix(b *testing.B) {
	reverses := config.IndexReverses{
		{Prefix: "test2.", Suffix: ".e", Reverse: config.IndexDirect},
	}
	if err := reverses.Compile(); err != nil {
		b.Fatal("failed to compile reverses")
	}

	for i := 0; i < b.N; i++ {
		_ = checkReverses("test2.b.c*.d.e", config.IndexAuto, reverses)
	}
}

func Benchmark_useReverseDepthRegex(b *testing.B) {
	reverses := config.IndexReverses{
		{RegexStr: `^a\..*\.max$`, Reverse: config.IndexAuto},
	}
	if err := reverses.Compile(); err != nil {
		b.Fatal("failed to compile reverses")
	}

	for i := 0; i < b.N; i++ {
		_ = checkReverses("a.b.c*.d.max", config.IndexAuto, reverses)
	}
}
