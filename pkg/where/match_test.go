package where

import (
	"fmt"
	"testing"

	"github.com/lomik/graphite-clickhouse/config"
)

func Test_clearGlob(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		query string
		want  string
	}{
		{"a.{a,b}.te{s}t.b", "a.{a,b}.test.b"},
		{"a.{a,b}.te{s,t}*.b", "a.{a,b}.te{s,t}*.b"},
		{"a.{a,b}.test*.b", "a.{a,b}.test*.b"},
		{"a.[b].te{s}t.b", "a.b.test.b"},
		{"a.[ab].te{s,t}*.b", "a.[ab].te{s,t}*.b"},
		{"a.{a,b.}.te{s,t}*.b", "a.{a,b.}.te{s,t}*.b"}, // some broken
		{"О.[б].те{s}t.b", "О.б.теst.b"},               // utf-8 string
		{"О.[].те{}t.b", "О..теt.b"},                   // utf-8 string with empthy blocks
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			if got := clearGlob(tt.query); got != tt.want {
				t.Errorf("clearGlob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGlob(t *testing.T) {
	field := "test"
	tests := []struct {
		query       string
		expandMax   int
		expandDepth int
		reverse     config.IndexDirection
		reverses    config.IndexReverses
		want        string
		wantReverse bool
	}{
		{"a.b.test.*", -1, 0, config.IndexDirect, nil, "test LIKE 'a.b.test.%'", false},
		{"a.b.test.c?", -1, 0, config.IndexDirect, nil, "test LIKE 'a.b.test.c%' AND match(test, '^a[.]b[.]test[.]c[^.]$')", false},
		{"a.{a,b}.te{s}t.b", 0, 0, config.IndexDirect, nil, "test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]test[.]b$')", false},
		{"a.{a,b}.te{s}t.b", -1, 0, config.IndexDirect, nil, "test IN ('a.a.test.b','a.b.test.b')", false},
		{"a.{a,b}.te{s,t}*.b", 0, 0, config.IndexDirect, nil, "test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]te(s|t)([^.]*?)[.]b$')", false},
		{
			"a.{a,b}.te{s,t}*.b", 2, 0, config.IndexDirect, nil,
			"(test LIKE 'a.a.te%' AND match(test, '^a[.]a[.]te(s|t)([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.b.te%' AND match(test, '^a[.]b[.]te(s|t)([^.]*?)[.]b$'))",
			false,
		},
		{
			"a.{a,b}.te{s,t}*.b", -1, 1, config.IndexDirect, nil,
			"test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]te(s|t)([^.]*?)[.]b$')",
			false,
		},
		{
			"a.{a,b}.te{s,t}*.b", -1, 2, config.IndexDirect, nil,
			"(test LIKE 'a.a.te%' AND match(test, '^a[.]a[.]te(s|t)([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.b.te%' AND match(test, '^a[.]b[.]te(s|t)([^.]*?)[.]b$'))",
			false,
		},
		{
			"a.{a,b}.te{s,t}*.b", -1, 3, config.IndexDirect, nil,
			"(test LIKE 'a.a.tes%' AND match(test, '^a[.]a[.]tes([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.a.tet%' AND match(test, '^a[.]a[.]tet([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.b.tes%' AND match(test, '^a[.]b[.]tes([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.b.tet%' AND match(test, '^a[.]b[.]tet([^.]*?)[.]b$'))",
			false,
		},
		{
			"a.{a,b}.te{s,t}*.b", -1, 0, config.IndexDirect, nil,
			"(test LIKE 'a.a.tes%' AND match(test, '^a[.]a[.]tes([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.a.tet%' AND match(test, '^a[.]a[.]tet([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.b.tes%' AND match(test, '^a[.]b[.]tes([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.b.tet%' AND match(test, '^a[.]b[.]tet([^.]*?)[.]b$'))",
			false,
		},
		{"a.{a,b}.test*.b", 0, 0, config.IndexDirect, nil, "test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]test([^.]*?)[.]b$')", false},
		{
			"a.{a,b}.test*.b", -1, 0, config.IndexDirect, nil,
			"(test LIKE 'a.a.test%' AND match(test, '^a[.]a[.]test([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.b.test%' AND match(test, '^a[.]b[.]test([^.]*?)[.]b$'))",
			false,
		},
		{"a.[b].te{s}t.b", 0, 0, config.IndexDirect, nil, "test='a.b.test.b'", false},
		{"a.[ab].te{s,t}*.b", 0, 0, config.IndexDirect, nil, "test LIKE 'a.%' AND match(test, '^a[.][ab][.]te(s|t)([^.]*?)[.]b$')", false},
		{
			"a.[ab].te{s,t}*.b", -1, 0, config.IndexDirect, nil,
			"(test LIKE 'a.a.tes%' AND match(test, '^a[.]a[.]tes([^.]*?)[.]b$')) " +
				"OR (test LIKE 'a.a.tet%' AND match(test, '^a[.]a[.]tet([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.b.tes%' AND match(test, '^a[.]b[.]tes([^.]*?)[.]b$')) OR " +
				"(test LIKE 'a.b.tet%' AND match(test, '^a[.]b[.]tet([^.]*?)[.]b$'))",
			false,
		},
		{
			"{a,b}.c.d.*", -1, 0, config.IndexDirect, nil,
			"(test LIKE 'a.c.d.%') OR (test LIKE 'b.c.d.%')",
			false,
		},
		{
			"{a,b}.c.d.*", 0, 0, config.IndexDirect, nil,
			"match(test, '^(a|b)[.]c[.]d[.]([^.]*?)$')",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s [%d:%d]", tt.query, tt.expandMax, tt.expandDepth), func(t *testing.T) {
			if got, gotReverse := Glob(field, tt.query, tt.expandMax, tt.expandDepth, tt.reverse, tt.reverses); got != tt.want {
				t.Errorf("Glob() =\n%v\nwant\n%v", got, tt.want)
				if gotReverse != tt.wantReverse {
					t.Errorf("Glob() reverse = %v, want %v", gotReverse, tt.wantReverse)
				}
			}
		})
	}
}

// func TestGlobs(t *testing.T) {
// 	field := "test"
// 	tests := []struct {
// 		queries []string
// 		want    string
// 	}{
// 		{
// 			[]string{"a.{a,b}.te{s}t.b", "b.{e,d}.TE{S}T.b"},
// 			"(test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]test[.]b$')) OR" +
// 				" (test LIKE 'b.%' AND match(test, '^b[.](e|d)[.]TEST[.]b$'))",
// 		},
// 		{[]string{"a.{a,b}.te{s,t}*.b"}, "(test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]te(s|t)([^.]*?)[.]b$'))"},
// 		{[]string{"a.{a,b}.test*.b"}, "(test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]test([^.]*?)[.]b$'))"},
// 		{[]string{"a.[b].te{s}t.b"}, "test='a.b.test.b'"},
// 		{[]string{"a.[ab].te{s,t}*.b"}, "(test LIKE 'a.%' AND match(test, '^a[.][ab][.]te(s|t)([^.]*?)[.]b$'))"},
// 		{[]string{"a.[a].te{s}t.b", "b.[c].te{s}t.b"}, "test IN ('a.a.test.b','b.c.test.b')"},
// 		{
// 			[]string{"a.[b].te{s}t.b", "b.[c].te{s}t.b", "a.{a,b}.te{s,t}*.b", "a.{d,e}.te{s,t}*.b"},
// 			"(test LIKE 'a.%' AND match(test, '^a[.][ab][.]te(s|t)([^.]*?)[.]b$')) OR " +
// 				"(test LIKE 'a.%' AND match(test, '^a[.](d|e)[.]te(s|t)([^.]*?)[.]b$')) OR " +
// 				"test IN ('a.a.test.b','b.c.test.b')",
// 		},
// 	}
// 	for n, tt := range tests {
// 		t.Run(fmt.Sprintf("[%d] %s", n, strings.Join(tt.queries, " ")), func(t *testing.T) {
// 			if got := Globs(field, tt.queries); got != tt.want {
// 				t.Errorf("Globs() =\n%v\nwant\n%v", got, tt.want)
// 			}
// 		})
// 	}
// }

func BenchmarkGlob(b *testing.B) {
	field := "test"
	tests := []string{
		"a.[a].te{s}.b",
		"a.{a,b}.te{s,t}*.b",
	}
	for _, query := range tests {
		b.Run(query, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = Glob(field, query, -1, 0, config.IndexDirect, nil)
			}
		})
	}
}

// func BenchmarkGlobs(b *testing.B) {
// 	field := "test"
// 	tests := [][]string{
// 		{"a.[a].te{s}.b", "b.[c].te{s}.b"},
// 		{"a.{a,b}.te{s,t}*.b", "b.{e,d}.TE{S,T}*.b"},
// 	}
// 	for _, queries := range tests {
// 		b.Run(strings.Join(queries, " "), func(b *testing.B) {
// 			for i := 0; i < b.N; i++ {
// 				_ = Globs(field, queries)
// 			}
// 		})
// 	}
// }
