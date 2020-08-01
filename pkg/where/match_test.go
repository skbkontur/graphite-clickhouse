package where

import "testing"

func Test_ClearGlob(t *testing.T) {
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
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			if got := ClearGlob(tt.query); got != tt.want {
				t.Errorf("clearGlob() = %v, want %v", got, tt.want)
			}
		})
	}
}
