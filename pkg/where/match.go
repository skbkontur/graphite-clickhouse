package where

import (
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/pkg/reverse"
	"github.com/msaf1980/go-matcher/expand"
)

var (
	opEq string = "="
)

// clearGlob cleanup grafana globs like {name}
func clearGlob(query string) string {
	p := 0
	s := strings.IndexAny(query, "{[")
	if s == -1 {
		return query
	}

	found := false
	var builder strings.Builder

	for {
		var e int
		if query[s] == '{' {
			e = strings.IndexAny(query[s:], "}.")
			if e == -1 || query[s+e] == '.' {
				// { not closed, glob with error
				break
			}
			e += s + 1
			delim := strings.IndexRune(query[s+1:e], ',')
			if delim == -1 {
				if !found {
					builder.Grow(len(query) - 2)
					found = true
				}
				builder.WriteString(query[p:s])
				builder.WriteString(query[s+1 : e-1])
				p = e
			}
		} else {
			e = strings.IndexAny(query[s+1:], "].")
			if e == -1 || query[s+e] == '.' {
				// [ not closed, glob with error
				break
			} else {
				symbols := 0
				for _, c := range query[s+1 : s+e+1] {
					_ = c // for loop over runes
					symbols++
					if symbols == 2 {
						break
					}
				}
				if symbols <= 1 {
					if !found {
						builder.Grow(len(query) - 2)
						found = true
					}
					builder.WriteString(query[p:s])
					builder.WriteString(query[s+1 : s+e+1])
					p = e + s + 2
				}
			}
			e += s + 2
		}

		if e >= len(query) {
			break
		}
		s = strings.IndexAny(query[e:], "{[")
		if s == -1 {
			break
		}
		s += e
	}

	if found {
		if p < len(query) {
			builder.WriteString(query[p:])
		}
		return builder.String()
	}
	return query
}

func globMatchBuild(buf *strings.Builder, field string, query string, optionalDotAtEnd bool) {
	// before any wildcard symbol
	simplePrefix := query[:strings.IndexAny(query, "[]{}*?")]

	// prefix search like "metric.name.xx*"
	if len(simplePrefix) == len(query)-1 && query[len(query)-1] == '*' {
		HasPrefixBuild(buf, field, simplePrefix)
		return
	}

	// Q() replaces \ with \\, so using \. does not work here.
	// work around with [.]
	postfix := `$`
	if optionalDotAtEnd {
		postfix = `[.]?$`
	}

	if simplePrefix == "" {
		// return fmt.Sprintf("match(%s, %s)", field, quote(`^`+GlobToRegexp(query)+postfix))
		buf.WriteString("match(")
		buf.WriteString(field)
		buf.WriteString(", '^")
		buf.WriteString(GlobToRegexp(query))
		buf.WriteString(postfix)
		buf.WriteString("')")
		return
	}

	// return fmt.Sprintf("%s AND match(%s, %s)",
	//
	//	HasPrefix(field, simplePrefix),
	//	field, quote(`^`+GlobToRegexp(query)+postfix),
	//
	// )
	HasPrefixBuild(buf, field, simplePrefix)
	buf.WriteString(" AND match(")
	buf.WriteString(field)
	buf.WriteString(", '^")
	buf.WriteString(GlobToRegexp(query))
	buf.WriteString(postfix)
	buf.WriteString("')")
}

func glob(field string, query string, optionalDotAtEnd bool, expandMax, expandDepth int,
	reversed config.IndexDirection, reverses config.IndexReverses) (string, bool) {

	var r bool
	if query == "*" {
		return "", r
	}

	if expandMax == 0 {
		query = clearGlob(query)
	} else {
		queries, err := expand.Expand(query, expandMax, expandDepth)
		if err != nil {
			query = clearGlob(query)
		} else if len(queries) == 1 {
			query = queries[0]
		} else {
			if useReverse(queries[0], reversed, reverses) {
				query = reverse.StringNoTag(query)
				queries_r, err := expand.Expand(query, expandMax, expandDepth)
				if err == nil {
					r = true
					queries = queries_r
				}
			}
			return globs(field, queries, optionalDotAtEnd, expandMax, expandDepth), r
		}
	}

	if !HasWildcard(query) {
		if optionalDotAtEnd {
			return In(field, []string{query, query + "."}), r
		} else {
			return Eq(field, query), r
		}
	}
	var buf strings.Builder
	buf.Grow(len(query) * 2)

	if useReverse(query, reversed, reverses) {
		query = reverse.StringNoTag(query)
		r = true
	}

	globMatchBuild(&buf, field, query, optionalDotAtEnd)

	return buf.String(), r
}

func globs(field string, queries []string, optionalDotAtEnd bool, expandMax, expandDepth int) string {
	var (
		buf strings.Builder
		in  []string
	)

	for n, query := range queries {
		// TODO (msaf1980): cleanup after expand queires complete
		query = clearGlob(query)
		if HasWildcard(query) {
			if buf.Len() == 0 {
				buf.WriteString("(")
			} else {
				buf.WriteString(" OR (")
			}
			globMatchBuild(&buf, field, query, optionalDotAtEnd)
			buf.WriteString(")")
		} else {
			if optionalDotAtEnd {
				if in == nil {
					in = make([]string, 0, 2*(len(query)-n))
				}
				in = append(in, query)
				in = append(in, query+".")
			} else {
				if in == nil {
					in = make([]string, 0, len(query)-n)
				}
				in = append(in, query)
			}
		}
	}
	if len(in) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(" OR ")
		}
		InBuild(&buf, field, in)
	}

	return buf.String()
}

// Glob ...
func Glob(field string, query string, expandMax, expandDepth int,
	reverse config.IndexDirection, reverses config.IndexReverses) (string, bool) {
	return glob(field, query, false, expandMax, expandDepth, reverse, reverses)
}

// // Globs ...
// func Globs(field string, queries []string) string {
// 	return globs(field, queries, false)
// }

// TreeGlob ...
func TreeGlob(field string, query string, expandMax, expandDepth int,
	reverse config.IndexDirection, reverses config.IndexReverses) (string, bool) {
	return glob(field, query, true, expandMax, expandDepth, reverse, reverses)
}

// // TreeGlobs ...
// func TreeGlobs(field string, queries []string) string {
// 	return globs(field, queries, true)
// }

func ConcatMatchKV(key, value string) string {
	startLine := value[0] == '^'
	endLine := value[len(value)-1] == '$'
	if startLine {
		return key + opEq + value[1:]
	} else if endLine {
		return key + opEq + value + "\\\\%"
	}
	return key + opEq + "\\\\%" + value
}

func Match(field string, key, value string) string {
	if len(value) == 0 || value == "*" {
		return Like(field, key+"=%")
	}
	expr := ConcatMatchKV(key, value)
	simplePrefix := NonRegexpPrefix(expr)
	if len(simplePrefix) == len(expr) {
		return Eq(field, expr)
	} else if len(simplePrefix) == len(expr)-1 && expr[len(expr)-1] == '$' {
		return Eq(field, simplePrefix)
	}

	if simplePrefix == "" {
		// return fmt.Sprintf("match(%s, %s)", field, quoteRegex(key, value))
		return "match(" + field + ", " + quoteRegex(key, value) + ")"
	}

	// return fmt.Sprintf("%s AND match(%s, %s)",
	// 	HasPrefix(field, simplePrefix),
	// 	field, quoteRegex(key, value),
	// )
	return HasPrefix(field, simplePrefix) + " AND match(" + field + ", " + quoteRegex(key, value) + ")"
}
