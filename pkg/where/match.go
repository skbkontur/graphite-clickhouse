package where

import (
	"fmt"
	"strings"
)

// ClearGlob cleanup grafana false glob like {name}
func ClearGlob(query string) string {
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
			e = strings.IndexByte(query[s+1:], '}')
			if e == -1 {
				break
			}
			e += s + 1
			delim := strings.IndexAny(query[s+1:e], ".,")
			if delim == -1 {
				if !found {
					builder.Grow(len(query) - 2)
					found = true
				}
				builder.WriteString(query[p:s])
				builder.WriteString(query[s+1 : e])
				p = e + 1
			}
		} else {
			e = strings.IndexByte(query[s+1:], ']')
			if e == -1 {
				break
			} else if e == 1 {
				if !found {
					builder.Grow(len(query) - 2)
					found = true
				}
				builder.WriteString(query[p:s])
				builder.WriteByte(query[s+1])
				p = s + 3
			}
			e = s + 3
		}

		if e >= len(query) {
			break
		}
		s = strings.IndexAny(query[e+1:], "{[")
		if s == -1 {
			break
		}
		s += e + 1
	}

	if found {
		if p < len(query) {
			builder.WriteString(query[p:])
		}
		return builder.String()
	} else {
		return query
	}
}

func glob(field string, query string, optionalDotAtEnd bool) string {
	if query == "*" {
		return ""
	}

	if !HasWildcard(query) {
		if optionalDotAtEnd {
			return In(field, []string{query, query + "."})
		} else {
			return Eq(field, query)
		}
	}

	w := New()

	// before any wildcard symbol
	simplePrefix := query[:strings.IndexAny(query, "[]{}*?")]

	if len(simplePrefix) > 0 {
		w.And(HasPrefix(field, simplePrefix))
	}

	// prefix search like "metric.name.xx*"
	if len(simplePrefix) == len(query)-1 && query[len(query)-1] == '*' {
		return HasPrefix(field, simplePrefix)
	}

	// Q() replaces \ with \\, so using \. does not work here.
	// work around with [.]
	postfix := `$`
	if optionalDotAtEnd {
		postfix = `[.]?$`
	}

	if simplePrefix == "" {
		return fmt.Sprintf("match(%s, %s)", field, quote(`^`+GlobToRegexp(query)+postfix))
	}

	return fmt.Sprintf("%s AND match(%s, %s)",
		HasPrefix(field, simplePrefix),
		field, quote(`^`+GlobToRegexp(query)+postfix),
	)
}

// Glob ...
func Glob(field string, query string) string {
	return glob(field, query, false)
}

// TreeGlob ...
func TreeGlob(field string, query string) string {
	return glob(field, query, true)
}

func Match(field string, expr string) string {
	simplePrefix := NonRegexpPrefix(expr)
	if len(simplePrefix) == len(expr) {
		return Eq(field, expr)
	}

	if simplePrefix == "" {
		return fmt.Sprintf("match(%s, %s)", field, quote(expr))
	}

	return fmt.Sprintf("%s AND match(%s, %s)", HasPrefix(field, simplePrefix), field, quote(expr))
}
