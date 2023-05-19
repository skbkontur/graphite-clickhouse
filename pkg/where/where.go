package where

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/helper/errs"
	"github.com/msaf1980/go-stringutils"
)

// workaraund for Grafana multi-value variables, expand S{a,b,c}E to [SaE,SbE,ScE]
func GlobExpandSimple(value, prefix string, result *[]string) error {
	// TODO: replace with go-matcher/expand
	if len(value) == 0 {
		// we at the end of glob
		*result = append(*result, prefix)
		return nil
	}

	start := strings.IndexAny(value, "{}")
	if start == -1 {
		*result = append(*result, prefix+value)
	} else {
		end := strings.Index(value[start:], "}")
		if end <= 1 {
			return errs.NewErrorWithCode("malformed glob: "+value, http.StatusBadRequest)
		}
		if end == -1 || strings.IndexAny(value[start+1:start+end], "{}") != -1 {
			return errs.NewErrorWithCode("malformed glob: "+value, http.StatusBadRequest)
		}
		if start > 0 {
			prefix = prefix + value[0:start]
		}
		g := value[start+1 : start+end]
		values := strings.Split(g, ",")
		var postfix string
		if end+start-1 < len(value) {
			postfix = value[start+end+1:]
		}
		for _, v := range values {
			if err := GlobExpandSimple(postfix, prefix+v, result); err != nil {
				return err
			}
		}
	}

	return nil
}

func GlobToRegexp(g string) string {
	s := g
	s = strings.ReplaceAll(s, ".", "[.]")
	s = strings.ReplaceAll(s, "$", "[$]")
	s = strings.ReplaceAll(s, "{", "(")
	s = strings.ReplaceAll(s, "}", ")")
	s = strings.ReplaceAll(s, "?", "[^.]")
	s = strings.ReplaceAll(s, ",", "|")
	s = strings.ReplaceAll(s, "*", "([^.]*?)")
	return s
}

func HasWildcard(target string) bool {
	return strings.IndexAny(target, "[]{}*?") > -1
}

func IndexLastWildcard(target string) int {
	return strings.LastIndexAny(target, "[]{}*?")
}

func IndexWildcard(target string) int {
	return strings.IndexAny(target, "[]{}*?")
}

func NonRegexpPrefix(expr string) string {
	s := regexp.QuoteMeta(expr)
	for i := 0; i < len(expr); i++ {
		if expr[i] != s[i] || expr[i] == '\\' {
			if len(expr) > i+1 && expr[i] == '|' {
				eq := strings.LastIndexAny(expr[:i], "=~")
				if eq > 0 {
					return expr[:eq+1]
				}
			}
			return expr[:i]
		}
	}
	return expr
}

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

func likeEscape(s string) string {
	s = strings.ReplaceAll(s, `_`, `\_`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

func quote(value interface{}) string {
	switch v := value.(type) {
	case int:
		return strconv.Itoa(v)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case string:
		return "'" + escape(v) + "'"
	case []byte:
		return "'" + escape(stringutils.UnsafeString(v)) + "'"
	default:
		panic("not implemented")
	}
}

func quoteRegex(key, value string) string {
	startLine := value[0] == '^'
	endLine := value[len(value)-1] == '$'
	if startLine {
		// return fmt.Sprintf("'^%s%s%s'", key, opEq, escape(value[1:]))
		return "'^" + key + opEq + escape(value[1:]) + "'"
	} else if endLine {
		// return fmt.Sprintf("'^%s%s.*%s'", key, opEq, escape(value))
		return "'^" + key + opEq + ".*" + escape(value) + "'"
	}
	// return fmt.Sprintf("'^%s%s.*%s'", key, opEq, escape(value))
	return "'^" + key + opEq + ".*" + escape(value) + "'"
}

func Like(field, s string) string {
	return field + " LIKE '" + s + "'"
}

func Eq(field string, value interface{}) string {
	return field + "=" + quote(value)
}

func HasPrefix(field, prefix string) string {
	return field + " LIKE '" + likeEscape(prefix) + "%'"
}

func HasPrefixBuild(buf *strings.Builder, field, prefix string) {
	buf.WriteString(field)
	buf.WriteString(" LIKE '")
	buf.WriteString(likeEscape(prefix))
	buf.WriteString("%'")
}

func HasPrefixAndNotEq(field, prefix string) string {
	return field + " LIKE '" + likeEscape(prefix) + "_%'"
}

func HasPrefixBytes(field, prefix []byte) string {
	return stringutils.UnsafeString(field) + " LIKE '" + likeEscape(stringutils.UnsafeString(prefix)) + "%'"
}

func In(field string, list []string) string {
	if len(list) == 1 {
		return Eq(field, list[0])
	}

	var buf strings.Builder
	buf.WriteString(field)
	buf.WriteString(" IN (")
	for i, v := range list {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(quote(v))
	}
	buf.WriteByte(')')
	return buf.String()
}

func InBuild(buf *strings.Builder, field string, list []string) {
	if len(list) == 1 {
		buf.WriteString(field)
		buf.WriteByte('=')
		buf.WriteString(quote(list[0]))
		return
	}

	buf.WriteString(field)
	buf.WriteString(" IN (")
	for i, v := range list {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(quote(v))
	}
	buf.WriteByte(')')
}

func InTable(field string, table string) string {
	return field + " IN " + table
}

func DateBetween(field string, from int64, until int64) string {
	var b stringutils.Builder
	b.WriteString(field)
	b.WriteString(" >= '")
	b.WriteString(date.FromTimestampToDaysFormat(from))
	b.WriteString("' AND ")
	b.WriteString(field)
	b.WriteString(" <= '")
	b.WriteString(date.UntilTimestampToDaysFormat(until))
	b.WriteByte('\'')
	return b.String()
}

func TimestampBetween(field string, from int64, until int64) string {
	var b stringutils.Builder
	b.WriteString(field)
	b.WriteString(" >= ")
	b.WriteInt(from, 10)
	b.WriteString(" AND ")
	b.WriteString(field)
	b.WriteString(" <= ")
	b.WriteInt(until, 10)
	return b.String()
}

type Where struct {
	where string
}

func New() *Where {
	return &Where{}
}

func (w *Where) And(exp string) {
	if exp == "" {
		return
	}
	if w.where != "" {
		w.where = "(" + w.where + ") AND (" + exp + ")"
	} else {
		w.where = exp
	}
}

func (w *Where) Or(exp string) {
	if exp == "" {
		return
	}
	if w.where != "" {
		w.where = "(" + w.where + ") OR (" + exp + ")"
	} else {
		w.where = exp
	}
}

func (w *Where) Andf(format string, obj ...interface{}) {
	w.And(fmt.Sprintf(format, obj...))
}

func (w *Where) String() string {
	return w.where
}

func (w *Where) SQL() string {
	if w.where == "" {
		return ""
	}
	return "WHERE " + w.where
}

func (w *Where) PreWhereSQL() string {
	if w.where == "" {
		return ""
	}
	return "PREWHERE " + w.where
}
