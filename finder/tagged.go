package finder

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"

	stringutils "github.com/msaf1980/go-stringutils"
)

type TaggedTermOp int

const (
	TaggedTermEq       TaggedTermOp = 1
	TaggedTermMatch    TaggedTermOp = 2
	TaggedTermNe       TaggedTermOp = 3
	TaggedTermNotMatch TaggedTermOp = 4
)

type TaggedTerm struct {
	Key         string
	Op          TaggedTermOp
	Value       string
	HasWildcard bool // only for TaggedTermEq
	Cost        int  // tag cost for use ad primary filter (use tag with maximal selectivity). 0 by default, minimal is better.
	// __name__ tag is prefered, if some tag has better selectivity than name, set it cost to < 0
	// values with wildcards or regex matching also has lower priority, set if needed it cost to < 0
}

type TaggedTermList []TaggedTerm

func (s TaggedTermList) Len() int {
	return len(s)
}
func (s TaggedTermList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s TaggedTermList) Less(i, j int) bool {
	if s[i].Op < s[j].Op {
		return true
	}
	if s[i].Op > s[j].Op {
		return false
	}

	if s[i].Op == TaggedTermEq && !s[i].HasWildcard && s[j].HasWildcard {
		// globs as fist eq might be have a bad perfomance
		return true
	}

	if s[i].Key == "__name__" && s[j].Key != "__name__" {
		return true
	}
	return false
}

type TaggedFinder struct {
	url            string              // clickhouse dsn
	table          string              // graphite_tag table
	absKeepEncoded bool                // Abs returns url encoded value. For queries from prometheus
	opts           clickhouse.Options  // clickhouse query timeout
	taggedCosts    *config.TaggedCosts // costs for taggs (sor tune index search)
	body           []byte              // clickhouse response
}

func NewTagged(url string, table string, absKeepEncoded bool, opts clickhouse.Options, taggedCosts *config.TaggedCosts) *TaggedFinder {
	return &TaggedFinder{
		url:            url,
		table:          table,
		absKeepEncoded: absKeepEncoded,
		opts:           opts,
		taggedCosts:    taggedCosts,
	}
}

func (term *TaggedTerm) concat() string {
	return term.Key + "=" + term.Value
}

func (term *TaggedTerm) concatMask() string {
	v := strings.ReplaceAll(term.Value, "*", "%")
	return fmt.Sprintf("%s=%s", term.Key, v)
}

func TaggedTermWhere1(term *TaggedTerm) (string, error) {
	// positive expression check only in Tag1
	// negative check in all Tags
	switch term.Op {
	case TaggedTermEq:
		if strings.Index(term.Value, "*") >= 0 {
			return where.Like("Tag1", term.concatMask()), nil
		}
		var values []string
		if err := where.GlobExpandSimple(term.Value, term.Key+"=", &values); err != nil {
			return "", err
		}
		if len(values) == 1 {
			return where.Eq("Tag1", values[0]), nil
		} else if len(values) > 1 {
			return where.In("Tag1", values), nil
		} else {
			return where.Eq("Tag1", term.concat()), nil
		}
	case TaggedTermNe:
		if term.Value == "" {
			// special case
			// container_name!=""  ==> container_name exists and it is not empty
			return where.HasPrefixAndNotEq("Tag1", term.Key+"="), nil
		}
		if strings.Index(term.Value, "*") >= 0 {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Like("x", term.concatMask())), nil
		}
		var values []string
		if err := where.GlobExpandSimple(term.Value, term.Key+"=", &values); err != nil {
			return "", err
		}
		if len(values) == 1 {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Eq("x", values[0])), nil
		} else if len(values) > 1 {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.In("x", values)), nil
		} else {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Eq("x", term.concat())), nil
		}
	case TaggedTermMatch:
		return where.Match("Tag1", term.Key, term.Value), nil
	case TaggedTermNotMatch:
		// return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", term.Key, term.Value), nil
		return "NOT " + where.Match("Tag1", term.Key, term.Value), nil
	default:
		return "", nil
	}
}

func TaggedTermWhereN(term *TaggedTerm) (string, error) {
	// arrayExists((x) -> %s, Tags)
	switch term.Op {
	case TaggedTermEq:
		if strings.Index(term.Value, "*") >= 0 {
			return fmt.Sprintf("arrayExists((x) -> %s, Tags)", where.Like("x", term.concatMask())), nil
		}
		var values []string
		if err := where.GlobExpandSimple(term.Value, term.Key+"=", &values); err != nil {
			return "", err
		}
		if len(values) == 1 {
			return "arrayExists((x) -> " + where.Eq("x", values[0]) + ", Tags)", nil
		} else if len(values) > 1 {
			return "arrayExists((x) -> " + where.In("x", values) + ", Tags)", nil
		} else {
			return "arrayExists((x) -> " + where.Eq("x", term.concat()) + ", Tags)", nil
		}
	case TaggedTermNe:
		if term.Value == "" {
			// special case
			// container_name!=""  ==> container_name exists and it is not empty
			return fmt.Sprintf("arrayExists((x) -> %s, Tags)", where.HasPrefixAndNotEq("x", term.Key+"=")), nil
		}
		if strings.Index(term.Value, "*") >= 0 {
			return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Like("x", term.concatMask())), nil
		}
		var values []string
		if err := where.GlobExpandSimple(term.Value, term.Key+"=", &values); err != nil {
			return "", err
		}
		if len(values) == 1 {
			return "NOT arrayExists((x) -> " + where.Eq("x", values[0]) + ", Tags)", nil
		} else if len(values) > 1 {
			return "NOT arrayExists((x) -> " + where.In("x", values) + ", Tags)", nil
		} else {
			return "NOT arrayExists((x) -> " + where.Eq("x", term.concat()) + ", Tags)", nil
		}
	case TaggedTermMatch:
		return fmt.Sprintf("arrayExists((x) -> %s, Tags)", where.Match("x", term.Key, term.Value)), nil
	case TaggedTermNotMatch:
		return fmt.Sprintf("NOT arrayExists((x) -> %s, Tags)", where.Match("x", term.Key, term.Value)), nil
	default:
		return "", nil
	}
}

func setCost(term *TaggedTerm, tagCosts *config.TaggedCosts) {
	tagCosts.RLock()
	if costs, ok := tagCosts.Costs[term.Key]; ok {
		if v, ok := costs.Values[term.Value]; ok {
			term.Cost = v
		} else if term.Op == TaggedTermEq && !term.HasWildcard {
			term.Cost = costs.Default
		} else {
			term.Cost = costs.Total
		}
	} else if term.Op == TaggedTermEq && !term.HasWildcard {
		term.Cost = tagCosts.Default
	} else {
		term.Cost = tagCosts.Total
	}
	tagCosts.RUnlock()
}

func ParseTaggedConditions(conditions []string, taggedCosts *config.TaggedCosts) ([]TaggedTerm, error) {
	terms := make([]TaggedTerm, len(conditions))

	for i := 0; i < len(conditions); i++ {
		s := conditions[i]

		a := strings.SplitN(s, "=", 2)
		if len(a) != 2 {
			return nil, fmt.Errorf("wrong seriesByTag expr: %#v", s)
		}

		a[0] = strings.TrimSpace(a[0])
		a[1] = strings.TrimSpace(a[1])

		op := "="

		if len(a[0]) > 0 && a[0][len(a[0])-1] == '!' {
			op = "!" + op
			a[0] = strings.TrimSpace(a[0][:len(a[0])-1])
		}

		if len(a[1]) > 0 && a[1][0] == '~' {
			op = op + "~"
			a[1] = strings.TrimSpace(a[1][1:])
		}

		terms[i].Key = a[0]
		terms[i].Value = a[1]

		if terms[i].Key == "name" {
			terms[i].Key = "__name__"
		}

		switch op {
		case "=":
			terms[i].Op = TaggedTermEq
			terms[i].HasWildcard = where.HasWildcard(terms[i].Value)
		case "!=":
			terms[i].Op = TaggedTermNe
		case "=~":
			terms[i].Op = TaggedTermMatch
		case "!=~":
			terms[i].Op = TaggedTermNotMatch
		default:
			return nil, fmt.Errorf("wrong seriesByTag expr: %#v", s)
		}
		if taggedCosts != nil {
			setCost(&terms[i], taggedCosts)
		}
	}

	if taggedCosts == nil {
		sort.Sort(TaggedTermList(terms))
	} else {
		// compare with taggs costs
		sort.Slice(terms, func(i, j int) bool {
			if terms[i].Cost == terms[j].Cost {
				if terms[i].Op < terms[j].Op {
					return true
				}
				if terms[i].Op > terms[j].Op {
					return false
				}
				if terms[i].Op == TaggedTermEq && !terms[i].HasWildcard && terms[j].HasWildcard {
					// globs as fist eq might be have a bad perfomance
					return true
				}
				// __name__ have a priority when costs equal
				if terms[i].Key == "__name__" && terms[j].Key != "__name__" {
					return true
				}
			} else {
				return terms[i].Cost < terms[j].Cost
			}

			return false
		})
	}

	return terms, nil
}

func ParseSeriesByTag(query string, tagCosts *config.TaggedCosts) ([]TaggedTerm, error) {
	expr, _, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	validationError := fmt.Errorf("wrong seriesByTag call: %#v", query)

	// check
	if !expr.IsFunc() {
		return nil, validationError
	}
	if expr.Target() != "seriesByTag" {
		return nil, validationError
	}

	args := expr.Args()
	if len(args) < 1 {
		return nil, validationError
	}

	for i := 0; i < len(args); i++ {
		if !args[i].IsString() {
			return nil, validationError
		}
	}

	conditions := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		s := args[i].StringValue()
		if s == "" {
			continue
		}
		conditions = append(conditions, s)
	}

	return ParseTaggedConditions(conditions, tagCosts)
}

func TaggedWhere(terms []TaggedTerm) (*where.Where, *where.Where, error) {
	w := where.New()
	pw := where.New()
	x, err := TaggedTermWhere1(&terms[0])
	if err != nil {
		return nil, nil, err
	}
	if terms[0].Op == TaggedTermMatch {
		pw.And(x)
	}
	w.And(x)

	for i := 1; i < len(terms); i++ {
		and, err := TaggedTermWhereN(&terms[i])
		if err != nil {
			return nil, nil, err
		}
		w.And(and)
	}

	return w, pw, nil
}

func NewCachedTags(body []byte) *TaggedFinder {
	return &TaggedFinder{
		body: body,
	}
}

func (t *TaggedFinder) Execute(ctx context.Context, query string, from int64, until int64) error {
	terms, err := ParseSeriesByTag(query, t.taggedCosts)
	if err != nil {
		return err
	}

	return t.ExecutePrepared(ctx, terms, from, until)
}

func (t *TaggedFinder) ExecutePrepared(ctx context.Context, terms []TaggedTerm, from int64, until int64) error {
	w, pw, err := TaggedWhere(terms)
	if err != nil {
		return err
	}

	w.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(from, 0).Format("2006-01-02"),
		time.Unix(until, 0).Format("2006-01-02"),
	)

	// TODO: consider consistent query generator
	sql := fmt.Sprintf("SELECT Path FROM %s %s %s GROUP BY Path FORMAT TabSeparatedRaw", t.table, pw.PreWhereSQL(), w.SQL())
	t.body, err = clickhouse.Query(scope.WithTable(ctx, t.table), t.url, sql, t.opts, nil)
	return err
}

func (t *TaggedFinder) List() [][]byte {
	if t.body == nil {
		return [][]byte{}
	}

	rows := bytes.Split(t.body, []byte{'\n'})

	skip := 0
	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
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

func (t *TaggedFinder) Series() [][]byte {
	return t.List()
}

func tagsParse(path string) (string, []string, error) {
	name, args, n := stringutils.Split2(path, "?")
	if n == 1 || args == "" {
		return name, nil, fmt.Errorf("incomplete tags in '%s'", path)
	}
	tags := strings.Split(args, "&")
	return name, tags, nil
}

func TaggedDecode(v []byte) []byte {
	s := stringutils.UnsafeString(v)
	name, tags, err := tagsParse(s)
	if err != nil {
		return v
	}

	if len(tags) == 0 {
		return stringutils.UnsafeStringBytes(&name)
	}
	sort.Strings(tags)

	var sb stringutils.Builder

	length := len(name)
	for _, tag := range tags {
		length += len(tag) + 1
	}

	sb.Grow(length)

	sb.WriteString(name)
	for _, tag := range tags {
		sb.WriteString(";")
		sb.WriteString(tag)
	}
	return sb.Bytes()
}

func (t *TaggedFinder) Abs(v []byte) []byte {
	if t.absKeepEncoded {
		return v
	}

	return TaggedDecode(v)
}

func (t *TaggedFinder) Bytes() ([]byte, error) {
	return nil, ErrNotImplemented
}
