package expand

import (
	"errors"
	"io"
	"strings"

	"github.com/msaf1980/go-matcher/pkg/utils"
)

var (
	ErrNotClosed = errors.New("expression not closed")

	asciiSet = utils.MakeASCIISetMust("[]{}*?")
)

// Expand takes the string contains the shell expansion expression and returns list of strings after they are expanded (from begin).
//
// Argument max for restict max expanded results, > 0 - restuct  max expamnded results, 0 - disables expand, -1 - unlimited, etc.
func Expand(in string, max, depth int) ([]string, error) {
	exps := ParseExpr(in)

	return exps.Expand(max, depth, false)
}

// ExpandTry like Expand, but restrict only one element (in list or runes range).
//
// Argument max for restict max expanded results, > 0 - restuct  max expamnded results, 0 - disables expand, -1 - unlimited, etc.
func ExpandTry(in string, max, depth int) ([]string, error) {
	exps := ParseExpr(in)

	return exps.Expand(max, depth, true)
}

// getPair returns the top level expression.
func getPair(in string) (start, stop int) {
	start = -1
	stop = -1
	for i, c := range in {
		switch c {
		case '*', '?':
			// break, no expand after star
			if start == -1 {
				start = i
			}
			return
		case '{', '[':
			if start == -1 {
				start = i
			} else {
				return
			}
		case '}':
			if start == -1 || in[start] == '[' {
				if start == -1 {
					start = i
				}
				return
			}
			stop = i
			return
		case ']':
			if start == -1 || in[start] == '{' {
				if start == -1 {
					start = i
				}
				return
			}
			stop = i
			return
		}
	}

	return
}

type Expressions struct {
	exps []Expression
	buf  []byte
}

// Expand expand expresions
func (e *Expressions) Expand(max, depth int, try bool) ([]string, error) {
	if len(e.exps) == 1 && e.exps[0].typ == expString {
		return []string{e.exps[0].body}, nil
	}

	count := 1
	for i := 0; i < len(e.exps); i++ {
		count *= e.exps[i].count()
		if e.exps[i].typ == expWildcard {
			break
		}
	}

	result := make([]string, 0, count)

	var err error
	if depth > 0 {
		depth++
	}
	if result, _, err = expand(e.exps, result, 0, max, depth, try, e.buf[:0]); err != nil {
		return nil, err
	}

	return result, nil
}

func expand(exps []Expression, result []string, count, max, depth int, try bool, buf []byte) ([]string, []byte, error) {
	if len(exps) == 0 {
		// end of expressions, write result string
		result = append(result, string(buf))
		return result, buf, nil
	}

	var err error
	cur := len(buf)

	if max > 0 {
		if count <= 0 {
			count = 1
		}
		count *= exps[0].count()
		if max < count {
			max = 0
		}
	}
	if depth > 0 {
		switch exps[0].typ {
		case expString, expWildcard:
			depth -= strings.Count(exps[0].body, ".")
			if depth <= 1 {
				max = 0
			}
		}
	}

	if max == 0 {
		buf = append(buf, exps[0].body...)
		result, buf, err = expand(exps[1:], result, count, max, depth, try, buf)
		if err != nil {
			return nil, buf, err
		}
	} else {
		for {
			buf, err = exps[0].appendNext(buf)
			if err == io.EOF {
				exps[0].reset()
				break
			} else if err != nil {
				return nil, buf, err
			}

			result, buf, err = expand(exps[1:], result, count, max, depth, try, buf)
			buf = buf[:cur]
			if err != nil {
				return nil, buf, err
			}

			if try {
				break
			}
		}
	}

	return result, buf, nil
}

func ParseExpr(in string) (e *Expressions) {
	// var starBreak int
	e = &Expressions{buf: make([]byte, 0, len(in))}
	start, stop := getPair(in)
	if stop == -1 {
		if start == -1 {
			e.exps = []Expression{{body: in}}
			return
		} else if start == 0 {
			pos := asciiSet.LastIndex(in) + 1
			if pos < len(in) {
				e.exps = []Expression{
					{typ: expWildcard, body: in[:pos]},
					{body: in[pos:]},
				}
				return
			}
			e.exps = []Expression{
				{typ: expWildcard, body: in},
			}
			return
		} else {
			pos := asciiSet.LastIndex(in) + 1
			if pos < len(in) {
				e.exps = []Expression{
					{body: in[:start]},
					{typ: expWildcard, body: in[start:pos]},
					{body: in[pos:]},
				}
				return
			}
			e.exps = []Expression{
				{body: in[:start]},
				{typ: expWildcard, body: in[start:]},
			}
			return
		}
	}

	count := strings.Count(in, "[") + strings.Count(in, "{") + 2
	e.exps = make([]Expression, 0, count)
	for {
		if start == -1 {
			e.exps = append(e.exps, Expression{body: in})
			break
		} else if stop == -1 {
			if start > 0 {
				e.exps = append(e.exps, Expression{body: in[:start]})
			}
			e.exps = append(e.exps, Expression{typ: expWildcard, body: in[start:]})
			break
		} else if start > 0 {
			e.exps = append(e.exps, Expression{body: in[:start]})
		}

		stop++
		exp := getExpression(in[start:stop])
		if exp.typ == expString {
			if len(e.exps) > 0 && e.exps[len(e.exps)-1].typ == expString {
				e.exps[len(e.exps)-1].body += exp.body
			} else if exp.body != "" {
				e.exps = append(e.exps, exp)
			}
		} else {
			e.exps = append(e.exps, exp)
		}

		in = in[stop:]
		if in == "" {
			break
		}
		start, stop = getPair(in)
	}

	last := len(e.exps) - 1
	if e.exps[last].typ == expWildcard {
		s := e.exps[last].body
		pos := asciiSet.LastIndex(s) + 1
		if pos < len(s) {
			e.exps[last].body = s[:pos]
			e.exps = append(e.exps, Expression{body: s[pos:]})
		}
	}

	return e
}
