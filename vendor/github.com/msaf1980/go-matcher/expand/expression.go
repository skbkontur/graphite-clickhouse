package expand

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

type expTyp int8

const (
	expString expTyp = iota
	expWildcard
	expList
	expRunes
)

var (
	expTypMap = map[int]string{
		0: "",
		1: "{",
		2: "[",
	}
)

type ErrUnsupportedExp expTyp

func (e ErrUnsupportedExp) Error() string {
	t := int(e)
	n, ok := expTypMap[t]
	if !ok {
		n = strconv.Itoa(t)
	}
	return "unsupported expression expand: " + n
}

// Expression represents all possible expandable types
type Expression struct {
	typ   expTyp
	body  string
	list  []string
	runes []runes
	pos   int // -1 for EOF
}

func (e *Expression) count() int {
	n := 0
	switch e.typ {
	case expString, expWildcard:
		n = 1
	case expList:
		n = len(e.list)
	case expRunes:
		for i := 0; i < len(e.runes); i++ {
			n += e.runes[i].count()
		}
	default:
		panic(fmt.Errorf("BUG: not implemented for %d", e.typ))
	}
	return n
}

func (e *Expression) minLen() int {
	n := 0
	switch e.typ {
	case expString:
		return len(e.body)
	case expWildcard:
		return -1
	case expList:
		n = len(e.list[0])
	case expRunes:
		return utf8.RuneLen(e.runes[0].start)
	default:
		panic(fmt.Errorf("BUG: not implemented for %d", e.typ))
	}
	return n
}

func (e *Expression) reset() {
	e.pos = 0
	for i := 0; i < len(e.runes); i++ {
		e.runes[i].reset()
	}
}

func (e *Expression) appendNext(out []byte) ([]byte, error) {
	if e.pos == -1 {
		return out, io.EOF
	}
	switch e.typ {
	case expString, expWildcard:
		out = append(out, e.body...)
		e.pos = -1
	case expList:
		out = append(out, e.list[e.pos]...)
		if e.pos < len(e.list)-1 {
			e.pos++
		} else {
			e.pos = -1
		}
	case expRunes:
		for {
			if c := e.runes[e.pos].next(); c == utf8.RuneError {
				if e.pos < len(e.runes)-1 {
					e.pos++
				} else {
					return out, io.EOF
				}
				e.runes[e.pos].reset()
			} else {
				out = utf8.AppendRune(out, c)
				break
			}
		}
	default:
		panic(fmt.Errorf("BUG: not implemented for %d", e.typ))
	}
	return out, nil
}

// getExpression returns expression depends on the input
func getExpression(in string) Expression {
	orig := in
	in = in[1 : len(in)-1]
	if len(in) == 0 {
		return Expression{body: in}
	}
	switch orig[0] {
	case '{':
		if strings.ContainsRune(in, ',') {
			return Expression{typ: expList, body: orig, list: strings.Split(in, ",")}
		} else {
			return Expression{body: in}
		}
	case '[':
		if len(in) == 1 {
			return Expression{body: in}
		} else {
			// TODO
			// return rune{in}
			rs, ok := runesRangeExpand(in)
			if !ok {
				return Expression{typ: expWildcard, body: in}
			}
			if len(rs) == 0 {
				return Expression{body: ""}
			}
			return Expression{typ: expRunes, body: orig, runes: rs}
		}
	default:
		if asciiSet.Index(orig) != -1 {
			return Expression{typ: expWildcard, body: orig}
		}
		return Expression{body: orig}
	}
}
