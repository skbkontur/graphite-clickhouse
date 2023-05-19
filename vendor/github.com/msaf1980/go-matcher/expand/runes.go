package expand

import (
	"fmt"
	"sort"
	"unicode/utf8"
)

// runes generates a sequence of runes from first to second element
type runes struct {
	start rune
	stop  rune
	pos   rune
}

func runesRangeMerge(rs []runes) []runes {
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].start == rs[j].start {
			return rs[i].stop < rs[j].stop
		}
		return rs[i].start < rs[j].start
	})

	// merge ranges
	j := 0
	n := len(rs)
	for i := 0; i < n; i++ {
		pos := i
		if pos < n-1 {
			for next := i + 1; next < n; next++ {
				if rs[pos].start == rs[next].start || rs[pos].stop+1 >= rs[next].start {
					if rs[pos].stop < rs[next].stop {
						// merge two ranges, like [1-3 1-4] [1-3 2-5] [1-3 4-5 5-7]
						rs[pos].stop = rs[next].stop
					}
					// skip to next, merged
					i++
					rs[i].stop = 0
				} else {
					break
				}
			}
		}

		if pos != j {
			rs[j] = rs[pos]
			rs[pos].stop = 0
		}
		if rs[j].stop != 0 {
			j++
		}
	}

	return rs[:j]
}

func runesRangeExpandMust(s string) (rs []runes) {
	rs, ok := runesRangeExpand(s)
	if !ok {
		panic(fmt.Errorf("expand %q failed", s))
	}
	return rs
}

// RunesExpand expand runes like a-z0 ([] stiped)
func runesRangeExpand(s string) (rs []runes, ok bool) {
	if len(s) > 0 {
		rs = make([]runes, 0, len(s)+4)
		start := utf8.RuneError
		isRange := false
		for i, c := range s {
			if s[i] == '-' {
				isRange = true
			} else if isRange {
				if start == utf8.RuneError {
					start = c
					isRange = false
				} else {
					if start <= c {
						// TODO: detect overlapped ranges like Я-你 beetween codepages
						n := utf8.RuneLen(start)
						cn := utf8.RuneLen(c)
						if n != cn {
							return rs, false
						}
						rs = append(rs, runes{start: start, stop: c})
					} else {
						return rs, false
					}
					start = utf8.RuneError
					isRange = false
				}
			} else {
				if start != utf8.RuneError {
					rs = append(rs, runes{start: start, stop: start})
				}
				start = c
			}
		}
		if start != utf8.RuneError {
			if start != utf8.RuneError {
				rs = append(rs, runes{start: start, stop: start})
			}
		}

		rs = runesRangeMerge(rs)
	} else {
		return rs, false
	}

	for i := 0; i < len(rs); i++ {
		rs[i].reset()
	}

	return rs, true
}

func (r *runes) count() int {
	return int(r.stop - r.start + 1)
}

func (r *runes) reset() {
	r.pos = r.start
}

func (r *runes) next() rune {
	if r.pos == -1 {
		return utf8.RuneError
	}
	c := r.pos
	if c > r.stop {
		r.pos = -1
		return utf8.RuneError
	}
	r.pos++

	return c
}
