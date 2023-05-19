package utils

import (
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

// RuneRange is a runes range
type RuneRange struct {
	First rune
	Last  rune
}

func RunesRangeMerge(ranges []RuneRange) []RuneRange {
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].First == ranges[j].First {
			return ranges[i].Last < ranges[j].Last
		}
		return ranges[i].First < ranges[j].First
	})

	// merge ranges
	j := 0
	n := len(ranges)
	for i := 0; i < n; i++ {
		pos := i
		if pos < n-1 {
			for next := i + 1; next < n; next++ {
				if ranges[pos].First == ranges[next].First || ranges[pos].Last+1 >= ranges[next].First {
					if ranges[pos].Last < ranges[next].Last {
						// merge two ranges, like [1-3 1-4] [1-3 2-5] [1-3 4-5 5-7]
						ranges[pos].Last = ranges[next].Last
					}
					// skip to next, merged
					i++
					ranges[i].Last = 0
				} else {
					break
				}
			}
		}

		if pos != j {
			ranges[j] = ranges[pos]
			ranges[pos].Last = 0
		}
		if ranges[j].Last != 0 {
			j++
		}
	}

	return ranges[:j]
}

// RunesExpand expand runes like [a-z0]
func RunesRangeExpand(s string) (rs RunesRanges, ok bool) {
	if len(s) > 1 && s[0] == '[' && s[len(s)-1] == ']' {
		// TODO: support escape symbols
		s = s[1 : len(s)-1]
		if len(s) == 0 {
			return rs, true
		}
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
					if start <= 127 {
						if c > 127 {
							return rs, false
						}
						if start <= c {
							for i := byte(start); i <= byte(c); i++ {
								rs.ASCII.Add(i)
							}
							rs.setSizes(1)
						} else {
							return rs, false
						}
					} else if start <= c {
						// TODO: detect overlapped ranges like Я-你 beetween codepages
						n := utf8.RuneLen(start)
						cn := utf8.RuneLen(c)
						if n != cn {
							return rs, false
						}
						rs.setSizes(n)
						rs.UnicodeRanges = append(rs.UnicodeRanges, RuneRange{First: start, Last: c})
					} else {
						return rs, false
					}
					start = utf8.RuneError
					isRange = false
				}
			} else {
				if start != utf8.RuneError {
					if start <= 127 {
						rs.ASCII.Add(byte(start))
						rs.setSizes(1)
					} else {
						rs.UnicodeRanges = append(rs.UnicodeRanges, RuneRange{First: start, Last: start})
						rs.setSizes(utf8.RuneLen(start))
					}
				}
				start = c
			}
		}
		if start != utf8.RuneError {
			if start != utf8.RuneError {
				if start <= math.MaxInt8 {
					rs.ASCII.Add(byte(start))
					rs.setSizes(1)
				} else {
					rs.UnicodeRanges = append(rs.UnicodeRanges, RuneRange{First: start, Last: start})
					rs.setSizes(utf8.RuneLen(start))
				}
			}
		}

		rs.UnicodeRanges = RunesRangeMerge(rs.UnicodeRanges)
	} else {
		return rs, false
	}

	return rs, true
}

// RunesRanges is a range of ascii/unicode symbols
type RunesRanges struct {
	ASCII         ASCIISet
	UnicodeRanges []RuneRange
	NeedMerge     bool
	MinSize       int
	MaxSize       int
}

func (rs *RunesRanges) Equal(a *RunesRanges) bool {
	if a == nil {
		return false
	}
	if a.ASCII != rs.ASCII {
		return false
	}
	if rs.MinSize != a.MinSize || rs.MaxSize != a.MaxSize {
		return false
	}
	return SliceEqual(rs.UnicodeRanges, a.UnicodeRanges)
}

func (rs *RunesRanges) WriteString(buf *strings.Builder) {
	buf.WriteRune('[')
	rs.ASCII.WriteString(buf)
	for _, r := range rs.UnicodeRanges {
		buf.WriteRune(r.First)
		if r.First != r.Last {
			buf.WriteRune('-')
			buf.WriteRune(r.Last)
		}
	}
	buf.WriteRune(']')
}

func (rs *RunesRanges) String() string {
	var buf strings.Builder
	buf.Grow(127)
	rs.WriteString(&buf)
	return buf.String()
}

func (rs *RunesRanges) setSizes(n int) {
	if rs.MaxSize < n {
		rs.MaxSize = n
	}
	if rs.MinSize > n || rs.MinSize == 0 {
		rs.MinSize = n
	}
}

// Add add a rune to RunesRanges (need call Merge after complete)
func (rs *RunesRanges) Add(r rune) {
	if r <= 127 {
		rs.ASCII.Add(byte(r))
		rs.setSizes(1)
	} else {
		rs.setSizes(utf8.RuneLen(r))
		rs.UnicodeRanges = append(rs.UnicodeRanges, RuneRange{First: r, Last: r})
		rs.NeedMerge = true
	}
}

// Adds add runes to RunesRanges (need call Merge after complete)
func (rs *RunesRanges) Adds(runes string) {
	for _, r := range runes {
		rs.Add(r)
	}
}

// Merge merge runes ranges (need to call after Add/Adds before usage)
func (rs *RunesRanges) Merge() {
	if rs.NeedMerge {
		rs.UnicodeRanges = RunesRangeMerge(rs.UnicodeRanges)
		rs.NeedMerge = false
	}
}

func (rs *RunesRanges) ContainsUnicode(c rune) bool {
	if len(rs.UnicodeRanges) == 0 {
		return false
	} else if len(rs.UnicodeRanges) == 1 {
		return c >= rs.UnicodeRanges[0].First && c <= rs.UnicodeRanges[0].Last
	} else {
		high := len(rs.UnicodeRanges) - 1
		if c < rs.UnicodeRanges[0].First || c > rs.UnicodeRanges[high].Last {
			return false
		}

		if c <= rs.UnicodeRanges[0].Last {
			// first range match
			return true
		}
		if c >= rs.UnicodeRanges[high].First {
			// last range match
			return true
		}
		low := 1
		high--
		for low <= high {
			mid := (low + high) / 2
			if c >= rs.UnicodeRanges[mid].First {
				if c <= rs.UnicodeRanges[mid].Last {
					return true
				} else {
					low = mid + 1
				}
			} else {
				high = mid - 1
			}
		}
	}
	return false
}

func (rs *RunesRanges) Contains(c rune) bool {
	if c <= 127 {
		return rs.ASCII.Contains(byte(c))
	}
	return rs.ContainsUnicode(c)
}

// Index return index in string whether symbol is first inside.
func (rs *RunesRanges) IndexByte(b []byte) (pos int, c rune, n int) {
	if len(rs.UnicodeRanges) == 0 {
		for i := range b {
			if rs.ASCII.Contains(b[i]) {
				return i, rune(b[i]), 1
			}
		}
		return -1, utf8.RuneError, 0
	}

	for i := 0; i < len(b); {
		if b[i] < utf8.RuneSelf {
			if rs.ASCII.Contains(b[i]) {
				return i, rune(b[i]), 1
			}
		} else {
			c, n = utf8.DecodeRune(b[i:])
			if c == utf8.RuneError {
				return -1, utf8.RuneError, 0
			}
			if rs.ContainsUnicode(c) {
				pos = i
				return
			}
			i += n
		}
	}

	return -1, utf8.RuneError, 0
}

// // Index return index in string whether symbol is first inside.
func (rs *RunesRanges) Index(s string) (pos int, c rune, n int) {
	if len(rs.UnicodeRanges) == 0 {
		for i := 0; i < len(s); i++ {
			if rs.ASCII.Contains(s[i]) {
				return i, rune(s[i]), 1
			}
		}
		return -1, utf8.RuneError, 0
	}
	for i := 0; i < len(s); {
		if s[i] < utf8.RuneSelf {
			if rs.ASCII.Contains(s[i]) {
				return i, rune(s[i]), 1
			}
			i++
		} else {
			c, n = utf8.DecodeRuneInString(s[i:])
			if c == utf8.RuneError {
				return -1, utf8.RuneError, 0
			}
			if rs.ContainsUnicode(c) {
				pos = i
				return
			}
			i += n
		}
	}

	return -1, utf8.RuneError, 0
}

// StartsWith check than first symbol in string in range
func (rs *RunesRanges) StartsWith(s string) (c rune, n int) {
	if s == "" {
		return utf8.RuneError, -1
	}

	// detect ASCII
	if s[0] < utf8.RuneSelf {
		if rs.ASCII.Contains(s[0]) {
			return rune(s[0]), 1
		} else {
			return utf8.RuneError, -1
		}
	}

	// Unicode
	c, n = utf8.DecodeRuneInString(s)
	if c != utf8.RuneError && rs.ContainsUnicode(c) {
		return
	}

	return utf8.RuneError, -1
}

// EndsWith check than first symbol in string in range
func (rs *RunesRanges) EndsWith(s string) (c rune, n int) {
	if s == "" {
		return utf8.RuneError, -1
	}

	c, n = utf8.DecodeLastRuneInString(s)
	if c == utf8.RuneError {
		return utf8.RuneError, -1
	}

	// detect ASCII
	if c < utf8.RuneSelf {
		if rs.ASCII.Contains(byte(c)) {
			return c, 1
		} else {
			return utf8.RuneError, -1
		}
	}

	// Unicode
	if rs.ContainsUnicode(c) {
		n = len(s) - n
		return
	}

	return utf8.RuneError, -1
}
