package utils

import (
	"strings"
	"unicode/utf8"
)

// ASCIISet is a 32-byte value, where each bit represents the presence of a
// given ASCII character in the set. The 128-bits of the lower 16 bytes,
// starting with the least-significant bit of the lowest word to the
// most-significant bit of the highest word, map to the full range of all
// 128 ASCII characters. The 128-bits of the upper 16 bytes will be zeroed,
// ensuring that any non-ASCII character will be reported as not in the set.
// This allocates a total of 32 bytes even though the upper half
// is unused to avoid bounds checks in asciiSet.contains.
type ASCIISet [8]uint32

func (ac *ASCIISet) WriteString(buf *strings.Builder) {
	if !ac.IsEmpty() {
		var start, i byte
		for i = 1; i <= 127; i++ {
			if ac.Contains(i) {
				if start == 0 {
					start = i
				}
			} else if start != 0 {
				buf.WriteByte(start)
				if i > start+1 {
					buf.WriteByte('-')
					buf.WriteByte(i - 1)
				}
				start = 0
			}
		}
		if start != 0 {
			buf.WriteByte(start)
		}
	}
}

func (ac *ASCIISet) IsEmpty() bool {
	return ac[0] == 0 && ac[1] == 0 && ac[2] == 0 && ac[3] == 0 &&
		ac[4] == 0 && ac[5] == 0 && ac[6] == 0 && ac[7] == 0
}

func (ac *ASCIISet) Count() (n int, first byte) {
	if !ac.IsEmpty() {
		var i byte = 1
		for ; i <= 127; i++ {
			if ac.Contains(i) {
				n++
				if first == 0 {
					first = i
				}
			}
		}
	}
	return
}

func (ac *ASCIISet) Clear() {
	ac[0] = 0
	ac[1] = 0
	ac[2] = 0
	ac[3] = 0
	ac[4] = 0
	ac[5] = 0
	ac[6] = 0
	ac[7] = 0
}

func (ac *ASCIISet) String() string {
	var buf strings.Builder
	buf.Grow(127)
	ac.WriteString(&buf)
	return buf.String()
}

func (ac *ASCIISet) WriteStringAll(buf *strings.Builder) {
	var i byte
	for i = 1; i < 127; i++ {
		if ac.Contains(i) {
			buf.WriteByte(i)
		}
	}
}

func (ac *ASCIISet) StringAll() string {
	var buf strings.Builder
	buf.Grow(127)
	ac.WriteStringAll(&buf)
	return buf.String()
}

// MakeASCIISet creates a set of ASCII characters and reports whether all characters in chars are ASCII.
func MakeASCIISet(chars string) (as ASCIISet, ok bool) {
	for i := 0; i < len(chars); i++ {
		c := chars[i]
		if c >= utf8.RuneSelf {
			return as, false
		}
		as[c/32] |= 1 << (c % 32)
	}
	return as, true
}

// MakeASCIISetMust creates a set of ASCII characters and reports whether all characters in chars are ASCII.
func MakeASCIISetMust(chars string) ASCIISet {
	as, ok := MakeASCIISet(chars)
	if !ok {
		panic("invalid ASCII set: " + chars)
	}
	return as
}

// MakeASCIISet creates a set of ASCII characters and reports whether all characters in chars are ASCII.
func (as *ASCIISet) Add(c byte) bool {
	if c >= utf8.RuneSelf {
		return false
	}
	as[c/32] |= 1 << (c % 32)
	return true
}

// Contains reports whether c is inside the set.
func (as *ASCIISet) Contains(c byte) bool {
	if c >= utf8.RuneSelf {
		return false
	}
	return (as[c/32] & (1 << (c % 32))) != 0
}

// IndexByte return index in byte slice whether symbol is first inside.
func (as *ASCIISet) IndexByte(b []byte) int {
	for i := 0; i < len(b); i++ {
		if as.Contains(b[i]) {
			return i
		}
	}
	return -1
}

// Index return index in string whether symbol is first inside.
func (as *ASCIISet) Index(s string) int {
	for i := 0; i < len(s); i++ {
		if as.Contains(s[i]) {
			return i
		}
	}
	return -1
}

// LastIndexByte return index in byte slice whether symbol is first inside.
func (as *ASCIISet) LastIndexByte(b []byte) int {
	for i := len(b) - 1; i >= 0; i-- {
		if as.Contains(b[i]) {
			return i
		}
	}
	return -1
}

// LastIndex return index in string whether symbol is first inside.
func (as *ASCIISet) LastIndex(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if as.Contains(s[i]) {
			return i
		}
	}
	return -1
}
