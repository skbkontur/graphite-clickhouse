package utils

import (
	"unicode/utf8"
	"unsafe"
)

// CloneString returns a fresh copy of s.
// It guarantees to make a copy of s into a new allocation,
// which can be important when retaining only a small substring
// of a much larger string. Using Clone can help such programs
// use less memory. Of course, since using Clone makes a copy,
// overuse of Clone can make programs use more memory.
// Clone should typically be used only rarely, and only when
// profiling indicates that it is needed.
// For strings of length zero the string "" will be returned
// and no allocation is made.
func CloneString(s string) string {
	if len(s) == 0 {
		return ""
	}
	b := make([]byte, len(s))
	copy(b, s)
	return UnsafeString(b)
}

// UnsafeString returns the string under byte buffer
func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func StringSkipRunes(s string, runes int) (next int) {
	for i := 0; i < runes; i++ {
		_, n := utf8.DecodeRuneInString(s)
		if n == 0 {
			// failback
			return -1
		}
		s = s[n:]
		next += n
	}
	return
}

func StringSkipRunesLast(s string, runes int) (end int) {
	l := len(s)
	var n int
	for i := 0; i < runes; i++ {
		_, n = utf8.DecodeLastRuneInString(s)
		if n == 0 {
			// failback
			return -1
		}
		s = s[:len(s)-n]
		end += n
	}
	end = l - end
	return
}

func SplitString(s string, start int) (string, string) {
	return s[:start], s[start:]
}
