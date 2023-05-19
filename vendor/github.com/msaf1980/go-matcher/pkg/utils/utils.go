package utils

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func SliceEqual[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ContainsReverse[T comparable](a []T, k T) bool {
	for i := len(a) - 1; i >= 0; i-- {
		if k == a[i] {
			return true
		}
	}
	return false
}
