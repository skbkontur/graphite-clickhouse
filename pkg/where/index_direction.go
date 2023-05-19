package where

import (
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
)

func checkReverses(query string, reverse config.IndexDirection, reverses config.IndexReverses) config.IndexDirection {
	for _, rule := range reverses {
		if len(rule.Prefix) > 0 && !strings.HasPrefix(query, rule.Prefix) {
			continue
		}
		if len(rule.Suffix) > 0 && !strings.HasSuffix(query, rule.Suffix) {
			continue
		}
		if rule.Regex != nil && rule.Regex.FindStringIndex(query) == nil {
			continue
		}
		return rule.Reverse
	}
	return reverse
}

func useReverse(query string, reverse config.IndexDirection, reverses config.IndexReverses) bool {
	if reverse == config.IndexDirect {
		return false
	} else if reverse == config.IndexReversed {
		return true
	}

	if reverse = checkReverses(query, reverse, reverses); reverse != config.IndexAuto {
		return useReverse(query, reverse, reverses)
	}

	w := IndexWildcard(query)
	if w == -1 {
		return useReverse(query, config.IndexDirect, reverses)
	}
	firstWildcardNode := strings.Count(query[:w], ".")

	w = IndexLastWildcard(query)
	lastWildcardNode := strings.Count(query[w:], ".")

	if firstWildcardNode < lastWildcardNode {
		return useReverse(query, config.IndexReversed, reverses)
	}
	return useReverse(query, config.IndexDirect, reverses)
}
