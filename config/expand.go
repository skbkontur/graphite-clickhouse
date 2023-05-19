package config

import (
	"github.com/msaf1980/go-expirecache"
)

var (
	BaseFinderQueryCache  *expirecache.Cache[string, string]
	IndexFinderQueryCache *expirecache.Cache[string, string]
	TaggedQueryCache      *expirecache.Cache[string, string]
	ExpandTTL             int32
	ExpandDepth           int
	ExpandMax             int
)

func SetQueryCache(n uint64, ttl int32, expandMax, expandDepth int) {
	if n > 0 {
		if ttl <= 0 {
			ttl = 7200
		}
		BaseFinderQueryCache = expirecache.New[string, string](n)
		IndexFinderQueryCache = expirecache.New[string, string](n)
		ExpandTTL = ttl
	} else {
		BaseFinderQueryCache = nil
		IndexFinderQueryCache = nil
		ExpandTTL = 0
	}
	ExpandDepth = expandDepth
	ExpandMax = expandMax
}
