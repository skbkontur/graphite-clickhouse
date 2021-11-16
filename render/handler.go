package render

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/utils"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
	"github.com/lomik/graphite-clickhouse/render/reply"
)

// Handler serves /render requests
type Handler struct {
	config *config.Config
}

// NewHandler generates new *Handler
func NewHandler(config *config.Config) *Handler {
	h := &Handler{
		config: config,
	}

	return h
}

func TargetKey(fromAndUntil string, ts int64, target string) string {
	return fromAndUntil + ";ts=" + strconv.FormatInt(ts, 10) + ";" + target
}

func getCacheTimeout(now time.Time, from, until int64, cacheConfig *config.CacheConfig) int32 {
	duration := time.Second * time.Duration(until-from)
	if duration <= cacheConfig.ShortDuration && now.Unix()-until <= 61 {
		// short cache ttl
		return cacheConfig.ShortTimeoutSec
	}
	return cacheConfig.DefaultTimeoutSec
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("render")

	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	var err error

	defer func() {
		if rec := recover(); rec != nil {
			logger.Error("panic during eval:",
				zap.String("requestID", scope.String(r.Context(), "requestID")),
				zap.Any("reason", rec),
				zap.Stack("stack"),
			)
			answer := fmt.Sprintf("%v\nStack trace: %v", rec, zap.Stack("").String)
			http.Error(w, answer, http.StatusInternalServerError)
		}
	}()

	r.ParseMultipartForm(1024 * 1024)
	formatter, err := reply.GetFormatter(r)
	if err != nil {
		logger.Error("formatter", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), http.StatusBadRequest)
		return
	}

	fetchRequests, err := formatter.ParseRequest(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), http.StatusBadRequest)
		return
	}

	// TODO: move to a function
	var wg sync.WaitGroup
	var lock sync.RWMutex
	var cachedFind bool
	errors := make([]error, 0, len(fetchRequests))
	useCache := h.config.Common.FindCache != nil && !parser.TruthyBool(r.FormValue("noCache"))
	now := time.Now()
	var metricsLen int
	for tf, target := range fetchRequests {
		for _, expr := range target.List {
			wg.Add(1)
			go func(tf data.TimeFrame, target string, am *alias.Map) {
				defer wg.Done()

				var fndResult finder.Result
				var err error

				var cacheTimeout int32
				var key string
				var ts int64

				if useCache {
					var fromAndUntil string
					cacheTimeout = getCacheTimeout(now, tf.From, tf.Until, &h.config.Common.FindCacheConfig)
					ts = utils.TimestampTruncate(time.Now().Unix(), time.Duration(cacheTimeout)*time.Second)
					fromAndUntil = time.Unix(tf.From, 0).Format("2006-01-02") + ";" + time.Unix(tf.Until, 0).Format("2006-01-02")
					key = TargetKey(fromAndUntil, ts, target)
					body, err := h.config.Common.FindCache.Get(key)
					if err == nil {
						cachedFind = true
						if len(body) > 0 {
							// ApiMetrics.RequestCacheHits.Add(1)
							var f finder.Finder
							if strings.Index(target, "seriesByTag(") == -1 {
								f = finder.NewCachedIndex(body)
							} else {
								f = finder.NewCachedTags(body)
							}

							am.MergeTarget(f.(finder.Result), target, false)
							lock.Lock()
							metricsLen += am.Len()
							lock.Unlock()

							logger.Info("finder", zap.String("get_cache", target), zap.Int64("timestamp", ts),
								zap.Int("metrics", am.Len()), zap.Bool("find_cached", true))
						}
						return
					}
				}

				// Search in small index table first
				fndResult, err = finder.Find(h.config, r.Context(), target, tf.From, tf.Until)
				if err != nil {
					logger.Error("find", zap.Error(err))
					lock.Lock()
					errors = append(errors, err)
					lock.Unlock()
					return
				}

				body := am.MergeTarget(fndResult, target, useCache)
				if useCache {
					h.config.Common.FindCache.Set(key, body, cacheTimeout)
					logger.Info("finder", zap.String("set_cache", target), zap.Int64("timestamp", ts),
						zap.Int("metrics", am.Len()), zap.Bool("find_cached", false), zap.Int32("ttl", cacheTimeout))
				}
				lock.Lock()
				metricsLen += am.Len()
				lock.Unlock()
			}(tf, expr, target.AM)
		}
	}
	wg.Wait()
	if len(errors) != 0 {
		clickhouse.HandleError(w, errors[0])
		return
	}

	logger.Info("finder", zap.Int("metrics", metricsLen), zap.Bool("find_cached", cachedFind))

	if cachedFind {
		w.Header().Set("X-Cached-Find", "true")
	}

	if metricsLen == 0 {
		formatter.Reply(w, r, data.EmptyResponse())
		return
	}

	reply, err := fetchRequests.Fetch(r.Context(), h.config, config.ContextGraphite)
	if err != nil {
		clickhouse.HandleError(w, err)
		return
	}

	if len(reply) == 0 {
		formatter.Reply(w, r, data.EmptyResponse())
		return
	}

	start := time.Now()
	formatter.Reply(w, r, reply)
	d := time.Since(start)
	logger.Debug("reply", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))
}
