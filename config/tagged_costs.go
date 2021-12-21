package config

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type Costs struct {
	Total   int            `toml:"total" json:"total" comment:"cost for wildcarded equalence or matched with regex (if not found in values)"`
	Default int            `toml:"default" json:"default" comment:"default cost for equalence without wildcards (if not found in values)"`
	Values  map[string]int `toml:"values" json:"values" comment:"cost with some value (for equalence or with wildcard or regex) (additional tuning, usually not needed)"`
}

type TaggedCosts struct {
	StoreFile string            `toml:"store" json:"store" comment:"save loaded costs"`
	AutoLoad  time.Duration     `toml:"auto-load" json:"auto-load" comment:"periodic auto load costs from database"`
	Costs     map[string]*Costs `toml:"costs" json:"costs" commented:"true" comment:"tune cost for tags values (with  or without wildcards or regex"`
	Default   int               `toml:"default" json:"default" commented:"true" comment:"default cost (without wildcard or regex)"`
	Total     int               `toml:"wildcard" json:"wildcard" commented:"true" comment:"default cost (with wildcard or regex)"`

	costs   map[string]*Costs `toml:"-" json:"-"` // store custom costs
	lock    sync.RWMutex      `toml:"-" json:"-"`
	updated bool              `toml:"-" json:"-"` // last update success
}

func (t *TaggedCosts) Lock() {
	t.lock.Lock()
}

func (t *TaggedCosts) Unlock() {
	t.lock.Unlock()
}

func (t *TaggedCosts) RLock() {
	t.lock.RLock()
}

func (t *TaggedCosts) RUnlock() {
	t.lock.RUnlock()
}

func (t *TaggedCosts) Check() error {
	if t != nil {
		if t.Default < 1 || t.Default > 1000 {
			return fmt.Errorf("default tagged cost must be > 0 and <= 1000")
		}
		if t.Default >= t.Total {
			return fmt.Errorf("total tagged cost must be greater than default")
		}

		for key, costs := range t.Costs {
			if costs.Default == 0 {
				costs.Default = t.Default
			} else if costs.Default > 1000 {
				return fmt.Errorf("default tagged cost[%s] must be > 0 and <= 1000 or -1", key)
			}
			if costs.Total == 0 {
				costs.Total = t.Total
			} else if costs.Total > 1000 {
				return fmt.Errorf("wildcard tagged cost[%s] must be > 0 and <= 1000 or -1", key)
			}
		}

		if costs, ok := t.Costs["name"]; ok {
			if _, ok := t.Costs["__name__"]; ok {
				return fmt.Errorf("duplicate tagged name and __name__ in tagged costs")
			}
			t.Costs["__name__"] = costs
			delete(t.Costs, "name")
		}
	}

	t.costs = t.Costs

	return nil
}

func (t *TaggedCosts) Update(addr, table string, taggedAutocompleDays int) error {
	t.updated = false

	var db string
	arr := strings.SplitN(table, ".", 2)
	if len(arr) == 1 {
		db = "default"
	} else {
		db, table = arr[0], arr[1]
	}

	untilDate := time.Now()
	fromDate := untilDate.AddDate(0, 0, -1)

	query := fmt.Sprintf(`SELECT Tag1, count(Tag1) AS Count FROM %s.%s WHERE Date>='%s' AND Date<='%s'  GROUP BY Tag1 FORMAT TabSeparatedRaw`,
		db, table, fromDate.Format("2006-01-02"), untilDate.Format("2006-01-02"))

	body, err := clickhouse.Query(
		scope.New(context.Background()).WithLogger(zapwriter.Logger("tagged_cost")).WithTable("graphite_tags"),
		addr,
		query,
		clickhouse.Options{Timeout: 10 * time.Second, ConnectTimeout: 10 * time.Second},
		nil,
	)
	if err != nil {
		return err
	}

	return t.updateFrom(body)
}

func (t *TaggedCosts) updateFrom(body []byte) (err error) {
	rows := strings.Split(string(body), "\n")

	costs := make(map[string]*Costs)

	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		v := strings.Split(row, "\t")
		if len(v) != 2 {
			return fmt.Errorf("bad line %d: %s", i, row)
		}
		index := strings.Index(v[0], "=")
		if index < 1 {
			return fmt.Errorf("bad line %d (name/value): %s", i, row)
		}

		name := v[0][0:index]
		value := v[0][index+1:]
		if n, err := strconv.Atoi(v[1]); err == nil {
			c, _ := costs[name]
			if c == nil {
				c = &Costs{Values: make(map[string]int)}
				costs[name] = c
			}
			c.Values[value] = n
			c.Total += n
		} else {
			return fmt.Errorf("bad line %d (count): %s", i, row)
		}
	}

	// normalize
	max := 0
	for _, c := range costs {
		if max < c.Total {
			max = c.Total
		}
	}
	div := float64(max) / float64(1000000)
	for _, c := range costs {
		c.Total = int(float64(c.Total) / div)
		if c.Total == 0 {
			c.Total++
		}
		values := make(sort.IntSlice, len(c.Values))
		i := 0
		for k, v := range c.Values {
			values[i] = int(float64(v) / div)
			if values[i] == 0 {
				values[i]++
			}
			c.Values[k] = values[i]
			i++
		}
		sort.Sort(values)
		i = (len(c.Values) - 1) / 2
		c.Default = values[i]
	}

	// store results
	if len(t.StoreFile) > 0 {
		err = saveCosts(costs, t.StoreFile)
	}

	// merge with customized from config
	t.mergeCosts(costs)
	t.Lock()
	t.Costs = costs
	t.updated = true
	t.Unlock()

	return err
}

func (t *TaggedCosts) mergeCosts(costs map[string]*Costs) {
	for k, cost := range t.costs {
		if cost.Default != 0 {
			costs[k].Default = cost.Default
		}
		if cost.Total != 0 {
			costs[k].Total = cost.Total
		}
	}
}

func saveCosts(costs map[string]*Costs, fileName string) error {
	body, err := json.Marshal(costs)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(fileName, body, 0644)
}

func (t *TaggedCosts) Updater(addr, table string, taggedAutocompleDays int) {
	for {
		// If update success
		if t.updated {
			time.Sleep(t.AutoLoad)
		} else {
			time.Sleep(5 * time.Minute)
		}

		start := time.Now()
		if err := t.Update(addr, table, taggedAutocompleDays); err != nil {
			zapwriter.Logger("tagged_costs").Error("unable to load", zap.Error(err), zap.Duration("time", time.Since(start)))
		} else {
			zapwriter.Logger("tagged_costs").Info("load", zap.Duration("time", time.Since(start)))
		}
	}
}
