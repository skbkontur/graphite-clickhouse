package finder

import (
	"fmt"
	"testing"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/stretchr/testify/assert"
)

func TestTaggedWhere(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		query    string
		where    string
		prewhere string
		isErr    bool
	}{
		// info about _tag "directory"
		{"seriesByTag('key=value')", "Tag1='key=value'", "", false},
		// test case for wildcarded name, must be not first check
		{"seriesByTag('name=*', 'key=value')", "(Tag1='key=value') AND (arrayExists((x) -> x LIKE '__name__=%', Tags))", "", false},
		{"seriesByTag('name=*', 'key=value*')", "(Tag1 LIKE '__name__=%') AND (arrayExists((x) -> x LIKE 'key=value%', Tags))", "", false},
		{"seriesByTag('name=rps')", "Tag1='__name__=rps'", "", false},
		{"seriesByTag('name=~cpu.usage')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu.usage')", false},
		{"seriesByTag('name=~cpu|mem')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu|mem')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu|mem')", false},
		{"seriesByTag('name=~cpu|mem$')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu|mem$')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=.*cpu|mem$')", false},
		{"seriesByTag('name=~^cpu|mem')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=cpu|mem')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=cpu|mem')", false},
		{"seriesByTag('name=~^cpu|mem$')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=cpu|mem$')", "Tag1 LIKE '\\\\_\\\\_name\\\\_\\\\_=%' AND match(Tag1, '^__name__=cpu|mem$')", false},
		{"seriesByTag('name=rps', 'key=~value')", "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*value'), Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~^value$')", "(Tag1='__name__=rps') AND (arrayExists((x) -> x='key=value', Tags))", "", false},
		{"seriesByTag('name=rps', 'key=~hello.world')", "(Tag1='__name__=rps') AND (arrayExists((x) -> x LIKE 'key=%' AND match(x, '^key=.*hello.world'), Tags))", "", false},
		{`seriesByTag('cpu=cpu-total','host=~Vladimirs-MacBook-Pro\.local')`, `(Tag1='cpu=cpu-total') AND (arrayExists((x) -> x LIKE 'host=%' AND match(x, '^host=.*Vladimirs-MacBook-Pro\\.local'), Tags))`, "", false},
		// grafana multi-value variable produce this
		{"seriesByTag('name=value','what=*')", "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false},        // If All masked to value with *
		{"seriesByTag('name=value','what=*x')", "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false},      // If All masked to value with *
		{"seriesByTag('name=value','what!=*x')", "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x LIKE 'what=%x', Tags))", "", false}, // If All masked to value with *
		{"seriesByTag('name={avg,max}')", "Tag1 IN ('__name__=avg','__name__=max')", "", false},
		{"seriesByTag('name=m{in}')", "Tag1='__name__=min'", "", false},
		{"seriesByTag('name=m{in,ax}')", "Tag1 IN ('__name__=min','__name__=max')", "", false},
		{"seriesByTag('name=m{in,ax')", "Tag1='__name__=m{in,ax'", "", true},
		{"seriesByTag('name=value','what={avg,max}')", "(Tag1='__name__=value') AND (arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
		{"seriesByTag('name=value','what!={avg,max}')", "(Tag1='__name__=value') AND (NOT arrayExists((x) -> x IN ('what=avg','what=max'), Tags))", "", false},
		// grafana workaround for multi-value variables default, masked with *
		{"seriesByTag('name=value','what=~*')", "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
		// empty tag value during autocompletion
		{"seriesByTag('name=value','what=~')", "(Tag1='__name__=value') AND (arrayExists((x) -> x LIKE 'what=%', Tags))", "", false}, // If All masked to value with *
	}

	for _, test := range table {
		testName := fmt.Sprintf("query: %#v", test.query)

		terms, err := ParseSeriesByTag(test.query, nil)

		if !test.isErr {
			assert.NoError(err, testName+", err")
		}

		w, pw, err := TaggedWhere(terms)

		if test.isErr {
			assert.Error(err, testName+", err")
			continue
		} else {
			assert.NoError(err, testName+", err")
		}

		assert.Equal(test.where, w.String(), testName+", where")
		assert.Equal(test.prewhere, pw.String(), testName+", prewhere")
	}
}

func TestParseSeriesByTag(t *testing.T) {
	assert := assert.New(t)

	ok := func(query string, expected []TaggedTerm) {
		p, err := ParseSeriesByTag(query, nil)
		assert.NoError(err)
		length := len(expected)
		if length < len(p) {
			length = len(p)
		}
		for i := 0; i < length; i++ {
			if i >= len(p) {
				t.Errorf("%s\n- [%d]=%+v", query, i, expected[i])
			} else if i >= len(expected) {
				t.Errorf("%s\n+ [%d]=%+v", query, i, p[i])
			} else if p[i] != expected[i] {
				t.Errorf("%s\n- [%d]=%+v\n+ [%d]=%+v", query, i, expected[i], i, p[i])
			}
		}
	}

	ok(`seriesByTag('key=value')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "value"},
	})

	ok(`seriesByTag('name=rps')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "__name__", Value: "rps"},
	})

	ok(`seriesByTag('name=~cpu.usage')`, []TaggedTerm{
		{Op: TaggedTermMatch, Key: "__name__", Value: "cpu.usage"},
	})

	ok(`seriesByTag('name!=cpu.usage')`, []TaggedTerm{
		{Op: TaggedTermNe, Key: "__name__", Value: "cpu.usage"},
	})

	ok(`seriesByTag('name!=~cpu.usage')`, []TaggedTerm{
		{Op: TaggedTermNotMatch, Key: "__name__", Value: "cpu.usage"},
	})

	ok(`seriesByTag('cpu=cpu-total','host=~Vladimirs-MacBook-Pro\.local')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "cpu", Value: "cpu-total"},
		{Op: TaggedTermMatch, Key: "host", Value: `Vladimirs-MacBook-Pro\.local`},
	})

}

func TestParseSeriesByTagWithCosts(t *testing.T) {
	assert := assert.New(t)

	taggedCosts := &config.TaggedCosts{
		Default:  2,
		Wildcard: 500,
		Costs: map[string]*config.Costs{
			"name":        {Default: 1, Total: 100},
			"environment": {Default: 100, Total: 100},
			"dc":          {Default: 60, Total: 100},
			"project":     {Default: 50, Total: 50},
			"key":         {Values: map[string]int{"value2": 70, "value3": -1, "val*4": -1, "^val.*4$": -1}},
		},
	}
	if err := taggedCosts.Check(); err != nil {
		t.Fatal(err)
	}

	ok := func(query string, expected []TaggedTerm) {
		p, err := ParseSeriesByTag(query, taggedCosts)
		assert.NoError(err)
		length := len(expected)
		if length < len(p) {
			length = len(p)
		}
		for i := 0; i < length; i++ {
			if i >= len(p) {
				t.Errorf("%s\n- [%d]=%+v", query, i, expected[i])
			} else if i >= len(expected) {
				t.Errorf("%s\n+ [%d]=%+v", query, i, p[i])
			} else if p[i] != expected[i] {
				t.Errorf("%s\n- [%d]=%+v\n+ [%d]=%+v", query, i, expected[i], i, p[i])
			}
		}
	}

	ok(`seriesByTag('environment=production', 'dc=west', 'key=value')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "value", Cost: 2},
		{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 60},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100},
	})

	// Check for values cost (key=value2)
	ok(`seriesByTag('environment=production', 'dc=west', 'key=value2')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 60},
		{Op: TaggedTermEq, Key: "key", Value: "value2", Cost: 70},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100},
	})

	// Check for __name_ preference
	ok(`seriesByTag('environment=production', 'dc=west', 'key=value', 'name=cpu.load_avg')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 1},
		{Op: TaggedTermEq, Key: "key", Value: "value", Cost: 2},
		{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 60},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100},
	})

	// Check for __name_ preference overrided
	ok(`seriesByTag('environment=production', 'dc=west', 'name=cpu.load_avg', 'key=value3')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "value3", Cost: -1},
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 1},
		{Op: TaggedTermEq, Key: "dc", Value: "west", Cost: 60},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100},
	})

	// wildcard (dc=west*)
	ok(`seriesByTag('environment=production', 'dc=west*', 'name=cpu.load_avg', 'key=value3')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "value3", Cost: -1},
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 1},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100},
		{Op: TaggedTermEq, Key: "dc", Value: "west*", Cost: 100, HasWildcard: true},
	})

	// wildcard cost -1
	ok(`seriesByTag('dc=west*', 'environment=production', 'name=cpu.load_avg', 'key=val*4')`, []TaggedTerm{
		{Op: TaggedTermEq, Key: "key", Value: "val*4", Cost: -1, HasWildcard: true},
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 1},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100},
		{Op: TaggedTermEq, Key: "dc", Value: "west*", Cost: 100, HasWildcard: true},
	})

	// match cost -1 - not as wildcard
	ok(`seriesByTag('dc=~west.*', 'environment=production', 'name=cpu.load_avg', 'key=~^val.*4$')`, []TaggedTerm{
		{Op: TaggedTermMatch, Key: "key", Value: "^val.*4$", Cost: -1},
		{Op: TaggedTermEq, Key: "__name__", Value: "cpu.load_avg", Cost: 1},
		{Op: TaggedTermEq, Key: "environment", Value: "production", Cost: 100},
		{Op: TaggedTermMatch, Key: "dc", Value: "west.*", Cost: 100},
	})
}
