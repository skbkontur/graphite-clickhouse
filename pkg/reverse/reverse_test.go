package reverse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReverse(t *testing.T) {
	assert := assert.New(t)
	table := map[string]string{
		"carbon.agents.carbon-clickhouse.graphite1.tcp.metricsReceived": "metricsReceived.tcp.graphite1.carbon-clickhouse.agents.carbon",
		"":                        "",
		".":                       ".",
		"carbon..xx":              "xx..carbon",
		".hello..world.":          ".world..hello.",
		"metric_name?label=value": "metric_name?label=value",
	}

	for k, expected := range table {
		assert.Equal(expected, String(k))
		p := string(k)
		assert.Equal([]byte(expected), Bytes([]byte(k)))
		// check k is unchanged
		assert.Equal(p, string(k))
		// inplace
		b := make([]byte, len(k))
		copy(b, k)
		Inplace(b)
		assert.Equal(expected, string(b))
	}
}

// func BenchmarkGlob(b *testing.B) {
// 	field := "test"
// 	tests := []string{
// 		"a.[a].te{s}.b",
// 		"a.{a,b}.te{s,t}*.b",
// 	}
// 	for _, query := range tests {
// 		b.Run(query, func(b *testing.B) {
// 			for i := 0; i < b.N; i++ {
// 				_ = Glob(field, query, -1, 0, config.IndexDirect, nil)
// 			}
// 		})
// 	}
// }
