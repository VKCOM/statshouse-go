package statshouse

import (
	"strconv"
	"testing"
	"time"
)

func benchmarkClient(b *testing.B, bounded bool) *Client {
	b.Helper()
	args := ConfigureArgs{Network: "udp", Logger: func(string, ...interface{}) {}}
	if !bounded {
		return NewClientEx(args)
	}
	budget, _ := NewMemoryBudget(4 << 20)
	c, err := NewClientWithBudget(args, budget)
	if err != nil {
		b.Fatal(err)
	}
	return c
}
func BenchmarkBudgetCachedCount(b *testing.B) {
	for _, bounded := range []bool{false, true} {
		b.Run(strconv.FormatBool(bounded), func(b *testing.B) {
			c := benchmarkClient(b, bounded)
			defer c.Close()
			m := c.MetricRef("counter", Tags{1: "label"})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.Count(1)
			}
			b.StopTimer()
		})
	}
}
func BenchmarkBudgetLookupCount(b *testing.B) {
	for _, bounded := range []bool{false, true} {
		b.Run(strconv.FormatBool(bounded), func(b *testing.B) {
			c := benchmarkClient(b, bounded)
			defer c.Close()
			c.Count("counter", Tags{1: "label"}, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.Count("counter", Tags{1: "label"}, 1)
			}
			b.StopTimer()
		})
	}
}
func BenchmarkBudgetCachedValue(b *testing.B) {
	for _, bounded := range []bool{false, true} {
		b.Run(strconv.FormatBool(bounded), func(b *testing.B) {
			c := benchmarkClient(b, bounded)
			defer c.Close()
			m := c.MetricRef("value", Tags{1: "label"})
			m.Value(1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.Value(float64(i))
			}
			b.StopTimer()
		})
	}
}
func BenchmarkBudgetStringTop(b *testing.B) {
	for _, bounded := range []bool{false, true} {
		b.Run(strconv.FormatBool(bounded), func(b *testing.B) {
			c := benchmarkClient(b, bounded)
			defer c.Close()
			m := c.MetricRef("strings", Tags{})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.StringTop("value")
				if i%1024 == 1023 {
					c.send(uint32(time.Now().Unix()))
				}
			}
			b.StopTimer()
		})
	}
}
func BenchmarkBudgetCardinalityOverflow(b *testing.B) {
	c := benchmarkClient(b, true)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Count("cardinality", Tags{1: strconv.Itoa(i)}, 1)
	}
	b.StopTimer()
}
func BenchmarkBudgetSnapshot(b *testing.B) {
	c := benchmarkClient(b, true)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Snapshot()
	}
	b.StopTimer()
}
