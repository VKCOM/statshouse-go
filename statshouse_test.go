// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statshouse_test

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse-go"
)

func TestCountRace(t *testing.T) {
	c := statshouse.NewClient(t.Logf, "udp", "" /* avoid sending anything */, "")

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Count("test_stat", statshouse.Tags{1: "hello", 2: "world"}, float64(j))
			}
		}()
	}
	wg.Wait()
}

func TestBucketRelease(t *testing.T) {
	var wg sync.WaitGroup
	c := statshouse.NewClient(t.Logf, "udp", "" /* avoid sending anything */, "")
	c.TrackBucketCount()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancel()
	for i := 1; i <= 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("test_metric%d", i)
			var tags statshouse.NamedTags
			for j := 0; j < i; j++ {
				tags = append(tags, [2]string{strconv.Itoa(j), fmt.Sprintf("name%d", j)})
			}
			n := 0
			for ; ctx.Err() == nil; n++ {
				c.MetricNamed(name, tags).Count(float64(i))
			}
			t.Logf("send # %d", n)
		}(i)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("test_metric%d", i)
			var tags statshouse.Tags
			for j := 0; j < i; j++ {
				tags[j] = strconv.Itoa(j)
			}
			n := 0
			for ; ctx.Err() == nil; n++ {
				c.Metric(name, tags).Count(float64(i))
			}
			t.Logf("send # %d", n)
		}(i)
	}
	wg.Wait()
	runtime.GC()
	for i := 0; i < 10 && c.BucketCount() != 0; i++ {
		time.Sleep(time.Second)
		runtime.GC()
	}
	require.Zero(t, c.BucketCount())
}

func BenchmarkValue2(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	defer c.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Value("test_stat", statshouse.Tags{1: "hello", 2: "world"}, float64(i))
	}
}

func BenchmarkRawValue(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	s := c.MetricRef("test_stat", statshouse.Tags{1: "hello", 2: "world"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Value(float64(i))
	}
}

func BenchmarkCount4(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Count("test_stat", statshouse.Tags{1: "hello", 2: "brave", 3: "new", 4: "world"}, float64(i))
	}
}

func BenchmarkRawCount(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	s := c.MetricRef("test_stat", statshouse.Tags{1: "hello", 2: "brave", 3: "new", 4: "world"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Count(float64(i))
	}
}

func BenchmarkLabeledValue2(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.NamedValue("test_stat", statshouse.NamedTags{{"hello", "world"}, {"world", "hello"}}, float64(i))
	}
}

func BenchmarkRawLabeledValue(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	s := c.MetricNamedRef("test_stat", statshouse.NamedTags{{"hello", "world"}, {"world", "hello"}})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Value(float64(i))
	}
}

func BenchmarkLabeledCount4(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.NamedCount("test_stat", statshouse.NamedTags{{"hello", "world"}, {"world", "hello"}, {"hello1", "world"}, {"world1", "hello"}}, float64(i))
	}
}

func BenchmarkRawLabeledCount(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	s := c.MetricNamedRef("test_stat", statshouse.NamedTags{{"hello", "world"}, {"world", "hello"}, {"hello1", "world"}, {"world1", "hello"}})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Count(float64(i))
	}
}
