// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statshouse_test

import (
	"sync"
	"testing"

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
				c.Metric("test_stat", statshouse.Tags{1: "hello", 2: "world"}).Count(float64(j))
			}
		}()
	}
	wg.Wait()
}

func BenchmarkValue2(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Metric("test_stat", statshouse.Tags{1: "hello", 2: "world"}).Value(float64(i))
	}
}

func BenchmarkRawValue(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	s := c.Metric("test_stat", statshouse.Tags{1: "hello", 2: "world"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Value(float64(i))
	}
}

func BenchmarkCount4(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Metric("test_stat", statshouse.Tags{1: "hello", 2: "brave", 3: "new", 4: "world"}).Count(float64(i))
	}
}

func BenchmarkRawCount(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	s := c.Metric("test_stat", statshouse.Tags{1: "hello", 2: "brave", 3: "new", 4: "world"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Count(float64(i))
	}
}

func BenchmarkLabeledValue2(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.MetricNamed("test_stat", statshouse.NamedTags{{"hello", "world"}, {"world", "hello"}}).Value(float64(i))
	}
}

func BenchmarkRawLabeledValue(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	s := c.MetricNamed("test_stat", statshouse.NamedTags{{"hello", "world"}, {"world", "hello"}})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Value(float64(i))
	}
}

func BenchmarkLabeledCount4(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.MetricNamed("test_stat", statshouse.NamedTags{{"hello", "world"}, {"world", "hello"}, {"hello1", "world"}, {"world1", "hello"}}).Count(float64(i))
	}
}

func BenchmarkRawLabeledCount(b *testing.B) {
	c := statshouse.NewClient(b.Logf, "udp", "" /* avoid sending anything */, "")
	s := c.MetricNamed("test_stat", statshouse.NamedTags{{"hello", "world"}, {"world", "hello"}, {"hello1", "world"}, {"world1", "hello"}})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Count(float64(i))
	}
}
