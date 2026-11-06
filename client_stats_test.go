package statshouse

import (
	"math"
	"sync/atomic"
	"testing"
)

func TestCumulativeCountersAboveInt32AndSaturation(t *testing.T) {
	var a atomic.Uint64
	saturatingAdd(&a, 1<<31)
	saturatingAdd(&a, 123)
	if a.Load() != (1<<31)+123 {
		t.Fatal(a.Load())
	}
	saturatingAdd(&a, math.MaxUint64)
	saturatingAdd(&a, 1)
	if a.Load() != math.MaxUint64 {
		t.Fatal("wrapped")
	}
}
func TestSnapshotDoesNotAllocate(t *testing.T) {
	c, _ := testBudgetClient(t, 128<<10)
	if n := testing.AllocsPerRun(100, func() { _ = c.Snapshot() }); n != 0 {
		t.Fatalf("snapshot allocations: %g", n)
	}
	c.dropPacket(1 << 30)
	c.dropPacket(1 << 30)
	if c.Snapshot().TransportDroppedBytes != 1<<31 {
		t.Fatal("counter wrapped")
	}
}

func TestRejectedReferenceCountsDroppedObservations(t *testing.T) {
	c, b := testBudgetClient(t, 128<<10)
	remaining := b.limit - b.used.Load()
	if !b.reserve(remaining) {
		t.Fatal("reserve")
	}
	defer b.release(remaining)
	m := c.MetricRef("rejected", Tags{})
	if got := c.Snapshot().AggregationDroppedObservations; got != 0 {
		t.Fatalf("reference creation is not an observation: %d", got)
	}
	m.Values([]float64{1, 2, 3})
	m.Count(50)
	m.StringsTop([]string{"a", "b"})
	if got := c.Snapshot().AggregationDroppedObservations; got != 6 {
		t.Fatalf("lost batch accounting: %d", got)
	}
}
