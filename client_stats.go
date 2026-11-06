package statshouse

import (
	"math"
	"sync/atomic"
)

func saturatingAdd(a *atomic.Uint64, n uint64) {
	for {
		v := a.Load()
		w := v + n
		if n > math.MaxUint64-v {
			w = math.MaxUint64
		}
		if a.CompareAndSwap(v, w) {
			return
		}
	}
}

type clientStats struct {
	buckets            atomic.Int64
	queued             atomic.Int64
	queueBytes         atomic.Int64
	inflight           atomic.Int64
	inflightBytes      atomic.Int64
	transportDrops     atomic.Uint64
	transportDropBytes atomic.Uint64
	aggregationDrops   atomic.Uint64
	dialAttempts       atomic.Uint64
	dialErrors         atomic.Uint64
	reconnects         atomic.Uint64
	writeErrors        atomic.Uint64
}

// ClientStats reports local SDK state. Cumulative counters saturate at MaxUint64.
// Aggregation drops count observations; transport drops count packets and bytes.
// Gauges are weakly consistent across fields and exclude kernel buffers.
type ClientStats struct {
	AggregationBuckets             int64
	QueuePackets                   int64
	QueueBytes                     int64
	InflightPackets                int64
	InflightBytes                  int64
	TransportDroppedPackets        uint64
	TransportDroppedBytes          uint64
	AggregationDroppedObservations uint64
	DialAttempts                   uint64
	DialErrors                     uint64
	Reconnects                     uint64
	WriteErrors                    uint64
	Closed                         bool
	Memory                         MemoryBudgetStats
}

// Snapshot returns local diagnostics without allocation, network I/O or a bucket walk.
// Safe for concurrent use; individual fields can represent different instants.
func (c *Client) Snapshot() ClientStats {
	s := &c.stats
	return ClientStats{
		AggregationBuckets: s.buckets.Load(), QueuePackets: s.queued.Load(), QueueBytes: s.queueBytes.Load(),
		InflightPackets: s.inflight.Load(), InflightBytes: s.inflightBytes.Load(), TransportDroppedPackets: s.transportDrops.Load(), TransportDroppedBytes: s.transportDropBytes.Load(),
		AggregationDroppedObservations: s.aggregationDrops.Load(), DialAttempts: s.dialAttempts.Load(), DialErrors: s.dialErrors.Load(), Reconnects: s.reconnects.Load(), WriteErrors: s.writeErrors.Load(), Closed: c.stopped.Load(), Memory: c.budget.Snapshot(),
	}
}

// Snapshot reports the global client's local diagnostics without network I/O.
func Snapshot() ClientStats { return globalClient.Snapshot() }
func (c *Client) dropPacket(n int) {
	saturatingAdd(&c.stats.transportDrops, 1)
	saturatingAdd(&c.stats.transportDropBytes, uint64(n))
}
