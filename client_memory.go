package statshouse

import (
	"encoding/binary"
	"errors"
	"hash/maphash"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"unsafe"

	"pgregory.net/rand"
)

var (
	ErrMemoryBudgetExceeded = errors.New("statshouse: memory budget exceeded")
	ErrMemoryBudgetInUse    = errors.New("statshouse: memory budget cannot be replaced")
	ErrClientStarted        = errors.New("statshouse: client has already been used")
	ErrClientClosed         = errors.New("statshouse: client is closed")
	ErrInvalidMemoryBudget  = errors.New("statshouse: invalid memory budget")
)

// MemoryBudget limits accounted SDK-owned retained storage across clients.
// It is safe for concurrent use and must not be copied. The zero value is invalid.
// It does not limit process RSS, runtime/allocator overhead or kernel buffers.
type MemoryBudget struct {
	limit    int64
	used     atomic.Int64
	peak     atomic.Int64
	rejected atomic.Uint64
}

// MemoryBudgetStats is an allocation-free, weakly consistent accounting snapshot.
type MemoryBudgetStats struct {
	LimitBytes           int64
	UsedBytes            int64
	PeakBytes            int64
	RejectedReservations uint64
}

// NewMemoryBudget creates a shared, immutable byte limit. No clients are started.
// Limits smaller than the budget's own metadata return ErrInvalidMemoryBudget.
func NewMemoryBudget(limitBytes int64) (*MemoryBudget, error) {
	base := int64(unsafe.Sizeof(MemoryBudget{}))
	if limitBytes < base {
		return nil, ErrInvalidMemoryBudget
	}
	b := &MemoryBudget{limit: limitBytes}
	b.used.Store(base)
	b.peak.Store(base)
	return b, nil
}

// Snapshot returns local accounting without allocations or network access.
// Fields may reflect different instants during concurrent updates.
func (b *MemoryBudget) Snapshot() MemoryBudgetStats {
	if b == nil {
		return MemoryBudgetStats{}
	}
	return MemoryBudgetStats{LimitBytes: b.limit, UsedBytes: b.used.Load(), PeakBytes: b.peak.Load(), RejectedReservations: b.rejected.Load()}
}
func (b *MemoryBudget) reserve(n int64) bool {
	if n == 0 {
		return true
	}
	if n < 0 {
		saturatingAdd(&b.rejected, 1)
		return false
	}
	for {
		u := b.used.Load()
		if n > b.limit-u {
			saturatingAdd(&b.rejected, 1)
			return false
		}
		if b.used.CompareAndSwap(u, u+n) {
			for p := b.peak.Load(); u+n > p; p = b.peak.Load() {
				if b.peak.CompareAndSwap(p, u+n) {
					break
				}
			}
			return true
		}
	}
}
func (b *MemoryBudget) release(n int64) {
	if n < 0 || b.used.Add(-n) < 0 {
		panic("statshouse: invalid memory release")
	}
}

// A separate finalizable leaf avoids finalizer cycles through Client and bucket.
type memoryLease struct {
	budget  *MemoryBudget
	bytes   atomic.Int64
	tracked *atomic.Int32
}

func makeLease(b *MemoryBudget, n int64) *memoryLease {
	l := &memoryLease{budget: b}
	l.bytes.Store(n)
	runtime.SetFinalizer(l, func(l *memoryLease) {
		l.budget.release(l.bytes.Load())
		if l.tracked != nil {
			l.tracked.Add(-1)
		}
	})
	return l
}
func (l *memoryLease) grow(n int64) bool {
	if n == 0 {
		return true
	}
	if !l.budget.reserve(n) {
		return false
	}
	l.bytes.Add(n)
	return true
}
func (l *memoryLease) shrink(n int64) {
	if n == 0 {
		return
	}
	l.bytes.Add(-n)
	l.budget.release(n)
}
func stringBytes(k metricKey, kn metricKeyNamed) (int64, bool) {
	n := int64(len(k.name))
	add := func(s string) bool {
		v := int64(len(s))
		if v > math.MaxInt64-n {
			return false
		}
		n += v
		return true
	}
	if !add(kn.name) {
		return 0, false
	}
	for _, s := range k.tags {
		if !add(s) {
			return 0, false
		}
	}
	for _, p := range kn.tags {
		if !add(p[0]) || !add(p[1]) {
			return 0, false
		}
	}
	return n, true
}
func cloneKey(k metricKey, kn metricKeyNamed) (metricKey, metricKeyNamed) {
	k.name = strings.Clone(k.name)
	for i := range k.tags {
		k.tags[i] = strings.Clone(k.tags[i])
	}
	kn.name = strings.Clone(kn.name)
	for i := range kn.tags {
		kn.tags[i][0] = strings.Clone(kn.tags[i][0])
		kn.tags[i][1] = strings.Clone(kn.tags[i][1])
	}
	return k, kn
}
func hashString(h *maphash.Hash, s string) {
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(len(s)))
	_, _ = h.Write(n[:])
	_, _ = h.WriteString(s)
}
func keyHash(seed maphash.Seed, k *metricKey, kn *metricKeyNamed) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	if k != nil {
		h.WriteByte(0)
		hashString(&h, k.name)
		for i, v := range k.tags {
			if v != "" {
				h.WriteByte(byte(i))
				hashString(&h, v)
			}
		}
	} else {
		h.WriteByte(1)
		hashString(&h, kn.name)
		for i, p := range kn.tags {
			if p[0] != "" || p[1] != "" {
				h.WriteByte(byte(i))
				hashString(&h, p[0])
				hashString(&h, p[1])
			}
		}
	}
	return h.Sum64()
}
func (c *Client) lookupBounded(hash uint64, k *metricKey, kn *metricKeyNamed) *bucket {
	if len(c.index) == 0 {
		return nil
	}
	for b := c.index[hash%uint64(len(c.index))]; b != nil; b = b.hashNext {
		if k != nil {
			if b.k == *k && b.kn.name == "" {
				return b
			}
		} else if b.kn == *kn && b.k.name == "" {
			return b
		}
	}
	return nil
}
func (c *Client) boundedRef(k *metricKey, kn *metricKeyNamed) MetricRef {
	if c.stopped.Load() {
		return MetricRef{SurviveNilWrite: true, rejectedClient: c}
	}
	hash := keyHash(c.seed, k, kn)
	c.mu.RLock()
	b := c.lookupBounded(hash, k, kn)
	closed := c.stopped.Load()
	c.mu.RUnlock()
	if b != nil && !closed {
		return MetricRef{bucket: b}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped.Load() {
		return MetricRef{SurviveNilWrite: true, rejectedClient: c}
	}
	if b = c.lookupBounded(hash, k, kn); b != nil {
		return MetricRef{bucket: b}
	}
	var actualK metricKey
	var actualKn metricKeyNamed
	if k != nil {
		actualK = *k
	} else {
		actualKn = *kn
	}
	if c.indexed+1 > len(c.index)/2 {
		size := len(c.index) * 2
		if size == 0 {
			size = 16
		}
		n := int64(size) * int64(unsafe.Sizeof((*bucket)(nil)))
		if !c.lease.grow(n) {
			return MetricRef{SurviveNilWrite: true, rejectedClient: c}
		}
		a := make([]*bucket, size)
		for b := c.head; b != nil; b = b.listNext {
			i := b.hash % uint64(size)
			b.hashNext = a[i]
			a[i] = b
		}
		old := int64(cap(c.index)) * int64(unsafe.Sizeof((*bucket)(nil)))
		c.index = a
		c.lease.shrink(old)
	}
	stringsSize, ok := stringBytes(actualK, actualKn)
	base := int64(unsafe.Sizeof(bucket{})) + int64(unsafe.Sizeof(memoryLease{}))
	if !ok || stringsSize > math.MaxInt64-base || !c.budget.reserve(base+stringsSize) {
		return MetricRef{SurviveNilWrite: true, rejectedClient: c}
	}
	actualK, actualKn = cloneKey(actualK, actualKn)
	b = &bucket{c: c, k: actualK, kn: actualKn, maxSize: c.maxBucketSize, tsUnixSec: c.tsUnixSec, attached: true, hash: hash, lease: makeLease(c.budget, base+stringsSize)}
	if c.bucketCount != nil {
		c.bucketCount.Add(1)
		b.lease.tracked = c.bucketCount
	}
	c.attachBounded(b)
	return MetricRef{bucket: b}
}

func (c *Client) attachBounded(b *bucket) {
	i := b.hash % uint64(len(c.index))
	b.hashNext = c.index[i]
	c.index[i] = b
	b.listNext = c.head
	c.head = b
	b.attached = true
	c.indexed++
	c.stats.buckets.Add(1)
}
func (c *Client) detachBounded(b *bucket) {
	i := b.hash % uint64(len(c.index))
	p := &c.index[i]
	for *p != nil && *p != b {
		p = &(*p).hashNext
	}
	if *p == b {
		*p = b.hashNext
	}
	p = &c.head
	for *p != nil && *p != b {
		p = &(*p).listNext
	}
	if *p == b {
		*p = b.listNext
	}
	b.hashNext = nil
	b.listNext = nil
	b.attached = false
	c.indexed--
	c.stats.buckets.Add(-1)
}
func (c *Client) sendBounded(now uint32) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.mu.Lock()
	b := c.head
	ts := c.tsUnixSec
	c.tsUnixSec = now
	c.mu.Unlock()
	for b != nil {
		c.mu.RLock()
		next := b.listNext
		c.mu.RUnlock()
		b.swapToSend(now)
		c.transportMu.Lock()
		if !b.emptySend() {
			b.send(c, ts)
		}
		c.transportMu.Unlock()
		b.mu.Lock()
		empty := b.emptySend()
		b.clearSentStrings()
		if empty {
			b.emptySendCount++
		} else {
			b.emptySendCount = 0
		}
		if b.emptySendCount > maxEmptySendCount && b.count == 0 && len(b.value) == 0 && len(b.unique) == 0 && len(b.stop) == 0 {
			c.mu.Lock()
			c.detachBounded(b)
			c.mu.Unlock()
			b.releasePayload()
		}
		b.mu.Unlock()
		b = next
	}
	c.transportMu.Lock()
	c.flush()
	c.transportMu.Unlock()
}

func (m *MetricRef) writeBounded(ts uint32, observations int, fn func(*bucket)) {
	b := m.bucket
	c := b.c
	for tries := 0; tries < 3; tries++ {
		b.mu.Lock()
		if c.stopped.Load() {
			b.mu.Unlock()
			saturatingAdd(&c.stats.aggregationDrops, uint64(observations))
			return
		}
		if ts != 0 && b.tsUnixSec > ts {
			base := int64(unsafe.Sizeof(bucket{})) + int64(unsafe.Sizeof(memoryLease{}))
			if !c.budget.reserve(base) {
				b.mu.Unlock()
				saturatingAdd(&c.stats.aggregationDrops, uint64(observations))
				return
			}
			tmp := bucket{c: c, k: b.k, kn: b.kn, maxSize: b.maxSize, lease: makeLease(c.budget, base)}
			b.mu.Unlock()
			fn(&tmp)
			tmp.swapToSend(ts)
			c.transportMu.Lock()
			if !c.stopped.Load() {
				tmp.send(c, ts)
			}
			c.transportMu.Unlock()
			tmp.releasePayload()
			tmp.lease.shrink(tmp.lease.bytes.Load())
			return
		}
		if b.attached {
			fn(b)
			b.mu.Unlock()
			return
		}
		c.mu.Lock()
		var found *bucket
		if len(c.index) > 0 {
			for p := c.index[b.hash%uint64(len(c.index))]; p != nil; p = p.hashNext {
				if p.k == b.k && p.kn == b.kn {
					found = p
					break
				}
			}
		}
		if found == nil {
			if c.stopped.Load() || len(c.index) == 0 || c.indexed+1 > len(c.index)/2 {
				c.mu.Unlock()
				b.mu.Unlock()
				saturatingAdd(&c.stats.aggregationDrops, uint64(observations))
				return
			}
			b.tsUnixSec = c.tsUnixSec
			c.attachBounded(b)
			c.mu.Unlock()
			fn(b)
			b.mu.Unlock()
			return
		}
		c.mu.Unlock()
		b.mu.Unlock()
		b = found
	}
	saturatingAdd(&c.stats.aggregationDrops, uint64(observations))
}

func (b *bucket) growValues(need int) bool {
	if need <= cap(b.value) {
		return true
	}
	size := cap(b.value)*2 + 1
	if size > b.maxSize {
		size = b.maxSize
	}
	n := int64(size) * 8
	if !b.lease.grow(n) {
		return false
	}
	old := int64(cap(b.value)) * 8
	a := make([]float64, len(b.value), size)
	copy(a, b.value)
	b.value = a
	b.lease.shrink(old)
	return true
}
func (b *bucket) growUniques(need int) bool {
	if need <= cap(b.unique) {
		return true
	}
	size := cap(b.unique)*2 + 1
	if size > b.maxSize {
		size = b.maxSize
	}
	n := int64(size) * 8
	if !b.lease.grow(n) {
		return false
	}
	old := int64(cap(b.unique)) * 8
	a := make([]int64, len(b.unique), size)
	copy(a, b.unique)
	b.unique = a
	b.lease.shrink(old)
	return true
}
func (b *bucket) appendStrings(values ...string) {
	if b.lease == nil {
		b.stop = append(b.stop, values...)
		return
	}
	for _, v := range values {
		if len(b.stop) == cap(b.stop) {
			size := cap(b.stop)*2 + 1
			n := int64(size) * int64(unsafe.Sizeof(""))
			if !b.lease.grow(n) {
				saturatingAdd(&b.c.stats.aggregationDrops, 1)
				continue
			}
			old := int64(cap(b.stop)) * int64(unsafe.Sizeof(""))
			a := make([]string, len(b.stop), size)
			copy(a, b.stop)
			b.stop = a
			b.lease.shrink(old)
		}
		n := int64(len(v))
		owned := ""
		if len(b.stop) > 0 && b.stop[len(b.stop)-1] == v {
			owned = b.stop[len(b.stop)-1]
			n = 0
		}
		if !b.lease.grow(n) {
			saturatingAdd(&b.c.stats.aggregationDrops, 1)
			continue
		}
		if n > 0 {
			owned = strings.Clone(v)
		}
		b.stop = append(b.stop, owned)
		b.stopBytes += n
	}
}
func (b *bucket) clearSentStrings() {
	if b.lease == nil {
		return
	}
	for i := range b.stopToSend {
		b.stopToSend[i] = ""
	}
	b.stopToSend = b.stopToSend[:0]
	b.lease.shrink(b.stopSendBytes)
	b.stopSendBytes = 0
}
func (b *bucket) releasePayload() {
	if b.lease == nil {
		return
	}
	n := int64(cap(b.value)+cap(b.valueToSend)+cap(b.unique)+cap(b.uniqueToSend))*8 + int64(cap(b.stop)+cap(b.stopToSend))*int64(unsafe.Sizeof("")) + b.stopBytes + b.stopSendBytes
	if b.r != nil {
		n += int64(unsafe.Sizeof(rand.Rand{}))
	}
	b.value = nil
	b.valueToSend = nil
	b.unique = nil
	b.uniqueToSend = nil
	b.stop = nil
	b.stopToSend = nil
	b.stopBytes = 0
	b.stopSendBytes = 0
	b.r = nil
	b.lease.shrink(n)
}
