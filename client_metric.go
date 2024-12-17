package statshouse

import "runtime"

// MetricRef pointer is obtained via [*Client.Metric] or [*Client.MetricNamed]
// and is used to record attributes of observed events.
type MetricRef struct {
	*bucket
	SurviveNilWrite bool
}

// MetricRef is the preferred way to record observations, save the result for later use:
//
//	var countPacketOK = statshouse.GetMetricRef("foo", statshouse.Tags{1: "ok"})
//	countPacketOK.Count(1)  // lowest overhead possible
func (c *Client) MetricRef(metric string, tags Tags) MetricRef {
	// We must do absolute minimum of work here
	k := metricKey{name: metric, tags: tags}
	c.mu.RLock()
	e, ok := c.w[k]
	c.mu.RUnlock()
	if ok {
		return MetricRef{bucket: e}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok = c.w[k]; ok {
		return MetricRef{bucket: e}
	}
	b := &bucket{
		c:         c,
		k:         k,
		maxSize:   c.maxBucketSize,
		tsUnixSec: c.tsUnixSec,
		attached:  true,
	}
	if c.bucketCount != nil {
		c.bucketCount.Add(1)
		runtime.SetFinalizer(b, func(_ *bucket) {
			c.bucketCount.Add(-1)
		})
	}
	c.w[k] = b
	c.r = append(c.r, b)
	return MetricRef{bucket: b}
}

// MetricNamedRef is similar to [*Client.MetricRef] but slightly slower, and allows to specify tags by name.
func (c *Client) MetricNamedRef(metric string, tags NamedTags) MetricRef {
	// We must do absolute minimum of work here
	k := metricKeyNamed{name: metric}
	copy(k.tags[:], tags)

	c.mu.RLock()
	e, ok := c.wn[k]
	c.mu.RUnlock()
	if ok {
		return MetricRef{bucket: e}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok = c.wn[k]; ok {
		return MetricRef{bucket: e}
	}
	b := &bucket{
		c:         c,
		kn:        k,
		maxSize:   c.maxBucketSize,
		tsUnixSec: c.tsUnixSec,
		attached:  true,
	}
	if c.bucketCount != nil {
		c.bucketCount.Add(1)
		runtime.SetFinalizer(b, func(_ *bucket) {
			c.bucketCount.Add(-1)
		})
	}
	c.wn[k] = b
	c.rn = append(c.rn, b)
	return MetricRef{bucket: b}
}

// Deprecated: causes unnecessary memory allocation.
// Use either [Client.MetricRef] or direct write func like [Client.Count].
func (c *Client) Metric(metric string, tags Tags) *MetricRef {
	v := c.MetricRef(metric, tags)
	return &v
}

// Deprecated: causes unnecessary memory allocation.
// Use either [Client.MetricNamedRef] or direct write func like [Client.Count].
func (c *Client) MetricNamed(metric string, tags NamedTags) *MetricRef {
	v := c.MetricNamedRef(metric, tags)
	return &v
}

func (m *MetricRef) IsValid() bool {
	return m.bucket != nil
}

func (m *MetricRef) IsNil() bool {
	return m.bucket == nil
}

func (m *MetricRef) Equal(other MetricRef) bool {
	return m.bucket == other.bucket
}

// Count records the number of events or observations.
func (m *MetricRef) Count(n float64) {
	m.CountHistoric(n, 0)
}

func (m *MetricRef) CountHistoric(n float64, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.count += n
	})
}

// Value records the observed value for distribution estimation.
func (m *MetricRef) Value(value float64) {
	m.ValueHistoric(value, 0)
}

func (m *MetricRef) ValueHistoric(value float64, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.appendValue(value)
	})
}

// Values records the observed values for distribution estimation.
func (m *MetricRef) Values(values []float64) {
	m.write(0, func(b *bucket) {
		b.appendValue(values...)
	})
}

func (m *MetricRef) ValuesHistoric(values []float64, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.value = append(b.value, values...)
	})
}

// Unique records the observed value for cardinality estimation.
func (m *MetricRef) Unique(value int64) {
	m.write(0, func(b *bucket) {
		b.appendUnique(value)
	})
}

func (m *MetricRef) UniqueHistoric(value int64, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.appendUnique(value)
	})
}

// Uniques records the observed values for cardinality estimation.
func (m *MetricRef) Uniques(values []int64) {
	m.write(0, func(b *bucket) {
		b.unique = append(b.unique, values...)
	})
}

func (m *MetricRef) UniquesHistoric(values []int64, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.appendUnique(values...)
	})
}

// StringTop records the observed value for popularity estimation.
func (m *MetricRef) StringTop(value string) {
	m.write(0, func(b *bucket) {
		b.stop = append(b.stop, value)
	})
}

func (m *MetricRef) StringTopHistoric(value string, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.stop = append(b.stop, value)
	})
}

// StringsTop records the observed values for popularity estimation.
func (m *MetricRef) StringsTop(values []string) {
	m.write(0, func(b *bucket) {
		b.stop = append(b.stop, values...)
	})
}

func (m *MetricRef) StringsTopHistoric(values []string, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.stop = append(b.stop, values...)
	})
}

func (m *MetricRef) write(tsUnixSec uint32, fn func(*bucket)) {
	if m.bucket == nil && m.SurviveNilWrite {
		return
	}
	m.mu.Lock()
	tsZeroOrEqual := tsUnixSec == 0 || m.tsUnixSec <= tsUnixSec
	if m.attached && tsZeroOrEqual {
		// fast path
		fn(m.bucket)
		m.mu.Unlock()
		return
	}
	c := m.c
	if !tsZeroOrEqual {
		m.mu.Unlock()
		b := bucket{
			k:  m.k,
			kn: m.kn,
		}
		fn(&b)
		b.swapToSend(tsUnixSec)
		c.transportMu.Lock()
		b.send(c, tsUnixSec)
		c.transportMu.Unlock()
		return
	}
	// attach then write
	for i := 0; i < 2; i++ {
		var b *bucket
		var emptyMetricName bool
		c.mu.Lock()
		if m.k.name != "" {
			if b = c.w[m.k]; b == nil {
				fn(m.bucket)
				c.w[m.k] = m.bucket
				c.r = append(c.r, m.bucket)
				m.tsUnixSec = c.tsUnixSec
				m.attached = true // attach current
			}
		} else if m.kn.name != "" {
			if b = c.wn[m.kn]; b == nil {
				fn(m.bucket)
				c.wn[m.kn] = m.bucket
				c.rn = append(c.rn, m.bucket)
				m.tsUnixSec = c.tsUnixSec
				m.attached = true // attach current
			}
		} else {
			emptyMetricName = true
		}
		c.mu.Unlock()
		if emptyMetricName {
			m.mu.Unlock()
			c.rareLog("[statshouse] empty metric name, discarding")
			return
		}
		if m.attached { // attached current
			fn(m.bucket)
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		m.bucket = b
		// now state is equvalent to what it was at function invocation, start over
		m.mu.Lock()
		if m.attached {
			fn(b)
			m.mu.Unlock()
			return
		}
	}
	m.mu.Unlock()
	c.rareLog("[statshouse] send safety counter limit reached, discarding")
}
