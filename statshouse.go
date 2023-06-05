// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statshouse

import (
	"encoding/binary"
	"log"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vkcom/statshouse-go/internal/basictl"
)

const (
	DefaultStatsHouseAddr = "127.0.0.1:13337"

	defaultSendPeriod    = 1 * time.Second
	errorReportingPeriod = time.Minute
	maxPayloadSize       = 1232 // IPv6 mandated minimum MTU size of 1280 (minus 40 byte IPv6 header and 8 byte UDP header)
	tlInt32Size          = 4
	tlInt64Size          = 8
	tlFloat64Size        = 8
	metricsBatchTag      = 0x56580239
	counterFieldsMask    = uint32(1 << 0)
	valueFieldsMask      = uint32(1 << 1)
	uniqueFieldsMask     = uint32(1 << 2)
	tsFieldsMask         = uint32(1 << 4)
	batchHeaderLen       = 3 * tlInt32Size // tag, fields_mask, # of batches
	maxTags              = 16
)

var (
	globalClient = NewClient(log.Printf, DefaultStatsHouseAddr, "")
)

type LoggerFunc func(format string, args ...interface{})

// Configure is expected to be called once during app startup to configure the global [Client].
// Specifying empty StatsHouse address will make the client silently discard all metrics.
func Configure(logf LoggerFunc, statsHouseAddr string, defaultEnv string) {
	globalClient.configure(logf, statsHouseAddr, defaultEnv)
}

// Close calls [*Client.Close] on the global [Client].
// Make sure to call Close during app exit to avoid losing the last batch of metrics.
func Close() error {
	return globalClient.Close()
}

// AccessMetricRaw calls [*Client.AccessMetricRaw] on the global [Client].
// It is valid to call AccessMetricRaw before [Configure].
func AccessMetricRaw(metric string, tags RawTags) *Metric {
	return globalClient.AccessMetricRaw(metric, tags)
}

// AccessMetric calls [*Client.AccessMetric] on the global [Client].
// It is valid to call AccessMetric before [Configure].
func AccessMetric(metric string, tags Tags) *Metric {
	return globalClient.AccessMetric(metric, tags)
}

// StartRegularMeasurement calls [*Client.StartRegularMeasurement] on the global [Client].
// It is valid to call StartRegularMeasurement before [Configure].
func StartRegularMeasurement(f func(*Client)) (id int) {
	return globalClient.StartRegularMeasurement(f)
}

// StopRegularMeasurement calls [*Client.StopRegularMeasurement] on the global [Client].
// It is valid to call StopRegularMeasurement before [Configure].
func StopRegularMeasurement(id int) {
	globalClient.StopRegularMeasurement(id)
}

// NewClient creates a new [Client] to send metrics. Use NewClient only if you are
// sending metrics to two or more StatsHouse clusters. Otherwise, simply [Configure]
// the default global [Client].
//
// Specifying empty StatsHouse address will make the client silently discard all metrics.
func NewClient(logf LoggerFunc, statsHouseAddr string, defaultEnv string) *Client {
	c := &Client{
		logf:         logf,
		addr:         statsHouseAddr,
		packetBuf:    make([]byte, batchHeaderLen, maxPayloadSize), // can grow larger than maxPayloadSize if writing huge header
		close:        make(chan chan struct{}),
		cur:          &Metric{},
		w:            map[metricKey]*Metric{},
		wn:           map[metricKeyNamed]*Metric{},
		env:          defaultEnv,
		regularFuncs: map[int]func(*Client){},
	}
	go c.run()
	return c
}

type metricKey struct {
	name string
	tags RawTags
}

type internalTags [16][2]string

type metricKeyNamed struct {
	name string
	tags internalTags
}

// Tags are used to call [*Client.AccessMetric].
type Tags [][2]string

// RawTags are used to call [*Client.AccessMetricRaw].
type RawTags struct {
	Env, Tag1, Tag2, Tag3, Tag4, Tag5, Tag6, Tag7, Tag8, Tag9, Tag10, Tag11, Tag12, Tag13, Tag14, Tag15 string
}

type metricKeyValue struct {
	k metricKey
	v *Metric
}

type metricKeyValueNamed struct {
	k metricKeyNamed
	v *Metric
}

// Client manages metric aggregation and transport to a StatsHouse agent.
type Client struct {
	confMu sync.Mutex
	logf   LoggerFunc
	addr   string
	conn   *net.UDPConn

	writeErrTime time.Time // we use it to reduce # of errors reported

	closeOnce  sync.Once
	closeErr   error
	close      chan chan struct{}
	packetBuf  []byte
	batchCount int
	cur        *Metric

	mu             sync.RWMutex
	w              map[metricKey]*Metric
	r              []metricKeyValue
	wn             map[metricKeyNamed]*Metric
	rn             []metricKeyValueNamed
	env            string // if set, will be put into key0/env
	regularFuncsMu sync.Mutex
	regularFuncs   map[int]func(*Client)
	nextRegularID  int
}

// SetEnv changes the default environment associated with [Client].
func (c *Client) SetEnv(env string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.env = env
}

// Close the [Client] and flush unsent metrics to the StatsHouse agent.
// No data will be sent after Close has returned.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		ch := make(chan struct{})
		c.close <- ch
		<-ch

		c.confMu.Lock()
		defer c.confMu.Unlock()

		if c.conn != nil {
			c.closeErr = c.conn.Close()
		}
	})
	return c.closeErr
}

// StartRegularMeasurement will call f once per collection interval with no gaps or drift,
// until StopRegularMeasurement is called with the same ID.
func (c *Client) StartRegularMeasurement(f func(*Client)) (id int) {
	c.regularFuncsMu.Lock()
	defer c.regularFuncsMu.Unlock()
	c.nextRegularID++
	c.regularFuncs[c.nextRegularID] = f
	return c.nextRegularID
}

// StopRegularMeasurement cancels StartRegularMeasurement with the specified ID.
func (c *Client) StopRegularMeasurement(id int) {
	c.regularFuncsMu.Lock()
	defer c.regularFuncsMu.Unlock()
	delete(c.regularFuncs, id)
}

func (c *Client) callRegularFuncs(regularCache []func(*Client)) []func(*Client) {
	c.regularFuncsMu.Lock()
	for _, f := range c.regularFuncs { // TODO - call in order of registration. Use RB-tree when available
		regularCache = append(regularCache, f)
	}
	c.regularFuncsMu.Unlock()
	defer func() {
		if p := recover(); p != nil {
			c.getLog()("[statshouse] panic inside regular measurement function, ignoring: %v", p)
		}
	}()
	for _, f := range regularCache { // called without locking to prevent deadlock
		f(c)
	}
	return regularCache
}

func (c *Client) configure(logf LoggerFunc, statsHouseAddr string, env string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.env = env

	c.confMu.Lock()
	defer c.confMu.Unlock()

	c.logf = logf
	c.addr = statsHouseAddr
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			logf("[statshouse] failed to close connection: %v", err)
		}
		c.conn = nil
		c.writeErrTime = time.Time{}
	}
	if c.addr == "" {
		c.logf("[statshouse] configured with empty address, all statistics will be silently dropped")
	}
}

func (c *Client) getLog() LoggerFunc {
	c.confMu.Lock()
	defer c.confMu.Unlock()
	return c.logf // return func instead of calling it here to not alter the callstack information in the log
}

func tillNextHalfPeriod(now time.Time) time.Duration {
	return now.Truncate(defaultSendPeriod).Add(defaultSendPeriod * 3 / 2).Sub(now)
}

func (c *Client) run() {
	var regularCache []func(*Client)
	tick := time.After(tillNextHalfPeriod(time.Now()))
	for {
		select {
		case now := <-tick:
			regularCache = c.callRegularFuncs(regularCache[:0])
			c.send()
			tick = time.After(tillNextHalfPeriod(now))
		case ch := <-c.close:
			c.send() // last send: we will lose all metrics produced "after"
			close(ch)
			return
		}
	}
}

func (c *Client) load() ([]metricKeyValue, []metricKeyValueNamed, string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.r, c.rn, c.env
}

func (c *Client) swapToCur(s *Metric) {
	n := atomicLoadFloat64(&s.atomicCount)
	for !atomicCASFloat64(&s.atomicCount, n, 0) {
		n = atomicLoadFloat64(&s.atomicCount)
	}
	atomicStoreFloat64(&c.cur.atomicCount, n)

	s.mu.Lock()
	defer s.mu.Unlock()

	c.cur.value = append(c.cur.value[:0], s.value...)
	c.cur.unique = append(c.cur.unique[:0], s.unique...)
	c.cur.stop = append(c.cur.stop[:0], s.stop...)
	s.value = s.value[:0]
	s.unique = s.unique[:0]
	s.stop = s.stop[:0]
}

type metricKeyTransport struct {
	name   string
	tags   internalTags
	numSet int
	hasEnv bool
}

func fillTag(k *metricKeyTransport, tagName string, tagValue string) {
	if tagValue == "" || k.numSet >= maxTags { // both checks are not strictly required
		return
	}
	k.tags[k.numSet] = [2]string{tagName, tagValue}
	k.numSet++
	k.hasEnv = k.hasEnv || tagName == "0" || tagName == "env" || tagName == "key0" // TODO - keep only "0", rest are legacy
}

func (c *Client) send() {
	ss, ssn, env := c.load()
	for _, s := range ss {
		k := metricKeyTransport{name: s.k.name}
		fillTag(&k, "0", s.k.tags.Env)
		fillTag(&k, "1", s.k.tags.Tag1)
		fillTag(&k, "2", s.k.tags.Tag2)
		fillTag(&k, "3", s.k.tags.Tag3)
		fillTag(&k, "4", s.k.tags.Tag4)
		fillTag(&k, "5", s.k.tags.Tag5)
		fillTag(&k, "6", s.k.tags.Tag6)
		fillTag(&k, "7", s.k.tags.Tag7)
		fillTag(&k, "8", s.k.tags.Tag8)
		fillTag(&k, "9", s.k.tags.Tag9)
		fillTag(&k, "10", s.k.tags.Tag10)
		fillTag(&k, "11", s.k.tags.Tag11)
		fillTag(&k, "12", s.k.tags.Tag12)
		fillTag(&k, "13", s.k.tags.Tag13)
		fillTag(&k, "14", s.k.tags.Tag14)
		fillTag(&k, "15", s.k.tags.Tag15)
		if !k.hasEnv {
			fillTag(&k, "0", env)
		}

		c.swapToCur(s.v)
		if n := atomicLoadFloat64(&c.cur.atomicCount); n > 0 {
			c.sendCounter(&k, "", n, 0)
		}
		c.sendValues(&k, "", 0, 0, c.cur.value)
		c.sendUniques(&k, "", 0, 0, c.cur.unique)
		for _, skey := range c.cur.stop {
			c.sendCounter(&k, skey, 1, 0)
		}
	}
	for _, s := range ssn {
		k := metricKeyTransport{name: s.k.name}
		for _, v := range s.k.tags {
			fillTag(&k, v[0], v[1])
		}
		if !k.hasEnv {
			fillTag(&k, "0", env)
		}

		c.swapToCur(s.v)
		if n := atomicLoadFloat64(&c.cur.atomicCount); n > 0 {
			c.sendCounter(&k, "", n, 0)
		}
		c.sendValues(&k, "", 0, 0, c.cur.value)
		c.sendUniques(&k, "", 0, 0, c.cur.unique)
		for _, skey := range c.cur.stop {
			c.sendCounter(&k, skey, 1, 0)
		}
	}

	c.flush()
}

func (c *Client) sendCounter(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32) {
	_ = c.writeHeader(k, skey, counter, tsUnixSec, counterFieldsMask, 0)
}

func (c *Client) sendUniques(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, values []int64) {
	fieldsMask := uniqueFieldsMask
	if counter != 0 && counter != float64(len(values)) {
		fieldsMask |= counterFieldsMask
	}
	for len(values) > 0 {
		left := c.writeHeader(k, skey, counter, tsUnixSec, fieldsMask, tlInt32Size+tlInt64Size)
		if left < 0 {
			return // header did not fit into empty buffer
		}
		writeCount := 1 + left/tlInt64Size
		if writeCount > len(values) {
			writeCount = len(values)
		}
		c.packetBuf = basictl.NatWrite(c.packetBuf, uint32(writeCount))
		for i := 0; i < writeCount; i++ {
			c.packetBuf = basictl.LongWrite(c.packetBuf, values[i])
		}
		values = values[writeCount:]
	}
}

func (c *Client) sendValues(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, values []float64) {
	fieldsMask := valueFieldsMask
	if counter != 0 && counter != float64(len(values)) {
		fieldsMask |= counterFieldsMask
	}
	for len(values) > 0 {
		left := c.writeHeader(k, skey, counter, tsUnixSec, fieldsMask, tlInt32Size+tlFloat64Size)
		if left < 0 {
			return // header did not fit into empty buffer
		}
		writeCount := 1 + left/tlFloat64Size
		if writeCount > len(values) {
			writeCount = len(values)
		}
		c.packetBuf = basictl.NatWrite(c.packetBuf, uint32(writeCount))
		for i := 0; i < writeCount; i++ {
			c.packetBuf = basictl.DoubleWrite(c.packetBuf, values[i])
		}
		values = values[writeCount:]
	}
}

func (c *Client) flush() {
	if c.batchCount <= 0 {
		return
	}
	binary.LittleEndian.PutUint32(c.packetBuf, metricsBatchTag)
	binary.LittleEndian.PutUint32(c.packetBuf[tlInt32Size:], 0) // fields_mask
	binary.LittleEndian.PutUint32(c.packetBuf[2*tlInt32Size:], uint32(c.batchCount))
	data := c.packetBuf
	c.packetBuf = c.packetBuf[:batchHeaderLen]
	c.batchCount = 0

	c.confMu.Lock()
	defer c.confMu.Unlock()

	if c.conn == nil && c.addr != "" {
		conn, err := net.Dial("udp", c.addr)
		if err != nil {
			c.logf("[statshouse] failed to dial statshouse: %v", err) // not using getLog() because confMu is already locked
			return
		}
		c.conn = conn.(*net.UDPConn)
	}
	if c.conn != nil && c.addr != "" {
		_, err := c.conn.Write(data)
		if err != nil {
			now := time.Now()
			if now.Sub(c.writeErrTime) > errorReportingPeriod {
				c.writeErrTime = now
				c.logf("[statshouse] failed to send data to statshouse: %v", err) // not using getLog() because confMu is already locked
			}
		}
	}
}

func (c *Client) writeHeaderImpl(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, fieldsMask uint32) {
	if tsUnixSec != 0 {
		fieldsMask |= tsFieldsMask
	}
	c.packetBuf = basictl.NatWrite(c.packetBuf, fieldsMask)
	c.packetBuf = basictl.StringWriteTruncated(c.packetBuf, k.name)
	// can write more than maxTags pairs, but this is allowed by statshouse
	numSet := k.numSet
	if skey != "" {
		numSet++
	}
	c.packetBuf = basictl.NatWrite(c.packetBuf, uint32(numSet))
	if skey != "" {
		c.writeTag("_s", skey)
	}
	for i := 0; i < k.numSet; i++ {
		c.writeTag(k.tags[i][0], k.tags[i][1])
	}
	if fieldsMask&counterFieldsMask != 0 {
		c.packetBuf = basictl.DoubleWrite(c.packetBuf, counter)
	}
	if fieldsMask&tsFieldsMask != 0 {
		c.packetBuf = basictl.NatWrite(c.packetBuf, tsUnixSec)
	}
}

// returns space reserve or <0 if did not fit
func (c *Client) writeHeader(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, fieldsMask uint32, reserveSpace int) int {
	wasLen := len(c.packetBuf)
	c.writeHeaderImpl(k, skey, counter, tsUnixSec, fieldsMask)
	left := maxPayloadSize - len(c.packetBuf) - reserveSpace
	if left >= 0 {
		c.batchCount++
		return left
	}
	if wasLen != batchHeaderLen {
		c.packetBuf = c.packetBuf[:wasLen]
		c.flush()
		c.writeHeaderImpl(k, skey, counter, tsUnixSec, fieldsMask)
		left = maxPayloadSize - len(c.packetBuf) - reserveSpace
		if left >= 0 {
			c.batchCount++
			return left
		}
	}
	c.packetBuf = c.packetBuf[:wasLen]
	c.getLog()("[statshouse] metric %q payload too big to fit into packet, discarding", k.name)
	return -1
}

func (c *Client) writeTag(tagName string, tagValue string) {
	c.packetBuf = basictl.StringWriteTruncated(c.packetBuf, tagName)
	c.packetBuf = basictl.StringWriteTruncated(c.packetBuf, tagValue)
}

// AccessMetricRaw is the preferred way to access [Metric] to record observations.
// AccessMetricRaw calls should be encapsulated in helper functions. Direct calls like
//
//	statshouse.AccessMetricRaw("packet_size", statshouse.RawTags{Tag1: "ok"}).Value(float64(len(pkg)))
//
// should be replaced with calls via higher-level helper functions:
//
//	RecordPacketSize(true, len(pkg))
//
//	func RecordPacketSize(ok bool, size int) {
//	    status := "fail"
//	    if ok {
//	        status = "ok"
//	    }
//	    statshouse.AccessMetricRaw("packet_size", statshouse.RawTags{Tag1: status}).Value(float64(size))
//	}
//
// As an optimization, it is possible to save the result of AccessMetricRaw for later use:
//
//	var countPacketOK = statshouse.AccessMetricRaw("foo", statshouse.RawTags{Tag1: "ok"})
//
//	countPacketOK.Count(1)  // lowest overhead possible
func (c *Client) AccessMetricRaw(metric string, tags RawTags) *Metric {
	// We must do absolute minimum of work here
	k := metricKey{name: metric, tags: tags}
	c.mu.RLock()
	e, ok := c.w[k]
	c.mu.RUnlock()
	if ok {
		return e
	}

	c.mu.Lock()
	e, ok = c.w[k]
	if !ok {
		e = &Metric{}
		c.w[k] = e
		c.r = append(c.r, metricKeyValue{k: k, v: e})
	}
	c.mu.Unlock()
	return e
}

// AccessMetric is similar to [*Client.AccessMetricRaw] but slightly slower, and allows to specify tags by name.
func (c *Client) AccessMetric(metric string, tags Tags) *Metric {
	// We must do absolute minimum of work here
	k := metricKeyNamed{name: metric}
	copy(k.tags[:], tags)

	c.mu.RLock()
	e, ok := c.wn[k]
	c.mu.RUnlock()
	if ok {
		return e
	}

	c.mu.Lock()
	e, ok = c.wn[k]
	if !ok {
		e = &Metric{}
		c.wn[k] = e
		c.rn = append(c.rn, metricKeyValueNamed{k: k, v: e})
	}
	c.mu.Unlock()
	return e
}

// Metric pointer is obtained via [*Client.AccessMetricRaw] or [*Client.AccessMetric]
// and is used to record attributes of observed events.
type Metric struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	atomicCount uint64

	mu     sync.Mutex
	value  []float64
	unique []int64
	stop   []string
}

// Count records the number of events or observations.
func (m *Metric) Count(n float64) {
	c := atomicLoadFloat64(&m.atomicCount)
	for !atomicCASFloat64(&m.atomicCount, c, c+n) {
		c = atomicLoadFloat64(&m.atomicCount)
	}
}

// Value records the observed value for distribution estimation.
func (m *Metric) Value(value float64) {
	m.mu.Lock()
	m.value = append(m.value, value)
	m.mu.Unlock()
}

// Values records the observed values for distribution estimation.
func (m *Metric) Values(values []float64) {
	m.mu.Lock()
	m.value = append(m.value, values...)
	m.mu.Unlock()
}

// Unique records the observed value for cardinality estimation.
func (m *Metric) Unique(value int64) {
	m.mu.Lock()
	m.unique = append(m.unique, value)
	m.mu.Unlock()
}

// Uniques records the observed values for cardinality estimation.
func (m *Metric) Uniques(values []int64) {
	m.mu.Lock()
	m.unique = append(m.unique, values...)
	m.mu.Unlock()
}

// StringTop records the observed value for popularity estimation.
func (m *Metric) StringTop(value string) {
	m.mu.Lock()
	m.stop = append(m.stop, value)
	m.mu.Unlock()
}

// StringsTop records the observed values for popularity estimation.
func (m *Metric) StringsTop(values []string) {
	m.mu.Lock()
	m.stop = append(m.stop, values...)
	m.mu.Unlock()
}

func atomicLoadFloat64(addr *uint64) float64 {
	return math.Float64frombits(atomic.LoadUint64(addr))
}

func atomicStoreFloat64(addr *uint64, val float64) {
	atomic.StoreUint64(addr, math.Float64bits(val))
}

func atomicCASFloat64(addr *uint64, old float64, new float64) (swapped bool) {
	return atomic.CompareAndSwapUint64(addr, math.Float64bits(old), math.Float64bits(new))
}
