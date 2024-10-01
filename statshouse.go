// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statshouse

import (
	"encoding/binary"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vkcom/statshouse-go/internal/basictl"
)

const (
	DefaultAddr    = "127.0.0.1:13337"
	DefaultNetwork = "udp"

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
	maxEmptySendCount    = 2 // before bucket detach
)

var (
	globalClient = NewClient(log.Printf, DefaultNetwork, DefaultAddr, "")

	tagIDs = [maxTags]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}
)

type LoggerFunc func(format string, args ...interface{})

// Configure is expected to be called once during app startup to configure the global [Client].
// Specifying empty StatsHouse address will make the client silently discard all metrics.
func Configure(logf LoggerFunc, statsHouseAddr string, defaultEnv string) {
	globalClient.configure(logf, DefaultNetwork, statsHouseAddr, defaultEnv)
}

// network must be either "udp" or "unixgram"
func ConfigureNetwork(logf LoggerFunc, network string, statsHouseAddr string, defaultEnv string) {
	globalClient.configure(logf, network, statsHouseAddr, defaultEnv)
}

func TrackBucketCount() {
	globalClient.TrackBucketCount()
}

func BucketCount() {
	globalClient.BucketCount()
}

// Close calls [*Client.Close] on the global [Client].
// Make sure to call Close during app exit to avoid losing the last batch of metrics.
func Close() error {
	return globalClient.Close()
}

// GetMetricRef calls [*Client.MetricRef] on the global [Client].
// It is valid to call Metric before [Configure].
func GetMetricRef(metric string, tags Tags) MetricRef {
	return globalClient.MetricRef(metric, tags)
}

// Deprecated: causes unnecessary memory allocation.
// Use either [Client.MetricRef] or direct write func like [Client.Count].
func Metric(metric string, tags Tags) *MetricRef {
	return globalClient.Metric(metric, tags)
}

// MetricNamedRef calls [*Client.MetricNamedRef] on the global [Client].
// It is valid to call MetricNamedRef before [Configure].
func MetricNamedRef(metric string, tags NamedTags) MetricRef {
	return globalClient.MetricNamedRef(metric, tags)
}

// Deprecated: causes unnecessary memory allocation.
// Use either [Client.MetricNamedRef] or direct write func like [Client.Count].
func MetricNamed(metric string, tags NamedTags) *MetricRef {
	return globalClient.MetricNamed(metric, tags)
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
// if you get compiler error after updating to recent version of library, pass statshouse.DefaultNetwork to network parameter
func NewClient(logf LoggerFunc, network string, statsHouseAddr string, defaultEnv string) *Client {
	if logf == nil {
		logf = log.Printf
	}
	c := &Client{
		logF:         logf,
		addr:         statsHouseAddr,
		network:      network,
		packetBuf:    make([]byte, batchHeaderLen, maxPayloadSize), // can grow larger than maxPayloadSize if writing huge header
		close:        make(chan chan struct{}),
		w:            map[metricKey]*bucket{},
		wn:           map[metricKeyNamed]*bucket{},
		env:          defaultEnv,
		regularFuncs: map[int]func(*Client){},
		tsUnixSec:    uint32(time.Now().Unix()),
	}
	go c.run()
	return c
}

type metricKey struct {
	name string
	tags Tags
}

type internalTags [maxTags][2]string

type metricKeyNamed struct {
	name string
	tags internalTags
}

// NamedTags are used to call [*Client.MetricNamed].
type NamedTags [][2]string

// Tags are used to call [*Client.Metric].
type Tags [maxTags]string

// Client manages metric aggregation and transport to a StatsHouse agent.
type Client struct {
	// logging
	logMu sync.Mutex
	logF  LoggerFunc
	logT  time.Time // last write time, to reduce # of errors reported

	// transport
	transportMu sync.RWMutex
	env         string // if set, will be put into key0/env
	network     string
	addr        string
	conn        net.Conn
	packetBuf   []byte
	batchCount  int

	// data
	mu        sync.RWMutex // taken after [bucket.mu]
	w         map[metricKey]*bucket
	r         []*bucket
	wn        map[metricKeyNamed]*bucket
	rn        []*bucket
	tsUnixSec uint32

	// external callbacks
	regularFuncsMu sync.Mutex
	regularFuncs   map[int]func(*Client)
	nextRegularID  int

	// shutdown
	closeOnce sync.Once
	closeErr  error
	close     chan chan struct{}

	// debug
	bucketCount *atomic.Int32
}

// SetEnv changes the default environment associated with [Client].
func (c *Client) SetEnv(env string) {
	c.transportMu.Lock()
	c.env = env
	c.transportMu.Unlock()
}

// For debug purposes. If necessary then it should be first method called on [Client].
func (c *Client) TrackBucketCount() {
	c.bucketCount = &atomic.Int32{}
}

// For debug purposes. Panics if [Client.TrackBucketCount] wasn't called.
func (c *Client) BucketCount() int32 {
	return c.bucketCount.Load()
}

// Close the [Client] and flush unsent metrics to the StatsHouse agent.
// No data will be sent after Close has returned.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		ch := make(chan struct{})
		c.close <- ch
		<-ch

		c.transportMu.Lock()
		defer c.transportMu.Unlock()

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
			c.rareLog("[statshouse] panic inside regular measurement function, ignoring: %v", p)
		}
	}()
	for _, f := range regularCache { // called without locking to prevent deadlock
		f(c)
	}
	return regularCache
}

func (c *Client) configure(logf LoggerFunc, network string, statsHouseAddr string, env string) {
	if logf == nil {
		logf = log.Printf
	}
	// update logger first
	c.logMu.Lock()
	c.logF = logf
	c.logMu.Unlock()
	// then transport
	c.transportMu.Lock()
	defer c.transportMu.Unlock()
	c.env = env
	c.network = network
	c.addr = statsHouseAddr
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			logf("[statshouse] failed to close connection: %v", err)
		}
		c.conn = nil
	}
	if c.addr == "" {
		logf("[statshouse] configured with empty address, all statistics will be silently dropped")
	}
}

func (c *Client) rareLog(format string, args ...interface{}) {
	c.logMu.Lock()
	if time.Since(c.logT) < errorReportingPeriod {
		c.logMu.Unlock()
		return
	}
	logf := c.logF
	c.logT = time.Now()
	c.logMu.Unlock()
	logf(format, args...)
}

func (c *Client) run() {
	var regularCache []func(*Client)
	// get a resettable timer
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	c.mu.Lock()
	now := time.Unix(int64(c.tsUnixSec), 0)
	c.mu.Unlock()
	// loop
	for {
		timer.Reset(now.Truncate(defaultSendPeriod).Add(defaultSendPeriod).Sub(now))
		select {
		case now = <-timer.C:
			regularCache = c.callRegularFuncs(regularCache[:0])
			c.send(uint32(now.Unix()))
		case ch := <-c.close:
			c.send(0) // last send: we will lose all metrics produced "after"
			close(ch)
			return
		}
		now = time.Now()
	}
}

func (b *bucket) swapToSend(nowUnixSec uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tsUnixSec = nowUnixSec
	b.count, b.countToSend = 0, b.count
	b.value, b.valueToSend = b.valueToSend[:0], b.value
	b.unique, b.uniqueToSend = b.uniqueToSend[:0], b.unique
	b.stop, b.stopToSend = b.stopToSend[:0], b.stop
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

func (c *Client) send(nowUnixSec uint32) {
	// load & switch second
	c.mu.Lock()
	ss, ssn := c.r, c.rn
	sendUnixSec := c.tsUnixSec
	c.tsUnixSec = nowUnixSec
	c.mu.Unlock()
	// swap
	for i := 0; i < len(ss); i++ {
		ss[i].swapToSend(nowUnixSec)
	}
	for i := 0; i < len(ssn); i++ {
		ssn[i].swapToSend(nowUnixSec)
	}
	// send
	c.transportMu.Lock()
	for i := 0; i < len(ss); i++ {
		if ss[i].emptySend() {
			continue
		}
		ss[i].send(c, sendUnixSec)
	}
	for i := 0; i < len(ssn); i++ {
		if ssn[i].emptySend() {
			continue
		}
		ssn[i].send(c, sendUnixSec)
	}
	c.flush()
	c.transportMu.Unlock()
	// remove unused & compact
	i, n := 0, len(ss)
	for i < n {
		b := c.r[i]
		if !b.emptySend() {
			i++
			b.emptySendCount = 0
			continue
		}
		if b.emptySendCount < maxEmptySendCount {
			i++
			b.emptySendCount++
			continue
		} else {
			b.emptySendCount = 0
		}
		// remove
		b.mu.Lock()
		c.mu.Lock()
		c.w[b.k] = nil // release bucket reference
		delete(c.w, b.k)
		n--
		c.r[i] = c.r[n]
		c.r[n] = nil // release bucket reference
		b.attached = false
		c.mu.Unlock()
		b.mu.Unlock()
	}
	if d := len(ss) - n; d != 0 {
		c.mu.Lock()
		for i := len(ss); i < len(c.r); i++ {
			c.r[i-d] = c.r[i]
			c.r[i] = nil // release bucket reference
		}
		c.r = c.r[:len(c.r)-d]
		c.mu.Unlock()
	}
	// remove unused & compact (named)
	i, n = 0, len(ssn)
	for i < n {
		b := c.rn[i]
		if !b.emptySend() {
			i++
			b.emptySendCount = 0
			continue
		}
		if b.emptySendCount < maxEmptySendCount {
			i++
			b.emptySendCount++
			continue
		} else {
			b.emptySendCount = 0
		}
		// remove
		b.mu.Lock()
		c.mu.Lock()
		c.wn[b.kn] = nil // release bucket reference
		delete(c.wn, b.kn)
		n--
		c.rn[i] = c.rn[n]
		c.rn[n] = nil // release bucket reference
		b.attached = false
		c.mu.Unlock()
		b.mu.Unlock()
	}
	if d := len(ssn) - n; d != 0 {
		c.mu.Lock()
		for i := len(ssn); i < len(c.rn); i++ {
			c.rn[i-d] = c.rn[i]
			c.rn[i] = nil // release bucket reference
		}
		c.rn = c.rn[:len(c.rn)-d]
		c.mu.Unlock()
	}
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

	if c.conn == nil && c.addr != "" {
		conn, err := net.Dial(c.network, c.addr)
		if err != nil {
			c.rareLog("[statshouse] failed to dial statshouse: %v", err)
			return
		}
		c.conn = conn
	}
	if c.conn != nil && c.addr != "" {
		_, err := c.conn.Write(data)
		if err != nil {
			c.rareLog("[statshouse] failed to send data to statshouse: %v", err)
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
		} else {
			wasLen = batchHeaderLen
		}
	}
	c.packetBuf = c.packetBuf[:wasLen]
	c.rareLog("[statshouse] metric %q payload too big to fit into packet, discarding", k.name)
	return -1
}

func (c *Client) writeTag(tagName string, tagValue string) {
	c.packetBuf = basictl.StringWriteTruncated(c.packetBuf, tagName)
	c.packetBuf = basictl.StringWriteTruncated(c.packetBuf, tagValue)
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
		return MetricRef{e}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b := &bucket{
		c:         c,
		k:         k,
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
	return MetricRef{b}
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
		return MetricRef{e}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b := &bucket{
		c:         c,
		kn:        k,
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
	return MetricRef{b}
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

// MetricRef pointer is obtained via [*Client.Metric] or [*Client.MetricNamed]
// and is used to record attributes of observed events.
type MetricRef struct {
	*bucket
}

type bucket struct {
	// readonly
	c  *Client
	k  metricKey
	kn metricKeyNamed

	mu        sync.Mutex // taken before [Client.mu]
	tsUnixSec uint32
	attached  bool
	count     float64
	value     []float64
	unique    []int64
	stop      []string

	// access only by "send" goroutine, not protected
	countToSend    float64
	valueToSend    []float64
	uniqueToSend   []int64
	stopToSend     []string
	emptySendCount int
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

func (c *Client) Count(name string, tags Tags, n float64) {
	m := c.MetricRef(name, tags)
	m.Count(n)
}

func (c *Client) CountHistoric(name string, tags Tags, n float64, tsUnixSec uint32) {
	m := c.MetricRef(name, tags)
	m.CountHistoric(n, tsUnixSec)
}

func (c *Client) NamedCount(name string, tags NamedTags, n float64) {
	m := c.MetricNamedRef(name, tags)
	m.Count(n)
}

func (c *Client) NamedCountHistoric(name string, tags NamedTags, n float64, tsUnixSec uint32) {
	m := c.MetricNamedRef(name, tags)
	m.CountHistoric(n, tsUnixSec)
}

// Value records the observed value for distribution estimation.
func (m *MetricRef) Value(value float64) {
	m.ValueHistoric(value, 0)
}

func (m *MetricRef) ValueHistoric(value float64, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.value = append(b.value, value)
	})
}

func (c *Client) Value(name string, tags Tags, value float64) {
	m := c.MetricRef(name, tags)
	m.Value(value)
}

func (c *Client) ValueHistoric(name string, tags Tags, value float64, tsUnixSec uint32) {
	m := c.MetricRef(name, tags)
	m.ValueHistoric(value, tsUnixSec)
}

func (c *Client) NamedValue(name string, tags NamedTags, value float64) {
	m := c.MetricNamedRef(name, tags)
	m.Value(value)
}

func (c *Client) NamedValueHistoric(name string, tags NamedTags, value float64, tsUnixSec uint32) {
	m := c.MetricNamedRef(name, tags)
	m.ValueHistoric(value, tsUnixSec)
}

// Values records the observed values for distribution estimation.
func (m *MetricRef) Values(values []float64) {
	m.write(0, func(b *bucket) {
		b.value = append(b.value, values...)
	})
}

func (m *MetricRef) ValuesHistoric(values []float64, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.value = append(b.value, values...)
	})
}

func (c *Client) Values(name string, tags Tags, values []float64) {
	m := c.MetricRef(name, tags)
	m.Values(values)
}

func (c *Client) ValuesHistoric(name string, tags Tags, values []float64, tsUnixSec uint32) {
	m := c.MetricRef(name, tags)
	m.ValuesHistoric(values, tsUnixSec)
}

func (c *Client) NamedValues(name string, tags NamedTags, values []float64) {
	m := c.MetricNamedRef(name, tags)
	m.Values(values)
}

func (c *Client) NamedValuesHistoric(name string, tags NamedTags, values []float64, tsUnixSec uint32) {
	m := c.MetricNamedRef(name, tags)
	m.ValuesHistoric(values, tsUnixSec)
}

// Unique records the observed value for cardinality estimation.
func (m *MetricRef) Unique(value int64) {
	m.write(0, func(b *bucket) {
		b.unique = append(b.unique, value)
	})
}

func (m *MetricRef) UniqueHistoric(value int64, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.unique = append(b.unique, value)
	})
}

func (c *Client) Unique(name string, tags Tags, value int64) {
	m := c.MetricRef(name, tags)
	m.Unique(value)
}

func (c *Client) UniqueHistoric(name string, tags Tags, value int64, tsUnixSec uint32) {
	m := c.MetricRef(name, tags)
	m.UniqueHistoric(value, tsUnixSec)
}

func (c *Client) NamedUnique(name string, tags NamedTags, value int64) {
	m := c.MetricNamedRef(name, tags)
	m.Unique(value)
}

func (c *Client) NamedUniqueHistoric(name string, tags NamedTags, value int64, tsUnixSec uint32) {
	m := c.MetricNamedRef(name, tags)
	m.UniqueHistoric(value, tsUnixSec)
}

// Uniques records the observed values for cardinality estimation.
func (m *MetricRef) Uniques(values []int64) {
	m.write(0, func(b *bucket) {
		b.unique = append(b.unique, values...)
	})
}

func (m *MetricRef) UniquesHistoric(values []int64, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.unique = append(b.unique, values...)
	})
}

func (c *Client) Uniques(name string, tags Tags, values []int64) {
	m := c.MetricRef(name, tags)
	m.Uniques(values)
}

func (c *Client) UniquesHistoric(name string, tags Tags, values []int64, tsUnixSec uint32) {
	m := c.MetricRef(name, tags)
	m.UniquesHistoric(values, tsUnixSec)
}

func (c *Client) NamedUniques(name string, tags NamedTags, values []int64) {
	m := c.MetricNamedRef(name, tags)
	m.Uniques(values)
}

func (c *Client) NamedUniquesHistoric(name string, tags NamedTags, values []int64, tsUnixSec uint32) {
	m := c.MetricNamedRef(name, tags)
	m.UniquesHistoric(values, tsUnixSec)
}

// StringTop records the observed value for popularity estimation.
func (m *MetricRef) StringTop(value string) {
	m.write(0, func(b *bucket) {
		b.stop = append(b.stop, value)
	})
}

// StringsTop records the observed values for popularity estimation.
func (m *MetricRef) StringsTop(values []string) {
	m.write(0, func(b *bucket) {
		b.stop = append(b.stop, values...)
	})
}

func (m *MetricRef) write(tsUnixSec uint32, fn func(*bucket)) {
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
		var b bucket
		fn(&b)
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

func (b *bucket) send(c *Client, tsUnixSec uint32) {
	var k metricKeyTransport
	if b.k.name != "" {
		k.name = b.k.name
		for i, v := range b.k.tags {
			fillTag(&k, tagIDs[i], v)
		}
	} else if b.kn.name != "" {
		k.name = b.kn.name
		for _, v := range b.kn.tags {
			fillTag(&k, v[0], v[1])
		}
	} else {
		c.rareLog("[statshouse] empty metric name, discarding")
		return
	}
	if !k.hasEnv {
		fillTag(&k, "0", c.env)
	}
	if b.countToSend > 0 {
		c.sendCounter(&k, "", b.countToSend, tsUnixSec)
	}
	c.sendValues(&k, "", 0, tsUnixSec, b.valueToSend)
	c.sendUniques(&k, "", 0, tsUnixSec, b.uniqueToSend)
	for _, skey := range b.stopToSend {
		c.sendCounter(&k, skey, 1, tsUnixSec)
	}
}

func (b *bucket) emptySend() bool {
	return b.countToSend == 0 &&
		len(b.valueToSend) == 0 &&
		len(b.uniqueToSend) == 0 &&
		len(b.stopToSend) == 0
}
