// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statshouse

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
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
	tlInt32Size          = 4
	tlInt64Size          = 8
	tlFloat64Size        = 8
	metricsBatchTag      = 0x56580239
	counterFieldsMask    = uint32(1 << 0)
	valueFieldsMask      = uint32(1 << 1)
	uniqueFieldsMask     = uint32(1 << 2)
	tsFieldsMask         = uint32(1 << 4)
	batchHeaderLen       = 4 * tlInt32Size // data length (for TCP), tag, fields_mask, # of batches
	maxTags              = 16
	maxEmptySendCount    = 2   // before bucket detach
	tcpConnBucketCount   = 512 // 32 MiB max TCP send buffer size
)

var (
	globalClient       = NewClient(log.Printf, DefaultNetwork, DefaultAddr, "")
	errWouldBlock      = fmt.Errorf("would block")
	errWriteAfterClose = fmt.Errorf("write after close")

	tagIDs = [maxTags]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}
)

func maxPayloadSize(network string) int {
	switch network {
	case "tcp":
		return math.MaxUint16
	default:
		// "udp" or "unixgram"
		// IPv6 mandated minimum MTU size of 1280 (minus 40 byte IPv6 header and 8 byte UDP header)
		return 1232
	}
}

type LoggerFunc func(format string, args ...interface{})

// Configure is expected to be called once during app startup to configure the global [Client].
// Specifying empty StatsHouse address will make the client silently discard all metrics.
func Configure(logf LoggerFunc, statsHouseAddr string, defaultEnv string) {
	globalClient.configure(logf, DefaultNetwork, statsHouseAddr, defaultEnv)
}

// network must be either "tcp", "udp" or "unixgram"
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

func Count(name string, tags Tags, n float64) {
	globalClient.Count(name, tags, n)
}

func CountHistoric(name string, tags Tags, n float64, tsUnixSec uint32) {
	globalClient.CountHistoric(name, tags, n, tsUnixSec)
}

func NamedCount(name string, tags NamedTags, n float64) {
	globalClient.NamedCount(name, tags, n)
}

func NamedCountHistoric(name string, tags NamedTags, n float64, tsUnixSec uint32) {
	globalClient.NamedCountHistoric(name, tags, n, tsUnixSec)
}

func Value(name string, tags Tags, value float64) {
	globalClient.Value(name, tags, value)
}

func ValueHistoric(name string, tags Tags, value float64, tsUnixSec uint32) {
	globalClient.ValueHistoric(name, tags, value, tsUnixSec)
}

func NamedValue(name string, tags NamedTags, value float64) {
	globalClient.NamedValue(name, tags, value)
}

func NamedValueHistoric(name string, tags NamedTags, value float64, tsUnixSec uint32) {
	globalClient.NamedValueHistoric(name, tags, value, tsUnixSec)
}

func Unique(name string, tags Tags, value int64) {
	globalClient.Unique(name, tags, value)
}

func Uniques(name string, tags Tags, values []int64) {
	globalClient.Uniques(name, tags, values)
}

func UniqueHistoric(name string, tags Tags, value int64, tsUnixSec uint32) {
	globalClient.UniqueHistoric(name, tags, value, tsUnixSec)
}

func NamedUnique(name string, tags NamedTags, value int64) {
	globalClient.NamedUnique(name, tags, value)
}

func NamedUniqueHistoric(name string, tags NamedTags, value int64, tsUnixSec uint32) {
	globalClient.NamedUniqueHistoric(name, tags, value, tsUnixSec)
}

func StringTop(name string, tags Tags, value string) {
	globalClient.StringTop(name, tags, value)
}

func StringTopHistoric(name string, tags Tags, value string, tsUnixSec uint32) {
	globalClient.StringTopHistoric(name, tags, value, tsUnixSec)
}

func NamedStringTop(name string, tags NamedTags, value string) {
	globalClient.NamedStringTop(name, tags, value)
}

func NamedStringTopHistoric(name string, tags NamedTags, value string, tsUnixSec uint32) {
	globalClient.NamedStringTopHistoric(name, tags, value, tsUnixSec)
}

func StringsTop(name string, tags Tags, values []string) {
	globalClient.StringsTop(name, tags, values)
}

func StringsTopHistoric(name string, tags Tags, values []string, tsUnixSec uint32) {
	globalClient.StringsTopHistoric(name, tags, values, tsUnixSec)
}

func NamedStringsTop(name string, tags NamedTags, values []string) {
	globalClient.NamedStringsTop(name, tags, values)
}

func NamedStringsTopHistoric(name string, tags NamedTags, values []string, tsUnixSec uint32) {
	globalClient.NamedStringsTopHistoric(name, tags, values, tsUnixSec)
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
	maxSize := maxPayloadSize(network)
	c := &Client{
		logF:    logf,
		addr:    statsHouseAddr,
		network: network,
		packet: packet{
			buf:     make([]byte, batchHeaderLen, maxSize),
			maxSize: maxSize,
		},
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
	conn        netConn
	packet

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

type packet struct {
	buf        []byte
	maxSize    int
	batchCount int
}

type netConn interface {
	Write(b []byte) ([]byte, error)
	Close() error
}

type datagramConn struct { // either UDP or unixgram
	net.Conn
}

type tcpConn struct {
	wouldBlockSize atomic.Int32

	*Client
	env string
	net.Conn

	w chan []byte
	r chan []byte

	closed   bool
	closeErr chan error
}

func (c *Client) netDial(env, network, addr string) (netConn, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		c.rareLog("[statshouse] failed to dial statshouse: %v", err)
		return nil, err
	}
	if network != "tcp" {
		return &datagramConn{Conn: conn}, nil
	}
	_, err = conn.Write([]byte("statshousev1"))
	if err != nil {
		c.rareLog("[statshouse] failed to send header to statshouse: %v", err)
		conn.Close()
		return nil, err
	}
	t := &tcpConn{
		Client:   c,
		env:      env,
		Conn:     conn,
		r:        make(chan []byte, tcpConnBucketCount),
		w:        make(chan []byte, tcpConnBucketCount),
		closeErr: make(chan error),
	}
	go t.send()
	return t, nil
}

func (t *tcpConn) read(b []byte) []byte {
	res := <-t.w
	if len(res) != 0 && len(b) != 0 {
		t.r <- b
	}
	return res
}

func (t *tcpConn) Write(b []byte) (_ []byte, err error) {
	if len(b) == 0 {
		return b, nil
	}
	if t.closed {
		return b, errWriteAfterClose
	}
	n := cap(b)
	select {
	case t.w <- b:
		select {
		case v := <-t.r:
			b = v
		default:
			b = make([]byte, n)
		}
		return b, nil
	default:
		t.wouldBlockSize.Add(int32(len(b)))
		return b, errWouldBlock
	}
}

func (t *tcpConn) Close() error {
	t.closed = true
	close(t.w)
	return <-t.closeErr
}

func (t *tcpConn) send() {
	var buf []byte
	var err error          // last write or connect error
	var dialTime time.Time // time of last reconnect start
	for {
		if err != nil {
			// reconnect (no more than once per second)
			_ = t.Conn.Close()
			time.Sleep(time.Second - time.Since(dialTime))
			dialTime = time.Now()
			var conn net.Conn
			conn, err = net.Dial("tcp", t.RemoteAddr().String())
			if err != nil {
				t.rareLog("[statshouse] failed to dial statshouse: %v", err)
				continue
			}
			_, err = conn.Write([]byte("statshousev1"))
			if err != nil {
				t.rareLog("[statshouse] failed to send header to statshouse: %v", err)
				conn.Close()
				continue
			}
			t.Conn = conn
		}
		if buf = t.read(buf); len(buf) == 0 {
			break
		}
		if _, err = t.Conn.Write(buf); err != nil {
			t.rareLog("[statshouse] failed to send data to statshouse: %v", err)
			continue // reconnect and send again
		}
		if n := t.wouldBlockSize.Swap(0); n != 0 {
			// report data loss
			t.rareLog("[statshouse] lost %v bytes", n)
			p := packet{
				buf:     buf[:batchHeaderLen],
				maxSize: cap(buf),
			}
			k := metricKeyTransport{
				name: "__src_client_write_err",
			}
			fillTag(&k, "0", t.env)
			fillTag(&k, "1", "1") // lang: golang
			fillTag(&k, "2", "1") // kind: would block
			p.sendValues(nil, &k, "", 0, 0, []float64{float64(n)})
			p.writeBatchHeader()
			if _, err = t.Conn.Write(p.buf); err != nil {
				t.rareLog("[statshouse] failed to send data to statshouse: %v", err)
			}
		}
	}
	t.closeErr <- t.Conn.Close()
}

func (t *datagramConn) Write(b []byte) ([]byte, error) {
	_, err := t.Conn.Write(b[tlInt32Size:]) // skip data length
	return b, err
}

func (t *datagramConn) Close() error {
	return t.Conn.Close()
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

	if maxSize := maxPayloadSize(network); maxSize != c.packet.maxSize {
		c.packet = packet{
			buf:     make([]byte, batchHeaderLen, maxSize),
			maxSize: maxSize,
		}
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
		b := ss[i]
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
		b := ssn[i]
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

func (p *packet) sendCounter(c *Client, k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32) {
	_ = p.writeHeader(c, k, skey, counter, tsUnixSec, counterFieldsMask, 0)
}

func (p *packet) sendUniques(c *Client, k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, values []int64) {
	fieldsMask := uniqueFieldsMask
	if counter != 0 && counter != float64(len(values)) {
		fieldsMask |= counterFieldsMask
	}
	for len(values) > 0 {
		left := p.writeHeader(c, k, skey, counter, tsUnixSec, fieldsMask, tlInt32Size+tlInt64Size)
		if left < 0 {
			return // header did not fit into empty buffer
		}
		writeCount := 1 + left/tlInt64Size
		if writeCount > len(values) {
			writeCount = len(values)
		}
		p.buf = basictl.NatWrite(p.buf, uint32(writeCount))
		for i := 0; i < writeCount; i++ {
			p.buf = basictl.LongWrite(p.buf, values[i])
		}
		values = values[writeCount:]
	}
}

func (p *packet) sendValues(c *Client, k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, values []float64) {
	fieldsMask := valueFieldsMask
	if counter != 0 && counter != float64(len(values)) {
		fieldsMask |= counterFieldsMask
	}
	for len(values) > 0 {
		left := p.writeHeader(c, k, skey, counter, tsUnixSec, fieldsMask, tlInt32Size+tlFloat64Size)
		if left < 0 {
			return // header did not fit into empty buffer
		}
		writeCount := 1 + left/tlFloat64Size
		if writeCount > len(values) {
			writeCount = len(values)
		}
		p.buf = basictl.NatWrite(p.buf, uint32(writeCount))
		for i := 0; i < writeCount; i++ {
			p.buf = basictl.DoubleWrite(p.buf, values[i])
		}
		values = values[writeCount:]
	}
}

func (c *Client) flush() {
	if c.batchCount <= 0 {
		return
	}
	c.writeBatchHeader()

	if c.conn == nil && c.addr != "" {
		conn, err := c.netDial(c.env, c.network, c.addr)
		if err != nil {
			c.rareLog("[statshouse] failed to dial statshouse: %v", err)
		} else {
			c.conn = conn
		}
	}
	if c.conn != nil && c.addr != "" {
		var err error
		c.buf, err = c.conn.Write(c.buf)
		if err != nil {
			c.rareLog("[statshouse] failed to send data to statshouse: %v", err)
		}
	}
	c.buf = c.buf[:batchHeaderLen]
	c.batchCount = 0
}

func (p *packet) writeBatchHeader() {
	binary.LittleEndian.PutUint32(p.buf, uint32(len(p.buf)-tlInt32Size))
	binary.LittleEndian.PutUint32(p.buf[tlInt32Size:], metricsBatchTag)
	binary.LittleEndian.PutUint32(p.buf[2*tlInt32Size:], 0) // fields_mask
	binary.LittleEndian.PutUint32(p.buf[3*tlInt32Size:], uint32(p.batchCount))
}

func (p *packet) writeHeaderImpl(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, fieldsMask uint32) {
	if tsUnixSec != 0 {
		fieldsMask |= tsFieldsMask
	}
	p.buf = basictl.NatWrite(p.buf, fieldsMask)
	p.buf = basictl.StringWriteTruncated(p.buf, k.name)
	// can write more than maxTags pairs, but this is allowed by statshouse
	numSet := k.numSet
	if skey != "" {
		numSet++
	}
	p.buf = basictl.NatWrite(p.buf, uint32(numSet))
	if skey != "" {
		p.writeTag("_s", skey)
	}
	for i := 0; i < k.numSet; i++ {
		p.writeTag(k.tags[i][0], k.tags[i][1])
	}
	if fieldsMask&counterFieldsMask != 0 {
		p.buf = basictl.DoubleWrite(p.buf, counter)
	}
	if fieldsMask&tsFieldsMask != 0 {
		p.buf = basictl.NatWrite(p.buf, tsUnixSec)
	}
}

// returns space reserve or <0 if did not fit
func (p *packet) writeHeader(c *Client, k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, fieldsMask uint32, reserveSpace int) int {
	wasLen := len(p.buf)
	p.writeHeaderImpl(k, skey, counter, tsUnixSec, fieldsMask)
	left := p.maxSize - len(p.buf) - reserveSpace
	if left >= 0 {
		p.batchCount++
		return left
	}
	if wasLen != batchHeaderLen {
		p.buf = p.buf[:wasLen]
		if c != nil {
			c.flush()
			p.writeHeaderImpl(k, skey, counter, tsUnixSec, fieldsMask)
			left = p.maxSize - len(p.buf) - reserveSpace
			if left >= 0 {
				p.batchCount++
				return left
			}
		}
		wasLen = batchHeaderLen
	}
	p.buf = p.buf[:wasLen]
	if c != nil {
		c.rareLog("[statshouse] metric %q payload too big to fit into packet, discarding", k.name)
	}
	return -1
}

func (c *packet) writeTag(tagName string, tagValue string) {
	c.buf = basictl.StringWriteTruncated(c.buf, tagName)
	c.buf = basictl.StringWriteTruncated(c.buf, tagValue)
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

// MetricRef pointer is obtained via [*Client.Metric] or [*Client.MetricNamed]
// and is used to record attributes of observed events.
type MetricRef struct {
	*bucket
	SurviveNilWrite bool
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

func (m *MetricRef) StringTopHistoric(value string, tsUnixSec uint32) {
	m.write(tsUnixSec, func(b *bucket) {
		b.stop = append(b.stop, value)
	})
}

func (c *Client) StringTop(name string, tags Tags, value string) {
	m := c.MetricRef(name, tags)
	m.StringTop(value)
}

func (c *Client) StringTopHistoric(name string, tags Tags, value string, tsUnixSec uint32) {
	m := c.MetricRef(name, tags)
	m.StringTopHistoric(value, tsUnixSec)
}

func (c *Client) NamedStringTop(name string, tags NamedTags, value string) {
	m := c.MetricNamedRef(name, tags)
	m.StringTop(value)
}

func (c *Client) NamedStringTopHistoric(name string, tags NamedTags, value string, tsUnixSec uint32) {
	m := c.MetricNamedRef(name, tags)
	m.StringTopHistoric(value, tsUnixSec)
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

func (c *Client) StringsTop(name string, tags Tags, values []string) {
	m := c.MetricRef(name, tags)
	m.StringsTop(values)
}

func (c *Client) StringsTopHistoric(name string, tags Tags, values []string, tsUnixSec uint32) {
	m := c.MetricRef(name, tags)
	m.StringsTopHistoric(values, tsUnixSec)
}

func (c *Client) NamedStringsTop(name string, tags NamedTags, values []string) {
	m := c.MetricNamedRef(name, tags)
	m.StringsTop(values)
}

func (c *Client) NamedStringsTopHistoric(name string, tags NamedTags, values []string, tsUnixSec uint32) {
	m := c.MetricNamedRef(name, tags)
	m.StringsTopHistoric(values, tsUnixSec)
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
		c.sendCounter(c, &k, "", b.countToSend, tsUnixSec)
	}
	c.sendValues(c, &k, "", 0, tsUnixSec, b.valueToSend)
	c.sendUniques(c, &k, "", 0, tsUnixSec, b.uniqueToSend)
	for _, skey := range b.stopToSend {
		c.sendCounter(c, &k, skey, 1, tsUnixSec)
	}
}

func (b *bucket) emptySend() bool {
	return b.countToSend == 0 &&
		len(b.valueToSend) == 0 &&
		len(b.uniqueToSend) == 0 &&
		len(b.stopToSend) == 0
}
