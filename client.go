package statshouse

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	// logging
	logMu sync.Mutex
	logF  LoggerFunc
	logT  time.Time // last write time, to reduce # of errors reported

	// transport
	transportMu sync.RWMutex
	app         string
	env         string // if set, will be put into key0/env
	network     string
	addr        string
	conn        netConn
	packet

	// data
	mu            sync.RWMutex // taken after [bucket.mu]
	w             map[metricKey]*bucket
	r             []*bucket
	wn            map[metricKeyNamed]*bucket
	rn            []*bucket
	tsUnixSec     uint32
	maxBucketSize int

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

// NewClient creates a new [Client] to send metrics. Use NewClient only if you are
// sending metrics to two or more StatsHouse clusters. Otherwise, simply [Configure]
// the default global [Client].
//
// Specifying empty StatsHouse address will make the client silently discard all metrics.
// if you get compiler error after updating to recent version of library, pass statshouse.DefaultNetwork to network parameter
func NewClient(logf LoggerFunc, network string, statsHouseAddr string, defaultEnv string) *Client {
	return NewClientEx(ConfigureArgs{
		Logger:         logf,
		Network:        network,
		StatsHouseAddr: statsHouseAddr,
		DefaultEnv:     defaultEnv,
	})
}

func NewClientEx(args ConfigureArgs) *Client {
	c := &Client{
		close:        make(chan chan struct{}),
		w:            map[metricKey]*bucket{},
		wn:           map[metricKeyNamed]*bucket{},
		regularFuncs: map[int]func(*Client){},
		tsUnixSec:    uint32(time.Now().Unix()),
	}
	c.ConfigureEx(args)
	go c.run()
	return c
}

func (c *Client) ConfigureEx(args ConfigureArgs) {
	if args.Logger == nil {
		args.Logger = log.Printf
	}
	if args.Network == "" {
		args.Network = DefaultNetwork
	}
	if args.StatsHouseAddr == "" {
		args.StatsHouseAddr = DefaultAddr
	}
	if args.MaxBucketSize <= 0 {
		args.MaxBucketSize = defaultMaxBucketSize
	}
	// update logger
	c.logMu.Lock()
	c.logF = args.Logger
	c.logMu.Unlock()
	// update max bucket size
	c.mu.Lock()
	c.maxBucketSize = args.MaxBucketSize
	c.mu.Unlock()
	// update transport
	c.transportMu.Lock()
	defer c.transportMu.Unlock()
	c.app = args.AppName
	c.env = args.DefaultEnv
	c.network = args.Network
	c.addr = args.StatsHouseAddr
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			args.Logger("[statshouse] failed to close connection: %v", err)
		}
		c.conn = nil
	}
	// update packet size
	if maxSize := maxPacketSize(args.Network); maxSize != c.packet.maxSize {
		c.packet = packet{
			buf:     make([]byte, batchHeaderLen, maxSize),
			maxSize: maxSize,
		}
	}
	if c.addr == "" {
		args.Logger("[statshouse] configured with empty address, all statistics will be silently dropped")
	}
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

func (c *Client) flush() {
	if c.batchCount <= 0 {
		return
	}
	c.writeBatchHeader()

	if c.conn == nil && c.addr != "" {
		conn, err := c.netDial()
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
