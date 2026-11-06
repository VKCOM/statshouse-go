package statshouse

import (
	"hash/maphash"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

func clientBaseCharge() int64 {
	return int64(unsafe.Sizeof(Client{})) + int64(unsafe.Sizeof(memoryLease{})) + int64(unsafe.Sizeof(atomic.Int32{}))
}

// NewClientWithBudget creates a client sharing budget with any other clients.
// It returns an error without starting goroutines if mandatory state cannot fit.
// Close the client when done. Budget is not a process RSS limit.
func NewClientWithBudget(args ConfigureArgs, budget *MemoryBudget) (*Client, error) {
	if budget == nil || budget.limit <= 0 {
		return nil, ErrInvalidMemoryBudget
	}
	base := clientBaseCharge()
	if !budget.reserve(base) {
		return nil, ErrMemoryBudgetExceeded
	}
	c := &Client{bucketTracker: &atomic.Int32{}, budget: budget, lease: makeLease(budget, base), close: make(chan chan struct{}), seed: maphash.MakeSeed(), tsUnixSec: uint32(time.Now().Unix())}
	if err := c.ConfigureWithBudget(args, budget); err != nil {
		c.lease.shrink(c.lease.bytes.Load())
		return nil, err
	}
	go c.run()
	return c, nil
}

// ConfigureWithBudget enables bounded storage before the first metric, callback or
// dial, or reconfigures a bounded client using the same budget. Call at startup,
// before concurrent use. A failure preserves the previous configuration.
// Cached references created before enabling the budget cause ErrClientStarted.
func (c *Client) ConfigureWithBudget(args ConfigureArgs, budget *MemoryBudget) error {
	c.startupMu.Lock()
	defer c.startupMu.Unlock()
	if budget == nil || budget.limit <= 0 {
		return ErrInvalidMemoryBudget
	}
	if c.stopped.Load() {
		return ErrClientClosed
	}
	if c.budget != nil && c.budget != budget {
		return ErrMemoryBudgetInUse
	}
	if c.budget == nil && c.started.Load() {
		return ErrClientStarted
	}
	if args.Logger == nil {
		args.Logger = log.Printf
	}
	if args.Network == "" {
		args.Network = DefaultNetwork
	}
	if args.MaxBucketSize <= 0 {
		args.MaxBucketSize = defaultMaxBucketSize
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	hostname = forceValidHostTag(hostname)
	config := int64(len(args.AppName)) + int64(len(args.DefaultEnv)) + int64(len(args.Network)) + int64(len(args.StatsHouseAddr)) + int64(len(hostname))
	size := maxPacketSize(args.Network, args.StatsHouseAddr)
	n := config + int64(size)
	initial := c.budget == nil
	if initial {
		n += clientBaseCharge()
	}
	if !budget.reserve(n) {
		return ErrMemoryBudgetExceeded
	}
	app, env, network, addr, host := strings.Clone(args.AppName), strings.Clone(args.DefaultEnv), strings.Clone(args.Network), strings.Clone(args.StatsHouseAddr), strings.Clone(hostname)
	buf := make([]byte, batchHeaderLen, size)
	c.transportMu.Lock()
	defer c.transportMu.Unlock()
	if c.stopped.Load() {
		budget.release(n)
		return ErrClientClosed
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if initial {
		c.budget = budget
		c.lease = makeLease(budget, n)
		c.seed = maphash.MakeSeed()
		c.w = nil
		c.wn = nil
		c.r = nil
		c.rn = nil
		c.regularFuncs = nil
	} else {
		c.lease.bytes.Add(n)
		c.lease.shrink(c.configBytes + int64(cap(c.buf)))
	}
	c.app, c.env, c.network, c.addr, c.hostTag = app, env, network, addr, host
	c.configBytes = config
	c.packet = packet{buf: buf, maxSize: size}
	c.dialTargets = nil
	c.maxBucketSize = args.MaxBucketSize
	c.logMu.Lock()
	c.logF = args.Logger
	c.logMu.Unlock()
	return nil
}

// ConfigureWithBudget configures the global client before any references or
// measurements have been created. It returns an error rather than falling back
// to unlimited storage. See Client.ConfigureWithBudget.
func ConfigureWithBudget(args ConfigureArgs, budget *MemoryBudget) error {
	return globalClient.ConfigureWithBudget(args, budget)
}

func (c *Client) cleanupBounded() {
	c.mu.Lock()
	b := c.head
	c.mu.Unlock()
	for b != nil {
		b.mu.Lock()
		c.mu.Lock()
		next := b.listNext
		c.detachBounded(b)
		c.mu.Unlock()
		b.releasePayload()
		b.mu.Unlock()
		b = next
	}
	c.mu.Lock()
	n := int64(cap(c.index)) * int64(unsafe.Sizeof((*bucket)(nil)))
	c.index = nil
	c.lease.shrink(n)
	c.mu.Unlock()
	c.lease.shrink(int64(cap(c.buf)))
	c.packet = packet{}
	c.regularFuncsMu.Lock()
	n = int64(cap(c.callbacks))*int64(unsafe.Sizeof(regularEntry{})) + int64(cap(c.callbackCache))*int64(unsafe.Sizeof((func(*Client))(nil)))
	c.callbacks = nil
	c.callbackCache = nil
	c.lease.shrink(n)
	c.regularFuncsMu.Unlock()
}

type regularEntry struct {
	id int
	f  func(*Client)
}

func (c *Client) startBoundedRegular(f func(*Client)) int {
	c.regularFuncsMu.Lock()
	defer c.regularFuncsMu.Unlock()
	if c.stopped.Load() {
		return 0
	}
	if len(c.callbacks) == cap(c.callbacks) {
		size := 2*cap(c.callbacks) + 1
		n := int64(size) * (int64(unsafe.Sizeof(regularEntry{})) + int64(unsafe.Sizeof((func(*Client))(nil))))
		if !c.lease.grow(n) {
			return 0
		}
		a := make([]regularEntry, len(c.callbacks), size)
		copy(a, c.callbacks)
		cache := make([]func(*Client), 0, size)
		old := int64(cap(c.callbacks))*int64(unsafe.Sizeof(regularEntry{})) + int64(cap(c.callbackCache))*int64(unsafe.Sizeof((func(*Client))(nil)))
		// The running callback snapshot may still refer to the old backing array.
		c.callbacks = a
		c.callbackCache = cache
		if c.callbackRunning {
			c.callbackRetired += old
		} else {
			c.lease.shrink(old)
		}
	}
	c.nextRegularID++
	c.callbacks = append(c.callbacks, regularEntry{c.nextRegularID, f})
	return c.nextRegularID
}
func (c *Client) stopBoundedRegular(id int) {
	c.regularFuncsMu.Lock()
	defer c.regularFuncsMu.Unlock()
	for i, e := range c.callbacks {
		if e.id == id {
			copy(c.callbacks[i:], c.callbacks[i+1:])
			c.callbacks[len(c.callbacks)-1] = regularEntry{}
			c.callbacks = c.callbacks[:len(c.callbacks)-1]
			return
		}
	}
}
func (c *Client) callBoundedRegular() {
	c.regularCallMu.Lock()
	defer c.regularCallMu.Unlock()
	c.regularFuncsMu.Lock()
	ss := c.callbackCache[:0]
	for _, e := range c.callbacks {
		ss = append(ss, e.f)
	}
	c.callbackRunning = true
	c.regularFuncsMu.Unlock()
	defer func() {
		for i := range ss {
			ss[i] = nil
		}
		c.regularFuncsMu.Lock()
		c.callbackRunning = false
		if c.callbackRetired > 0 {
			c.lease.shrink(c.callbackRetired)
			c.callbackRetired = 0
		}
		c.regularFuncsMu.Unlock()
		if p := recover(); p != nil {
			c.rareLog("[statshouse] panic inside regular measurement function, ignoring: %v", p)
		}
	}()
	for _, f := range ss {
		f(c)
	}
}
