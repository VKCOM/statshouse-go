package statshouse

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	tcpPrefix              = "statshousev"
	tcpMagicV2             = byte('2')
	defaultDialTimeout     = 5 * time.Second
	defaultReconnectDelay  = time.Second
	defaultWriteTimeout    = 15 * time.Second
	writeTimeoutAccuracy   = 2 * time.Second
	defaultStuckReconDelay = 30 * time.Second
)

type netConn interface {
	Write([]byte) ([]byte, error)
	Close() error
}
type datagramConn struct{ net.Conn }

func (t *datagramConn) Write(b []byte) ([]byte, error) {
	_, err := t.Conn.Write(b[tlInt32Size:])
	return b, err
}

type tcpOptions struct {
	writeTimeout   time.Duration
	closeTimeout   time.Duration
	reconnectDelay time.Duration
	dialTimeout    time.Duration
	dnsInterval    time.Duration
	dial           func(context.Context, string) (net.Conn, error)
	now            func() time.Time
}

func (c *Client) transportOptions() tcpOptions {
	o := tcpOptions{writeTimeout: defaultWriteTimeout, closeTimeout: defaultWriteTimeout, reconnectDelay: defaultReconnectDelay, dialTimeout: defaultDialTimeout, dnsInterval: time.Minute, now: time.Now}
	if c.tcpOptions != nil {
		o = *c.tcpOptions
	}
	return o
}

type tcpConn struct {
	writeDeadline      time.Time
	lastDeadlineUpdate time.Time

	*Client
	wouldBlockSize atomic.Uint64
	app, env, host string
	handshake      string
	poolMu         sync.Mutex
	pool           addressPool
	qMu            sync.Mutex
	ring           [][]byte
	head, count    int
	wake           chan struct{}
	reconCh        chan struct{}
	closed         atomic.Bool
	done           chan struct{}
	closeOnce      sync.Once
	closeErr       error
	socketMu       sync.Mutex
	socket         net.Conn
	ctx            context.Context
	cancel         context.CancelFunc
	options        tcpOptions
	dial           func(string) (net.Conn, error)
}

type tcpPoolConn struct {
	primary, secondary *tcpConn
	primPtr, secPtr    **tcpConn
	closed             chan struct{}
	closeOnce          sync.Once
	closeErr           error
	ctx                context.Context
	cancel             context.CancelFunc
	dnsDone            chan struct{}
	lease              *memoryLease
	targets            []string
	targetBytes        int64
}

func (c *Client) netDial() (netConn, error) {
	c.markStarted()
	deadline := time.Now().Add(defaultDialTimeout)
	if n := c.closeDeadline.Load(); n != 0 {
		cutoff := time.Unix(0, n)
		if cutoff.Before(deadline) {
			deadline = cutoff
		}
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	c.dialMu.Lock()
	c.dialCancel = cancel
	c.dialMu.Unlock()
	defer func() { cancel(); c.dialMu.Lock(); c.dialCancel = nil; c.dialMu.Unlock() }()
	targets, resolvedLease, err := resolveDialTargetsBudget(ctx, c.network, c.addr, c.budget)
	defer func() {
		targets = nil
		if resolvedLease != nil {
			resolvedLease.shrink(resolvedLease.bytes.Load())
		}
	}()
	if err != nil {
		c.rareLog("[statshouse] resolve address: %v", err)
		return nil, err
	}
	if c.network == "tcp" {
		return c.netDialTCP(targets)
	}
	addr := c.addr
	if len(targets) > 0 {
		addr = targets[0]
	}
	conn, err := (&net.Dialer{Timeout: defaultDialTimeout}).DialContext(ctx, c.network, addr)
	if err != nil {
		c.rareLog("[statshouse] failed to dial statshouse: %v", err)
		return nil, err
	}
	return &datagramConn{conn}, nil
}

func targetsCharge(targets []string) int64 {
	n := int64(len(targets)) * int64(unsafe.Sizeof(""))
	for _, s := range targets {
		n += int64(len(s))
	}
	return n
}
func cloneTargets(targets []string) []string {
	a := make([]string, len(targets))
	for i, s := range targets {
		a[i] = strings.Clone(s)
	}
	return a
}
func (c *Client) netDialTCP(targets []string) (netConn, error) {
	options := c.transportOptions()
	base := int64(unsafe.Sizeof(tcpPoolConn{})) + 2*int64(unsafe.Sizeof(tcpConn{})) + int64(unsafe.Sizeof(memoryLease{})) + 2*tcpConnBucketCount*int64(unsafe.Sizeof([]byte(nil)))
	extra := targetsCharge(targets) + int64(len(c.app)+len(c.env)+len(c.hostTag)) + int64(len(tcpPrefix)+1+tlInt32Size+len(c.hostTag))
	if c.budget != nil && !c.budget.reserve(base+extra) {
		return nil, ErrMemoryBudgetExceeded
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &tcpPoolConn{closed: make(chan struct{}), ctx: ctx, cancel: cancel, dnsDone: make(chan struct{}), targets: cloneTargets(targets), targetBytes: targetsCharge(targets)}
	if c.budget != nil {
		d.lease = makeLease(c.budget, base+extra)
	}
	p, s := newAddressPools(d.targets)
	app, env, host := strings.Clone(c.app), strings.Clone(c.env), strings.Clone(c.hostTag)
	handshake := buildTCPHandshakeV2(host)
	newWorker := func(pool addressPool) *tcpConn {
		wctx, wcancel := context.WithCancel(context.Background())
		return &tcpConn{Client: c, app: app, env: env, host: host, handshake: handshake, pool: pool, ring: make([][]byte, tcpConnBucketCount), wake: make(chan struct{}, 1), reconCh: make(chan struct{}, 1), done: make(chan struct{}), ctx: wctx, cancel: wcancel, options: options}
	}
	d.primary = newWorker(p)
	d.secondary = newWorker(s)
	d.primPtr = &d.primary
	d.secPtr = &d.secondary
	go d.primary.send()
	go d.secondary.send()
	go d.runDNSRefresh(c.network, c.addr)
	return d, nil
}
func (d *tcpPoolConn) Write(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return b, nil
	}
	select {
	case <-d.closed:
		return b, errWriteAfterClose
	default:
	}
	out, err := (*d.primPtr).Write(b)
	if err == nil {
		return out, nil
	}
	if !errors.Is(err, errWouldBlock) {
		return b, err
	}
	out, err = (*d.secPtr).Write(b)
	if err == nil {
		select {
		case (*d.primPtr).reconCh <- struct{}{}:
		default:
		}
		d.primPtr, d.secPtr = d.secPtr, d.primPtr
		return out, nil
	}
	if errors.Is(err, errWouldBlock) {
		saturatingAdd(&d.primary.wouldBlockSize, uint64(len(b)))
		d.primary.dropPacket(len(b))
	}
	return b, err
}
func (d *tcpPoolConn) Close() error {
	d.closeOnce.Do(func() {
		close(d.closed)
		d.cancel()
		cutoff := time.Now().Add(d.primary.options.closeTimeout)
		if n := d.primary.closeDeadline.Load(); n != 0 {
			limit := time.Unix(0, n)
			if limit.Before(cutoff) {
				cutoff = limit
			}
		}
		d.primary.beginClose(cutoff)
		d.secondary.beginClose(cutoff)
		<-d.primary.done
		<-d.secondary.done
		<-d.dnsDone
		d.closeErr = d.primary.closeErr
		if d.closeErr == nil {
			d.closeErr = d.secondary.closeErr
		}
		d.targets = nil
		d.primary.pool = addressPool{}
		d.secondary.pool = addressPool{}
		d.primary.app = ""
		d.primary.env = ""
		d.primary.host = ""
		d.primary.handshake = ""
		d.secondary.app = ""
		d.secondary.env = ""
		d.secondary.host = ""
		d.secondary.handshake = ""
		if d.lease != nil {
			fixed := int64(unsafe.Sizeof(tcpPoolConn{})) + 2*int64(unsafe.Sizeof(tcpConn{})) + int64(unsafe.Sizeof(memoryLease{}))
			d.lease.shrink(d.lease.bytes.Load() - fixed)
		}
	})
	return d.closeErr
}
func (d *tcpPoolConn) runDNSRefresh(network, addr string) {
	defer close(d.dnsDone)
	ticker := time.NewTicker(d.primary.options.dnsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(d.ctx, d.primary.options.dialTimeout)
			targets, resolvedLease, err := resolveDialTargetsBudget(ctx, network, addr, d.primary.budget)
			cancel()
			if err != nil {
				d.primary.rareLog("[statshouse] dns refresh: %v", err)
				continue
			}
			n := targetsCharge(targets)
			if d.lease != nil && !d.lease.grow(n) {
				targets = nil
				if resolvedLease != nil {
					resolvedLease.shrink(resolvedLease.bytes.Load())
				}
				continue
			}
			targets = cloneTargets(targets)
			if resolvedLease != nil {
				resolvedLease.shrink(resolvedLease.bytes.Load())
			}
			p, s := newAddressPools(targets)
			d.primary.replacePool(p)
			d.secondary.replacePool(s)
			old := d.targetBytes
			d.targets = targets
			d.targetBytes = n
			if d.lease != nil {
				d.lease.shrink(old)
			}
		}
	}
}
func (t *tcpConn) signal() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}
func (t *tcpConn) Write(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return b, nil
	}
	t.qMu.Lock()
	defer t.qMu.Unlock()
	if t.closed.Load() {
		return b, errWriteAfterClose
	}
	if t.count == len(t.ring) {
		return b, errWouldBlock
	}
	if t.budget != nil && !t.budget.reserve(int64(cap(b))) {
		return b, errWouldBlock
	}
	replacement := make([]byte, cap(b))
	t.ring[(t.head+t.count)%len(t.ring)] = b
	t.count++
	t.stats.queued.Add(1)
	t.stats.queueBytes.Add(int64(len(b)))
	t.signal()
	return replacement, nil
}
func (t *tcpConn) pop() ([]byte, bool) {
	t.qMu.Lock()
	defer t.qMu.Unlock()
	if t.count == 0 {
		return nil, false
	}
	b := t.ring[t.head]
	t.ring[t.head] = nil
	t.head = (t.head + 1) % len(t.ring)
	t.count--
	t.stats.queued.Add(-1)
	t.stats.queueBytes.Add(-int64(len(b)))
	return b, true
}
func (t *tcpConn) releasePacket(b []byte, drop bool) {
	if drop {
		t.dropPacket(len(b))
	}
	if t.budget != nil {
		t.budget.release(int64(cap(b)))
	}
}
func (t *tcpConn) beginClose(cutoff time.Time) {
	t.closeOnce.Do(func() {
		t.qMu.Lock()
		t.closed.Store(true)
		t.qMu.Unlock()
		t.signal()
		go func() {
			timer := time.NewTimer(time.Until(cutoff))
			defer timer.Stop()
			select {
			case <-t.done:
				return
			case <-timer.C:
				t.cancel()
				t.closeSocket()
				t.signal()
			}
		}()
	})
}
func (t *tcpConn) Close() error {
	t.beginClose(time.Now().Add(t.options.closeTimeout))
	<-t.done
	return t.closeErr
}
func (t *tcpConn) closeSocket() {
	t.socketMu.Lock()
	if t.socket != nil {
		_ = t.socket.Close()
		t.socket = nil
	}
	t.socketMu.Unlock()
}
func (t *tcpConn) write(conn net.Conn, b []byte) error {
	now := time.Now()
	timeout := defaultWriteTimeout
	if t.options.now != nil {
		now = t.options.now()
	}
	if t.options.writeTimeout > 0 {
		timeout = t.options.writeTimeout
	}
	accuracy := writeTimeoutAccuracy
	if timeout/4 < accuracy {
		accuracy = timeout / 4
	}
	if t.writeDeadline.IsZero() || !t.writeDeadline.After(now) || now.Sub(t.lastDeadlineUpdate) >= accuracy {
		deadline := now.Add(timeout)
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
		t.writeDeadline = deadline
		t.lastDeadlineUpdate = now
	}
	n, err := conn.Write(b)
	if err == nil && n != len(b) {
		err = errors.New("statshouse: short write")
	}
	return err
}
func (t *tcpConn) wait(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-t.ctx.Done():
		return false
	case <-timer.C:
		return true
	case <-t.wake:
		return true
	}
}
func (t *tcpConn) send() {
	defer func() {
		t.closeSocket()
		t.cancel()
		for {
			b, ok := t.pop()
			if !ok {
				break
			}
			t.releasePacket(b, true)
		}
		t.qMu.Lock()
		t.ring = nil
		t.qMu.Unlock()
		close(t.done)
	}()
	var conn net.Conn
	lastDial := time.Time{}
	lastStuck := time.Now()
	for {
		if t.ctx.Err() != nil {
			t.closeErr = t.ctx.Err()
			return
		}
		select {
		case <-t.reconCh:
			if time.Since(lastStuck) >= defaultStuckReconDelay {
				t.closeSocket()
				conn = nil
				lastStuck = time.Now()
			}
		default:
		}
		if conn == nil {
			if t.closed.Load() {
				t.qMu.Lock()
				empty := t.count == 0
				t.qMu.Unlock()
				if empty {
					return
				}
			}
			delay := t.options.reconnectDelay - time.Since(lastDial)
			if delay > 0 {
				if !t.wait(delay) {
					continue
				}
				if time.Since(lastDial) < t.options.reconnectDelay {
					continue
				}
			}
			lastDial = time.Now()
			var err error
			conn, err = t.reconnect()
			if err != nil {
				if !errors.Is(err, errEmptyAddr) {
					t.rareLog("[statshouse] reconnect failed: %v", err)
				}
				if t.closed.Load() {
					t.closeErr = err
					return
				}
				continue
			}
		}
		b, ok := t.pop()
		if !ok {
			if t.closed.Load() {
				return
			}
			select {
			case <-t.ctx.Done():
				continue
			case <-t.wake:
				continue
			case <-t.reconCh:
				if time.Since(lastStuck) >= defaultStuckReconDelay {
					t.closeSocket()
					conn = nil
					lastStuck = time.Now()
				}
				continue
			}
		}
		t.stats.inflight.Add(1)
		t.stats.inflightBytes.Add(int64(len(b)))
		err := t.write(conn, b)
		if err != nil {
			saturatingAdd(&t.stats.writeErrors, 1)
			t.rareLog("[statshouse] write failed: %v", err)
			t.closeSocket()
			conn = nil
		} else {
			t.reportWouldBlockIfAny(conn, b)
		}
		t.stats.inflight.Add(-1)
		t.stats.inflightBytes.Add(-int64(len(b)))
		t.releasePacket(b, err != nil)
	}
}
func (t *tcpConn) reconnect() (net.Conn, error) {
	t.poolMu.Lock()
	addr, ok := t.pool.pick()
	t.poolMu.Unlock()
	if !ok {
		return nil, errEmptyAddr
	}
	ctx := t.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := t.options.dialTimeout
	if timeout <= 0 {
		timeout = defaultDialTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t.writeDeadline = time.Time{}
	saturatingAdd(&t.stats.dialAttempts, 1)
	var conn net.Conn
	var err error
	if t.dial != nil {
		conn, err = t.dial(addr)
	} else if t.options.dial != nil {
		conn, err = t.options.dial(ctx, addr)
	} else {
		conn, err = (&net.Dialer{Timeout: timeout}).DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		saturatingAdd(&t.stats.dialErrors, 1)
		return nil, err
	}
	t.socketMu.Lock()
	t.socket = conn
	t.socketMu.Unlock()
	if t.ctx != nil && t.ctx.Err() != nil {
		t.closeSocket()
		return nil, t.ctx.Err()
	}
	// Handshake storage is temporary; reserve it before making the write buffer.
	n := int64(len(t.handshake))
	if t.budget != nil && !t.budget.reserve(n) {
		t.closeSocket()
		return nil, ErrMemoryBudgetExceeded
	}
	err = t.write(conn, []byte(t.handshake))
	if t.budget != nil {
		t.budget.release(n)
	}
	if err != nil {
		saturatingAdd(&t.stats.writeErrors, 1)
		t.closeSocket()
		return nil, err
	}
	saturatingAdd(&t.stats.reconnects, 1)
	return conn, nil
}
func (t *tcpConn) replacePool(p addressPool) { t.poolMu.Lock(); t.pool = p; t.poolMu.Unlock() }
func (t *tcpConn) reportWouldBlockIfAny(conn net.Conn, buf []byte) {
	n := t.wouldBlockSize.Swap(0)
	if n == 0 {
		return
	}
	t.rareLog("[statshouse] lost %v bytes", n)
	p := packet{buf: buf[:batchHeaderLen], maxSize: cap(buf)}
	k := metricKeyTransport{name: "__src_client_write_err"}
	fillTag(&k, "0", t.env)
	fillTag(&k, "1", "1")
	fillTag(&k, "2", "1")
	fillTag(&k, "3", t.app)
	fillTag(&k, "_h", t.host)
	p.sendValues(nil, &k, "", 0, 0, []float64{float64(n)})
	p.writeBatchHeader()
	if err := t.write(conn, p.buf); err != nil {
		saturatingAdd(&t.stats.writeErrors, 1)
		t.rareLog("[statshouse] loss report failed: %v", err)
	}
}
func buildTCPHandshakeV2(hostTag string) string {
	buf := make([]byte, 0, len(tcpPrefix)+1+tlInt32Size+len(hostTag))
	buf = append(buf, tcpPrefix...)
	buf = append(buf, tcpMagicV2)
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(hostTag)))
	buf = append(buf, n[:]...)
	buf = append(buf, hostTag...)
	return string(buf)
}
func forceValidHostTag(s string) string {
	if len(s) > maxHostTagLen {
		return s[:maxHostTagLen]
	}
	return s
}
