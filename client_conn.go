package statshouse

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
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
	Write(b []byte) ([]byte, error)
	Close() error
}

// Client manages metric aggregation and transport to a StatsHouse agent.
type datagramConn struct { // either UDP or unixgram
	net.Conn
}

type tcpConn struct {
	wouldBlockSize atomic.Int32

	*Client
	app       string
	env       string
	handshake string // precomputed reconnect key (copy every connect for safety)

	poolMu  sync.Mutex
	pool    addressPool
	w       chan []byte
	reconCh chan struct{}

	closed   atomic.Bool
	closeErr chan error
}

type tcpPoolConn struct {
	primPtr   **tcpConn // ptr to primary, nonblocking swap
	secPtr    **tcpConn // ptr to secondary, nonblocking swap
	primary   *tcpConn
	secondary *tcpConn
	closed    chan struct{}
	closeOnce sync.Once
}

func (d *tcpPoolConn) Write(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return b, nil
	}
	select {
	case <-d.closed:
		return make([]byte, cap(b)), errWriteAfterClose
	default:
	}
	b, err := (*d.primPtr).Write(b)
	if err == nil {
		return b, nil
	}
	if !errors.Is(err, errWouldBlock) {
		return b, err
	}
	b, err = (*d.secPtr).Write(b)
	if err == nil {
		select {
		case (*d.primPtr).reconCh <- struct{}{}:
		default:
		}
		d.primPtr, d.secPtr = d.secPtr, d.primPtr
		return b, nil
	}
	if errors.Is(err, errWouldBlock) {
		d.primary.wouldBlockSize.Add(int32(len(b)))
		return b, errWouldBlock
	}
	return b, err
}

func (d *tcpPoolConn) Close() (err error) {
	d.closeOnce.Do(func() {
		close(d.closed)
		err = d.primary.Close()
		err2 := d.secondary.Close()
		if err == nil {
			err = err2
		}
	})
	return
}

func (c *Client) netDial() (netConn, error) {
	targets, err := resolveDialTargets(c.network, c.addr)
	if err != nil {
		c.rareLog("[statshouse] resolve address %q: %v", c.addr, err)
		return nil, err
	}
	c.dialTargets = targets

	if c.network == "tcp" {
		return c.netDialTCP()
	}
	addr := c.addr
	if len(c.dialTargets) > 0 {
		addr = c.dialTargets[0]
	}
	conn, err := net.Dial(c.network, addr)
	if err != nil {
		c.rareLog("[statshouse] failed to dial statshouse: %v", err)
		return nil, err
	}
	return &datagramConn{Conn: conn}, nil
}

func (c *Client) netDialTCP() (netConn, error) {
	primaryPool, secondaryPool := newAddressPools(c.dialTargets)
	handshake := buildTCPHandshakeV2(c.hostTag)
	primary := &tcpConn{
		Client:    c,
		app:       c.app,
		env:       c.env,
		handshake: handshake,
		pool:      primaryPool,
		w:         make(chan []byte, tcpConnBucketCount),
		reconCh:   make(chan struct{}, 1),
		closeErr:  make(chan error, 1),
	}
	go primary.send()

	secondary := &tcpConn{
		Client:    c,
		app:       c.app,
		env:       c.env,
		handshake: handshake,
		pool:      secondaryPool,
		w:         make(chan []byte, tcpConnBucketCount),
		reconCh:   make(chan struct{}, 1),
		closeErr:  make(chan error, 1),
	}
	go secondary.send()
	poolConn := &tcpPoolConn{
		primary:   primary,
		secondary: secondary,
		closed:    make(chan struct{}),
	}
	poolConn.primPtr = &poolConn.primary
	poolConn.secPtr = &poolConn.secondary
	go poolConn.runDNSRefresh(c.network, c.addr)
	return poolConn, nil
}

func (d *tcpPoolConn) runDNSRefresh(network, addr string) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	cl := d.primary.Client
	for {
		select {
		case <-d.closed:
			return
		case <-ticker.C:
			targets, err := resolveDialTargets(network, addr)
			if err != nil {
				cl.rareLog("[statshouse] dns refresh resolve %q: %v", addr, err)
				continue
			}
			primaryPool, secondaryPool := newAddressPools(targets)
			d.primary.replacePool(primaryPool)
			d.secondary.replacePool(secondaryPool)
		}
	}
}

func (t *datagramConn) Write(b []byte) ([]byte, error) {
	_, err := t.Conn.Write(b[tlInt32Size:]) // skip data length
	return b, err
}

func (t *datagramConn) Close() error {
	return t.Conn.Close()
}

func (t *tcpConn) Write(b []byte) (_ []byte, err error) {
	if len(b) == 0 {
		return b, nil
	}
	if t.closed.Load() {
		return b, errWriteAfterClose
	}
	n := cap(b)
	select {
	case t.w <- b:
		return make([]byte, n), nil
	default:
		return b, errWouldBlock
	}
}

func (t *tcpConn) Close() error {
	if t.closed.CompareAndSwap(false, true) {
		close(t.w)
	}
	return <-t.closeErr
}

func (t *tcpConn) send() {
	var conn net.Conn
	var err error
	var lastDial time.Time
	var lastStuckRecon = time.Now()
	var writeDeadline time.Time
loop:
	for {
		select {
		case <-t.reconCh:
			if lastStuckRecon.Add(defaultStuckReconDelay).After(time.Now()) {
				continue
			}
			if conn != nil {
				_ = conn.Close()
				conn = nil
				writeDeadline = time.Time{}
				lastStuckRecon = time.Now()
			}
			continue
		default:
		}
		if conn == nil {
			time.Sleep(defaultReconnectDelay - time.Since(lastDial))
			lastDial = time.Now()
			conn, err = t.reconnect()
			if err != nil {
				if t.closed.Load() {
					break loop
				}
				if !errors.Is(err, errEmptyAddr) { // ignore secondary without address
					t.rareLog("[statshouse] failed to dial statshouse: %v", err)
				}
				continue
			}
			writeDeadline = time.Time{}
		}
		if defaultWriteTimeout-time.Until(writeDeadline) > writeTimeoutAccuracy {
			deadline := time.Now().Add(defaultWriteTimeout)
			if err = conn.SetWriteDeadline(deadline); err != nil {
				t.rareLog("[statshouse] failed to set write deadline: %v", err)
				_ = conn.Close()
				conn = nil
				writeDeadline = time.Time{}
				continue
			}
			writeDeadline = deadline
		}
		buf, ok := <-t.w
		if !ok {
			break
		}
		if _, err = conn.Write(buf); err != nil {
			t.rareLog("[statshouse] failed to send data to statshouse: %v", err)
			_ = conn.Close()
			conn = nil
			writeDeadline = time.Time{}
			continue // not resend for tcp connect
		}
		t.reportWouldBlockIfAny(conn, buf)
	}
	err = nil
	if conn != nil {
		err = conn.Close()
	}
	t.closeErr <- err
}

func (t *tcpConn) reconnect() (net.Conn, error) {
	t.poolMu.Lock()
	addr, ok := t.pool.pick()
	t.poolMu.Unlock()
	if !ok {
		return nil, errEmptyAddr
	}

	conn, err := (&net.Dialer{Timeout: defaultDialTimeout}).Dial("tcp", addr)
	if err != nil {
		t.rareLog("[statshouse] failed to dial statshouse: %v", err)
		return nil, err
	}
	if _, err = conn.Write([]byte(t.handshake)); err != nil {
		t.rareLog("[statshouse] failed to send header to statshouse: %v", err)
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (t *tcpConn) replacePool(p addressPool) {
	t.poolMu.Lock()
	t.pool = p
	t.poolMu.Unlock()
}

func (t *tcpConn) reportWouldBlockIfAny(conn net.Conn, buf []byte) {
	n := t.wouldBlockSize.Swap(0)
	if n == 0 {
		return
	}
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
	fillTag(&k, "1", "1")   // lang: golang
	fillTag(&k, "2", "1")   // kind: would block
	fillTag(&k, "3", t.app) // application name
	fillTag(&k, "_h", t.hostTag)
	p.sendValues(nil, &k, "", 0, 0, []float64{float64(n)})
	p.writeBatchHeader()
	if _, err := conn.Write(p.buf); err != nil {
		t.rareLog("[statshouse] failed to send data to statshouse: %v", err)
	}
}

func buildTCPHandshakeV2(hostTag string) string {
	buf := make([]byte, 0, len(tcpPrefix)+1+tlInt32Size+len(hostTag))
	buf = append(buf, tcpPrefix...)
	buf = append(buf, tcpMagicV2)
	var lenH [tlInt32Size]byte
	binary.LittleEndian.PutUint32(lenH[:], uint32(len(hostTag)))
	buf = append(buf, lenH[:]...)
	buf = append(buf, hostTag...)
	return string(buf)
}

func forceValidHostTag(s string) string {
	if len(s) <= maxHostTagLen {
		return s
	}
	return s[:maxHostTagLen]
}
