package statshouse

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
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
	app string
	env string
	net.Conn

	pool *addressPool
	w    chan []byte

	closed   atomic.Bool
	closeErr chan error
}

type tcpPoolConn struct {
	primary   *tcpConn
	secondary *tcpConn
	routeMu   sync.Mutex
	closed    atomic.Bool
}

func (d *tcpPoolConn) Write(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return b, nil
	}
	if d.closed.Load() {
		return make([]byte, cap(b)), errWriteAfterClose
	}
	d.routeMu.Lock()
	defer d.routeMu.Unlock()

	b, err := d.primary.Write(b)
	if err == nil {
		return b, nil
	}
	if !errors.Is(err, errWouldBlock) {
		return b, err
	}
	b, err = d.secondary.Write(b)
	if err == nil {
		d.primary, d.secondary = d.secondary, d.primary
		return b, nil
	}
	if errors.Is(err, errWouldBlock) {
		d.primary.wouldBlockSize.Add(int32(len(b)))
		return b, errWouldBlock
	}
	return b, err
}

func (d *tcpPoolConn) Close() error {
	d.closed.Store(true)
	err1 := d.primary.Close()
	err2 := d.secondary.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (c *Client) netDial() (netConn, error) {
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
	targets := c.dialTargets
	if len(targets) == 0 {
		targets = []string{c.addr}
	}
	pool := newAddressPool(targets)
	primary := &tcpConn{
		Client:   c,
		app:      c.app,
		env:      c.env,
		pool:     pool,
		w:        make(chan []byte, tcpConnBucketCount),
		closeErr: make(chan error, 1),
	}
	secondary := &tcpConn{
		Client:   c,
		app:      c.app,
		env:      c.env,
		pool:     pool,
		w:        make(chan []byte, tcpConnBucketCount),
		closeErr: make(chan error, 1),
	}
	go primary.send()
	go secondary.send()
	return &tcpPoolConn{primary: primary, secondary: secondary}, nil
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
	var err error          // last write or connect error
	var dialTime time.Time // time of last reconnect start
	for buf := range t.w {
		if len(buf) == 0 {
			continue
		}
		for err != nil {
			// reconnect (no more than once per second)
			_ = t.Conn.Close()
			if err = t.reconnect(dialTime); err != nil {
				continue
			}
		}
		if _, err = t.Conn.Write(buf); err != nil {
			t.rareLog("[statshouse] failed to send data to statshouse: %v", err)
			continue // not resend for tcp connect
		}
		t.reportWouldBlockIfAny(buf)
	}
	t.closeErr <- t.Conn.Close()
}

func (t *tcpConn) reconnect(dialTime time.Time) error {
	time.Sleep(time.Second - time.Since(dialTime))
	dialTime = time.Now()
	addr, ok := t.pool.pick()
	if !ok {
		return errEmptyAddr
	}
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).Dial("tcp", addr)
	if err != nil {
		t.rareLog("[statshouse] failed to dial statshouse: %v", err)
		return err
	}
	_, err = conn.Write([]byte("statshousev1"))
	if err != nil {
		t.rareLog("[statshouse] failed to send header to statshouse: %v", err)
		conn.Close()
		return err
	}
	t.Conn = conn
	return nil
}

func (t *tcpConn) reportWouldBlockIfAny(buf []byte) {
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
	p.sendValues(nil, &k, "", 0, 0, []float64{float64(n)})
	p.writeBatchHeader()
	if _, err := t.Conn.Write(p.buf); err != nil {
		t.rareLog("[statshouse] failed to send data to statshouse: %v", err)
	}
}
