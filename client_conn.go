package statshouse

import (
	"net"
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

	w chan []byte
	r chan []byte

	closed   bool
	closeErr chan error
}

func (c *Client) netDial() (netConn, error) {
	conn, err := net.Dial(c.network, c.addr)
	if err != nil {
		c.rareLog("[statshouse] failed to dial statshouse: %v", err)
		return nil, err
	}
	if c.network != "tcp" {
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
		app:      c.app,
		env:      c.env,
		Conn:     conn,
		r:        make(chan []byte, tcpConnBucketCount),
		w:        make(chan []byte, tcpConnBucketCount),
		closeErr: make(chan error),
	}
	go t.send()
	return t, nil
}

func (t *datagramConn) Write(b []byte) ([]byte, error) {
	_, err := t.Conn.Write(b[tlInt32Size:]) // skip data length
	return b, err
}

func (t *datagramConn) Close() error {
	return t.Conn.Close()
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
			fillTag(&k, "1", "1")   // lang: golang
			fillTag(&k, "2", "1")   // kind: would block
			fillTag(&k, "3", t.app) // application name
			p.sendValues(nil, &k, "", 0, 0, []float64{float64(n)})
			p.writeBatchHeader()
			if _, err = t.Conn.Write(p.buf); err != nil {
				t.rareLog("[statshouse] failed to send data to statshouse: %v", err)
			}
		}
	}
	t.closeErr <- t.Conn.Close()
}
