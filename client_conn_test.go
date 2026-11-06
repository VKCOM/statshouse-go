package statshouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

type deadlineConn struct {
	mu       sync.Mutex
	deadline time.Time
	writes   int
	closed   bool
}

func (c *deadlineConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.deadline.After(time.Now()) {
		return 0, errors.New("write without a current deadline")
	}
	c.writes++
	return len(p), nil
}
func (c *deadlineConn) Read([]byte) (int, error)        { return 0, errors.New("unused") }
func (c *deadlineConn) Close() error                    { c.mu.Lock(); c.closed = true; c.mu.Unlock(); return nil }
func (c *deadlineConn) LocalAddr() net.Addr             { return &net.TCPAddr{} }
func (c *deadlineConn) RemoteAddr() net.Addr            { return &net.TCPAddr{} }
func (c *deadlineConn) SetDeadline(t time.Time) error   { return c.SetWriteDeadline(t) }
func (c *deadlineConn) SetReadDeadline(time.Time) error { return nil }
func (c *deadlineConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return nil
}

func TestTCPHandshakeHasWriteDeadline(t *testing.T) {
	c := &Client{logF: t.Logf}
	conn := &deadlineConn{}
	w := &tcpConn{Client: c, pool: addressPool{addrs: []string{"test:1"}}, handshake: "header", dial: func(string) (net.Conn, error) { return conn, nil }}
	if _, err := w.reconnect(); err != nil {
		t.Fatalf("handshake must have a deadline: %v", err)
	}
}

func TestTCPDeadlineRefreshAfterIdleAndReconnect(t *testing.T) {
	c := &Client{logF: t.Logf}
	conn := &deadlineConn{}
	now := time.Now()
	w := &tcpConn{Client: c, options: tcpOptions{now: func() time.Time { return now }, writeTimeout: time.Second}}
	if err := w.write(conn, []byte("first")); err != nil {
		t.Fatal(err)
	}
	first := conn.deadline
	now = now.Add(time.Hour)
	if err := w.write(conn, []byte("after idle")); err != nil {
		t.Fatal(err)
	}
	if !conn.deadline.After(first) {
		t.Fatal("stale deadline reused after idle")
	}
	w.pool = addressPool{addrs: []string{"test:1"}}
	w.dial = func(string) (net.Conn, error) { return conn, nil }
	w.handshake = "header"
	if _, err := w.reconnect(); err != nil {
		t.Fatal(err)
	}
	if conn.deadline != now.Add(time.Second) {
		t.Fatal("reconnect did not install deadline")
	}
}

func TestTCPStalledHandshakeTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	defer client.Close()
	w := &tcpConn{Client: &Client{logF: t.Logf}, handshake: "header", pool: addressPool{addrs: []string{"test:1"}}, options: tcpOptions{writeTimeout: 20 * time.Millisecond}, dial: func(string) (net.Conn, error) { return client, nil }}
	start := time.Now()
	if _, err := w.reconnect(); err == nil {
		t.Fatal("stalled handshake succeeded")
	}
	if time.Since(start) > time.Second {
		t.Fatal("handshake was not bounded")
	}
}

func TestTCPStallRecoveryAndConcurrentClose(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(fmt.Sprint(bounded), func(t *testing.T) {
			var c *Client
			var budget *MemoryBudget
			args := ConfigureArgs{Network: "tcp", StatsHouseAddr: "127.0.0.1:1", Logger: t.Logf}
			if bounded {
				var err error
				budget, _ = NewMemoryBudget(1 << 20)
				c, err = NewClientWithBudget(args, budget)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				c = NewClientEx(args)
			}
			stop := make(chan struct{})
			defer close(stop)
			var healthy atomic.Bool
			c.tcpOptions = &tcpOptions{writeTimeout: 20 * time.Millisecond, closeTimeout: 40 * time.Millisecond, reconnectDelay: time.Millisecond, dialTimeout: time.Second, dnsInterval: time.Hour, now: time.Now, dial: func(context.Context, string) (net.Conn, error) {
				conn, peer := net.Pipe()
				go func() {
					defer peer.Close()
					header := make([]byte, len(buildTCPHandshakeV2(c.hostTag)))
					if _, err := io.ReadFull(peer, header); err != nil {
						return
					}
					if healthy.Load() {
						_, _ = io.Copy(io.Discard, peer)
					} else {
						<-stop
					}
				}()
				return conn, nil
			}}
			// All network operations go through the injected in-process connection.
			c.Count("count", Tags{}, 1)
			c.send(uint32(time.Now().Unix()))
			for i := 0; i < 40; i++ {
				c.StringTop("s", Tags{}, strings.Repeat("x", 4096))
				c.send(uint32(time.Now().Unix()))
			}
			deadline := time.Now().Add(time.Second)
			for c.Snapshot().WriteErrors == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if c.Snapshot().WriteErrors == 0 {
				t.Fatal("stalled write never timed out")
			}
			if bounded {
				assertBound(t, budget)
			}
			before := c.Snapshot()
			healthy.Store(true)
			for i := 0; i < 4; i++ {
				c.Count("recovery", Tags{}, 1)
				c.send(uint32(time.Now().Unix()))
				time.Sleep(5 * time.Millisecond)
			}
			if c.Snapshot().Reconnects <= before.Reconnects {
				t.Fatal("did not reconnect")
			}
			start := time.Now()
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _ = c.Close() }()
			}
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Close stuck")
			}
			if time.Since(start) > time.Second {
				t.Fatal("unbounded shutdown")
			}
			stats := c.Snapshot()
			if stats.QueuePackets != 0 || stats.InflightPackets != 0 {
				t.Fatalf("retained packets: %+v", stats)
			}
			if stats.TransportDroppedBytes < before.TransportDroppedBytes {
				t.Fatal("cumulative drops reset during recovery")
			}
		})
	}
}

func TestTCPRefusedDialCloseIsBounded(t *testing.T) {
	c := &Client{logF: t.Logf, tcpOptions: &tcpOptions{writeTimeout: time.Second, closeTimeout: 20 * time.Millisecond, reconnectDelay: time.Second, dialTimeout: time.Second, dnsInterval: time.Hour, now: time.Now, dial: func(ctx context.Context, _ string) (net.Conn, error) { return nil, errors.New("refused") }}}
	conn, err := c.netDialTCP([]string{"test:1"})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write(make([]byte, 128))
	start := time.Now()
	_ = conn.Close()
	if time.Since(start) > time.Second {
		t.Fatal("Close waited for retry delay")
	}
}

type failedWriteConn struct{ deadlineConn }

func (c *failedWriteConn) Write([]byte) (int, error) { return 0, errors.New("write failure") }
func TestFailedHandshakeAndReport(t *testing.T) {
	c := &Client{logF: t.Logf}
	conn := &failedWriteConn{}
	w := &tcpConn{Client: c, handshake: "h", pool: addressPool{addrs: []string{"test:1"}}, dial: func(string) (net.Conn, error) { return conn, nil }}
	if _, err := w.reconnect(); err == nil || !conn.closed {
		t.Fatal("failed handshake did not close connection")
	}
	c.dropPacket(1 << 30)
	c.dropPacket(1 << 30)
	before := c.Snapshot().TransportDroppedBytes
	w.wouldBlockSize.Store((1 << 31) + 100)
	w.reportWouldBlockIfAny(conn, make([]byte, batchHeaderLen, 1024))
	if c.Snapshot().TransportDroppedBytes != before || before != 1<<31 {
		t.Fatal("in-band report changed cumulative counters")
	}
	if w.wouldBlockSize.Load() != 0 {
		t.Fatal("pending report not consumed")
	}
}
func TestTCPQueueReservationsReleasedAtClose(t *testing.T) {
	c, b := testBudgetClient(t, 1<<20)
	c.tcpOptions = &tcpOptions{writeTimeout: time.Second, closeTimeout: 20 * time.Millisecond, reconnectDelay: time.Second, dialTimeout: time.Second, dnsInterval: time.Hour, now: time.Now, dial: func(ctx context.Context, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }}
	before := b.Snapshot().UsedBytes
	conn, err := c.netDialTCP([]string{"test:1"})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64, 128)
	for i := 0; i < 1200; i++ {
		buf, err = conn.Write(buf)
		if err != nil && !errors.Is(err, errWouldBlock) {
			t.Fatal(err)
		}
	}
	assertBound(t, b)
	if c.Snapshot().QueuePackets == 0 {
		t.Fatal("no queued packets")
	}
	_ = conn.Close()
	if c.Snapshot().QueuePackets != 0 || c.Snapshot().InflightPackets != 0 {
		t.Fatal("queue was not released")
	}
	fixed := int64(unsafe.Sizeof(tcpPoolConn{})) + 2*int64(unsafe.Sizeof(tcpConn{})) + int64(unsafe.Sizeof(memoryLease{}))
	if got := b.Snapshot().UsedBytes; got != before+fixed {
		t.Fatalf("packet reservation leaked: got %d, want %d", got, before+fixed)
	}
}
