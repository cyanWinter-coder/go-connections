package proxy

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

// waitForCondition polls cond every 5ms until it returns true or the timeout
// expires, in which case the test fails with msg.
func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", timeout, msg)
}

func (proxy *UDPProxy) trackedEntry(key connTrackKey) *connTrackEntry {
	proxy.connTrackLock.Lock()
	defer proxy.connTrackLock.Unlock()
	return proxy.connTrackTable[key]
}

func (proxy *UDPProxy) trackedConns() int {
	proxy.connTrackLock.Lock()
	defer proxy.connTrackLock.Unlock()
	return len(proxy.connTrackTable)
}

func newTestUDPConn(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func udpConnIsClosed(conn *net.UDPConn) bool {
	// A WriteToUDP with no payload does not emit a datagram, but still
	// surfaces whether the connection has been closed.
	_, err := conn.WriteToUDP(nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9})
	return errors.Is(err, net.ErrClosed)
}

// A replyLoop must only remove its own generation from the tracking table:
// if a newer connection has been tracked for the same client key, the stale
// loop must leave both the table entry and the newer connection untouched,
// and must not close its own connection either (ownership has moved on).
func TestUDPReplyLoopRemovesOnlyOwnGeneration(t *testing.T) {
	proxy := &UDPProxy{
		connTrackTable:   make(connTrackMap),
		connTrackTimeout: 10 * time.Millisecond,
		Logger:           &noopLogger{},
	}
	clientAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	key := newConnTrackKey(clientAddr)

	staleEntry := &connTrackEntry{conn: newTestUDPConn(t), gen: 1}
	currentEntry := &connTrackEntry{conn: newTestUDPConn(t), gen: 2}
	proxy.connTrackTable[*key] = currentEntry

	// The stale loop finds nothing to read and exits on its idle timeout.
	proxy.replyLoop(staleEntry, clientAddr, key)

	if got := proxy.trackedEntry(*key); got != currentEntry {
		t.Fatalf("stale replyLoop removed the current generation from the tracking table (got %+v)", got)
	}
	if udpConnIsClosed(currentEntry.conn) {
		t.Fatal("stale replyLoop closed the current generation's connection")
	}
	if udpConnIsClosed(staleEntry.conn) {
		t.Fatal("replyLoop closed a connection it no longer owns")
	}
}

// A replyLoop that still owns its table entry must remove it and close the
// connection when it exits.
func TestUDPReplyLoopCleansUpOwnEntry(t *testing.T) {
	proxy := &UDPProxy{
		connTrackTable:   make(connTrackMap),
		connTrackTimeout: 10 * time.Millisecond,
		Logger:           &noopLogger{},
	}
	clientAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	key := newConnTrackKey(clientAddr)

	entry := &connTrackEntry{conn: newTestUDPConn(t), gen: 1}
	proxy.connTrackTable[*key] = entry

	proxy.replyLoop(entry, clientAddr, key)

	if got := proxy.trackedEntry(*key); got != nil {
		t.Fatalf("replyLoop did not remove its own entry from the tracking table (got %+v)", got)
	}
	if !udpConnIsClosed(entry.conn) {
		t.Fatal("replyLoop did not close its own connection on exit")
	}
}

// After an idle expiry, a new datagram from the same client (same source
// address, hence the same tracking key) must establish a new generation and
// keep working.
func TestUDPProxyReconnectAfterIdleExpiry(t *testing.T) {
	backend := NewEchoServer(t, "udp", "127.0.0.1:0")
	defer backend.Close()
	backend.Run()

	proxy, err := NewUDPProxy(
		&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0},
		backend.LocalAddr().(*net.UDPAddr),
	)
	if err != nil {
		t.Fatal(err)
	}
	proxy.connTrackTimeout = 50 * time.Millisecond
	defer proxy.Close()
	go proxy.Run()

	client, err := net.Dial("udp", proxy.FrontendAddr().String())
	if err != nil {
		t.Fatalf("can't connect to the proxy: %v", err)
	}
	defer func() { _ = client.Close() }()
	_ = client.SetDeadline(time.Now().Add(10 * time.Second))

	exchange := func() {
		t.Helper()
		if _, err := client.Write(testBuf); err != nil {
			t.Fatal(err)
		}
		recvBuf := make([]byte, testBufSize)
		if _, err := client.Read(recvBuf); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(testBuf, recvBuf) {
			t.Fatalf("expected [%v] but got [%v]", testBuf, recvBuf)
		}
	}

	exchange()
	clientKey := newConnTrackKey(client.LocalAddr().(*net.UDPAddr))
	first := proxy.trackedEntry(*clientKey)
	if first == nil {
		t.Fatal("no connection tracked after the first datagram")
	}

	// Let the first generation expire from inactivity.
	waitForCondition(t, 5*time.Second, func() bool {
		return proxy.trackedEntry(*clientKey) == nil
	}, "idle connection was not expired")

	// The same client reconnects: a new generation must be tracked and the
	// echo must keep working.
	exchange()
	second := proxy.trackedEntry(*clientKey)
	if second == nil {
		t.Fatal("no connection tracked after reconnecting")
	}
	if second.gen <= first.gen {
		t.Fatalf("expected a new generation after reconnect, got gen %d after gen %d", second.gen, first.gen)
	}
	if got := proxy.trackedConns(); got != 1 {
		t.Fatalf("expected exactly one tracked connection, got %d", got)
	}
}

// Fire datagrams around the idle-expiry boundary: when expiry and new data
// race, the proxy must never lose the client's newly established connection,
// and must never keep more than one connection for the client.
func TestUDPProxyIdleExpiryRace(t *testing.T) {
	backend := NewEchoServer(t, "udp", "127.0.0.1:0")
	defer backend.Close()
	backend.Run()

	proxy, err := NewUDPProxy(
		&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0},
		backend.LocalAddr().(*net.UDPAddr),
	)
	if err != nil {
		t.Fatal(err)
	}
	proxy.connTrackTimeout = 40 * time.Millisecond
	defer proxy.Close()
	go proxy.Run()

	client, err := net.Dial("udp", proxy.FrontendAddr().String())
	if err != nil {
		t.Fatalf("can't connect to the proxy: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Hover around the expiry boundary so that idle timeouts and new
	// datagrams keep racing each other.
	for i := 0; i < 25; i++ {
		if _, err := client.Write(testBuf); err != nil {
			t.Fatal(err)
		}
		// A datagram written just as its connection expires may be lost
		// (as on any UDP network); retransmit like a real client would.
		_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
		recvBuf := make([]byte, testBufSize)
		if _, err := client.Read(recvBuf); err != nil {
			var netErr net.Error
			if !errors.As(err, &netErr) || !netErr.Timeout() {
				t.Fatalf("unexpected read error: %v", err)
			}
			i--
			continue
		}
		if !bytes.Equal(testBuf, recvBuf) {
			t.Fatalf("expected [%v] but got [%v]", testBuf, recvBuf)
		}
		if got := proxy.trackedConns(); got > 1 {
			t.Fatalf("expected at most one tracked connection, got %d", got)
		}
		time.Sleep(30 * time.Millisecond)
	}

	clientKey := newConnTrackKey(client.LocalAddr().(*net.UDPAddr))
	if got := proxy.trackedEntry(*clientKey); got == nil {
		t.Fatal("the client's connection was lost while it was still active")
	}
}

// Close racing with replyLoop exits must clean up every tracked connection
// exactly once, and Close itself must be idempotent.
func TestUDPProxyConcurrentClose(t *testing.T) {
	backend := NewEchoServer(t, "udp", "127.0.0.1:0")
	defer backend.Close()
	backend.Run()

	for i := 0; i < 50; i++ {
		proxy, err := NewUDPProxy(
			&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0},
			backend.LocalAddr().(*net.UDPAddr),
		)
		if err != nil {
			t.Fatal(err)
		}
		proxy.connTrackTimeout = 20 * time.Millisecond
		go proxy.Run()

		client, err := net.Dial("udp", proxy.FrontendAddr().String())
		if err != nil {
			t.Fatalf("can't connect to the proxy: %v", err)
		}
		if _, err := client.Write(testBuf); err != nil {
			t.Fatal(err)
		}
		clientKey := newConnTrackKey(client.LocalAddr().(*net.UDPAddr))
		waitForCondition(t, 5*time.Second, func() bool {
			return proxy.trackedEntry(*clientKey) != nil
		}, "no connection tracked")

		// Race Close against the replyLoop's idle exit, and call Close
		// more than once.
		done := make(chan struct{})
		go func() {
			defer close(done)
			proxy.Close()
		}()
		proxy.Close()
		<-done
		proxy.Close()

		if got := proxy.trackedConns(); got != 0 {
			t.Fatalf("expected no tracked connections after Close, got %d", got)
		}
		_ = client.Close()
	}
}
