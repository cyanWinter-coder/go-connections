package proxy

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"
)

const (
	// UDPConnTrackTimeout is the timeout used for UDP connection tracking
	UDPConnTrackTimeout = 90 * time.Second
	// UDPBufSize is the buffer size for the UDP proxy
	UDPBufSize = 65507
)

// A net.Addr where the IP is split into two fields so you can use it as a key
// in a map:
type connTrackKey struct {
	IPHigh uint64
	IPLow  uint64
	Port   int
}

func newConnTrackKey(addr *net.UDPAddr) *connTrackKey {
	if len(addr.IP) == net.IPv4len {
		return &connTrackKey{
			IPHigh: 0,
			IPLow:  uint64(binary.BigEndian.Uint32(addr.IP)),
			Port:   addr.Port,
		}
	}
	return &connTrackKey{
		IPHigh: binary.BigEndian.Uint64(addr.IP[:8]),
		IPLow:  binary.BigEndian.Uint64(addr.IP[8:]),
		Port:   addr.Port,
	}
}

// connTrackEntry is a tracked proxied connection. Each entry carries a
// unique generation so that the replyLoop which owns it can be told apart
// from any newer connection tracked for the same client.
type connTrackEntry struct {
	conn *net.UDPConn
	gen  uint64
}

type connTrackMap map[connTrackKey]*connTrackEntry

// UDPProxy is proxy for which handles UDP datagrams. It implements the Proxy
// interface to handle UDP traffic forwarding between the frontend and backend
// addresses.
type UDPProxy struct {
	Logger         logger
	listener       *net.UDPConn
	frontendAddr   *net.UDPAddr
	backendAddr    *net.UDPAddr
	connTrackTable connTrackMap
	connTrackLock  sync.Mutex
	// connTrackTimeout is the idle timeout applied to tracked connections.
	// It defaults to UDPConnTrackTimeout.
	connTrackTimeout time.Duration
	// nextGen is the generation counter for tracked connections. It is
	// guarded by connTrackLock.
	nextGen uint64
	// closed reports whether Close has been called. It is guarded by
	// connTrackLock.
	closed bool
}

// NewUDPProxy creates a new UDPProxy.
func NewUDPProxy(frontendAddr, backendAddr *net.UDPAddr, ops ...func(*UDPProxy)) (*UDPProxy, error) {
	listener, err := net.ListenUDP("udp", frontendAddr)
	if err != nil {
		return nil, err
	}

	proxy := &UDPProxy{
		listener:         listener,
		frontendAddr:     listener.LocalAddr().(*net.UDPAddr),
		backendAddr:      backendAddr,
		connTrackTable:   make(connTrackMap),
		connTrackTimeout: UDPConnTrackTimeout,
		Logger:           &noopLogger{},
	}

	for _, op := range ops {
		op(proxy)
	}

	return proxy, nil
}

func (proxy *UDPProxy) replyLoop(entry *connTrackEntry, clientAddr *net.UDPAddr, clientKey *connTrackKey) {
	defer func() {
		// Only clean up the tracked entry if it is still the generation
		// this loop belongs to. A newer connection for the same client
		// may have been tracked since; it belongs to another loop and is
		// not ours to remove or close.
		proxy.connTrackLock.Lock()
		if current, ok := proxy.connTrackTable[*clientKey]; ok && current.gen == entry.gen {
			delete(proxy.connTrackTable, *clientKey)
			proxy.connTrackLock.Unlock()
			_ = entry.conn.Close()
			return
		}
		proxy.connTrackLock.Unlock()
	}()

	readBuf := make([]byte, UDPBufSize)
	for {
		_ = entry.conn.SetReadDeadline(time.Now().Add(proxy.connTrackTimeout))
	again:
		read, err := entry.conn.Read(readBuf)
		if err != nil {
			if err, ok := err.(*net.OpError); ok && err.Err == syscall.ECONNREFUSED {
				// This will happen if the last write failed
				// (e.g: nothing is actually listening on the
				// proxied port on the container), ignore it
				// and continue until the connection track
				// timeout expires:
				goto again
			}
			return
		}
		for i := 0; i != read; {
			written, err := proxy.listener.WriteToUDP(readBuf[i:read], clientAddr)
			if err != nil {
				return
			}
			i += written
		}
	}
}

// Run starts forwarding the traffic using UDP.
func (proxy *UDPProxy) Run() {
	readBuf := make([]byte, UDPBufSize)
	for {
		read, from, err := proxy.listener.ReadFromUDP(readBuf)
		if err != nil {
			// NOTE: Apparently ReadFrom doesn't return ECONNREFUSED like
			// Read does (see comment in [UDPProxy.replyLoop]).
			if !errors.Is(err, net.ErrClosed) {
				proxy.Logger.Printf("Stopping proxy on udp/%v for udp/%v (%s)", proxy.frontendAddr, proxy.backendAddr, err)
			}
			break
		}

		fromKey := newConnTrackKey(from)
		proxy.connTrackLock.Lock()
		if proxy.closed {
			proxy.connTrackLock.Unlock()
			return
		}
		entry, hit := proxy.connTrackTable[*fromKey]
		if !hit {
			proxyConn, err := net.DialUDP("udp", nil, proxy.backendAddr)
			if err != nil {
				proxy.Logger.Printf("Can't proxy a datagram to udp/%s: %s\n", proxy.backendAddr, err)
				proxy.connTrackLock.Unlock()
				continue
			}
			proxy.nextGen++
			entry = &connTrackEntry{conn: proxyConn, gen: proxy.nextGen}
			proxy.connTrackTable[*fromKey] = entry
			go proxy.replyLoop(entry, from, fromKey)
		}
		proxy.connTrackLock.Unlock()
		for i := 0; i != read; {
			written, err := entry.conn.Write(readBuf[i:read])
			if err != nil {
				proxy.Logger.Printf("Can't proxy a datagram to udp/%s: %s\n", proxy.backendAddr, err)
				break
			}
			i += written
		}
	}
}

// Close stops forwarding the traffic. It is safe to call multiple times and
// concurrently with the connection-tracking loops: every tracked connection
// is closed exactly once, either by Close or by the replyLoop that owns it,
// whichever removes it from the tracking table first.
func (proxy *UDPProxy) Close() {
	_ = proxy.listener.Close()
	proxy.connTrackLock.Lock()
	if proxy.closed {
		proxy.connTrackLock.Unlock()
		return
	}
	proxy.closed = true
	table := proxy.connTrackTable
	proxy.connTrackTable = make(connTrackMap)
	proxy.connTrackLock.Unlock()
	for _, entry := range table {
		_ = entry.conn.Close()
	}
}

// FrontendAddr returns the UDP address on which the proxy is listening.
func (proxy *UDPProxy) FrontendAddr() net.Addr { return proxy.frontendAddr }

// BackendAddr returns the proxied UDP address.
func (proxy *UDPProxy) BackendAddr() net.Addr { return proxy.backendAddr }
