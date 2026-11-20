package fins

// Tests of the Meddle fork (CH-B01) that need the client's own socket: an
// ICMP port unreachable that reaches the listener while no request waits,
// one the socket hands to a request's write instead of sending it, one the
// listener reads while a request has taken its service ID but not yet
// written its datagram, and what the error says in each case. Each must
// leave the client sending and answered. One more checks, without an ICMP
// error, that the listener's count of write attempts decides which requests
// an error fails and that a request writing again drops what it was handed.
// They use the loopback only.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const (
	// meddleICMPGap separates two datagrams sent to a closed port (macOS
	// reports no second ICMP port unreachable within about 100 ms of the
	// first) and leaves the system time to report one.
	meddleICMPGap = 150 * time.Millisecond

	// meddleTimeoutMs is the clients' response timeout.
	meddleTimeoutMs = 1000

	// meddleWord is what the responder reads back in every word.
	meddleWord = 42

	// meddleAddress is the DM word the tests read.
	meddleAddress = 100

	// meddleOversize is the data of a write too large for the system to send
	// as one UDP datagram (more than 65,507 bytes on Linux, 9,216 on macOS).
	meddleOversize = 1 << 16
)

// meddleResponder is a FINS/UDP PLC on the loopback: it answers every memory
// area read with meddleWord in each word asked for and counts the reads. Its
// port can be closed and opened again, the same one.
type meddleResponder struct {
	t        *testing.T
	addr     *net.UDPAddr
	conn     *net.UDPConn
	served   chan struct{}
	requests atomic.Int32
}

func newMeddleResponder(t *testing.T) *meddleResponder {
	t.Helper()
	r := &meddleResponder{t: t}
	r.listen(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	r.addr = r.conn.LocalAddr().(*net.UDPAddr)
	t.Cleanup(r.stop)
	return r
}

func (r *meddleResponder) listen(addr *net.UDPAddr) {
	r.t.Helper()
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		r.t.Fatalf("listen on %s: %v", addr, err)
	}
	r.conn = conn
	r.served = make(chan struct{})
	go r.serve(conn, r.served)
}

// stop closes the port: the system answers what reaches it with an ICMP
// port unreachable. It may be called more than once.
func (r *meddleResponder) stop() {
	if r.conn == nil {
		return
	}
	_ = r.conn.Close()
	<-r.served
	r.conn = nil
}

// restart opens the same port again.
func (r *meddleResponder) restart() {
	r.t.Helper()
	r.listen(r.addr)
}

func (r *meddleResponder) serve(conn *net.UDPConn, served chan struct{}) {
	defer close(served)
	buf := make([]byte, readBufferLength)
	for {
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		// The header, the command code, the memory address and the count.
		if n < 18 {
			continue
		}
		req := decodeRequest(bytes.Clone(buf[:n]))
		if req.commandCode != CommandCodeMemoryAreaRead {
			continue
		}
		r.requests.Add(1)
		count := int(binary.BigEndian.Uint16(req.data[4:6]))
		data := make([]byte, 2*count)
		for i := range count {
			binary.BigEndian.PutUint16(data[2*i:], meddleWord)
		}
		answer := response{defaultResponseHeader(req.header), req.commandCode, EndCodeNormalCompletion, data}
		_, _ = conn.WriteToUDP(encodeResponse(answer), from)
	}
}

// skipWithoutICMPReports skips where Go turns off the report of an ICMP port
// unreachable on UDP sockets.
func skipWithoutICMPReports(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Go turns off the report of an ICMP port unreachable on Windows UDP sockets")
	}
}

// requireRefused sends a datagram to plc, a closed port, on a socket of its
// own, and fails the test unless the system reports its ICMP port
// unreachable: to the next write when onWrite, else to a read. It shows
// that the system does what the test then counts on.
func requireRefused(t *testing.T, plc *net.UDPAddr, onWrite bool) {
	t.Helper()
	conn, err := net.DialUDP("udp", nil, plc)
	if err != nil {
		t.Fatalf("dial %s: %v", plc, err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{0}); err != nil {
		t.Fatalf("probe write: %v", err)
	}
	if onWrite {
		time.Sleep(meddleICMPGap)
		_, err = conn.Write([]byte{0})
	} else {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		_, err = conn.Read(make([]byte, 1))
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("the system did not report the closed port's ICMP port unreachable (on write: %t): %v", onWrite, err)
	}
	// Leave the next datagram to a closed port its own ICMP report.
	time.Sleep(meddleICMPGap)
}

// staleRequest is a read request whose request has stopped waiting: the
// datagram a timed-out read left on the wire.
func staleRequest() []byte {
	header := defaultCommandHeader(finsAddress{0, 2, 0}, finsAddress{0, 10, 0}, 0)
	return append(encodeHeader(header), readCommand(memAddr(MemoryAreaDMWord, meddleAddress), 1)...)
}

// meddleClientAddresses are the client's and the PLC's addresses for a PLC
// on plc.
func meddleClientAddresses(plc *net.UDPAddr) (local, remote Address) {
	return NewAddress("127.0.0.1", 0, 0, 2, 0), NewAddress(plc.IP.String(), plc.Port, 0, 10, 0)
}

// newMeddleClient opens a client, listener started, to the PLC on plc; the
// cleanup closes it.
func newMeddleClient(t *testing.T, plc *net.UDPAddr) *Client {
	t.Helper()
	client, err := NewClient(meddleClientAddresses(plc))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.SetTimeoutMs(meddleTimeoutMs)
	t.Cleanup(client.Close)
	return client
}

type readResult struct {
	words []uint16
	err   error
}

func TestMeddleListen_RefusedWithNobodyWaitingKeepsTheClient(t *testing.T) {
	skipWithoutICMPReports(t)
	plc := newMeddleResponder(t)
	plc.stop()
	requireRefused(t, plc.addr, false)

	client := newMeddleClient(t, plc.addr)

	// No request waits: the ICMP port unreachable of this datagram reaches
	// the listener, which must not close the client because of it.
	if _, err := client.conn.Write(staleRequest()); err != nil {
		t.Fatalf("raw write: %v", err)
	}
	time.Sleep(meddleICMPGap)

	// The PLC answers again on the same port: the next read is sent and
	// answered on the same client.
	plc.restart()
	words, err := client.ReadWords(MemoryAreaDMWord, meddleAddress, 1)
	if err != nil {
		t.Fatalf("the read after a refusal with nobody waiting: %v", err)
	}
	if !slices.Equal(words, []uint16{meddleWord}) {
		t.Fatalf("read %v, want [%d]", words, meddleWord)
	}
	if got := plc.requests.Load(); got != 1 {
		t.Fatalf("the PLC got %d reads, want 1", got)
	}
}

func TestMeddleListen_RefusedOnAWriteIsWrittenAgain(t *testing.T) {
	skipWithoutICMPReports(t)
	plc := newMeddleResponder(t)
	plc.stop()
	requireRefused(t, plc.addr, true)

	client, startListener := newClientNotListening(t, plc.addr)

	// Nobody reads the socket: the ICMP port unreachable of this datagram
	// stays pending until a write takes it, without sending its datagram.
	if _, err := client.conn.Write(staleRequest()); err != nil {
		t.Fatalf("raw write: %v", err)
	}
	time.Sleep(meddleICMPGap)

	plc.restart()
	result := make(chan readResult, 1)
	go func() {
		words, err := client.ReadWords(MemoryAreaDMWord, meddleAddress, 1)
		result <- readResult{words, err}
	}()

	// The listener starts only once the read has reached the PLC, so the
	// pending error can only have gone to the read's write.
	deadline := time.Now().Add(2 * time.Second)
	for plc.requests.Load() == 0 {
		select {
		case r := <-result:
			t.Fatalf("the read returned without being sent: %v", r.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the read did not reach the PLC within 2 s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	startListener()

	select {
	case r := <-result:
		if r.err != nil {
			t.Fatalf("the read written again: %v", r.err)
		}
		if !slices.Equal(r.words, []uint16{meddleWord}) {
			t.Fatalf("read %v, want [%d]", r.words, meddleWord)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the read written again got no answer within 2 s")
	}
	if got := plc.requests.Load(); got != 1 {
		t.Fatalf("the PLC got %d reads, want 1", got)
	}
}

func TestMeddleListen_RefusedBeforeTheWriteSparesTheRequest(t *testing.T) {
	skipWithoutICMPReports(t)
	plc := newMeddleResponder(t)
	plc.stop()
	requireRefused(t, plc.addr, false)
	client := newMeddleClient(t, plc.addr)

	// The request takes its service ID and its slot, as every request does
	// before it writes its datagram, and does not write it yet.
	header, ch := client.nextHeader()
	defer client.release(header.serviceID, ch)

	// Meanwhile a read goes to the closed port and waits: its ICMP port
	// unreachable reaches the listener, which hands it to the requests it
	// fails in the order of their service IDs. The read took the service ID
	// after the request's, so once the read has the error the listener has
	// passed the request by too.
	_, err := client.ReadWords(MemoryAreaDMWord, meddleAddress, 1)
	var unreachable DestinationUnreachableError
	if !errors.As(err, &unreachable) {
		t.Fatalf("the read on the closed port: %v, want a DestinationUnreachableError", err)
	}

	// Only now does the request write its datagram, to the PLC answering
	// again on the same port. The error was read before the datagram was
	// written, so it is not the request's: the request gets the PLC's answer.
	plc.restart()
	datagram := append(encodeHeader(*header), readCommand(memAddr(MemoryAreaDMWord, meddleAddress), 1)...)
	if _, err := client.conn.Write(datagram); err != nil {
		t.Fatalf("the request's write: %v", err)
	}
	select {
	case r := <-ch:
		answer, err := r.result()
		if err != nil {
			t.Fatalf("the request got the error of a datagram written before its own: %v", err)
		}
		if want := []byte{0, meddleWord}; !bytes.Equal(answer.data, want) {
			t.Fatalf("the request's answer carries %v, want %v", answer.data, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the request got no answer within 2 s")
	}
	if got := plc.requests.Load(); got != 1 {
		t.Fatalf("the PLC got %d reads, want 1", got)
	}
}

func TestMeddleListen_UnreachableSaysWhetherTheRequestWasSent(t *testing.T) {
	skipWithoutICMPReports(t)
	plc := newMeddleResponder(t)
	plc.stop()
	requireRefused(t, plc.addr, false)
	client := newMeddleClient(t, plc.addr)

	// The read's datagram goes out to the closed port; the listener reads
	// its ICMP port unreachable and hands it to the waiting read.
	_, err := client.ReadWords(MemoryAreaDMWord, meddleAddress, 1)
	var unreachable DestinationUnreachableError
	if !errors.As(err, &unreachable) || !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("the read on the closed port: %v, want a DestinationUnreachableError for ECONNREFUSED", err)
	}
	if !unreachable.Sent {
		t.Fatalf("the error the listener handed over says the read was not sent: %v", err)
	}

	// The system refuses to send a datagram this large, on the write and
	// on the second one: nothing goes out.
	err = client.WriteBytes(MemoryAreaDMWord, meddleAddress, make([]byte, meddleOversize))
	unreachable = DestinationUnreachableError{}
	if !errors.As(err, &unreachable) || !errors.Is(err, syscall.EMSGSIZE) {
		t.Fatalf("the oversize write: %v, want a DestinationUnreachableError for EMSGSIZE", err)
	}
	if unreachable.Sent {
		t.Fatalf("the error the write got says the write was sent: %v", err)
	}

	// The client stays open: the PLC answers again and the next read is
	// sent and answered.
	plc.restart()
	words, err := client.ReadWords(MemoryAreaDMWord, meddleAddress, 1)
	if err != nil {
		t.Fatalf("the read after both errors: %v", err)
	}
	if !slices.Equal(words, []uint16{meddleWord}) {
		t.Fatalf("read %v, want [%d]", words, meddleWord)
	}
	if got := plc.requests.Load(); got != 1 {
		t.Fatalf("the PLC got %d reads, want 1", got)
	}
}

func TestMeddleListen_UnreachableFailsOnlyTheAttemptsCountedAndAWriteDropsIt(t *testing.T) {
	// A PLC that takes every datagram and answers none: writes go through and
	// nothing comes back, so only failWaiting fills a channel.
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	// No listener: the test hands the error over itself.
	client, _ := newClientNotListening(t, sink.LocalAddr().(*net.UDPAddr))

	datagram := func(header *Header) []byte {
		return append(encodeHeader(*header), readCommand(memAddr(MemoryAreaDMWord, meddleAddress), 1)...)
	}

	// Request A writes its datagram; the listener's count, read now, covers
	// A's attempt.
	headerA, chA := client.nextHeader()
	defer client.release(headerA.serviceID, chA)
	if err := client.write(headerA.serviceID, chA, datagram(headerA)); err != nil {
		t.Fatalf("A's write: %v", err)
	}
	count := client.attempts.Load()

	// Request B writes its datagram after the count was read.
	headerB, chB := client.nextHeader()
	defer client.release(headerB.serviceID, chB)
	if err := client.write(headerB.serviceID, chB, datagram(headerB)); err != nil {
		t.Fatalf("B's write: %v", err)
	}

	// The error read with that count fails A, whose attempt it covers, and
	// not B, whose attempt came after it.
	client.failWaiting(DestinationUnreachableError{Cause: syscall.ECONNREFUSED, Sent: true}, count)
	select {
	case r := <-chA:
		var unreachable DestinationUnreachableError
		if !errors.As(r.err, &unreachable) || !unreachable.Sent || !errors.Is(r.err, syscall.ECONNREFUSED) {
			t.Fatalf("A got %v, want the DestinationUnreachableError handed over", r.err)
		}
		// Left in A's channel for the write below to drop.
		chA <- r
	default:
		t.Fatal("A, whose attempt the count covers, was not handed the error")
	}
	if n := len(chB); n != 0 {
		t.Fatalf("B, whose attempt came after the count, holds %d replies, want 0", n)
	}

	// A writes again: numbering the new attempt drops what the earlier one was
	// handed.
	if err := client.write(headerA.serviceID, chA, datagram(headerA)); err != nil {
		t.Fatalf("A's second write: %v", err)
	}
	if n := len(chA); n != 0 {
		t.Fatalf("A holds %d replies after writing again, want 0", n)
	}
}

// newClientNotListening builds the client NewClient builds without starting
// its listener, so a socket error stays pending until a write takes it.
// start starts the listener, once; the cleanup starts it if the test did
// not, and closes the client.
func newClientNotListening(t *testing.T, plc *net.UDPAddr) (client *Client, start func()) {
	t.Helper()
	client, err := newClient(meddleClientAddresses(plc))
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	client.SetTimeoutMs(meddleTimeoutMs)
	var once sync.Once
	start = func() { once.Do(func() { go client.listenLoop() }) }
	t.Cleanup(func() {
		start()
		client.Close()
	})
	return client, start
}
