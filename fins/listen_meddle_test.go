package fins

// Tests of the Meddle fork (CH-B01) that need the client's own socket: an
// ICMP port unreachable that reaches the listener while no request waits,
// one the socket hands to a request's write instead of sending it, one the
// listener reads while a request has taken its service ID but not yet
// written its datagram, and what the error says in each case. Each must
// leave the client sending and answered. One more checks, without an ICMP
// error, that the listener's count of write attempts decides which requests
// an error fails and that a request writing again drops what it was handed.
// The tests of WB2 (TestMeddleReadClockIn…, TestMeddleWrite…,
// TestMeddleEndCode…) check the clock's decoding, the frames the writes send
// and the end code's flags against a scripted PLC. They use the loopback
// only.

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
	// The zone data the clock tests need, whatever the system has.
	_ "time/tzdata"
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
	header, ch, headerErr := client.nextHeader(CommandCodeMemoryAreaRead)
	if headerErr != nil {
		t.Fatalf("nextHeader: %v", headerErr)
	}
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
	// on the second one: nothing goes out. The write methods refuse it
	// before the socket (WriteTooLargeError), so the command goes to
	// sendCommand directly.
	oversize := writeCommand(memAddr(MemoryAreaDMWord, meddleAddress), meddleOversize/2, make([]byte, meddleOversize))
	_, err = client.sendCommand(oversize)
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
	headerA, chA, err := client.nextHeader(CommandCodeMemoryAreaRead)
	if err != nil {
		t.Fatalf("A's nextHeader: %v", err)
	}
	defer client.release(headerA.serviceID, chA)
	if err := client.write(headerA.serviceID, chA, datagram(headerA)); err != nil {
		t.Fatalf("A's write: %v", err)
	}
	count := client.attempts.Load()

	// Request B writes its datagram after the count was read.
	headerB, chB, err := client.nextHeader(CommandCodeMemoryAreaRead)
	if err != nil {
		t.Fatalf("B's nextHeader: %v", err)
	}
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

// meddleScripted is a FINS/UDP PLC on the loopback that answers every
// request with the end code and data answer returns for it, and keeps every
// request it got.
type meddleScripted struct {
	addr     *net.UDPAddr
	mu       sync.Mutex
	requests []request
}

func newMeddleScripted(t *testing.T, answer func(req request) (endCode uint16, data []byte)) *meddleScripted {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &meddleScripted{addr: conn.LocalAddr().(*net.UDPAddr)}
	served := make(chan struct{})
	go func() {
		defer close(served)
		buf := make([]byte, readBufferLength)
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			// The header and the command code.
			if n < 12 {
				continue
			}
			req := decodeRequest(bytes.Clone(buf[:n]))
			s.mu.Lock()
			s.requests = append(s.requests, req)
			s.mu.Unlock()
			endCode, data := answer(req)
			resp := response{defaultResponseHeader(req.header), req.commandCode, endCode, data}
			_, _ = conn.WriteToUDP(encodeResponse(resp), from)
		}
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		<-served
	})
	return s
}

// take returns the requests got so far and forgets them.
func (s *meddleScripted) take() []request {
	s.mu.Lock()
	defer s.mu.Unlock()
	got := s.requests
	s.requests = nil
	return got
}

// meddleWriteFrame is what a memory area write request carries after its
// command code: its item count and its data.
func meddleWriteFrame(t *testing.T, req request) (count int, data []byte) {
	t.Helper()
	if req.commandCode != CommandCodeMemoryAreaWrite {
		t.Fatalf("command code 0x%04x, want a memory area write", req.commandCode)
	}
	if len(req.data) < 6 {
		t.Fatalf("a write of %d bytes after its command code, want at least 6", len(req.data))
	}
	return int(binary.BigEndian.Uint16(req.data[4:6])), req.data[6:]
}

// meddleAcceptWrites answers every request with a normal completion.
func meddleAcceptWrites(request) (uint16, []byte) {
	return EndCodeNormalCompletion, nil
}

// meddleClock answers a clock read with clock (year, month, day, hour,
// minute, second, day of the week, as BCD).
func meddleClock(clock *[]byte, mu *sync.Mutex) func(request) (uint16, []byte) {
	return func(req request) (uint16, []byte) {
		mu.Lock()
		defer mu.Unlock()
		if req.commandCode != CommandCodeClockRead {
			return EndCodeNormalCompletion, nil
		}
		return EndCodeNormalCompletion, bytes.Clone(*clock)
	}
}

// Kills the mutant that checks the day of the month on the time built in loc
// (t.Day() != day): on the day a zone skips midnight, time.Date moves a
// valid reading into the day before, and the reading was refused.
func TestMeddleReadClockIn_DayIsCheckedOnTheCalendar(t *testing.T) {
	santiago, err := time.LoadLocation("America/Santiago")
	if err != nil {
		t.Fatalf("load America/Santiago: %v", err)
	}
	// 8 September 2024 is the day Chile's daylight saving time starts, at
	// midnight: 00:30 does not exist there.
	moved := time.Date(2024, time.September, 8, 0, 30, 0, 0, santiago)
	if moved.Day() == 8 {
		t.Fatalf("the zone data has no gap at midnight on 2024-09-08 (%v): the test proves nothing", moved)
	}

	var mu sync.Mutex
	clock := []byte{0x24, 0x09, 0x08, 0x00, 0x30, 0x00, 0x00}
	plc := newMeddleScripted(t, meddleClock(&clock, &mu))
	client := newMeddleClient(t, plc.addr)

	got, err := client.ReadClockIn(santiago)
	if err != nil {
		t.Fatalf("a reading in Santiago's gap: %v", err)
	}
	if !got.Equal(moved) || got.Location() != santiago {
		t.Fatalf("read %v, want %v, the time time.Date gives", got, moved)
	}
	got, err = client.ReadClockIn(nil)
	if err != nil {
		t.Fatalf("the same reading in UTC: %v", err)
	}
	if want := time.Date(2024, time.September, 8, 0, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("read %v in UTC, want %v", got, want)
	}

	// The days a month has, on the calendar, leap years included.
	for _, tc := range []struct {
		clock   []byte
		badDay  int
		wantDay int
	}{
		{[]byte{0x26, 0x02, 0x30, 0x12, 0x00, 0x00, 0x00}, 30, 0},
		{[]byte{0x26, 0x02, 0x29, 0x12, 0x00, 0x00, 0x00}, 29, 0},
		{[]byte{0x26, 0x04, 0x31, 0x12, 0x00, 0x00, 0x00}, 31, 0},
		{[]byte{0x24, 0x02, 0x29, 0x12, 0x00, 0x00, 0x00}, 0, 29},
		{[]byte{0x26, 0x12, 0x31, 0x23, 0x59, 0x59, 0x00}, 0, 31},
	} {
		mu.Lock()
		clock = tc.clock
		mu.Unlock()
		got, err := client.ReadClockIn(time.UTC)
		if tc.badDay != 0 {
			var bad ClockValueError
			if !errors.As(err, &bad) || bad != (ClockValueError{Field: "day", Value: tc.badDay}) {
				t.Fatalf("clock % x: read %v, %v, want ClockValueError{day %d}", tc.clock, got, err, tc.badDay)
			}
			continue
		}
		if err != nil || got.Day() != tc.wantDay {
			t.Fatalf("clock % x: read %v, %v, want day %d", tc.clock, got, err, tc.wantDay)
		}
	}
}

// Kills the mutant that decodes each clock byte with decodeBCD, which reads a
// low nibble of 0xF in the last byte as an end marker: minute 0x5F was 5.
func TestMeddleReadClockIn_RejectsANibbleAboveNine(t *testing.T) {
	var mu sync.Mutex
	var clock []byte
	plc := newMeddleScripted(t, meddleClock(&clock, &mu))
	client := newMeddleClient(t, plc.addr)

	for _, tc := range []struct {
		clock []byte
		want  BCDBadDigitError
	}{
		{[]byte{0x26, 0x11, 0x21, 0x10, 0x5f, 0x30, 0x06}, BCDBadDigitError{"lo", 15}},
		{[]byte{0x26, 0x1f, 0x21, 0x10, 0x30, 0x30, 0x06}, BCDBadDigitError{"lo", 15}},
		{[]byte{0x2f, 0x11, 0x21, 0x10, 0x30, 0x30, 0x06}, BCDBadDigitError{"lo", 15}},
		{[]byte{0x26, 0x11, 0x21, 0x10, 0x30, 0x5a, 0x06}, BCDBadDigitError{"lo", 10}},
		{[]byte{0x26, 0x11, 0xa1, 0x10, 0x30, 0x30, 0x06}, BCDBadDigitError{"hi", 10}},
	} {
		mu.Lock()
		clock = tc.clock
		mu.Unlock()
		got, err := client.ReadClockIn(time.UTC)
		var bad BCDBadDigitError
		if !errors.As(err, &bad) || bad != tc.want || got != nil {
			t.Fatalf("clock % x: read %v, %v, want %v", tc.clock, got, err, tc.want)
		}
	}

	mu.Lock()
	clock = []byte{0x26, 0x11, 0x21, 0x10, 0x59, 0x30, 0x06}
	mu.Unlock()
	got, err := client.ReadClockIn(time.UTC)
	if want := time.Date(2026, time.November, 21, 10, 59, 30, 0, time.UTC); err != nil || !got.Equal(want) {
		t.Fatalf("read %v, %v, want %v", got, err, want)
	}
}

// Kills the mutant that sends 2 × len(s) bytes for (len(s)+1)/2 words, which
// a PLC refuses (0x1003): the frame carries exactly the words it declares,
// an odd length padded with 0x00.
func TestMeddleWriteString_SendsTheWordsItDeclares(t *testing.T) {
	plc := newMeddleScripted(t, meddleAcceptWrites)
	client := newMeddleClient(t, plc.addr)

	for _, area := range []byte{MemoryAreaCIOWord, MemoryAreaDMWord} {
		for _, tc := range []struct {
			s     string
			count int
			data  []byte
		}{
			{"A", 1, []byte("A\x00")},
			{"AB", 1, []byte("AB")},
			{"ABC", 2, []byte("ABC\x00")},
			{"HELLO!", 3, []byte("HELLO!")},
		} {
			if err := client.WriteString(area, meddleAddress, tc.s); err != nil {
				t.Fatalf("area 0x%02x, %q: %v", area, tc.s, err)
			}
			reqs := plc.take()
			if len(reqs) != 1 {
				t.Fatalf("area 0x%02x, %q: the PLC got %d requests, want 1", area, tc.s, len(reqs))
			}
			if reqs[0].data[0] != area {
				t.Fatalf("%q written to area 0x%02x, want 0x%02x", tc.s, reqs[0].data[0], area)
			}
			count, data := meddleWriteFrame(t, reqs[0])
			if count != tc.count || !bytes.Equal(data, tc.data) || len(data) != 2*count {
				t.Fatalf("area 0x%02x, %q: sent %d words carrying % x, want %d carrying % x",
					area, tc.s, count, data, tc.count, tc.data)
			}
		}
	}
}

// Kills the mutant that declares len(b) words for len(b) bytes: the frame
// declares (len(b)+1)/2 words and carries them, an odd length padded with
// 0x00, and the caller's slice is left as it was.
func TestMeddleWriteBytes_DeclaresWordsAndPadsAnOddLength(t *testing.T) {
	plc := newMeddleScripted(t, meddleAcceptWrites)
	client := newMeddleClient(t, plc.addr)

	for _, tc := range []struct {
		b     []byte
		count int
		data  []byte
	}{
		{[]byte{0x11}, 1, []byte{0x11, 0x00}},
		{[]byte{0x11, 0x22, 0x33}, 2, []byte{0x11, 0x22, 0x33, 0x00}},
		{[]byte{0x01, 0x02, 0x03, 0x04}, 2, []byte{0x01, 0x02, 0x03, 0x04}},
	} {
		// Room after the bytes: padding in place would show there.
		b := append(make([]byte, 0, len(tc.b)+4), tc.b...)
		spare := b[:cap(b)]
		for i := len(b); i < len(spare); i++ {
			spare[i] = 0xEE
		}
		before := bytes.Clone(spare)
		if err := client.WriteBytes(MemoryAreaCIOWord, meddleAddress, b); err != nil {
			t.Fatalf("% x: %v", tc.b, err)
		}
		reqs := plc.take()
		if len(reqs) != 1 {
			t.Fatalf("% x: the PLC got %d requests, want 1", tc.b, len(reqs))
		}
		count, data := meddleWriteFrame(t, reqs[0])
		if count != tc.count || !bytes.Equal(data, tc.data) {
			t.Fatalf("% x: sent %d words carrying % x, want %d carrying % x", tc.b, count, data, tc.count, tc.data)
		}
		if !bytes.Equal(spare, before) {
			t.Fatalf("% x: the caller's backing array changed to % x", tc.b, spare)
		}
	}
}

// meddleNoPanic calls write and returns its error, failing the test if it
// panics.
func meddleNoPanic(t *testing.T, name string, write func() error) (err error) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("%s panicked: %v", name, p)
		}
	}()
	return write()
}

// Kills the mutants that size a write in uint16 (32,768 words panicked,
// 65,536 wrapped to an empty write, a count above 65,535 was truncated) and
// the mutant without the check: a write longer than one FINS frame returns a
// WriteTooLargeError and sends nothing, and the longest that fits is sent.
func TestMeddleWrite_RefusesMoreThanOneFrame(t *testing.T) {
	plc := newMeddleScripted(t, meddleAcceptWrites)
	client := newMeddleClient(t, plc.addr)

	tooLarge := []struct {
		name  string
		bytes int
		write func() error
	}{
		{"WriteWords 998", 1996, func() error {
			return client.WriteWords(MemoryAreaDMWord, meddleAddress, make([]uint16, 998))
		}},
		{"WriteWords 32768", 65536, func() error {
			return client.WriteWords(MemoryAreaDMWord, meddleAddress, make([]uint16, 32768))
		}},
		{"WriteWords 65536", 131072, func() error {
			return client.WriteWords(MemoryAreaCIOWord, meddleAddress, make([]uint16, 65536))
		}},
		{"WriteBytes 1995", 1996, func() error {
			return client.WriteBytes(MemoryAreaDMWord, meddleAddress, make([]byte, 1995))
		}},
		{"WriteBytes 70000", 70000, func() error {
			return client.WriteBytes(MemoryAreaDMWord, meddleAddress, make([]byte, 70000))
		}},
		{"WriteString 1995", 1996, func() error {
			return client.WriteString(MemoryAreaDMWord, meddleAddress, string(make([]byte, 1995)))
		}},
		{"WriteBits 1995", 1995, func() error {
			return client.WriteBits(MemoryAreaDMBit, meddleAddress, 0, make([]bool, 1995))
		}},
		{"WriteBits 65536", 65536, func() error {
			return client.WriteBits(MemoryAreaCIOBit, meddleAddress, 0, make([]bool, 65536))
		}},
	}
	for _, tc := range tooLarge {
		err := meddleNoPanic(t, tc.name, tc.write)
		var large WriteTooLargeError
		if !errors.As(err, &large) || large != (WriteTooLargeError{Bytes: tc.bytes, Max: maxWriteDataLength}) {
			t.Fatalf("%s: %v, want WriteTooLargeError{%d, %d}", tc.name, err, tc.bytes, maxWriteDataLength)
		}
	}
	if reqs := plc.take(); len(reqs) != 0 {
		t.Fatalf("the writes too large sent %d requests, want none", len(reqs))
	}

	fits := []struct {
		name  string
		count int
		write func() error
	}{
		{"WriteWords 997", 997, func() error {
			return client.WriteWords(MemoryAreaDMWord, meddleAddress, make([]uint16, 997))
		}},
		{"WriteBytes 1993", 997, func() error {
			return client.WriteBytes(MemoryAreaDMWord, meddleAddress, make([]byte, 1993))
		}},
		{"WriteString 1994", 997, func() error {
			return client.WriteString(MemoryAreaDMWord, meddleAddress, string(make([]byte, 1994)))
		}},
		{"WriteBits 1994", 1994, func() error {
			return client.WriteBits(MemoryAreaDMBit, meddleAddress, 0, make([]bool, 1994))
		}},
	}
	for _, tc := range fits {
		if err := meddleNoPanic(t, tc.name, tc.write); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		reqs := plc.take()
		if len(reqs) != 1 {
			t.Fatalf("%s: the PLC got %d requests, want 1", tc.name, len(reqs))
		}
		count, data := meddleWriteFrame(t, reqs[0])
		if count != tc.count || len(data) != maxWriteDataLength {
			t.Fatalf("%s: sent %d items carrying %d bytes, want %d carrying %d",
				tc.name, count, len(data), tc.count, maxWriteDataLength)
		}
	}
}

// Kills the mutant whose MainSub returns Code as received: with a flag set,
// the end code compares with its EndCode* constant through MainSub only.
func TestMeddleEndCodeError_MainSubDropsTheFlags(t *testing.T) {
	var endCode atomic.Uint32
	plc := newMeddleScripted(t, func(req request) (uint16, []byte) {
		return uint16(endCode.Load()), []byte{0, meddleWord}
	})
	client := newMeddleClient(t, plc.addr)

	for _, tc := range []struct {
		code, mainSub uint16
	}{
		{EndCodeAddressRangeExceeded | 0x8000, EndCodeAddressRangeExceeded},
		{EndCodeAddressRangeExceeded | 0x0040, EndCodeAddressRangeExceeded},
		{EndCodeAddressRangeExceeded | 0x0080, EndCodeAddressRangeExceeded},
		{0x8000 | 0x0080 | 0x0040, EndCodeNormalCompletion},
		{EndCodeAddressRangeError, EndCodeAddressRangeError},
	} {
		endCode.Store(uint32(tc.code))
		_, err := client.ReadWords(MemoryAreaDMWord, meddleAddress, 1)
		var ec EndCodeError
		if !errors.As(err, &ec) {
			t.Fatalf("end code 0x%04x: %v, want an EndCodeError", tc.code, err)
		}
		if ec.Code != tc.code || ec.MainSub() != tc.mainSub {
			t.Fatalf("end code 0x%04x: Code 0x%04x, MainSub 0x%04x, want 0x%04x and 0x%04x",
				tc.code, ec.Code, ec.MainSub(), tc.code, tc.mainSub)
		}
	}

	// The non-fatal CPU error flag alone is still a completion.
	endCode.Store(uint32(endCodeNonFatalCPUError))
	words, err := client.ReadWords(MemoryAreaDMWord, meddleAddress, 1)
	if err != nil || !slices.Equal(words, []uint16{meddleWord}) {
		t.Fatalf("end code 0x0040: read %v, %v, want [%d]", words, err, meddleWord)
	}
}
