package fins

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const DEFAULT_RESPONSE_TIMEOUT = 20 // ms

const (
	// minResponseLength is the shortest FINS response: a 10-byte header, a
	// 2-byte command code and a 2-byte end code. A shorter datagram is dropped.
	minResponseLength = 14

	// readBufferLength is the read buffer of the listener: 64 KiB, more than
	// any UDP datagram carries, so a datagram is never truncated and a large
	// one is never a read error (WSAEMSGSIZE on Windows).
	readBufferLength = 64 << 10

	// clockDataLength is the data ReadClock decodes: year, month, day, hour,
	// minute, second, as BCD.
	clockDataLength = 6

	// endCodeNonFatalCPUError is the flag of the end code that reports a
	// non-fatal error of the PLC's CPU (W342): the command itself completed,
	// so a response that carries it is not an error. The fatal CPU error flag
	// (0x0080) and the relay error flag (0x8000) still are.
	endCodeNonFatalCPUError uint16 = 0x0040

	// maxFrameLength is the longest FINS frame (W342): a 10-byte header, a
	// 2-byte command code and at most 2,000 bytes of text.
	maxFrameLength = 2012

	// maxWriteDataLength is the most data one memory area write carries: the
	// frame less its header and the 8 bytes of command code, memory address
	// and item count. A longer write returns WriteTooLargeError unsent.
	maxWriteDataLength = maxFrameLength - 10 - 8
)

// Client Omron FINS client
//
// A late, duplicated or malformed datagram is dropped, and so is a datagram
// that is not a response (its ICF has the response bit clear) or a response
// whose command code is not that of the request waiting for its service ID.
// The nodes of a response are not checked: the socket is connected to the
// PLC, so it only reads what that address and port send.
//
// A service ID is never taken while a request still waits on it: with 256
// requests waiting, the next one returns TooManyPendingRequestsError without
// sending anything.
//
// SetTimeoutMs and SetByteOrder may be called while requests run: a request
// keeps the timeout it took with its service ID.
//
// An ICMP error that the socket reports (ECONNREFUSED for a FINS port closed
// on a reachable host, EHOSTUNREACH, ENETUNREACH, EHOSTDOWN, EMSGSIZE for a
// fragmentation needed, EPROTO and ENOPROTOOPT for a parameter problem or a
// protocol unreachable, EACCES for an ICMPv6 administratively prohibited or
// reject route on Linux) never closes the client, on a read or on a write. It
// can arrive after the request that caused it has timed out. A request that
// gets one returns a DestinationUnreachableError, whose Sent field says
// whether its datagram went out:
//
//   - Sent true: the listener read the error after the request had started
//     writing its datagram, and the write went through. The error may be
//     another datagram's, so the datagram may have reached the PLC and been
//     carried out: the request's outcome is unknown. Every request in that
//     state gets the error; a request that had not started writing its
//     datagram when the error was read does not, and keeps waiting for its
//     answer. The client keeps reading.
//   - Sent false: the request's own write got the error and nothing went out.
//     The error is an earlier datagram's, which the socket hands to a write
//     instead of sending it, or the system refused to send (no route, a
//     datagram too large), so the request writes its datagram once more and
//     returns the error only if that write fails too.
//
// Either way the next request is sent as usual.
//
// The client ends only when Close is called or when a read of its socket
// fails with any other error: from then on every call returns a
// ClientClosedError carrying the cause, and the caller opens a new client.
// A write error of any other kind is returned as it is and leaves the client
// open. Nothing in the client logs or ends the process.
type Client struct {
	conn *net.UDPConn
	// resp holds, per service ID, the request waiting for that answer;
	// written and read under mu.
	resp []waiter
	// attempts counts the write attempts of every request: each is numbered
	// under mu just before its write, and the listener reads the count,
	// without mu, as soon as a read of the socket fails (see failWaiting).
	attempts atomic.Uint64
	// mu guards sid, resp, failure, responseTimeoutMs, byteOrder and the
	// closing of the client.
	mu  sync.Mutex
	dst finsAddress
	src finsAddress
	sid byte
	// closed is set once, by Close or by the listener when a read fails with
	// an error that is not a destination unreachable.
	closed atomic.Bool
	// done is closed by listenLoop, and only by it, when it returns.
	done chan struct{}
	// failure is the read error that closed the client, nil after Close.
	failure error
	// responseTimeoutMs and byteOrder are written by their setters and read
	// under mu: a request copies the timeout when it takes its service ID.
	responseTimeoutMs time.Duration
	byteOrder         binary.ByteOrder
}

// waiter is the slot of one service ID.
type waiter struct {
	// ch is the channel of the request waiting for that answer (buffered,
	// capacity 1), nil when no request waits.
	ch chan reply
	// attempt is the number (Client.attempts) of the request's latest write
	// attempt, 0 before its first one.
	attempt uint64
	// cmd is the command code of the request: an answer with another command
	// code is not its answer and is dropped.
	cmd uint16
}

// NewClient creates a new Omron FINS client
func NewClient(localAddr, plcAddr Address) (*Client, error) {
	c, err := newClient(localAddr, plcAddr)
	if err != nil {
		return nil, err
	}
	go c.listenLoop()
	return c, nil
}

// newClient builds the client NewClient returns, without starting its
// listener.
func newClient(localAddr, plcAddr Address) (*Client, error) {
	c := new(Client)
	c.dst = plcAddr.finsAddress
	c.src = localAddr.finsAddress
	c.responseTimeoutMs = DEFAULT_RESPONSE_TIMEOUT
	c.byteOrder = binary.BigEndian

	conn, err := net.DialUDP("udp", localAddr.udpAddress, plcAddr.udpAddress)
	if err != nil {
		return nil, err
	}
	c.conn = conn

	c.resp = make([]waiter, 256) //storage for all responses, sid is byte - only 256 values
	c.done = make(chan struct{})
	return c, nil
}

// Set byte order
// Default value: binary.BigEndian
// It may be called while requests run.
func (c *Client) SetByteOrder(o binary.ByteOrder) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byteOrder = o
}

// order is the byte order of the words, read under mu.
func (c *Client) order() binary.ByteOrder {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byteOrder
}

// Set response timeout duration (ms).
// Default value: 20ms.
// A timeout of zero can be used to block indefinitely.
// It may be called while requests run: a request already sent keeps the
// timeout it started with.
func (c *Client) SetTimeoutMs(t uint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.responseTimeoutMs = time.Duration(t)
}

// Close Closes an Omron FINS connection
//
// It may be called more than once and from any goroutine, and returns once
// the client's listener has stopped. A call waiting for an answer at that
// moment returns a ClientClosedError whose cause is net.ErrClosed, unless the
// socket had failed first (the cause is then that failure); an answer the
// client had already received is still returned.
func (c *Client) Close() {
	c.mu.Lock()
	wasClosed := c.closed.Swap(true)
	c.mu.Unlock()
	if !wasClosed {
		_ = c.conn.Close()
	}
	<-c.done
}

// ReadWords Reads words from the PLC data area
func (c *Client) ReadWords(memoryArea byte, address uint16, readCount uint16) ([]uint16, error) {
	if checkIsWordMemoryArea(memoryArea) == false {
		return nil, IncompatibleMemoryAreaError{memoryArea}
	}
	command := readCommand(memAddr(memoryArea, address), readCount)
	r, e := c.sendCommand(command)
	e = checkResponse(r, e)
	if e != nil {
		return nil, e
	}

	if want := 2 * int(readCount); len(r.data) < want {
		return nil, ShortResponseError{Want: want, Got: len(r.data)}
	}
	order := c.order()
	data := make([]uint16, readCount, readCount)
	for i := 0; i < int(readCount); i++ {
		data[i] = order.Uint16(r.data[i*2 : i*2+2])
	}

	return data, nil
}

// ReadBytes Reads bytes from the PLC data area
//
// readCount is a count of words, so it returns 2 × readCount bytes, or a
// ShortResponseError when the response carries fewer.
func (c *Client) ReadBytes(memoryArea byte, address uint16, readCount uint16) ([]byte, error) {
	if checkIsWordMemoryArea(memoryArea) == false {
		return nil, IncompatibleMemoryAreaError{memoryArea}
	}
	command := readCommand(memAddr(memoryArea, address), readCount)
	r, e := c.sendCommand(command)
	e = checkResponse(r, e)
	if e != nil {
		return nil, e
	}

	want := 2 * int(readCount)
	if len(r.data) < want {
		return nil, ShortResponseError{Want: want, Got: len(r.data)}
	}
	return r.data[:want], nil
}

// ReadString Reads a string from the PLC data area
func (c *Client) ReadString(memoryArea byte, address uint16, readCount uint16) (string, error) {
	data, e := c.ReadBytes(memoryArea, address, readCount)
	if e != nil {
		return "", e
	}
	n := bytes.IndexByte(data, 0)
	if n == -1 {
		n = len(data)
	}
	return string(data[:n]), nil
}

// ReadBits Reads bits from the PLC data area
func (c *Client) ReadBits(memoryArea byte, address uint16, bitOffset byte, readCount uint16) ([]bool, error) {
	if checkIsBitMemoryArea(memoryArea) == false {
		return nil, IncompatibleMemoryAreaError{memoryArea}
	}
	command := readCommand(memAddrWithBitOffset(memoryArea, address, bitOffset), readCount)
	r, e := c.sendCommand(command)
	e = checkResponse(r, e)
	if e != nil {
		return nil, e
	}

	if want := int(readCount); len(r.data) < want {
		return nil, ShortResponseError{Want: want, Got: len(r.data)}
	}
	data := make([]bool, readCount, readCount)
	for i := 0; i < int(readCount); i++ {
		data[i] = r.data[i]&0x01 > 0
	}

	return data, nil
}

// ReadClock Reads the PLC clock, as a time in the local time zone
// (time.Local). It is ReadClockIn(time.Local).
func (c *Client) ReadClock() (*time.Time, error) {
	return c.ReadClockIn(time.Local)
}

// ReadClockIn reads the PLC clock as a time in loc; a nil loc means UTC.
//
// A field whose byte is not two BCD digits (a nibble above 9, 0xF included)
// returns a BCDBadDigitError; a month, day, hour, minute or second out of its
// range, or a day the month does not have (30 February), returns a
// ClockValueError that names the field. The day is checked on the calendar,
// not in loc: a local time that loc skips (a daylight saving gap at
// midnight, say) is a valid reading, and the time returned is the one
// time.Date gives for it, which may lie on the day before or after.
func (c *Client) ReadClockIn(loc *time.Location) (*time.Time, error) {
	if loc == nil {
		loc = time.UTC
	}
	r, e := c.sendCommand(clockReadCommand())
	e = checkResponse(r, e)
	if e != nil {
		return nil, e
	}
	if len(r.data) < clockDataLength {
		return nil, ShortResponseError{Want: clockDataLength, Got: len(r.data)}
	}
	var fields [clockDataLength]int
	for i := range fields {
		// Each field is one byte of two digits. decodeBCD is not used: it
		// reads a low nibble of 0xF in the last byte as an end marker, so
		// 0x5F would be 5.
		b := r.data[i]
		hi, lo := b>>4, b&0x0f
		if hi > 9 {
			return nil, BCDBadDigitError{"hi", uint64(hi)}
		}
		if lo > 9 {
			return nil, BCDBadDigitError{"lo", uint64(lo)}
		}
		fields[i] = int(hi)*10 + int(lo)
	}
	year, month, day, hour, minute, second := fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]
	if year < 50 {
		year += 2000
	} else {
		year += 1900
	}
	// The range of every field is checked before the day the month has, so
	// an hour of 24, which time.Date would carry into the next day, is
	// reported as the hour.
	switch {
	case month < 1 || month > 12:
		return nil, ClockValueError{Field: "month", Value: month}
	case day < 1 || day > 31:
		return nil, ClockValueError{Field: "day", Value: day}
	case hour > 23:
		return nil, ClockValueError{Field: "hour", Value: hour}
	case minute > 59:
		return nil, ClockValueError{Field: "minute", Value: minute}
	case second > 59:
		return nil, ClockValueError{Field: "second", Value: second}
	case day > daysInMonth(year, month):
		return nil, ClockValueError{Field: "day", Value: day}
	}

	t := time.Date(
		year, time.Month(month), day, hour, minute, second,
		0, // nanosecond
		loc,
	)
	return &t, nil
}

// daysInMonth is the number of days of month (1 to 12) in year, on the
// calendar: no time zone is involved.
func daysInMonth(year, month int) int {
	// Day 0 of the next month is the last day of this one.
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// checkWriteLength returns a WriteTooLargeError when a write of n data bytes
// does not fit in one FINS frame. Every size is an int, so a count never
// wraps before it is checked.
func checkWriteLength(n int) error {
	if n > maxWriteDataLength {
		return WriteTooLargeError{Bytes: n, Max: maxWriteDataLength}
	}
	return nil
}

// WriteWords Writes words to the PLC data area
//
// More words than one FINS frame carries (997) return a WriteTooLargeError
// without sending anything.
func (c *Client) WriteWords(memoryArea byte, address uint16, data []uint16) error {
	if checkIsWordMemoryArea(memoryArea) == false {
		return IncompatibleMemoryAreaError{memoryArea}
	}
	if err := checkWriteLength(2 * len(data)); err != nil {
		return err
	}
	order := c.order()
	bts := make([]byte, 2*len(data))
	for i, w := range data {
		order.PutUint16(bts[i*2:i*2+2], w)
	}
	command := writeCommand(memAddr(memoryArea, address), uint16(len(data)), bts)

	return checkResponse(c.sendCommand(command))
}

// WriteString Writes a string to the PLC data area
//
// The string fills (len(s)+1)/2 words, two bytes a word, the first in the
// high byte; an odd length is padded with one 0x00 byte, which writes the low
// byte of the last word. A string longer than one FINS frame carries (1,994
// bytes) returns a WriteTooLargeError without sending anything.
func (c *Client) WriteString(memoryArea byte, address uint16, s string) error {
	if checkIsWordMemoryArea(memoryArea) == false {
		return IncompatibleMemoryAreaError{memoryArea}
	}
	words := (len(s) + 1) / 2
	if err := checkWriteLength(2 * words); err != nil {
		return err
	}
	bts := make([]byte, 2*words)
	copy(bts, s)

	command := writeCommand(memAddr(memoryArea, address), uint16(words), bts)

	return checkResponse(c.sendCommand(command))
}

// WriteBytes Writes bytes array to the PLC data area
//
// The bytes fill (len(b)+1)/2 words, two bytes a word, the first in the high
// byte; an odd length is padded with one 0x00 byte, which writes the low byte
// of the last word. b itself is not changed. More bytes than one FINS frame
// carries (1,994) return a WriteTooLargeError without sending anything.
func (c *Client) WriteBytes(memoryArea byte, address uint16, b []byte) error {
	if checkIsWordMemoryArea(memoryArea) == false {
		return IncompatibleMemoryAreaError{memoryArea}
	}
	words := (len(b) + 1) / 2
	if err := checkWriteLength(2 * words); err != nil {
		return err
	}
	bts := make([]byte, 2*words)
	copy(bts, b)
	command := writeCommand(memAddr(memoryArea, address), uint16(words), bts)
	return checkResponse(c.sendCommand(command))
}

// WriteBits Writes bits to the PLC data area
//
// Each bit is one byte of the frame: more bits than one FINS frame carries
// (1,994) return a WriteTooLargeError without sending anything.
func (c *Client) WriteBits(memoryArea byte, address uint16, bitOffset byte, data []bool) error {
	if checkIsBitMemoryArea(memoryArea) == false {
		return IncompatibleMemoryAreaError{memoryArea}
	}
	if err := checkWriteLength(len(data)); err != nil {
		return err
	}
	bts := make([]byte, 0, len(data))
	var d byte
	for _, bit := range data {
		if bit {
			d = 0x01
		} else {
			d = 0x00
		}
		bts = append(bts, d)
	}
	command := writeCommand(memAddrWithBitOffset(memoryArea, address, bitOffset), uint16(len(data)), bts)

	return checkResponse(c.sendCommand(command))
}

// SetBit Sets a bit in the PLC data area
func (c *Client) SetBit(memoryArea byte, address uint16, bitOffset byte) error {
	return c.bitTwiddle(memoryArea, address, bitOffset, 0x01)
}

// ResetBit Resets a bit in the PLC data area
func (c *Client) ResetBit(memoryArea byte, address uint16, bitOffset byte) error {
	return c.bitTwiddle(memoryArea, address, bitOffset, 0x00)
}

// ToggleBit Toggles a bit in the PLC data area
func (c *Client) ToggleBit(memoryArea byte, address uint16, bitOffset byte) error {
	b, e := c.ReadBits(memoryArea, address, bitOffset, 1)
	if e != nil {
		return e
	}
	var t byte
	if b[0] {
		t = 0x00
	} else {
		t = 0x01
	}
	return c.bitTwiddle(memoryArea, address, bitOffset, t)
}

func (c *Client) bitTwiddle(memoryArea byte, address uint16, bitOffset byte, value byte) error {
	if checkIsBitMemoryArea(memoryArea) == false {
		return IncompatibleMemoryAreaError{memoryArea}
	}
	mem := memoryAddress{memoryArea, address, bitOffset}
	command := writeCommand(mem, 1, []byte{value})

	return checkResponse(c.sendCommand(command))
}

// checkResponse returns e, or an EndCodeError when the response's end code
// is not a normal completion. The non-fatal CPU error flag is not an error
// (endCodeNonFatalCPUError); the error carries the end code as received, and
// its MainSub the code without the flags.
func checkResponse(r *response, e error) error {
	if e != nil {
		return e
	}
	if r.endCode&^endCodeNonFatalCPUError != EndCodeNormalCompletion {
		return EndCodeError{Code: r.endCode}
	}
	return nil
}

// reply is what the listener hands to a waiting request: its answer, or a
// destination unreachable it read after the request had started writing its
// datagram (failWaiting).
type reply struct {
	resp response
	err  error
}

func (r reply) result() (*response, error) {
	if r.err != nil {
		return nil, r.err
	}
	resp := r.resp
	return &resp, nil
}

// nextHeader takes a service ID for a request of command code cmd and
// returns its header and answer channel, or TooManyPendingRequestsError.
func (c *Client) nextHeader(cmd uint16) (*Header, chan reply, error) {
	header, ch, _, err := c.nextRequest(cmd)
	return header, ch, err
}

// nextRequest is nextHeader, with the response timeout the request keeps.
func (c *Client) nextRequest(cmd uint16) (*Header, chan reply, time.Duration, error) {
	sid, ch, timeout, err := c.incrementSid(cmd)
	if err != nil {
		return nil, nil, 0, err
	}
	header := defaultCommandHeader(c.src, c.dst, sid)
	return &header, ch, timeout, nil
}

// incrementSid takes the next free service ID after the last one taken, for
// a request of command code cmd, and gives it a fresh answer channel, so an
// answer left in the channel of an earlier request is never read. A service
// ID a request still waits on is skipped, never taken over; with all 256
// taken it returns TooManyPendingRequestsError. It also returns the response
// timeout, read under the same lock, which the request keeps.
func (c *Client) incrementSid(cmd uint16) (byte, chan reply, time.Duration, error) {
	c.mu.Lock() //thread-safe sid incrementation
	defer c.mu.Unlock()
	sid := c.sid
	for range len(c.resp) {
		sid++ // a byte: after 255 comes 0
		if c.resp[sid].ch != nil {
			continue
		}
		c.sid = sid
		ch := make(chan reply, 1) //clearing cell of storage for new response
		c.resp[sid] = waiter{ch: ch, cmd: cmd}
		return sid, ch, c.responseTimeoutMs, nil
	}
	return 0, nil, 0, TooManyPendingRequestsError{}
}

// release empties the slot of a request that stopped waiting, unless a later
// request has taken its service ID since.
func (c *Client) release(sid byte, ch chan reply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.resp[sid].ch == ch {
		c.resp[sid] = waiter{}
	}
}

// write writes the request's datagram, after numbering the attempt in its
// slot under mu: a destination unreachable that the listener reads from then
// on may be this datagram's and fails the request, one read before cannot be
// and does not (failWaiting). Numbering also drops what an earlier attempt of
// the same request, which sent nothing, was handed.
func (c *Client) write(sid byte, ch chan reply, bts []byte) error {
	c.mu.Lock()
	if w := &c.resp[sid]; w.ch == ch {
		select {
		case <-ch:
		default:
		}
		w.attempt = c.attempts.Add(1)
	}
	c.mu.Unlock()
	_, err := c.conn.Write(bts)
	return err
}

func (c *Client) sendCommand(command []byte) (*response, error) {
	if c.closed.Load() {
		return nil, ClientClosedError{Cause: c.closeCause()}
	}
	// Every command starts with its command code (readCommand, writeCommand,
	// clockReadCommand).
	header, ch, timeoutMs, err := c.nextRequest(binary.BigEndian.Uint16(command[0:2]))
	if err != nil {
		// No service ID is free: nothing is sent.
		return nil, err
	}
	defer c.release(header.serviceID, ch)
	bts := encodeHeader(*header)
	bts = append(bts, command...)
	err = c.write(header.serviceID, ch, bts)
	if err != nil && isDestinationUnreachable(err) && !c.closed.Load() {
		// The socket handed back an earlier datagram's ICMP error instead of
		// sending this one (Linux sock_error, BSD so_error): nothing went
		// out, so writing it once more cannot send it twice.
		err = c.write(header.serviceID, ch, bts)
	}
	if err != nil {
		if c.closed.Load() {
			return nil, ClientClosedError{Cause: c.closeCause()}
		}
		if isDestinationUnreachable(err) {
			// Nothing went out. The client stays open: the next request is
			// sent as usual.
			return nil, DestinationUnreachableError{Cause: err, Sent: false}
		}
		return nil, err
	}

	// if response timeout is zero, block until the answer or the client's end;
	// the timeout is the one read when the service ID was taken
	var timeout <-chan time.Time
	if timeoutMs > 0 {
		timer := time.NewTimer(timeoutMs * time.Millisecond)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case r := <-ch:
		return r.result()
	case <-c.done:
		// An answer delivered just before the client ended still counts.
		select {
		case r := <-ch:
			return r.result()
		default:
		}
		return nil, ClientClosedError{Cause: c.closeCause()}
	case <-timeout:
		return nil, ResponseTimeoutError{timeoutMs}
	}
}

// isDestinationUnreachable reports whether err is an ICMP error reported by
// the socket: a closed port (ECONNREFUSED), a host (EHOSTUNREACH, EHOSTDOWN)
// or a network (ENETUNREACH) that cannot be reached, a fragmentation needed
// (EMSGSIZE), a parameter problem (EPROTO on Linux, ENOPROTOOPT on BSD), a
// protocol unreachable (ENOPROTOOPT on Linux) or, on Linux, an ICMPv6
// administratively prohibited, source address policy failure or reject route
// (EACCES). The system reports each one once, on the next read or write, so
// the socket keeps working after it. A write may also get one from the
// system itself, which then sends nothing: no route to the PLC, or a datagram
// too large to send.
//
// These are the Unix errors: on Windows, Go's syscall values do not match
// the WSAE* codes the system reports, so nothing matches there.
func isDestinationUnreachable(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EHOSTDOWN) ||
		errors.Is(err, syscall.EMSGSIZE) ||
		errors.Is(err, syscall.EPROTO) ||
		errors.Is(err, syscall.ENOPROTOOPT) ||
		errors.Is(err, syscall.EACCES)
}

// closeCause is what closed the client: the read error, or net.ErrClosed
// after Close.
func (c *Client) closeCause() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	return net.ErrClosed
}

// fail ends the client after a read error that is not a destination
// unreachable, unless Close came first.
func (c *Client) fail(err error) {
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return
	}
	c.failure = err
	c.closed.Store(true)
	c.mu.Unlock()
	_ = c.conn.Close()
}

// failWaiting hands err, a destination unreachable the listener has just
// read, to every request whose write attempt is numbered no later than
// attempts, the count the listener read as soon as its read failed: that
// request may have sent the datagram the error answers. A request that took
// its service ID but had not started writing its datagram by then cannot
// have, so it is not failed and keeps waiting for its answer. A request whose
// slot already holds its answer keeps the answer. The sends never block (one
// slot, select with default), and are made under mu so that write's
// numbering, which also empties the slot, sees each one before or after it,
// never in between.
func (c *Client) failWaiting(err error, attempts uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.resp {
		if w.ch == nil || w.attempt == 0 || w.attempt > attempts {
			continue
		}
		select {
		case w.ch <- reply{err: err}:
		default:
		}
	}
}

// listenLoop reads the answers and hands each to the request waiting for its
// service ID. It never blocks on a delivery: an answer for a service ID no
// request waits on (a late answer, whose request has returned, or a service
// ID never used) is dropped, a duplicate finds the one-slot channel full and
// is dropped, and so is a datagram too short to be a response, a datagram
// whose ICF does not mark a response, and a response whose command code is
// not that of the request waiting for its service ID. A destination
// unreachable fails the requests that had started writing their datagram
// when it was read, and the loop keeps reading; any other read error ends the
// client.
func (c *Client) listenLoop() {
	defer close(c.done)
	buf := make([]byte, readBufferLength)
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			if isDestinationUnreachable(err) {
				// Counted at once: an attempt numbered later started after
				// the error was read, so it is not the error's cause.
				attempts := c.attempts.Load()
				// It may answer a request that has already timed out, so it
				// never closes the client.
				c.failWaiting(DestinationUnreachableError{Cause: err, Sent: true}, attempts)
				continue
			}
			// after Close the error is expected: fail leaves the client as Close left it
			c.fail(err)
			return
		}
		if n < minResponseLength {
			continue
		}
		// The raw ICF byte: decodeHeader's messageType is not reliable (its
		// check reads (icf&1)<<6, an operator precedence slip upstream), and
		// driver.go is left as it is.
		if buf[0]&(1<<icfMessageTypeBit) == 0 {
			continue
		}

		ans := decodeResponse(bytes.Clone(buf[:n]))
		c.mu.Lock()
		w := c.resp[ans.header.serviceID]
		c.mu.Unlock()
		if w.ch == nil || ans.commandCode != w.cmd {
			continue
		}
		ch := w.ch
		select {
		case ch <- reply{resp: ans}:
		default:
		}
	}
}

func checkIsWordMemoryArea(memoryArea byte) bool {
	if memoryArea == MemoryAreaCIOWord ||
		memoryArea == MemoryAreaDMWord ||
		memoryArea == MemoryAreaARWord ||
		memoryArea == MemoryAreaHRWord ||
		memoryArea == MemoryAreaWRWord {
		return true
	}
	return false
}

func checkIsBitMemoryArea(memoryArea byte) bool {
	if memoryArea == MemoryAreaCIOBit ||
		memoryArea == MemoryAreaDMBit ||
		memoryArea == MemoryAreaARBit ||
		memoryArea == MemoryAreaHRBit ||
		memoryArea == MemoryAreaWRBit {
		return true
	}
	return false
}
