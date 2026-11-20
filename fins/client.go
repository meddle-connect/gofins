package fins

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
)

// Client Omron FINS client
//
// A late, duplicated or malformed datagram is dropped.
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
	// mu guards sid, resp, failure and the closing of the client.
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
	failure           error
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
func (c *Client) SetByteOrder(o binary.ByteOrder) {
	c.byteOrder = o
}

// Set response timeout duration (ms).
// Default value: 20ms.
// A timeout of zero can be used to block indefinitely.
func (c *Client) SetTimeoutMs(t uint) {
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
	data := make([]uint16, readCount, readCount)
	for i := 0; i < int(readCount); i++ {
		data[i] = c.byteOrder.Uint16(r.data[i*2 : i*2+2])
	}

	return data, nil
}

// ReadBytes Reads bytes from the PLC data area
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

	return r.data, nil
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

// ReadClock Reads the PLC clock
func (c *Client) ReadClock() (*time.Time, error) {
	r, e := c.sendCommand(clockReadCommand())
	e = checkResponse(r, e)
	if e != nil {
		return nil, e
	}
	if len(r.data) < clockDataLength {
		return nil, ShortResponseError{Want: clockDataLength, Got: len(r.data)}
	}
	year, _ := decodeBCD(r.data[0:1])
	if year < 50 {
		year += 2000
	} else {
		year += 1900
	}
	month, _ := decodeBCD(r.data[1:2])
	day, _ := decodeBCD(r.data[2:3])
	hour, _ := decodeBCD(r.data[3:4])
	minute, _ := decodeBCD(r.data[4:5])
	second, _ := decodeBCD(r.data[5:6])

	t := time.Date(
		int(year), time.Month(month), int(day), int(hour), int(minute), int(second),
		0, // nanosecond
		time.Local,
	)
	return &t, nil
}

// WriteWords Writes words to the PLC data area
func (c *Client) WriteWords(memoryArea byte, address uint16, data []uint16) error {
	if checkIsWordMemoryArea(memoryArea) == false {
		return IncompatibleMemoryAreaError{memoryArea}
	}
	l := uint16(len(data))
	bts := make([]byte, 2*l, 2*l)
	for i := 0; i < int(l); i++ {
		c.byteOrder.PutUint16(bts[i*2:i*2+2], data[i])
	}
	command := writeCommand(memAddr(memoryArea, address), l, bts)

	return checkResponse(c.sendCommand(command))
}

// WriteString Writes a string to the PLC data area
func (c *Client) WriteString(memoryArea byte, address uint16, s string) error {
	if checkIsWordMemoryArea(memoryArea) == false {
		return IncompatibleMemoryAreaError{memoryArea}
	}
	bts := make([]byte, 2*len(s), 2*len(s))
	copy(bts, s)

	command := writeCommand(memAddr(memoryArea, address), uint16((len(s)+1)/2), bts) //TODO: test on real PLC

	return checkResponse(c.sendCommand(command))
}

// WriteBytes Writes bytes array to the PLC data area
func (c *Client) WriteBytes(memoryArea byte, address uint16, b []byte) error {
	if checkIsWordMemoryArea(memoryArea) == false {
		return IncompatibleMemoryAreaError{memoryArea}
	}
	command := writeCommand(memAddr(memoryArea, address), uint16(len(b)), b)
	return checkResponse(c.sendCommand(command))
}

// WriteBits Writes bits to the PLC data area
func (c *Client) WriteBits(memoryArea byte, address uint16, bitOffset byte, data []bool) error {
	if checkIsBitMemoryArea(memoryArea) == false {
		return IncompatibleMemoryAreaError{memoryArea}
	}
	l := uint16(len(data))
	bts := make([]byte, 0, l)
	var d byte
	for i := 0; i < int(l); i++ {
		if data[i] {
			d = 0x01
		} else {
			d = 0x00
		}
		bts = append(bts, d)
	}
	command := writeCommand(memAddrWithBitOffset(memoryArea, address, bitOffset), l, bts)

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

func checkResponse(r *response, e error) error {
	if e != nil {
		return e
	}
	if r.endCode != EndCodeNormalCompletion {
		return fmt.Errorf("error reported by destination, end code 0x%x", r.endCode)
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

func (c *Client) nextHeader() (*Header, chan reply) {
	sid, ch := c.incrementSid()
	header := defaultCommandHeader(c.src, c.dst, sid)
	return &header, ch
}

// incrementSid takes the next service ID and gives it a fresh answer channel,
// so an answer left in the channel of an earlier request is never read.
func (c *Client) incrementSid() (byte, chan reply) {
	c.mu.Lock() //thread-safe sid incrementation
	defer c.mu.Unlock()
	c.sid++
	sid := c.sid
	ch := make(chan reply, 1) //clearing cell of storage for new response
	c.resp[sid] = waiter{ch: ch}
	return sid, ch
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
	header, ch := c.nextHeader()
	defer c.release(header.serviceID, ch)
	bts := encodeHeader(*header)
	bts = append(bts, command...)
	err := c.write(header.serviceID, ch, bts)
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

	// if response timeout is zero, block until the answer or the client's end
	var timeout <-chan time.Time
	if c.responseTimeoutMs > 0 {
		timer := time.NewTimer(c.responseTimeoutMs * time.Millisecond)
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
		return nil, ResponseTimeoutError{c.responseTimeoutMs}
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
// is dropped, and so is a datagram too short to be a response. A destination
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

		ans := decodeResponse(bytes.Clone(buf[:n]))
		c.mu.Lock()
		ch := c.resp[ans.header.serviceID].ch
		c.mu.Unlock()
		if ch == nil {
			continue
		}
		select {
		case ch <- reply{resp: ans}:
		default:
		}
	}
}

func checkIsWordMemoryArea(memoryArea byte) bool {
	if memoryArea == MemoryAreaDMWord ||
		memoryArea == MemoryAreaARWord ||
		memoryArea == MemoryAreaHRWord ||
		memoryArea == MemoryAreaWRWord {
		return true
	}
	return false
}

func checkIsBitMemoryArea(memoryArea byte) bool {
	if memoryArea == MemoryAreaDMBit ||
		memoryArea == MemoryAreaARBit ||
		memoryArea == MemoryAreaHRBit ||
		memoryArea == MemoryAreaWRBit {
		return true
	}
	return false
}
