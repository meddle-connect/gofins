package fins

import (
	"fmt"
	"time"
)

// Client errors

type ResponseTimeoutError struct {
	duration time.Duration
}

func (e ResponseTimeoutError) Error() string {
	return fmt.Sprintf("Response timeout of %d has been reached", e.duration)
}

type IncompatibleMemoryAreaError struct {
	area byte
}

func (e IncompatibleMemoryAreaError) Error() string {
	return fmt.Sprintf("The memory area is incompatible with the data type to be read: 0x%X", e.area)
}

// Driver errors

type BCDBadDigitError struct {
	v   string
	val uint64
}

func (e BCDBadDigitError) Error() string {
	return fmt.Sprintf("Bad digit in BCD decoding: %s = %d", e.v, e.val)
}

type BCDOverflowError struct{}

func (e BCDOverflowError) Error() string {
	return "Overflow occurred in BCD decoding"
}

// ClientClosedError is returned by every call on a client that is closed:
// closed by Close, or closed by the client itself when a read of its socket
// failed with an error that is not a destination unreachable (see
// DestinationUnreachableError). Cause is what closed it: net.ErrClosed
// after Close, the read error otherwise. It is returned by value.
type ClientClosedError struct {
	Cause error
}

func (e ClientClosedError) Error() string {
	if e.Cause == nil {
		return "fins: client closed"
	}
	return "fins: client closed: " + e.Cause.Error()
}

// Unwrap returns the cause, so errors.Is and errors.As reach it.
func (e ClientClosedError) Unwrap() error {
	return e.Cause
}

// DestinationUnreachableError is returned by a request when the socket
// reports an ICMP error: ECONNREFUSED (a FINS port closed on a reachable
// host), EHOSTUNREACH, ENETUNREACH, EHOSTDOWN, EMSGSIZE (fragmentation
// needed), EPROTO (Linux, parameter problem), ENOPROTOOPT (Linux protocol
// unreachable, a BSD parameter problem) or EACCES (Linux, an ICMPv6
// administratively prohibited, source address policy failure or reject
// route). The client is not closed by it, and the next request is sent as
// usual. Cause is the socket's error, so errors.Is(err, syscall.ECONNREFUSED)
// holds for a closed port. It is returned by value.
//
// Sent tells apart the two ways a request gets it, which matter to a write
// that acts on the machine:
//
//   - Sent true, from the client's listener: the request's datagram was
//     written, and the listener read the error after the request had started
//     writing it. The error may be another datagram's (one whose request has
//     timed out, say), so the datagram may have reached the PLC and been
//     carried out: the outcome of the request is unknown, and a control write
//     that gets it must never be written again as if it had not been.
//   - Sent false, from the request's own write: the datagram was not written
//     and nothing went out. The socket handed the write an error left by an
//     earlier datagram instead of sending (Linux sock_error, BSD so_error), or
//     the system refused to send (no route to the PLC, a datagram too large);
//     the request wrote its datagram once more first, and returns this error
//     only if that write failed too. (On FreeBSD a datagram still held for
//     address resolution may leave later; see the README.)
type DestinationUnreachableError struct {
	Cause error
	// Sent reports whether the request's datagram was written: true when the
	// listener handed the error to the request, false when the request's own
	// write got it.
	Sent bool
}

func (e DestinationUnreachableError) Error() string {
	msg := "fins: destination unreachable, request not sent"
	if e.Sent {
		msg = "fins: destination unreachable after the request was sent"
	}
	if e.Cause == nil {
		return msg
	}
	return msg + ": " + e.Cause.Error()
}

// Unwrap returns the cause, so errors.Is and errors.As reach it.
func (e DestinationUnreachableError) Unwrap() error {
	return e.Cause
}

// ShortResponseError is returned when a response carries less data than the
// command asked for. It is returned by value.
type ShortResponseError struct {
	Want, Got int
}

func (e ShortResponseError) Error() string {
	return fmt.Sprintf("fins: response carries %d bytes of data, want %d", e.Got, e.Want)
}
