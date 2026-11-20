# Meddle fork

This is the Meddle fork of [l1va/gofins](https://github.com/l1va/gofins).

- **Source**: https://github.com/l1va/gofins
- **Base commit**: `37730db91d7d` (the pseudo-version `v0.0.0-20191108164614-37730db91d7d` Meddle used before the fork)
- **License**: MIT, in the unchanged [`LICENSE`](LICENSE) (Copyright (c) 2018 l1va)
- **Module path**: still `github.com/l1va/gofins`, so the importers do not change; this repository adds the `go.mod` (and its `go.sum`, for the upstream tests' `testify`) the upstream lacks
- **Changes**: see [Changes](#changes) below

It is used by meddle-connect/meddle through a `replace` in its `go.mod`. Update it only with a new commit here and a new `replace` there, never with `go get`.

## Changes

2026-11-19, Meddle CH-B01: a socket error or a datagram from the PLC no longer ends the process, an ICMP destination unreachable no longer closes the client, and a late answer no longer stops the client. The bytes on the wire are unchanged. The public API is unchanged but for three new error types and the client's mutex, which is now private (`Client` no longer has `Lock` and `Unlock`). Only `fins/client.go` and `fins/error.go` change, and one test file is added, `fins/listen_meddle_test.go`.

1. `fins/error.go`: three new error types, each returned by value with value receivers, as the upstream errors are. `ClientClosedError` (`Cause error`, with `Unwrap`) is returned by every call on a closed client; its cause is `net.ErrClosed` after `Close`, or the read error that closed the client. `DestinationUnreachableError` (`Cause error`, with `Unwrap`, and `Sent bool`) is returned by a request that an ICMP destination unreachable failed (item 4); its cause is the socket's error, so `errors.Is(err, syscall.ECONNREFUSED)` holds for a FINS port closed on a reachable host, and `Sent` says whether the request's datagram was written (item 4); its text says so too (`fins: destination unreachable after the request was sent: …`, `fins: destination unreachable, request not sent: …`). `ShortResponseError` (`Want`, `Got`) is returned when a response carries less data than asked for. Match them with a value target (`var closed fins.ClientClosedError; errors.As(err, &closed)`), not a pointer one. `ResponseTimeoutError` is unchanged.
2. `Client`: `closed` is a `sync/atomic.Bool`; a `done` channel is closed by the listener, and only by it, when it stops; the read error that closed the client is kept under the client's mutex, a private field (`mu`); each request's answer channel is buffered (one slot), created under the mutex, handed back to the request and emptied from its slot when the request returns, and the listener reads the slot under the mutex; each write attempt of a request is numbered in its slot, under the mutex, just before the write (item 4).
3. The listener reads the socket directly (no `bufio` reader per datagram), into a 64 KiB buffer, larger than any UDP datagram, so a large datagram is never truncated nor a read error (`WSAEMSGSIZE` on Windows); each datagram is copied before it is handed over. It never calls `log.Fatal`. A datagram shorter than 14 bytes (header, command code, end code) is dropped instead of panicking, and an answer is handed over without blocking, so a late, duplicated or unknown-service-ID answer is dropped instead of stopping the listener for good. Nothing in `fins/client.go` logs.
4. An ICMP error the socket reports, `ECONNREFUSED` (port), `EHOSTUNREACH` or `EHOSTDOWN` (host), `ENETUNREACH` (network), `EMSGSIZE` (fragmentation needed), `EPROTO` (Linux, parameter problem), `ENOPROTOOPT` (Linux protocol unreachable, a BSD parameter problem) or `EACCES` (Linux, an ICMPv6 administratively prohibited, source address policy failure or reject route), never closes the client, because it can arrive after the request that caused it has timed out. A request it fails returns a `DestinationUnreachableError` wrapping it, whose `Sent` field tells the two ways apart; for a write that acts on the machine they mean different things:
   - **`Sent: true` — from the listener; the datagram went out, the outcome is unknown.** The listener read the error after the request had started writing its datagram, and the write went through. The error may be another datagram's (one whose request has timed out, say), so the request may have reached the PLC and been carried out: a control write that gets it must never be written again as if it had not been. Each write attempt is numbered in the request's slot, under the client's mutex, just before the write, and the listener reads the count of attempts as soon as its read fails: it fails every request whose attempt is numbered no later than that, and no other. A request that has taken its service ID but not yet started writing its datagram when the error is read cannot have caused it, so it is not failed and gets its answer. The listener keeps reading.
   - **`Sent: false` — from the request's own write; nothing went out.** The error belongs to an earlier datagram, and the system reported it instead of sending this one (Linux `sock_error`, BSD `so_error`), or the system refused to send (see Known limits). The request writes its datagram once more, which cannot send it twice on Linux, macOS or Windows (on FreeBSD a datagram still held for address resolution may leave later, so a retry could repeat it), and returns the error only if that second write fails too: the datagram was never written and did not reach the PLC.

   Either way the next request is sent as usual. Only `Close`, or a read error of any other kind, ends the client; a write error of any other kind is returned as it is, as upstream did, and leaves the client open. On Windows, Go turns off the report of an ICMP port unreachable on UDP sockets, so a closed port gives a timeout there (and see Known limits).
5. A request on a closed client returns `ClientClosedError` at once; a waiting request returns on its answer, on a destination unreachable (`DestinationUnreachableError` with `Sent: true`), on the client's end (`ClientClosedError`) or on its timeout (`ResponseTimeoutError`, with a timer stopped on return). With a timeout of zero it still returns when the client ends or a destination unreachable comes.
6. `Close` may be called more than once and from any goroutine, and returns once the listener has stopped. A request waiting at that moment gets `ClientClosedError` with cause `net.ErrClosed`, unless a read error had closed the client first (the cause is then that error); an answer the client had already received is still returned.
7. `ReadWords`, `ReadBits` and `ReadClock` check the length of the response data (2 × count, count, 6 bytes) and return `ShortResponseError` instead of panicking or reading past the data.

Known limits:

- Service IDs are one byte, so the 257th request reuses the first one's, and an answer that arrives more than 256 requests late can be taken for the answer of the request that reused its service ID (unchanged from upstream).
- One ICMP error a connected UDP socket can report is left out of item 4: `ENONET` (an ICMP host isolated, Linux only, a name not every system defines). The listener that reads it still ends the client, so the next call returns `ClientClosedError` at once, unsent, and the caller opens a new client; a write that gets it returns it as it is, unsent, without writing it again. (`ENOPROTOOPT` — an ICMP protocol unreachable on Linux, a parameter problem on BSD systems — is handled like the errors of item 4.)
- A write can also get `EMSGSIZE`, `ENETUNREACH` or `EHOSTUNREACH` from the system itself, before anything is sent (a datagram too large to send, no route to the PLC): it is written once more like an ICMP error, and the error is then returned as a `DestinationUnreachableError` with `Sent: false`.
- The listener cannot tell when the system received an ICMP error, only when its read returned it, so the count it compares with (item 4) is read right after that read. A request that numbers its write attempt in the instant between the two (a few instructions of the listener, unless it is preempted there) counts as started before the error: it gets it, with `Sent: true` (its datagram did go out), and its answer is dropped.
- The ICMP classification of item 4 works on Unix systems only. It names the Unix `syscall` errors, and on Windows Go's `syscall.ECONNREFUSED` and the others are values of Go's own that do not match the `WSAE*` codes Windows reports, so nothing matches there: a read error of that kind ends the client as upstream did, except that the process no longer dies (the next call returns `ClientClosedError` and the caller opens a new client), and a write error of that kind is returned as it is, unsent, without a second write.
- The client names the `syscall` errors of item 4, which Plan 9 does not define, nor `EHOSTDOWN` on `wasip1`, so the fork does not build on Plan 9 nor on `wasip1`.

The upstream `.travis.yml` (Go 1.8 to 1.10) is kept as it is and is not used by this fork.

The tests of these changes live in the Meddle repository (`test/unit/omron`), against a fake FINS/UDP PLC on the loopback, and, for the cases that need the client's own socket (an ICMP error that reaches the listener while no request waits, one a write gets instead of sending its datagram, one the listener reads while a request has taken its service ID but not yet written its datagram, and the `Sent` field of the error from the listener and from a write), in `fins/listen_meddle_test.go`.

The upstream README follows, unchanged.

---

# GoFINS

[![Build Status](https://travis-ci.org/l1va/gofins.svg?branch=master)](https://travis-ci.org/l1va/gofins)

This is fins command client written by Go.

The library support communication to omron PLC from Go application.

Ideas were taken from https://github.com/hiroeorz/omron-fins-go and https://github.com/patrick--/node-omron-fins

Library was tested with <b>Omron PLC NJ501-1300</b>. Mean time of the cycle request-response is 4ms.
Additional work in the siyka-au repository was tested against a <b>CP1L-EM</b>.

There is simple Omron FINS Server (PLC emulator) in the fins/server.go 

Feel free to ask questions, raise issues and make pull requests!

 ### Thanks
 [malleblas](https://github.com/malleblas) for his PR: https://github.com/l1va/gofins/pull/1
