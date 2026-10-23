//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

/*
 * go-mysqlstack
 *
 * Copyright (c) 2026 Carl-Philip Hänsch
 * GPL License
 */

package driver

import (
	"errors"
	"net"
	"syscall"
)

// socketDisconnected peeks without consuming protocol bytes. It allows the
// session to cancel a running handler after the client has gone away.
func socketDisconnected(conn net.Conn) bool {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return false
	}
	raw, err := syscallConn.SyscallConn()
	if err != nil {
		return errors.Is(err, net.ErrClosed)
	}
	closed := false
	controlErr := raw.Read(func(fd uintptr) bool {
		var buffer [1]byte
		n, _, receiveErr := syscall.Recvfrom(int(fd), buffer[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		switch {
		case receiveErr == nil:
			closed = n == 0
		case receiveErr == syscall.EAGAIN || receiveErr == syscall.EWOULDBLOCK:
			closed = false
		default:
			closed = true
		}
		return true
	})
	// A local close makes RawConn.Read fail before invoking the callback.
	// Only a closed connection ends the session; a read deadline may expire
	// while the connection remains usable.
	return errors.Is(controlErr, net.ErrClosed) || closed
}
