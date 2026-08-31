//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

/*
 * go-mysqlstack
 *
 * Copyright (c) 2026 Carl-Philip Hänsch
 * GPL License
 */

package driver

import (
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
		return false
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
	return controlErr == nil && closed
}
