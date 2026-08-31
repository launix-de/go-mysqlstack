//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

/*
 * go-mysqlstack
 *
 * Copyright (c) 2026 Carl-Philip Hänsch
 * GPL License
 */

package driver

import "net"

func socketDisconnected(net.Conn) bool {
	return false
}
