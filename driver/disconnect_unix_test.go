//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

/*
 * go-mysqlstack
 * Copyright (c) 2026 Carl-Philip Hänsch
 * GPL License
 */

package driver

import (
	"net"
	"testing"
	"time"

	"github.com/launix-de/go-mysqlstack/xlog"
)

func tcpSessionPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return server, client
}

func TestSocketDisconnectedPreservesPendingBytes(t *testing.T) {
	server, client := tcpSessionPair(t)
	server.SetDeadline(time.Now().Add(3 * time.Second))
	if socketDisconnected(server) {
		t.Fatal("idle connected socket was reported disconnected")
	}
	if _, err := client.Write([]byte("q")); err != nil {
		t.Fatal(err)
	}
	if socketDisconnected(server) {
		t.Fatal("socket with protocol bytes was reported disconnected")
	}
	var data [1]byte
	if _, err := server.Read(data[:]); err != nil {
		t.Fatal(err)
	}
	if data[0] != 'q' {
		t.Fatal("disconnect probe consumed protocol bytes")
	}
}

func TestSocketDisconnectedAfterLocalClose(t *testing.T) {
	server, _ := tcpSessionPair(t)
	server.Close()
	if !socketDisconnected(server) {
		t.Fatal("locally closed socket was reported connected")
	}
}

func TestSessionSocketCloseEndsSession(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "peer"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			server, client := tcpSessionPair(t)
			session := newSession(xlog.NewStdLog(xlog.Level(xlog.ERROR)), 1, "test", server)
			t.Cleanup(session.Close)
			cancelled := make(chan struct{}, 1)
			session.SetQueryCancel(func() { cancelled <- struct{}{} })
			if local {
				server.Close()
			} else {
				client.Close()
			}
			select {
			case <-session.Done():
			case <-time.After(3 * time.Second):
				t.Fatal("closed socket retained an open session")
			}
			select {
			case <-cancelled:
			default:
				t.Fatal("closed socket did not cancel the running query")
			}
		})
	}
}

func TestSocketDisconnectedIgnoresReadDeadline(t *testing.T) {
	server, client := tcpSessionPair(t)
	if err := server.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if socketDisconnected(server) {
		t.Fatal("read deadline was mistaken for a closed connection")
	}
	if err := server.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("q")); err != nil {
		t.Fatal(err)
	}
	var data [1]byte
	if _, err := server.Read(data[:]); err != nil {
		t.Fatal(err)
	}
}
