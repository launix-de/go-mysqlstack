/*
 * go-mysqlstack
 *
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

func TestSessionDoneClosesOnClose(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	session := newSession(xlog.NewStdLog(xlog.Level(xlog.ERROR)), 1, "test", server)
	session.Close()
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("session Done channel remained open")
	}
}

func TestSessionCloseCancelsCurrentQueryOnce(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	session := newSession(xlog.NewStdLog(xlog.Level(xlog.ERROR)), 1, "test", server)
	cancelled := make(chan struct{}, 1)
	session.SetQueryCancel(func() { cancelled <- struct{}{} })
	session.Close()
	session.Close()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("current query was not cancelled")
	}
	select {
	case <-cancelled:
		t.Fatal("query cancellation ran more than once")
	default:
	}
}

func TestSessionClearQueryCancelPreservesCompletedQuery(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	session := newSession(xlog.NewStdLog(xlog.Level(xlog.ERROR)), 1, "test", server)
	cancelled := false
	id := session.SetQueryCancel(func() { cancelled = true })
	session.ClearQueryCancel(id)
	session.Close()
	if cancelled {
		t.Fatal("completed query was cancelled during connection close")
	}
}
