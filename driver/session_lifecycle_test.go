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

// lifecycleHandler observes the protocol session even when the connection is
// rejected before NewSession registers it with the application.
type lifecycleHandler struct {
	*TestHandler
	mode   string
	seen   chan *Session
	closed chan bool
}

func (h *lifecycleHandler) SessionCheck(s *Session) error {
	h.seen <- s
	if h.mode == "session check" {
		return errors.New("rejected connection")
	}
	if h.mode == "check panic" {
		panic("session check panic")
	}
	return nil
}

func (h *lifecycleHandler) SessionClosed(s *Session) {
	select {
	case <-s.Done():
		h.closed <- true
	default:
		h.closed <- false
	}
	h.TestHandler.SessionClosed(s)
}

func TestServerHandlerEndsSession(t *testing.T) {
	for _, mode := range []string{"quit", "disconnect", "auth failure", "schema failure", "query panic", "session check", "check panic"} {
		t.Run(mode, func(t *testing.T) {
			log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
			h := &lifecycleHandler{TestHandler: NewTestHandler(log), mode: mode,
				seen: make(chan *Session, 1), closed: make(chan bool, 1)}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			svr := NewListenerFromNetListener(log, listener, h)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				conn, err := listener.Accept()
				if err == nil {
					svr.handle(conn, 1)
				}
			}()
			user, schema := "mock", "test"
			if mode == "auth failure" {
				user = "denied"
			}
			if mode == "schema failure" {
				schema = "xx_rejected"
			}
			client, err := NewConn(user, "mock", listener.Addr().String(), schema, "")
			if mode == "quit" || mode == "disconnect" || mode == "query panic" {
				if err != nil {
					t.Fatal(err)
				}
				if mode == "query panic" {
					h.AddQueryPanic("PANIC")
					client.Exec("PANIC")
				}
				if mode == "disconnect" {
					client.Cleanup()
				} else {
					client.Close()
				}
			} else if err == nil {
				client.Close()
				t.Fatal("rejected handshake unexpectedly succeeded")
			}
			var session *Session
			select {
			case session = <-h.seen:
				t.Cleanup(session.Close)
			case <-time.After(3 * time.Second):
				t.Fatal("handler did not create a session")
			}
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("connection handler did not finish")
			}
			select {
			case <-session.Done():
			default:
				t.Fatal("handler returned but retained an open session")
			}
			session.mu.RLock()
			retainedConn := session.conn != nil
			session.mu.RUnlock()
			if retainedConn {
				t.Fatal("ended session retains its connection")
			}
			if mode != "session check" && mode != "check panic" {
				select {
				case ended := <-h.closed:
					if !ended {
						t.Fatal("SessionClosed ran before the session ended")
					}
				default:
					t.Fatal("registered session was not unregistered")
				}
			} else {
				select {
				case <-h.closed:
					t.Fatal("unregistered session received SessionClosed")
				default:
				}
			}
		})
	}
}

func TestServerGreetingFailureEndsSession(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	h := &lifecycleHandler{TestHandler: NewTestHandler(log), seen: make(chan *Session, 1), closed: make(chan bool, 1)}
	server, client := net.Pipe()
	client.Close()
	listener := &Listener{log: log, handler: h}
	listener.handle(server, 1)
	session := <-h.seen
	defer session.Close()
	select {
	case <-session.Done():
	default:
		t.Fatal("failed greeting retained an open session")
	}
}
