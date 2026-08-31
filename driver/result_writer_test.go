/*
 * go-mysqlstack
 *
 * Copyright (C) 2026 Carl-Philip Haensch
 * GPL License
 */

package driver

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/launix-de/go-mysqlstack/packet"
	"github.com/launix-de/go-mysqlstack/proto"
	querypb "github.com/launix-de/go-mysqlstack/sqlparser/depends/query"
	"github.com/launix-de/go-mysqlstack/sqlparser/depends/sqltypes"
)

type resultWriterConn struct {
	bytes.Buffer
}

func (c *resultWriterConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *resultWriterConn) Close() error                     { return nil }
func (c *resultWriterConn) LocalAddr() net.Addr              { return benchmarkAddr("local") }
func (c *resultWriterConn) RemoteAddr() net.Addr             { return benchmarkAddr("remote") }
func (c *resultWriterConn) SetDeadline(time.Time) error      { return nil }
func (c *resultWriterConn) SetReadDeadline(time.Time) error  { return nil }
func (c *resultWriterConn) SetWriteDeadline(time.Time) error { return nil }

func testResultWriter(mode RowMode, fields ...querypb.Type) (*ResultWriter, *resultWriterConn) {
	conn := &resultWriterConn{}
	session := &Session{
		auth:     proto.NewAuth(),
		greeting: proto.NewGreeting(1, "test"),
		packets:  packet.NewPackets(conn),
	}
	writer := newResultWriter(session, mode)
	writer.started = true
	for _, typ := range fields {
		writer.fields = append(writer.fields, &querypb.Field{Type: typ})
	}
	return writer, conn
}

func TestResultWriterPadsAndTruncatesTextRows(t *testing.T) {
	writer, conn := testResultWriter(TextRowMode, sqltypes.Int64, sqltypes.VarChar, sqltypes.Int32)
	row, err := writer.BeginRow()
	if err != nil {
		t.Fatal(err)
	}
	row.Int64(42)
	status, err := row.End()
	if err != nil {
		t.Fatal(err)
	}
	if status != RowPadded {
		t.Fatalf("got status %d, want padded", status)
	}
	if err := writer.session.packets.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []byte{5, 0, 0, 0, 2, '4', '2', 0xfb, 0xfb}
	if !bytes.Equal(conn.Bytes(), want) {
		t.Fatalf("got packet %v, want %v", conn.Bytes(), want)
	}

	writer, conn = testResultWriter(TextRowMode, sqltypes.Int64)
	row, err = writer.BeginRow()
	if err != nil {
		t.Fatal(err)
	}
	row.Int64(7)
	row.String("ignored")
	status, err = row.End()
	if err != nil {
		t.Fatal(err)
	}
	if status != RowTruncated {
		t.Fatalf("got status %d, want truncated", status)
	}
	if err := writer.session.packets.Flush(); err != nil {
		t.Fatal(err)
	}
	want = []byte{2, 0, 0, 0, 1, '7'}
	if !bytes.Equal(conn.Bytes(), want) {
		t.Fatalf("got packet %v, want %v", conn.Bytes(), want)
	}
}

func TestResultWriterPadsBinaryRowsInNullBitmap(t *testing.T) {
	writer, conn := testResultWriter(BinaryRowMode, sqltypes.Int64, sqltypes.VarChar, sqltypes.Int32)
	row, err := writer.BeginRow()
	if err != nil {
		t.Fatal(err)
	}
	row.Int64(42)
	status, err := row.End()
	if err != nil {
		t.Fatal(err)
	}
	if status != RowPadded {
		t.Fatalf("got status %d, want padded", status)
	}
	if err := writer.session.packets.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []byte{10, 0, 0, 0, 0, 0x18, 42, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(conn.Bytes(), want) {
		t.Fatalf("got packet %v, want %v", conn.Bytes(), want)
	}
}

func TestResultWriterReplacesUnencodableValuesWithNull(t *testing.T) {
	writer, conn := testResultWriter(BinaryRowMode, sqltypes.Int64)
	row, err := writer.BeginRow()
	if err != nil {
		t.Fatal(err)
	}
	row.String("not an integer")
	status, err := row.End()
	if err != nil {
		t.Fatal(err)
	}
	if status != RowTypeMismatch {
		t.Fatalf("got status %d, want type mismatch", status)
	}
	if err := writer.session.packets.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []byte{2, 0, 0, 0, 0, 0x04}
	if !bytes.Equal(conn.Bytes(), want) {
		t.Fatalf("got packet %v, want %v", conn.Bytes(), want)
	}
}
