/*
 * go-mysqlstack
 *
 * Copyright (C) 2026 Carl-Philip Haensch
 * GPL License
 */

package driver

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/launix-de/go-mysqlstack/packet"
	querypb "github.com/launix-de/go-mysqlstack/sqlparser/depends/query"
	"github.com/launix-de/go-mysqlstack/sqlparser/depends/sqltypes"
)

type benchmarkConn struct{}

func (benchmarkConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (benchmarkConn) Write(p []byte) (int, error)      { return len(p), nil }
func (benchmarkConn) Close() error                     { return nil }
func (benchmarkConn) LocalAddr() net.Addr              { return benchmarkAddr("local") }
func (benchmarkConn) RemoteAddr() net.Addr             { return benchmarkAddr("remote") }
func (benchmarkConn) SetDeadline(time.Time) error      { return nil }
func (benchmarkConn) SetReadDeadline(time.Time) error  { return nil }
func (benchmarkConn) SetWriteDeadline(time.Time) error { return nil }

type benchmarkAddr string

func (a benchmarkAddr) Network() string { return string(a) }
func (a benchmarkAddr) String() string  { return string(a) }

func benchmarkResult(rows, columns int) *sqltypes.Result {
	result := &sqltypes.Result{Fields: make([]*querypb.Field, columns), Rows: make([][]sqltypes.Value, rows)}
	for column := range result.Fields {
		result.Fields[column] = &querypb.Field{Type: querypb.Type_VARCHAR}
	}
	for row := range result.Rows {
		values := make([]sqltypes.Value, columns)
		for column := range values {
			values[column] = sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("abcdefghij"))
		}
		result.Rows[row] = values
	}
	return result
}

func BenchmarkAppendTextRows1024x10(b *testing.B) {
	session := &Session{packets: packet.NewPackets(benchmarkConn{})}
	result := benchmarkResult(1024, 10)
	b.ReportAllocs()
	b.SetBytes(int64(1024 * 10 * 10))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := session.appendTextRows(result); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendBinaryRows1024x10(b *testing.B) {
	session := &Session{packets: packet.NewPackets(benchmarkConn{})}
	result := benchmarkResult(1024, 10)
	b.ReportAllocs()
	b.SetBytes(int64(1024 * 10 * 10))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := session.appendBinaryRows(result); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkDirectWriter(mode RowMode) *ResultWriter {
	session := &Session{packets: packet.NewPackets(benchmarkConn{})}
	writer := newResultWriter(session, mode)
	writer.started = true
	for i := 0; i < 10; i++ {
		writer.fields = append(writer.fields, &querypb.Field{Type: querypb.Type_VARCHAR})
	}
	return writer
}

func BenchmarkResultWriterTextRows1024x10(b *testing.B) {
	writer := benchmarkDirectWriter(TextRowMode)
	b.ReportAllocs()
	b.SetBytes(int64(1024 * 10 * 10))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for rowIndex := 0; rowIndex < 1024; rowIndex++ {
			row, err := writer.BeginRow()
			if err != nil {
				b.Fatal(err)
			}
			for column := 0; column < 10; column++ {
				row.String("abcdefghij")
			}
			if _, err := row.End(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkResultWriterBinaryRows1024x10(b *testing.B) {
	writer := benchmarkDirectWriter(BinaryRowMode)
	b.ReportAllocs()
	b.SetBytes(int64(1024 * 10 * 10))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for rowIndex := 0; rowIndex < 1024; rowIndex++ {
			row, err := writer.BeginRow()
			if err != nil {
				b.Fatal(err)
			}
			for column := 0; column < 10; column++ {
				row.String("abcdefghij")
			}
			if _, err := row.End(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
