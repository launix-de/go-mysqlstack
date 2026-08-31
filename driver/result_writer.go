/*
 * go-mysqlstack
 *
 * Copyright (C) 2026 Carl-Philip Haensch
 * GPL License
 */

package driver

import (
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/launix-de/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/launix-de/go-mysqlstack/sqlparser/depends/query"
	"github.com/launix-de/go-mysqlstack/sqlparser/depends/sqltypes"
)

// RowStatus describes recoverable producer mistakes. End pads or truncates the
// row before committing it, so these mistakes can never desynchronize the
// MySQL protocol stream.
type RowStatus uint8

const RowComplete RowStatus = 0

const (
	RowPadded RowStatus = 1 << iota
	RowTruncated
	RowTypeMismatch
)

// ResultWriter streams a result without requiring [][]sqltypes.Value as an
// intermediate representation. A writer belongs to one MySQL command.
type ResultWriter struct {
	session  *Session
	mode     RowMode
	mu       sync.Mutex
	fields   []*querypb.Field
	rowBuf   *common.Buffer
	row      RowWriter
	started  bool
	finished bool
}

// RowWriter encodes one row. It is reused by ResultWriter and must be ended or
// aborted before another row is begun.
type RowWriter struct {
	owner       *ResultWriter
	column      int
	nullMaskLen int
	status      RowStatus
	active      bool
}

func newResultWriter(session *Session, mode RowMode) *ResultWriter {
	w := &ResultWriter{
		session: session,
		mode:    mode,
		rowBuf:  common.NewBuffer(256),
	}
	w.row.owner = w
	return w
}

// SetFields publishes the result schema before the first row.
func (w *ResultWriter) SetFields(fields []*querypb.Field) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.finished {
		return fmt.Errorf("result fields already written")
	}
	w.fields = fields
	w.started = true
	return w.session.writeBaseRows(w.mode, &sqltypes.Result{Fields: fields, State: sqltypes.RStateFields})
}

// BeginRow obtains the reusable row encoder. The writer serializes concurrent
// producers because packet sequence numbers and the socket stream are ordered.
func (w *ResultWriter) BeginRow() (*RowWriter, error) {
	w.mu.Lock()
	if !w.started || w.finished || len(w.fields) == 0 {
		w.mu.Unlock()
		return nil, fmt.Errorf("result fields must be written before rows")
	}
	w.row.column = 0
	w.row.status = RowComplete
	w.row.active = true
	w.row.nullMaskLen = 0
	w.rowBuf.Clear()
	if w.mode == BinaryRowMode {
		w.row.nullMaskLen = (len(w.fields) + 7 + 2) / 8
		w.rowBuf.WriteU8(0)
		w.rowBuf.WriteZero(w.row.nullMaskLen)
	}
	return &w.row, nil
}

func (r *RowWriter) nextField() (*querypb.Field, int, bool) {
	if !r.active {
		return nil, 0, false
	}
	column := r.column
	r.column++
	if column >= len(r.owner.fields) {
		r.status |= RowTruncated
		return nil, column, false
	}
	return r.owner.fields[column], column, true
}

func (r *RowWriter) writeNull(column int) {
	if r.owner.mode == TextRowMode {
		r.owner.rowBuf.WriteLenEncodeNUL()
		return
	}
	mask := r.owner.rowBuf.Datas()[1 : 1+r.nullMaskLen]
	mask[(column+2)/8] |= 1 << uint((column+2)%8)
}

// Null appends SQL NULL.
func (r *RowWriter) Null() {
	_, column, ok := r.nextField()
	if ok {
		r.writeNull(column)
	}
}

func writeTextInt64(buffer *common.Buffer, value int64) {
	var scratch [20]byte
	buffer.WriteLenEncodeBytes(strconv.AppendInt(scratch[:0], value, 10))
}

func writeTextFloat64(buffer *common.Buffer, value float64) {
	var scratch [32]byte
	buffer.WriteLenEncodeBytes(strconv.AppendFloat(scratch[:0], value, 'g', -1, 64))
}

func writeBinaryInt64(buffer *common.Buffer, typ querypb.Type, value int64) bool {
	switch typ {
	case sqltypes.Int8, sqltypes.Uint8:
		buffer.WriteU8(uint8(value))
	case sqltypes.Int16, sqltypes.Uint16, sqltypes.Year:
		buffer.WriteU16(uint16(value))
	case sqltypes.Int24, sqltypes.Uint24, sqltypes.Int32, sqltypes.Uint32:
		buffer.WriteU32(uint32(value))
	case sqltypes.Int64, sqltypes.Uint64:
		buffer.WriteU64(uint64(value))
	default:
		return false
	}
	return true
}

func isBinaryLengthEncoded(typ querypb.Type) bool {
	return sqltypes.IsQuoted(typ) && !sqltypes.IsTemporal(typ) || typ == sqltypes.Decimal
}

// Int64 appends an integer without first materializing its ASCII form.
func (r *RowWriter) Int64(value int64) {
	field, column, ok := r.nextField()
	if !ok {
		return
	}
	if r.owner.mode == TextRowMode {
		writeTextInt64(r.owner.rowBuf, value)
		return
	}
	if writeBinaryInt64(r.owner.rowBuf, field.Type, value) {
		return
	}
	if sqltypes.IsFloat(field.Type) {
		r.writeBinaryFloat(field.Type, float64(value))
		return
	}
	if isBinaryLengthEncoded(field.Type) {
		var scratch [20]byte
		r.owner.rowBuf.WriteLenEncodeBytes(strconv.AppendInt(scratch[:0], value, 10))
		return
	}
	r.status |= RowTypeMismatch
	r.writeNull(column)
}

func (r *RowWriter) writeBinaryFloat(typ querypb.Type, value float64) {
	if typ == sqltypes.Float32 {
		r.owner.rowBuf.WriteU32(math.Float32bits(float32(value)))
	} else {
		r.owner.rowBuf.WriteU64(math.Float64bits(value))
	}
}

// Float64 appends a floating-point value without an intermediate allocation.
func (r *RowWriter) Float64(value float64) {
	field, column, ok := r.nextField()
	if !ok {
		return
	}
	if r.owner.mode == TextRowMode {
		writeTextFloat64(r.owner.rowBuf, value)
		return
	}
	if sqltypes.IsFloat(field.Type) {
		r.writeBinaryFloat(field.Type, value)
		return
	}
	if isBinaryLengthEncoded(field.Type) {
		var scratch [32]byte
		r.owner.rowBuf.WriteLenEncodeBytes(strconv.AppendFloat(scratch[:0], value, 'g', -1, 64))
		return
	}
	r.status |= RowTypeMismatch
	r.writeNull(column)
}

// String appends a string directly from its backing storage.
func (r *RowWriter) String(value string) {
	field, column, ok := r.nextField()
	if !ok {
		return
	}
	if r.owner.mode == TextRowMode || isBinaryLengthEncoded(field.Type) {
		r.owner.rowBuf.WriteLenEncodeString(value)
		return
	}
	if sqltypes.IsTemporal(field.Type) {
		encoded, err := sqltypes.MakeTrusted(field.Type, common.StringToBytes(value)).ToMySQL()
		if err == nil {
			r.owner.rowBuf.WriteBytes(encoded)
			return
		}
	}
	if sqltypes.IsSigned(field.Type) {
		value, err := strconv.ParseInt(value, 10, 64)
		if err == nil && writeBinaryInt64(r.owner.rowBuf, field.Type, value) {
			return
		}
	} else if sqltypes.IsUnsigned(field.Type) {
		value, err := strconv.ParseUint(value, 10, 64)
		if err == nil && writeBinaryInt64(r.owner.rowBuf, field.Type, int64(value)) {
			return
		}
	} else if sqltypes.IsFloat(field.Type) {
		value, err := strconv.ParseFloat(value, 64)
		if err == nil {
			r.writeBinaryFloat(field.Type, value)
			return
		}
	}
	r.status |= RowTypeMismatch
	r.writeNull(column)
}

// Bool appends a MySQL-compatible boolean integer.
func (r *RowWriter) Bool(value bool) {
	if value {
		r.Int64(1)
	} else {
		r.Int64(0)
	}
}

// End pads missing columns with NULL and commits exactly one valid packet.
func (r *RowWriter) End() (RowStatus, error) {
	if !r.active {
		return r.status, fmt.Errorf("row is not active")
	}
	if r.column < len(r.owner.fields) {
		r.status |= RowPadded
		for r.column < len(r.owner.fields) {
			column := r.column
			r.column++
			r.writeNull(column)
		}
	}
	err := r.owner.session.packets.Append(r.owner.rowBuf.Datas())
	status := r.status
	r.active = false
	r.owner.mu.Unlock()
	return status, err
}

// Abort discards an unfinished row and releases its writer lock.
func (r *RowWriter) Abort() {
	if r.active {
		r.active = false
		r.owner.mu.Unlock()
	}
}

// Finish terminates the result or emits an OK packet for a fieldless result.
func (w *ResultWriter) Finish(rowsAffected, insertID uint64, warnings uint16) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return nil
	}
	w.finished = true
	state := sqltypes.RStateNone
	if w.started {
		state = sqltypes.RStateFinished
	}
	return w.session.writeBaseRows(w.mode, &sqltypes.Result{
		Fields:       w.fields,
		RowsAffected: rowsAffected,
		InsertID:     insertID,
		Warnings:     warnings,
		State:        state,
	})
}

// WriteResult keeps generic mysqlstack handlers concise while routing their
// existing sqltypes.Result values through the same command writer.
func (w *ResultWriter) WriteResult(result *sqltypes.Result) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(result.Fields) != 0 {
		w.fields = result.Fields
	}
	if result.State == sqltypes.RStateFields || (result.State == sqltypes.RStateNone && len(result.Fields) != 0) {
		w.started = true
	}
	if result.State == sqltypes.RStateFinished || result.State == sqltypes.RStateNone {
		w.finished = true
	}
	return w.session.writeBaseRows(w.mode, result)
}

func (w *ResultWriter) isFinished() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.finished
}
