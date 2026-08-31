/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * Copyright (C) 2026 Carl-Philip Haensch
 * GPL License
 *
 */

package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/launix-de/go-mysqlstack/sqlparser/depends/common"
)

// TEST EFFECTS:
// writes normal packet
//
// TEST PROCESSES:
// 1. write datas more than PACKET_BUFFER_SIZE
// 2. write checks
// 3. read checks
func TestStream(t *testing.T) {
	rBuf := NewMockConn()
	defer rBuf.Close()

	wBuf := NewMockConn()
	defer wBuf.Close()

	rStream := NewStream(rBuf, PACKET_MAX_SIZE)
	wStream := NewStream(wBuf, PACKET_MAX_SIZE)

	packet := common.NewBuffer(PACKET_BUFFER_SIZE)
	payload := common.NewBuffer(PACKET_BUFFER_SIZE)

	for i := 0; i < 1234; i++ {
		payload.WriteU8(byte(i))
	}

	packet.WriteU24(uint32(payload.Length()))
	packet.WriteU8(1)
	packet.WriteBytes(payload.Datas())

	// write checks
	{
		err := wStream.Write(packet.Datas())
		assert.Nil(t, err)

		want := packet.Datas()
		got := wBuf.Datas()
		assert.Equal(t, want, got)
	}

	// read checks
	{
		rBuf.Write(wBuf.Datas())
		ptk, err := rStream.Read()
		assert.Nil(t, err)

		assert.Equal(t, byte(0x01), ptk.SequenceID)
		assert.Equal(t, payload.Datas(), ptk.Datas)
	}
}

// TEST EFFECTS:
// write packet whoes payload length equals pktMaxSize
//
// TEST PROCESSES:
// 1. write payload whoes length equals pktMaxSize
// 2. read checks
// 3. write checks
func TestStreamWriteMax(t *testing.T) {
	rBuf := NewMockConn()
	defer rBuf.Close()

	wBuf := NewMockConn()
	defer wBuf.Close()

	pktMaxSize := 64
	rStream := NewStream(rBuf, pktMaxSize)
	wStream := NewStream(wBuf, pktMaxSize)

	packet := common.NewBuffer(PACKET_BUFFER_SIZE)
	expect := common.NewBuffer(PACKET_BUFFER_SIZE)
	payload := common.NewBuffer(PACKET_BUFFER_SIZE)

	{
		for i := 0; i < (pktMaxSize+1)/4; i++ {
			payload.WriteU32(uint32(i))
		}
	}
	packet.WriteU24(uint32(payload.Length()))
	packet.WriteU8(1)
	packet.WriteBytes(payload.Datas())

	// write checks
	{
		err := wStream.Write(packet.Datas())
		assert.Nil(t, err)

		// check length
		{
			want := packet.Length() + 4
			got := len(wBuf.Datas())
			assert.Equal(t, want, got)
		}

		// check chunks
		{
			// first chunk
			expect.WriteU24(uint32(pktMaxSize))
			expect.WriteU8(1)
			expect.WriteBytes(payload.Datas()[:pktMaxSize])

			// second chunk
			expect.WriteU24(0)
			expect.WriteU8(2)

			want := expect.Datas()
			got := wBuf.Datas()
			assert.Equal(t, want, got)
		}
	}

	// read checks
	{
		rBuf.Write(wBuf.Datas())
		ptk, err := rStream.Read()
		assert.Nil(t, err)

		assert.Equal(t, byte(0x02), ptk.SequenceID)
		assert.Equal(t, payload.Datas(), ptk.Datas)
	}
}

// TEST EFFECTS:
// write packet whoes payload length more than pktMaxSizie
//
// TEST PROCESSES:
// 1. write payload whoes length (pktMaxSizie + 8)
// 2. read checks
// 3. write checks
func TestStreamWriteOverMax(t *testing.T) {
	rBuf := NewMockConn()
	defer rBuf.Close()

	wBuf := NewMockConn()
	defer wBuf.Close()

	pktMaxSize := 63
	rStream := NewStream(rBuf, pktMaxSize)
	wStream := NewStream(wBuf, pktMaxSize)

	packet := common.NewBuffer(PACKET_BUFFER_SIZE)
	expect := common.NewBuffer(PACKET_BUFFER_SIZE)
	payload := common.NewBuffer(PACKET_BUFFER_SIZE)

	{
		for i := 0; i < pktMaxSize/4; i++ {
			payload.WriteU32(uint32(i))
		}
	}
	// fill with 8bytes
	payload.WriteU32(32)
	payload.WriteU32(32)

	packet.WriteU24(uint32(payload.Length()))
	packet.WriteU8(1)
	packet.WriteBytes(payload.Datas())

	// write checks
	{
		err := wStream.Write(packet.Datas())
		assert.Nil(t, err)

		// check length
		{
			want := packet.Length() + 4
			got := len(wBuf.Datas())
			assert.Equal(t, want, got)
		}

		// check chunks
		{
			// first chunk
			expect.WriteU24(uint32(pktMaxSize))
			expect.WriteU8(1)
			expect.WriteBytes(payload.Datas()[:pktMaxSize])

			// second chunk
			left := (packet.Length() - 4) - pktMaxSize
			expect.WriteU24(uint32(left))
			expect.WriteU8(2)
			expect.WriteBytes(payload.Datas()[pktMaxSize:])

			want := expect.Datas()
			got := wBuf.Datas()
			assert.Equal(t, want, got)
		}
	}

	// read checks
	{
		rBuf.Write(wBuf.Datas())
		ptk, err := rStream.Read()
		assert.Nil(t, err)

		assert.Equal(t, byte(0x02), ptk.SequenceID)
		assert.Equal(t, payload.Datas(), ptk.Datas)
		_, err = rStream.Read()
		assert.NotNil(t, err)
	}
}

func TestStreamAppendPayloadChunks(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    []byte
	}{
		{name: "empty", want: []byte{0, 0, 0, 7}},
		{name: "short", payload: []byte{1, 2, 3}, want: []byte{3, 0, 0, 7, 1, 2, 3}},
		{name: "exact", payload: []byte{1, 2, 3, 4}, want: []byte{4, 0, 0, 7, 1, 2, 3, 4, 0, 0, 0, 8}},
		{name: "split", payload: []byte{1, 2, 3, 4, 5}, want: []byte{4, 0, 0, 7, 1, 2, 3, 4, 1, 0, 0, 8, 5}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := NewMockConn()
			defer conn.Close()
			stream := NewStream(conn, 4)
			assert.NoError(t, stream.appendPayload(test.payload, 7))
			assert.NoError(t, stream.Flush())
			assert.Equal(t, test.want, conn.Datas())
		})
	}
}
