package eventlog

import (
	"encoding/binary"
	"hash/crc32"
)

// Record encoding: varint headerLen | header | payload | crc32c(header|payload)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func EncodeRecord(header, payload []byte) []byte {
	out := make([]byte, 0, 10+len(header)+len(payload)+4)
	// write varint header length
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], uint64(len(header)))
	out = append(out, tmp[:n]...)
	out = append(out, header...)
	out = append(out, payload...)

	crc := crc32.Update(0, castagnoli, header)
	crc = crc32.Update(crc, castagnoli, payload)
	var crcb [4]byte
	binary.BigEndian.PutUint32(crcb[:], crc)
	out = append(out, crcb[:]...)
	return out
}

type Decoded struct {
	Header  []byte
	Payload []byte
}

func DecodeRecord(b []byte) (Decoded, bool) {
	if len(b) < 1+4 {
		return Decoded{}, false
	}
	hlen, n := binary.Uvarint(b)
	if n <= 0 {
		return Decoded{}, false
	}
	if int(n)+int(hlen)+4 > len(b) {
		return Decoded{}, false
	}
	header := b[n : n+int(hlen)]
	payload := b[n+int(hlen) : len(b)-4]
	expect := binary.BigEndian.Uint32(b[len(b)-4:])
	crc := crc32.Update(0, castagnoli, header)
	crc = crc32.Update(crc, castagnoli, payload)
	if crc != expect {
		return Decoded{}, false
	}
	return Decoded{Header: append([]byte(nil), header...), Payload: append([]byte(nil), payload...)}, true
}
