package workqueue

import (
	"encoding/binary"
	"hash/crc32"
)

// Message record: headerLen(4B BE) | header | payload | crc32c(header|payload)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func EncodeMessage(header, payload []byte) []byte {
	hlen := uint32(len(header))
	out := make([]byte, 0, 4+len(header)+len(payload)+4)
	var hb [4]byte
	binary.BigEndian.PutUint32(hb[:], hlen)
	out = append(out, hb[:]...)
	out = append(out, header...)
	out = append(out, payload...)
	crc := crc32.Update(0, castagnoli, header)
	crc = crc32.Update(crc, castagnoli, payload)
	var cb [4]byte
	binary.BigEndian.PutUint32(cb[:], crc)
	out = append(out, cb[:]...)
	return out
}

type Decoded struct {
	Header  []byte
	Payload []byte
}

func DecodeMessage(b []byte) (Decoded, bool) {
	if len(b) < 8 {
		return Decoded{}, false
	}
	hlen := binary.BigEndian.Uint32(b[:4])
	if int(4+hlen+4) > len(b) {
		return Decoded{}, false
	}
	headerEnd := 4 + int(hlen)
	header := b[4:headerEnd]
	payload := b[headerEnd : len(b)-4]
	expect := binary.BigEndian.Uint32(b[len(b)-4:])
	crc := crc32.Update(0, castagnoli, header)
	crc = crc32.Update(crc, castagnoli, payload)
	if crc != expect {
		return Decoded{}, false
	}
	return Decoded{Header: append([]byte(nil), header...), Payload: append([]byte(nil), payload...)}, true
}
