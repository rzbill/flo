package workqueue

import "testing"

func TestRecordRoundtrip(t *testing.T) {
	h := []byte("h")
	p := []byte("payload")
	enc := EncodeMessage(h, p)
	dec, ok := DecodeMessage(enc)
	if !ok {
		t.Fatalf("decode failed")
	}
	if string(dec.Header) != string(h) || string(dec.Payload) != string(p) {
		t.Fatalf("mismatch")
	}
}

func TestRecordCRCFail(t *testing.T) {
	enc := EncodeMessage([]byte("a"), []byte("b"))
	enc[len(enc)-1] ^= 0xFF
	if _, ok := DecodeMessage(enc); ok {
		t.Fatalf("expected crc fail")
	}
}
