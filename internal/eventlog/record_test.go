package eventlog

import "testing"

func TestRecordRoundtrip(t *testing.T) {
	header := []byte("h")
	payload := []byte("payload")
	rec := EncodeRecord(header, payload)
	dec, ok := DecodeRecord(rec)
	if !ok {
		t.Fatalf("decode failed")
	}
	if string(dec.Header) != string(header) {
		t.Fatalf("header mismatch")
	}
	if string(dec.Payload) != string(payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestRecordCRCFail(t *testing.T) {
	h := []byte("x")
	p := []byte("y")
	rec := EncodeRecord(h, p)
	rec[len(rec)-1] ^= 0xFF // corrupt one byte
	if _, ok := DecodeRecord(rec); ok {
		t.Fatalf("expected crc failure")
	}
}
