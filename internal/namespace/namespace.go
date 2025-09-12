package namespace

import (
	"encoding/json"
	"time"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

// Meta holds namespace metadata and optional limits/overrides.
type Meta struct {
	Name            string `json:"name"`
	CreatedAtMs     int64  `json:"createdAtMs"`
	Partitions      int    `json:"partitions"`
	PayloadMaxBytes int    `json:"payloadMaxBytes"`
	HeadersMaxBytes int    `json:"headersMaxBytes"`
}

// Defaults returns opinionated defaults for new namespaces.
func Defaults() Meta {
	return Meta{
		Partitions:      16,
		PayloadMaxBytes: 1 << 20,  // 1 MiB
		HeadersMaxBytes: 16 << 10, // 16 KiB
	}
}

var (
	nsMetaPrefix = []byte("nsmeta/")
)

// nsMetaKey builds metadata key for a namespace.
func nsMetaKey(ns string) []byte {
	k := make([]byte, 0, len(nsMetaPrefix)+len(ns))
	k = append(k, nsMetaPrefix...)
	k = append(k, ns...)
	return k
}

// EnsureNamespace creates a namespace meta record if absent, returning the effective meta.
// Idempotent: returns existing if already present.
func EnsureNamespace(db *pebblestore.DB, name string) (Meta, error) {
	key := nsMetaKey(name)
	if b, err := db.Get(key); err == nil && len(b) > 0 {
		var m Meta
		if err := json.Unmarshal(b, &m); err == nil {
			return m, nil
		}
		// fallthrough to rewrite if corrupted
	}
	m := Defaults()
	m.Name = name
	m.CreatedAtMs = time.Now().UnixMilli()
	bytes, err := json.Marshal(m)
	if err != nil {
		return Meta{}, err
	}
	if err := db.Set(key, bytes); err != nil {
		return Meta{}, err
	}
	return m, nil
}
