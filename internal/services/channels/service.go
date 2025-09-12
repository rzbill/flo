package channelsvc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"time"

	"github.com/rzbill/flo/internal/eventlog"
	"github.com/rzbill/flo/internal/namespace"
	"github.com/rzbill/flo/internal/runtime"
)

const defaultMaxAttempts = 5

type Service struct{ rt *runtime.Runtime }

func New(rt *runtime.Runtime) *Service { return &Service{rt: rt} }

func (s *Service) EnsureNamespace(ctx context.Context, ns string) (namespace.Meta, error) {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}
	return s.rt.EnsureNamespace(ns)
}

// ChannelMeta stores per-channel overrides and labels (future use).
type ChannelMeta struct {
	Partitions int               `json:"partitions"`
	Labels     map[string]string `json:"labels,omitempty"`
}

func (s *Service) readChannelMeta(ns, channel string) (ChannelMeta, bool) {
	b, err := s.rt.DB().Get(chanMetaKey(ns, channel))
	if err != nil || len(b) == 0 {
		return ChannelMeta{}, false
	}
	var m ChannelMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return ChannelMeta{}, false
	}
	return m, true
}

func (s *Service) writeChannelMeta(ns, channel string, m ChannelMeta) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return s.rt.DB().Set(chanMetaKey(ns, channel), b)
}

func (s *Service) CreateChannel(ctx context.Context, ns, name string, partitions int) error {
	// Ensure namespace exists
	if _, err := s.EnsureNamespace(ctx, ns); err != nil {
		return err
	}
	if partitions > 0 {
		// Persist override
		meta := ChannelMeta{Partitions: partitions}
		return s.writeChannelMeta(ns, name, meta)
	}
	return nil
}

func (s *Service) Publish(ctx context.Context, ns, channel string, payload []byte, headers map[string]string, key string) ([]byte, error) {
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return nil, err
	}
	// Idempotency best-effort via per-channel index
	if idk := pickIdempotencyKey(headers); idk != "" {
		if b, err := s.rt.DB().Get(idemKey(metaNS.Name, channel, idk)); err == nil && len(b) == 8 {
			out := make([]byte, 8)
			copy(out, b)
			return out, nil
		}
	}
	// Determine partitions (channel override > namespace default)
	parts := metaNS.Partitions
	if cm, ok := s.readChannelMeta(metaNS.Name, channel); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}
	var part uint32
	if key != "" && parts > 0 {
		part = uint32(crc32.ChecksumIEEE([]byte(key)) % uint32(parts))
	}
	log, err := s.rt.OpenLog(metaNS.Name, channel, part)
	if err != nil {
		return nil, err
	}
	seqs, err := log.Append(ctx, []eventlog.AppendRecord{{Header: nil, Payload: payload}})
	if err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		return nil, nil
	}
	tok := tokenFromSeq(seqs[0])
	b := make([]byte, len(tok))
	copy(b, tok[:])
	if idk := pickIdempotencyKey(headers); idk != "" {
		_ = s.rt.DB().Set(idemKey(metaNS.Name, channel, idk), b)
	}
	return b, nil
}

func (s *Service) OpenLogForRead(ns, channel string, partition uint32) (*eventlog.Log, error) {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}
	return s.rt.OpenLog(ns, channel, partition)
}

func (s *Service) Ack(ctx context.Context, ns, channel, group string, id []byte) error {
	log, err := s.OpenLogForRead(ns, channel, 0)
	if err != nil {
		return err
	}
	var tok eventlog.Token
	if len(id) == 8 {
		copy(tok[:], id)
	}
	return log.CommitCursor(group, tok)
}

// Nack increments attempts, applies exp-jitter backoff, and appends to DLQ when attempts >= defaultMaxAttempts.
func (s *Service) Nack(ctx context.Context, ns, channel, group string, id []byte) error {
	if len(id) != 8 {
		return nil
	}
	key := attemptsKey(ns, channel, group, id)
	var attempts uint32
	if b, err := s.rt.DB().Get(key); err == nil && len(b) == 4 {
		attempts = binary.BigEndian.Uint32(b)
	}
	attempts++
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], attempts)
	_ = s.rt.DB().Set(key, buf[:])
	if attempts >= defaultMaxAttempts {
		// Append to DLQ topic with minimal payload (original id)
		dlqTopic := "dlq/" + channel + "/" + group
		if log, err := s.rt.OpenLog(ns, dlqTopic, 0); err == nil {
			_, _ = log.Append(ctx, []eventlog.AppendRecord{{Header: nil, Payload: append([]byte(nil), id...)}})
		}
		return nil
	}
	// Backoff: exp-jitter (deterministic cap), base=200ms, max=30s
	backoff := time.Duration(200*(1<<(attempts-1))) * time.Millisecond
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}
	retryAt := time.Now().Add(backoff).UnixMilli()
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], uint64(retryAt))
	retryTopic := "retry/" + channel + "/" + group
	if log, err := s.rt.OpenLog(ns, retryTopic, 0); err == nil {
		_, _ = log.Append(ctx, []eventlog.AppendRecord{{Header: hdr[:], Payload: append([]byte(nil), id...)}})
	}
	return nil
}

// SubscribeItem represents a delivered event for streaming.
type SubscribeItem struct {
	ID        []byte
	Payload   []byte
	Headers   map[string]string
	Partition uint32
	Seq       uint64
}

// SubscribeSink is implemented by transports to receive streamed items.
type SubscribeSink interface {
	Send(SubscribeItem) error
	Context() context.Context
	Flush() error
}

// StreamSubscribe performs the read + wait loop and pushes items to the sink.
func (s *Service) StreamSubscribe(ctx context.Context, ns, channel, group string, startToken []byte, sink SubscribeSink) error {
	// Determine partitions for fan-in
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return err
	}
	parts := metaNS.Partitions
	if cm, ok := s.readChannelMeta(metaNS.Name, channel); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}
	// Open retry stream for group
	var retryLog *eventlog.Log
	var retryTok eventlog.Token
	if group != "" {
		retryTopic := "retry/" + channel + "/" + group
		retryLog, _ = s.rt.OpenLog(metaNS.Name, retryTopic, 0)
	}
	if parts <= 1 {
		log, err := s.OpenLogForRead(metaNS.Name, channel, 0)
		if err != nil {
			return err
		}
		var start eventlog.Token
		if len(startToken) == 8 {
			copy(start[:], startToken)
		} else if group != "" {
			if cur, ok := log.GetCursor(group); ok {
				start = tokenFromSeq(cur.Seq() + 1)
			} else {
				items, _ := log.Read(eventlog.ReadOptions{Reverse: true, Limit: 1})
				if len(items) > 0 {
					start = tokenFromSeq(items[0].Seq + 1)
				}
			}
		}
		for {
			// Serve any due retries first
			if retryLog != nil {
				if items, next := retryLog.Read(eventlog.ReadOptions{Start: retryTok, Limit: 1}); len(items) > 0 {
					retryAt := uint64(0)
					if len(items[0].Header) >= 8 {
						retryAt = binary.BigEndian.Uint64(items[0].Header[:8])
					}
					now := uint64(time.Now().UnixMilli())
					if retryAt <= now {
						id := append([]byte(nil), items[0].Payload...)
						if err := sink.Send(SubscribeItem{ID: id, Payload: items[0].Payload, Partition: 0, Seq: items[0].Seq}); err != nil {
							return err
						}
						if err := sink.Flush(); err != nil {
							return err
						}
						retryTok = next
						continue
					}
				}
			}
			items, next := log.Read(eventlog.ReadOptions{Start: start, Limit: 128})
			if len(items) == 0 {
				if !log.WaitForAppend(2 * time.Second) {
					if err := sink.Context().Err(); err != nil {
						return err
					}
					continue
				}
				continue
			}
			for _, it := range items {
				idTok := tokenFromSeq(it.Seq)
				id := make([]byte, len(idTok))
				copy(id, idTok[:])
				if err := sink.Send(SubscribeItem{ID: id, Payload: it.Payload, Partition: 0, Seq: it.Seq}); err != nil {
					return err
				}
			}
			if err := sink.Flush(); err != nil {
				return err
			}
			start = next
		}
	}
	// Multi-partition fan-in
	logs := make([]*eventlog.Log, 0, parts)
	toks := make([]eventlog.Token, parts)
	for p := 0; p < parts; p++ {
		l, err := s.OpenLogForRead(metaNS.Name, channel, uint32(p))
		if err != nil {
			return err
		}
		logs = append(logs, l)
		if len(startToken) == 8 {
			copy(toks[p][:], startToken)
		} else if group != "" {
			if cur, ok := l.GetCursor(group); ok {
				toks[p] = tokenFromSeq(cur.Seq() + 1)
			} else {
				items, _ := l.Read(eventlog.ReadOptions{Reverse: true, Limit: 1})
				if len(items) > 0 {
					toks[p] = tokenFromSeq(items[0].Seq + 1)
				}
			}
		}
	}
	idx := 0
	for {
		// Serve any due retries
		if retryLog != nil {
			if items, next := retryLog.Read(eventlog.ReadOptions{Start: retryTok, Limit: 1}); len(items) > 0 {
				retryAt := uint64(0)
				if len(items[0].Header) >= 8 {
					retryAt = binary.BigEndian.Uint64(items[0].Header[:8])
				}
				now := uint64(time.Now().UnixMilli())
				if retryAt <= now {
					id := append([]byte(nil), items[0].Payload...)
					if err := sink.Send(SubscribeItem{ID: id, Payload: items[0].Payload, Partition: 0, Seq: items[0].Seq}); err != nil {
						return err
					}
					if err := sink.Flush(); err != nil {
						return err
					}
					retryTok = next
					continue
				}
			}
		}
		sent := 0
		for i := 0; i < parts; i++ {
			p := (idx + i) % parts
			l := logs[p]
			items, next := l.Read(eventlog.ReadOptions{Start: toks[p], Limit: 64})
			if len(items) == 0 {
				continue
			}
			for _, it := range items {
				idTok := tokenFromSeq(it.Seq)
				id := make([]byte, len(idTok))
				copy(id, idTok[:])
				if err := sink.Send(SubscribeItem{ID: id, Payload: it.Payload, Partition: uint32(p), Seq: it.Seq}); err != nil {
					return err
				}
				sent++
			}
			toks[p] = next
		}
		if sent > 0 {
			if err := sink.Flush(); err != nil {
				return err
			}
			idx = (idx + 1) % parts
			continue
		}
		// nothing sent; wait on partition 0
		if !logs[0].WaitForAppend(2 * time.Second) {
			if err := sink.Context().Err(); err != nil {
				return err
			}
		}
	}
}

func tokenFromSeq(seq uint64) (t eventlog.Token) {
	t = eventlog.Token{}
	t[0] = byte(seq >> 56)
	t[1] = byte(seq >> 48)
	t[2] = byte(seq >> 40)
	t[3] = byte(seq >> 32)
	t[4] = byte(seq >> 24)
	t[5] = byte(seq >> 16)
	t[6] = byte(seq >> 8)
	t[7] = byte(seq)
	return
}

func pickIdempotencyKey(headers map[string]string) string {
	if headers == nil {
		return ""
	}
	if v, ok := headers["idempotencyKey"]; ok {
		return v
	}
	if v, ok := headers["x-idempotency-key"]; ok {
		return v
	}
	return ""
}

func idemKey(ns, channel, key string) []byte {
	// ns/{ns}/chan/{channel}/idem/{key}
	b := make([]byte, 0, len(ns)+len(channel)+len(key)+20)
	b = append(b, 'n', 's', '/')
	b = append(b, ns...)
	b = append(b, '/', 'c', 'h', 'a', 'n', '/')
	b = append(b, channel...)
	b = append(b, '/', 'i', 'd', 'e', 'm', '/')
	b = append(b, key...)
	return b
}

func chanMetaKey(ns, channel string) []byte {
	// ns/{ns}/chan/{channel}/meta
	b := make([]byte, 0, len(ns)+len(channel)+10)
	b = append(b, 'n', 's', '/')
	b = append(b, ns...)
	b = append(b, '/', 'c', 'h', 'a', 'n', '/')
	b = append(b, channel...)
	b = append(b, '/', 'm', 'e', 't', 'a')
	return b
}

func attemptsKey(ns, channel, group string, id []byte) []byte {
	// ns/{ns}/chan/{channel}/attempts/{group}/{id}
	b := make([]byte, 0, len(ns)+len(channel)+len(group)+30)
	b = append(b, 'n', 's', '/')
	b = append(b, ns...)
	b = append(b, '/', 'c', 'h', 'a', 'n', '/')
	b = append(b, channel...)
	b = append(b, '/', 'a', 't', 't', 'e', 'm', 'p', 't', 's', '/')
	b = append(b, group...)
	b = append(b, '/')
	b = append(b, id...)
	return b
}
