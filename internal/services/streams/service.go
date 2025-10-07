package streamsvc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"strings"
	"time"

	"bytes"
	"encoding/base64"
	"os"
	"strconv"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/rzbill/flo/internal/eventlog"
	"github.com/rzbill/flo/internal/namespace"
	"github.com/rzbill/flo/internal/runtime"
	logpkg "github.com/rzbill/flo/pkg/log"
)

// Service provides publish/subscribe operations built on the internal EventLog.
// It implements idempotent publish, at-least-once delivery with group cursors,
// basic retry/DLQ, and subscribe with optional batching and concurrency.
//
// Performance tunables (env-driven, read at construction time):
//   - FLO_SUB_FLUSH_MS: optional subscribe flush window in ms (default 0).
//     When >0, the subscriber writer coalesces sends for up to the window
//     before flushing. Small values (2–5ms) reduce flush overhead at high QPS.
//   - FLO_SUB_BUF: buffered stream capacity per subscriber (default 1024).
//     Increase for bursty producers or slow transports to avoid backpressure.
type Service struct {
	rt     *runtime.Runtime
	logger logpkg.Logger
	// activeSubs tracks active subscribers per stream and per group.
	// Keys:
	//   total key:   ns + "|" + stream
	//   group key:   ns + "|" + stream + "|" + group
	subsMu     sync.Mutex
	activeSubs map[string]int
	// flushWindow batches subscribe sends up to this duration before flushing.
	flushWindow time.Duration
	// subBufLen controls the buffered stream size per subscriber writer.
	subBufLen int
	// retry policy and pacing
	policy    RetryPolicy
	retryPace time.Duration
}

// New returns a Service using a default logger.
func New(rt *runtime.Runtime) *Service {
	// Backward-compat: default logger
	l := logpkg.NewLogger().With(logpkg.Component("streams"))
	pol := defaultRetryPolicy()
	pace := defaultRetryPace()
	applyPolicyEnv(&pol, &pace)
	return &Service{rt: rt, logger: l, activeSubs: map[string]int{}, flushWindow: readFlushWindow(), subBufLen: readSubBufLen(), policy: pol, retryPace: pace}
}

// NewWithLogger constructs the service with an injected logger.
// NewWithLogger returns a Service using the provided logger.
func NewWithLogger(rt *runtime.Runtime, logger logpkg.Logger) *Service {
	if logger == nil {
		logger = logpkg.NewLogger().With(logpkg.Component("streams"))
	}
	pol := defaultRetryPolicy()
	pace := defaultRetryPace()
	applyPolicyEnv(&pol, &pace)
	return &Service{rt: rt, logger: logger, activeSubs: map[string]int{}, flushWindow: readFlushWindow(), subBufLen: readSubBufLen(), policy: pol, retryPace: pace}
}

func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{Type: BackoffExpJitter, Base: 200 * time.Millisecond, Cap: 30 * time.Second, Factor: 2.0, MaxAttempts: 5}
}

func defaultRetryPace() time.Duration { return 25 * time.Millisecond }

// applyPolicyEnv overrides policy and pace from environment variables when present.
// FLO_STREAMS_BACKOFF_TYPE: exp|exp-jitter|fixed|none
// FLO_STREAMS_BACKOFF_BASE_MS, _CAP_MS, _FACTOR, _MAX_ATTEMPTS, _RETRY_PACE_MS
func applyPolicyEnv(pol *RetryPolicy, pace *time.Duration) {
	if v := os.Getenv("FLO_STREAMS_BACKOFF_TYPE"); v != "" {
		switch strings.ToLower(v) {
		case "exp":
			pol.Type = BackoffExp
		case "exp-jitter":
			pol.Type = BackoffExpJitter
		case "fixed":
			pol.Type = BackoffFixed
		case "none":
			pol.Type = BackoffNone
		}
	}
	if v := os.Getenv("FLO_STREAMS_BACKOFF_BASE_MS"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms >= 0 {
			pol.Base = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv("FLO_STREAMS_BACKOFF_CAP_MS"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms >= 0 {
			pol.Cap = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv("FLO_STREAMS_BACKOFF_FACTOR"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			pol.Factor = f
		}
	}
	if v := os.Getenv("FLO_STREAMS_MAX_ATTEMPTS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			pol.MaxAttempts = uint32(n)
		}
	}
	if v := os.Getenv("FLO_STREAMS_RETRY_PACE_MS"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms >= 0 {
			*pace = time.Duration(ms) * time.Millisecond
		}
	}
}

func computeBackoff(pol RetryPolicy, attempts uint32) time.Duration {
	switch pol.Type {
	case BackoffNone:
		return 0
	case BackoffFixed:
		if pol.Base <= 0 {
			return 0
		}
		if pol.Cap > 0 && pol.Base > pol.Cap {
			return pol.Cap
		}
		return pol.Base
	case BackoffExp, BackoffExpJitter:
		base := pol.Base
		if base <= 0 {
			base = 200 * time.Millisecond
		}
		factor := pol.Factor
		if factor <= 0 {
			factor = 2.0
		}
		delay := float64(base) * pow(factor, float64(attempts-1))
		d := time.Duration(delay)
		if pol.Cap > 0 && d > pol.Cap {
			d = pol.Cap
		}
		if pol.Type == BackoffExpJitter {
			if d <= 0 {
				return 0
			}
			n := rand.Int63n(int64(d))
			return time.Duration(n)
		}
		return d
	default:
		return 0
	}
}

// simple power function to avoid importing math for Pow
func pow(a, b float64) float64 {
	// fast path for integer b in small range is not necessary; use math.Pow equivalent
	// but avoid dependency; repeated multiplication
	if b <= 0 {
		return 1
	}
	// approximate by repeated doubling; for simplicity, loop
	result := 1.0
	n := int(b)
	frac := b - float64(n)
	for i := 0; i < n; i++ {
		result *= a
	}
	if frac != 0 {
		// rough approximation using series for fractional exponent; fallback to e^(ln(a)*b)
		// to avoid math import we'll approximate ln via change-of-base on limited range; keep simple:
		// since precision is not critical for backoff, use a linear blend towards next integer power
		next := result * a
		result = result + (next-result)*frac
	}
	return result
}

// resolvePolicy returns the policy and pace for a given ns/stream/group,
// preferring a stored per-group config, otherwise falling back to service defaults.
func (s *Service) resolvePolicy(ns, stream, group string) (RetryPolicy, time.Duration) {
	pol := s.policy
	pace := s.retryPace
	if group == "" {
		return pol, pace
	}
	// read group config
	if b, err := s.rt.DB().Get(groupCfgKey(ns, stream, group)); err == nil && len(b) > 0 {
		var cfg groupConfig
		if json.Unmarshal(b, &cfg) == nil {
			if cfg.Policy != nil {
				pol = *cfg.Policy
			}
			if cfg.RetryPace != nil {
				pace = *cfg.RetryPace
			}
		}
	}
	return pol, pace
}

// upsertGroupConfig persists the provided policy and pace for a specific group.
// Callers should pass a fully materialized policy (not partial deltas).
func (s *Service) upsertGroupConfig(ns, stream, group string, pol RetryPolicy, pace time.Duration) error {
	cfg := groupConfig{Policy: &pol, RetryPace: &pace, UpdatedAtMs: time.Now().UnixMilli()}
	b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return s.rt.DB().Set(groupCfgKey(ns, stream, group), b)
}

func readFlushWindow() time.Duration {
	if v := os.Getenv("FLO_SUB_FLUSH_MS"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return 0
}

func readSubBufLen() int {
	if v := os.Getenv("FLO_SUB_BUF"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 65536 { // cap unbounded values
				n = 65536
			}
			return n
		}
	}
	return 1024
}

// ListNamespaces returns all namespaces known to the system.
func (s *Service) ListNamespaces(ctx context.Context) ([]string, error) {
	db := s.rt.DB()
	prefix := []byte("nsmeta/")
	hi := append(append([]byte{}, prefix...), 0xFF)
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out []string
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) > len(prefix) {
			out = append(out, string(k[len(prefix):]))
		}
	}
	return out, nil
}

// ListStreams returns known streams for a namespace from metadata and logs.
func (s *Service) ListStreams(ctx context.Context, ns string) ([]string, error) {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}
	db := s.rt.DB()
	set := map[string]struct{}{}
	// from stream meta
	prefix := []byte("ns/")
	prefix = append(prefix, []byte(ns)...)
	prefix = append(prefix, []byte("/stream/")...)
	hi := append(append([]byte{}, prefix...), 0xFF)
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err == nil {
		for ok := it.First(); ok; ok = it.Next() {
			k := it.Key()
			if !bytes.HasSuffix(k, []byte("/meta")) {
				continue
			}
			parts := bytes.Split(k, []byte{'/'})
			if len(parts) >= 5 && bytes.Equal(parts[4], []byte("meta")) {
				set[string(parts[3])] = struct{}{}
			}
		}
		_ = it.Close()
	}
	// from logs
	logPrefix := []byte("ns/")
	logPrefix = append(logPrefix, []byte(ns)...)
	logPrefix = append(logPrefix, []byte("/log/")...)
	logHi := append(append([]byte{}, logPrefix...), 0xFF)
	lit, err := db.NewIter(&pebble.IterOptions{LowerBound: logPrefix, UpperBound: logHi})
	if err == nil {
		for ok := lit.First(); ok; ok = lit.Next() {
			rest := lit.Key()[len(logPrefix):]
			idx := bytes.IndexByte(rest, '/')
			if idx <= 0 {
				continue
			}
			topic := string(rest[:idx])
			set[topic] = struct{}{}
		}
		_ = lit.Close()
	}
	out := make([]string, 0, len(set))
	for ch := range set {
		out = append(out, ch)
	}
	return out, nil
}

// ListAllStreams returns all streams across all namespaces.
func (s *Service) ListAllStreams(ctx context.Context) ([]StreamInfo, error) {
	// Get all namespaces
	namespaces, err := s.ListNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}

	var allStreams []StreamInfo
	for _, ns := range namespaces {
		streams, err := s.ListStreams(ctx, ns)
		if err != nil {
			continue // Skip namespaces with errors
		}
		for _, stream := range streams {
			allStreams = append(allStreams, StreamInfo{
				Namespace: ns,
				Stream:    stream,
			})
		}
	}
	return allStreams, nil
}

// StreamInfo represents a stream with its namespace.
type StreamInfo struct {
	Namespace string `json:"namespace"`
	Stream    string `json:"stream"`
}

func (s *Service) EnsureNamespace(ctx context.Context, ns string) (namespace.Meta, error) {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}
	return s.rt.EnsureNamespace(ns)
}

func (s *Service) readStreamMeta(ns, stream string) (StreamMeta, bool) {
	b, err := s.rt.DB().Get(streamMetaKey(ns, stream))
	if err != nil || len(b) == 0 {
		return StreamMeta{}, false
	}
	var m StreamMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return StreamMeta{}, false
	}
	return m, true
}

func (s *Service) writeStreamMeta(ns, stream string, m StreamMeta) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return s.rt.DB().Set(streamMetaKey(ns, stream), b)
}

func (s *Service) CreateStream(ctx context.Context, ns, name string, partitions int) error {
	// Ensure namespace exists
	if _, err := s.EnsureNamespace(ctx, ns); err != nil {
		return err
	}
	if partitions > 0 {
		// Persist override
		meta := StreamMeta{Partitions: partitions}
		return s.writeStreamMeta(ns, name, meta)
	}
	return nil
}

// CreateStreamWithOptions persists partitions and optional retention/max length.
func (s *Service) CreateStreamWithOptions(ctx context.Context, opts CreateStreamOptions) error {
	if _, err := s.EnsureNamespace(ctx, opts.Namespace); err != nil {
		return err
	}
	meta := StreamMeta{}
	if opts.Partitions > 0 {
		meta.Partitions = opts.Partitions
	}
	if opts.RetentionAgeMs > 0 {
		meta.RetentionAgeMs = opts.RetentionAgeMs
	}
	if opts.MaxLenBytes > 0 {
		meta.MaxLenBytes = opts.MaxLenBytes
	}
	if meta.Partitions == 0 && meta.RetentionAgeMs == 0 && meta.MaxLenBytes == 0 {
		return nil
	}
	return s.writeStreamMeta(opts.Namespace, opts.Name, meta)
}

func (s *Service) Publish(ctx context.Context, ns, stream string, payload []byte, headers map[string]string, key string) ([]byte, error) {
	t0 := time.Now()
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return nil, err
	}
	// Idempotency best-effort via per-stream index
	if idk := pickIdempotencyKey(headers); idk != "" {
		if b, err := s.rt.DB().Get(idemKey(metaNS.Name, stream, idk)); err == nil && len(b) == 8 {
			out := make([]byte, 8)
			copy(out, b)
			return out, nil
		}
	}
	// Determine partitions (stream override > namespace default)
	parts := metaNS.Partitions
	var retentionAgeMs int64
	var maxLenBytes int64
	if cm, ok := s.readStreamMeta(metaNS.Name, stream); ok {
		if cm.Partitions > 0 {
			parts = cm.Partitions
		}
		if cm.RetentionAgeMs > 0 {
			retentionAgeMs = cm.RetentionAgeMs
		}
		if cm.MaxLenBytes > 0 {
			maxLenBytes = cm.MaxLenBytes
		}
	}
	var part uint32
	if key != "" && parts > 0 {
		part = uint32(crc32.ChecksumIEEE([]byte(key)) % uint32(parts))
	}
	log, err := s.rt.OpenLog(metaNS.Name, stream, part)
	if err != nil {
		return nil, err
	}
	// Prepend 8-byte write timestamp (ms) in header for replay/retention decisions
	var hdrTs [8]byte
	binary.BigEndian.PutUint64(hdrTs[:], uint64(time.Now().UnixMilli()))
	headerBuf := make([]byte, 8)
	copy(headerBuf, hdrTs[:])
	if len(headers) > 0 {
		if hb, err := json.Marshal(headers); err == nil {
			headerBuf = append(headerBuf, hb...)
		}
	}
	seqs, err := log.Append(ctx, []eventlog.AppendRecord{{Header: headerBuf, Payload: payload}})
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
		_ = s.rt.DB().Set(idemKey(metaNS.Name, stream, idk), b)
	}
	// Apply best-effort trims based on stream meta
	if retentionAgeMs > 0 {
		_, _, _ = log.TrimOlderThan(context.Background(), time.Now().Add(-time.Duration(retentionAgeMs)*time.Millisecond).UnixMilli(), 2048, 0, func(header []byte) (int64, bool) {
			if len(header) >= 8 {
				return int64(binary.BigEndian.Uint64(header[:8])), true
			}
			return 0, false
		})
	}
	if maxLenBytes > 0 {
		_, _ = log.TrimToMaxBytes(context.Background(), maxLenBytes, 2048, 0)
	}

	// Structured debug log for publish latency/path using fields
	var firstSeq uint64
	if len(seqs) > 0 {
		firstSeq = seqs[0]
	}
	durMs := time.Since(t0).Milliseconds()
	s.logger.With(
		logpkg.Str("ns", metaNS.Name),
		logpkg.Str("stream", stream),
		logpkg.Int("part", int(part)),
		logpkg.Int("bytes", len(payload)),
		logpkg.Int64("seq", int64(firstSeq)),
		logpkg.Int64("dur_ms", durMs),
	).Debug("streams.publish")

	// Increment metrics counters (best-effort) at event timestamp
	tsMs := int64(binary.BigEndian.Uint64(hdrTs[:]))
	s.incrementPublishCounters(metaNS.Name, stream, part, tsMs, int64(len(payload)))
	return b, nil
}

func (s *Service) OpenLogForRead(ns, stream string, partition uint32) (*eventlog.Log, error) {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}
	return s.rt.OpenLog(ns, stream, partition)
}

// Ack marks a message as acknowledged.
func (s *Service) Ack(ctx context.Context, ns, stream, group string, id []byte) error {
	log, err := s.OpenLogForRead(ns, stream, 0)
	if err != nil {
		return err
	}
	var tok eventlog.Token
	if len(id) == 8 {
		copy(tok[:], id)
	}
	if err := log.CommitCursor(group, tok); err != nil {
		return err
	}
	// Accurate last-delivered time per stream/group for stats (co-located with cursor keyspace)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixMilli()))
	_ = s.rt.DB().Set(eventlog.KeyCursorLastDelivered(ns, stream, group, 0), b[:])

	// Update delivery status to acknowledged
	s.updateDeliveryStatus(ns, stream, group, id, true, false, 0, "")
	// Increment ack counter using current time as delivery time
	s.incrementAckCounters(ns, stream, time.Now().UnixMilli())
	// Best-effort cleanup of retry scheduling metadata now that the message is acknowledged
	_ = s.rt.DB().Delete(retryNextKey(ns, stream, group, id))
	return nil
}

// Nack increments attempts, applies exp-jitter backoff, and appends to DLQ when attempts >= defaultMaxAttempts.
func (s *Service) Nack(ctx context.Context, ns, stream, group string, id []byte, errorMsg string) error {
	if len(id) != 8 {
		return nil
	}
	// If already DLQed, ignore further nacks for this id/group
	if b, err := s.rt.DB().Get(failureTimeKey(ns, stream, group, id)); err == nil && len(b) == 8 {
		return nil
	}
	key := attemptsKey(ns, stream, group, id)
	var attempts uint32
	if b, err := s.rt.DB().Get(key); err == nil && len(b) == 4 {
		attempts = binary.BigEndian.Uint32(b)
	}
	attempts++
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], attempts)
	_ = s.rt.DB().Set(key, buf[:])

	// Update delivery status to nacked
	s.updateDeliveryStatus(ns, stream, group, id, false, true, attempts, errorMsg)

	// Store error message for debugging
	if errorMsg != "" {
		errorKey := errorKey(ns, stream, group, id)
		_ = s.rt.DB().Set(errorKey, []byte(errorMsg))
	}
	pol, _ := s.resolvePolicy(ns, stream, group)
	if attempts >= pol.MaxAttempts {
		// Store failure time
		failureTime := time.Now().UnixMilli()
		failureKey := failureTimeKey(ns, stream, group, id)
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(failureTime))
		_ = s.rt.DB().Set(failureKey, buf[:])

		// Append to DLQ topic with minimal payload (original id)
		dlqTopic := "dlq/" + stream + "/" + group
		if log, err := s.rt.OpenLog(ns, dlqTopic, 0); err == nil {
			_, _ = log.Append(ctx, []eventlog.AppendRecord{{Header: nil, Payload: append([]byte(nil), id...)}})
		}
		// Cleanup any pending retry schedule marker
		_ = s.rt.DB().Delete(retryNextKey(ns, stream, group, id))
		return nil
	}
	// Backoff by policy
	// compute backoff using resolved policy
	backoff := computeBackoff(pol, attempts)
	nowMs := time.Now().UnixMilli()
	retryAt := nowMs + backoff.Milliseconds()
	// If we already have a not-yet-due retry scheduled, don't append another entry
	if b, err := s.rt.DB().Get(retryNextKey(ns, stream, group, id)); err == nil && len(b) == 8 {
		existing := int64(binary.BigEndian.Uint64(b))
		if existing > nowMs {
			return nil
		}
	}
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], uint64(retryAt))
	retryTopic := "retry/" + stream + "/" + group
	if log, err := s.rt.OpenLog(ns, retryTopic, 0); err == nil {
		_, _ = log.Append(ctx, []eventlog.AppendRecord{{Header: hdr[:], Payload: append([]byte(nil), id...)}})
	}
	// Persist next retry time to avoid duplicate scheduling bursts
	var nbuf [8]byte
	binary.BigEndian.PutUint64(nbuf[:], uint64(retryAt))
	_ = s.rt.DB().Set(retryNextKey(ns, stream, group, id), nbuf[:])
	return nil
}

// resolveStartTokenSingle decides the starting token for a single-partition subscribe.
func (s *Service) resolveStartTokenSingle(l *eventlog.Log, group string, opts SubscribeOptions, explicit []byte) eventlog.Token {
	var start eventlog.Token
	if len(explicit) == 8 {
		copy(start[:], explicit)
		return start
	}
	if group != "" {
		if cur, ok := l.GetCursor(group); ok {
			return tokenFromSeq(cur.Seq() + 1)
		}
	}
	if opts.AtMs > 0 {
		return s.findStartTokenAt(l, opts.AtMs)
	}
	if opts.From == "earliest" {
		return eventlog.Token{}
	}
	if opts.From == "latest" {
		// latest+1
		items, _ := l.Read(eventlog.ReadOptions{Reverse: true, Limit: 1})
		if len(items) > 0 {
			return tokenFromSeq(items[0].Seq + 1)
		}
		return start
	}
	// Default for no cursor and no explicit from/at is earliest
	return eventlog.Token{}
}

// resolveStartTokensMulti produces per-partition start tokens for multi-partition fan-in.
func (s *Service) resolveStartTokensMulti(ns, stream, group string, parts int, startToken []byte, opts SubscribeOptions) ([]*eventlog.Log, []eventlog.Token, error) {
	logs := make([]*eventlog.Log, 0, parts)
	toks := make([]eventlog.Token, parts)
	for p := 0; p < parts; p++ {
		l, err := s.OpenLogForRead(ns, stream, uint32(p))
		if err != nil {
			return nil, nil, err
		}
		logs = append(logs, l)
		if len(startToken) == 8 {
			copy(toks[p][:], startToken)
			continue
		}
		if group != "" {
			if cur, ok := l.GetCursor(group); ok {
				toks[p] = tokenFromSeq(cur.Seq() + 1)
				continue
			}
		}
		if opts.AtMs > 0 {
			toks[p] = s.findStartTokenAt(l, opts.AtMs)
		} else if opts.From == "earliest" {
			toks[p] = eventlog.Token{}
		} else if opts.From == "latest" {
			items, _ := l.Read(eventlog.ReadOptions{Reverse: true, Limit: 1})
			if len(items) > 0 {
				toks[p] = tokenFromSeq(items[0].Seq + 1)
			}
		} else {
			// Default for no cursor and no explicit from/at is earliest
			toks[p] = eventlog.Token{}
		}
	}
	return logs, toks, nil
}

// StreamSubscribe supports from/at start positions.
func (s *Service) StreamSubscribe(ctx context.Context, ns, stream, group string, startToken []byte, opts SubscribeOptions, sink SubscribeSink) error {
	// Determine partitions for fan-in
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return err
	}
	// Track active subscribers (total and per-group)
	totalKey := subKeyTotal(metaNS.Name, stream)
	s.incSub(totalKey)
	defer s.decSub(totalKey)
	var groupKey string
	if group != "" {
		groupKey = subKeyGroup(metaNS.Name, stream, group)
		s.incSub(groupKey)
		defer s.decSub(groupKey)
	}
	parts := metaNS.Partitions
	if cm, ok := s.readStreamMeta(metaNS.Name, stream); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}

	// Open retry stream for group
	var retryLog *eventlog.Log
	if group != "" {
		retryTopic := "retry/" + stream + "/" + group
		retryLog, _ = s.rt.OpenLog(metaNS.Name, retryTopic, 0)
	}

	// Per-subscriber async writer to decouple slow transports
	outCh := make(chan SubscribeItem, s.subBufLen)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pending := 0
		var ticker *time.Timer
		if s.flushWindow > 0 {
			ticker = time.NewTimer(s.flushWindow)
			defer ticker.Stop()
		}
		flush := func() {
			if pending > 0 {
				_ = sink.Flush()
				pending = 0
			}
		}
		for {
			select {
			case it, ok := <-outCh:
				if !ok {
					flush()
					return
				}
				_ = sink.Send(it)
				pending++
				if s.flushWindow == 0 || pending >= 64 {
					flush()
					if ticker != nil {
						if !ticker.Stop() {
							select {
							case <-ticker.C:
							default:
							}
						}
						ticker.Reset(s.flushWindow)
					}
				}
			case <-sink.Context().Done():
				return
			case <-func() <-chan time.Time {
				if ticker != nil {
					return ticker.C
				}
				return make(chan time.Time)
			}():
				flush()
				if ticker != nil {
					ticker.Reset(s.flushWindow)
				}
			}
		}
	}()
	defer func() { close(outCh); wg.Wait() }()

	// Persist policy/pace overrides if provided and a group is set
	if group != "" {
		if opts.Policy != nil || opts.RetryPace != nil {
			pol, pace := s.resolvePolicy(metaNS.Name, stream, group)
			if opts.Policy != nil {
				pol = *opts.Policy
			}
			if opts.RetryPace != nil {
				pace = *opts.RetryPace
			}
			_ = s.upsertGroupConfig(metaNS.Name, stream, group, pol, pace)
		}
	}

	// CEL filter helper
	cfilter, err := newCELFilter(opts.Filter)
	if err != nil {
		return err
	}
	eval := func(p int, seq uint64, header []byte, payload []byte) bool {
		return cfilter.Eval(p, seq, header, payload)
	}

	if parts <= 1 {
		return s.streamSubscribeSingle(metaNS.Name, ns, stream, group, startToken, opts, sink, outCh, retryLog, eval)
	}
	return s.streamSubscribeMulti(metaNS.Name, ns, stream, group, parts, startToken, opts, sink, outCh, retryLog, eval)
}

// streamSubscribeSingle handles the single-partition subscribe loop.
func (s *Service) streamSubscribeSingle(metaName string, ns string, stream string, group string, startToken []byte, opts SubscribeOptions, sink SubscribeSink, outCh chan SubscribeItem, retryLog *eventlog.Log, eval func(p int, seq uint64, header []byte, payload []byte) bool) error {
	log, err := s.OpenLogForRead(metaName, stream, 0)
	if err != nil {
		return err
	}
	start := s.resolveStartTokenSingle(log, group, opts, startToken)
	var retryTok eventlog.Token
	messageCount := 0
	for {
		if err := sink.Context().Err(); err != nil {
			return err
		}
		if opts.Limit > 0 && messageCount >= opts.Limit {
			return nil
		}
		sendStart := time.Now()
		batchCount := 0
		e2eSumMs := int64(0)
		if retryLog != nil {
			if items, next := retryLog.Read(eventlog.ReadOptions{Start: retryTok, Limit: 1}); len(items) > 0 {
				retryAt := uint64(0)
				if len(items[0].Header) >= 8 {
					retryAt = binary.BigEndian.Uint64(items[0].Header[:8])
				}
				now := uint64(time.Now().UnixMilli())
				if retryAt <= now {
					id := append([]byte(nil), items[0].Payload...)
					// Load original message for payload and headers
					orig, err := s.GetMessageByID(sink.Context(), ns, stream, id)
					if err != nil {
						// Fallback to sending id-only payload if original missing
						outCh <- SubscribeItem{ID: id, Payload: id, Partition: 0, Seq: items[0].Seq}
					} else {
						hdrs := s.extractHeadersFromHeader(orig.Header)
						outCh <- SubscribeItem{ID: id, Payload: orig.Payload, Headers: hdrs, Partition: orig.Partition, Seq: orig.Seq, WriteTsMs: s.extractCreatedAtMsFromHeader(orig.Header)}
					}
					s.trackMessageDelivery(ns, stream, group, id)
					retryTok = next
					batchCount++
					// Count retry deliveries toward the limit to avoid repeated immediate re-deliveries in one session
					messageCount++
					_, pace := s.resolvePolicy(ns, stream, group)
					if pace > 0 {
						select {
						case <-time.After(pace):
						case <-sink.Context().Done():
							return sink.Context().Err()
						}
					}
					if len(items[0].Header) >= 8 {
						wms := int64(binary.BigEndian.Uint64(items[0].Header[:8]))
						if wms > 0 {
							e2eSumMs += time.Now().UnixMilli() - wms
						}
					}
					continue
				}
			}
		}
		if err := sink.Context().Err(); err != nil {
			return err
		}
		items, _ := log.Read(eventlog.ReadOptions{Start: start, Limit: 128})
		if len(items) == 0 {
			if !log.WaitForAppend(50 * time.Millisecond) {
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
			if eval(0, it.Seq, it.Header, it.Payload) {
				if opts.Limit > 0 && messageCount >= opts.Limit {
					return nil
				}
				hdrs := s.extractHeadersFromHeader(it.Header)
				outCh <- SubscribeItem{ID: id, Payload: it.Payload, Headers: hdrs, Partition: 0, Seq: it.Seq}
				s.trackMessageDelivery(ns, stream, group, id)
				messageCount++
				batchCount++
				if len(it.Header) >= 8 {
					wms := int64(binary.BigEndian.Uint64(it.Header[:8]))
					if wms > 0 {
						e2eSumMs += time.Now().UnixMilli() - wms
					}
				}
			}
		}
		lastSeq := items[len(items)-1].Seq
		start = tokenFromSeq(lastSeq + 1)
		durMs := time.Since(sendStart).Milliseconds()
		avgE2E := int64(0)
		if batchCount > 0 {
			avgE2E = e2eSumMs / int64(batchCount)
		}
		s.logger.With(
			logpkg.Str("ns", ns),
			logpkg.Str("stream", stream),
			logpkg.Str("group", group),
			logpkg.Int("batch_n", batchCount),
			logpkg.Int64("deliver_ms", durMs),
			logpkg.Int64("end_to_end_ms", avgE2E),
			logpkg.Int("q_depth", len(outCh)),
			logpkg.Int("q_cap", cap(outCh)),
		).Debug("streams.deliver")
	}
}

// streamSubscribeMulti handles the multi-partition fan-in subscribe loop.
func (s *Service) streamSubscribeMulti(metaName string, ns string, stream string, group string, parts int, startToken []byte, opts SubscribeOptions, sink SubscribeSink, outCh chan SubscribeItem, retryLog *eventlog.Log, eval func(p int, seq uint64, header []byte, payload []byte) bool) error {
	logs, toks, err := s.resolveStartTokensMulti(metaName, stream, group, parts, startToken, opts)
	if err != nil {
		return err
	}
	var retryTok eventlog.Token
	idx := 0
	messageCount := 0
	for {
		if err := sink.Context().Err(); err != nil {
			return err
		}
		if opts.Limit > 0 && messageCount >= opts.Limit {
			return nil
		}
		sendStart := time.Now()
		batchCount := 0
		e2eSumMs := int64(0)
		if retryLog != nil {
			if items, next := retryLog.Read(eventlog.ReadOptions{Start: retryTok, Limit: 1}); len(items) > 0 {
				retryAt := uint64(0)
				if len(items[0].Header) >= 8 {
					retryAt = binary.BigEndian.Uint64(items[0].Header[:8])
				}
				now := uint64(time.Now().UnixMilli())
				if retryAt <= now {
					id := append([]byte(nil), items[0].Payload...)
					// Load original message for payload and headers
					orig, err := s.GetMessageByID(sink.Context(), ns, stream, id)
					if err != nil {
						outCh <- SubscribeItem{ID: id, Payload: id, Partition: 0, Seq: items[0].Seq, WriteTsMs: int64(retryAt)}
					} else {
						hdrs := s.extractHeadersFromHeader(orig.Header)
						outCh <- SubscribeItem{ID: id, Payload: orig.Payload, Headers: hdrs, Partition: orig.Partition, Seq: orig.Seq, WriteTsMs: s.extractCreatedAtMsFromHeader(orig.Header)}
					}
					s.trackMessageDelivery(ns, stream, group, id)
					retryTok = next
					batchCount++
					// Count retry deliveries toward the limit in multi-part path as well
					messageCount++
					_, pace := s.resolvePolicy(ns, stream, group)
					if pace > 0 {
						select {
						case <-time.After(pace):
						case <-sink.Context().Done():
							return sink.Context().Err()
						}
					}
					if len(items[0].Header) >= 8 {
						wms := int64(binary.BigEndian.Uint64(items[0].Header[:8]))
						if wms > 0 {
							e2eSumMs += time.Now().UnixMilli() - wms
						}
					}
					continue
				}
			}
		}
		sent := 0
		type partBatch struct {
			p     int
			items []eventlog.Item
		}
		batches := make(chan partBatch, parts)
		var pfwg sync.WaitGroup
		pfwg.Add(parts)
		for i := 0; i < parts; i++ {
			p := (idx + i) % parts
			go func(p int) {
				defer pfwg.Done()
				l := logs[p]
				if err := sink.Context().Err(); err != nil {
					return
				}
				items, _ := l.Read(eventlog.ReadOptions{Start: toks[p], Limit: 64})
				if len(items) == 0 {
					return
				}
				batches <- partBatch{p: p, items: items}
			}(p)
		}
		pfwg.Wait()
		close(batches)
		for b := range batches {
			for _, it := range b.items {
				idTok := tokenFromSeq(it.Seq)
				id := make([]byte, len(idTok))
				copy(id, idTok[:])
				var wms int64
				if len(it.Header) >= 8 {
					wms = int64(binary.BigEndian.Uint64(it.Header[:8]))
				}
				if eval(b.p, it.Seq, it.Header, it.Payload) {
					if opts.Limit > 0 && messageCount >= opts.Limit {
						return nil
					}
					hdrs := s.extractHeadersFromHeader(it.Header)
					outCh <- SubscribeItem{ID: id, Payload: it.Payload, Headers: hdrs, Partition: uint32(b.p), Seq: it.Seq, WriteTsMs: wms}
					s.trackMessageDelivery(ns, stream, group, id)
					sent++
					messageCount++
					batchCount++
					if wms > 0 {
						e2eSumMs += time.Now().UnixMilli() - wms
					}
				}
			}
			lastSeq := b.items[len(b.items)-1].Seq
			toks[b.p] = tokenFromSeq(lastSeq + 1)
		}
		if sent > 0 {
			durMs := time.Since(sendStart).Milliseconds()
			avgE2E := int64(0)
			if batchCount > 0 {
				avgE2E = e2eSumMs / int64(batchCount)
			}
			s.logger.With(
				logpkg.Str("ns", ns),
				logpkg.Str("stream", stream),
				logpkg.Str("group", group),
				logpkg.Int("batch_n", batchCount),
				logpkg.Int64("deliver_ms", durMs),
				logpkg.Int64("end_to_end_ms", avgE2E),
				logpkg.Int("q_depth", len(outCh)),
				logpkg.Int("q_cap", cap(outCh)),
			).Debug("streams.deliver")
			idx = (idx + 1) % parts
			continue
		}
		if !logs[idx%parts].WaitForAppend(50 * time.Millisecond) {
			if err := sink.Context().Err(); err != nil {
				return err
			}
		}
		idx = (idx + 1) % parts
	}
}

// StreamTail streams new messages for a stream without using consumer groups,
// delivery tracking, retries, or acknowledgements. It is intended for UI tailing.
func (s *Service) StreamTail(ctx context.Context, ns, stream string, startToken []byte, opts SubscribeOptions, sink SubscribeSink) error {
	// Determine partitions
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return err
	}
	parts := metaNS.Partitions
	if cm, ok := s.readStreamMeta(metaNS.Name, stream); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}

	// Per-subscriber async writer
	outCh := make(chan SubscribeItem, s.subBufLen)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pending := 0
		var ticker *time.Timer
		if s.flushWindow > 0 {
			ticker = time.NewTimer(s.flushWindow)
			defer ticker.Stop()
		}
		flush := func() {
			if pending > 0 {
				_ = sink.Flush()
				pending = 0
			}
		}
		for {
			select {
			case it, ok := <-outCh:
				if !ok {
					flush()
					return
				}
				_ = sink.Send(it)
				pending++
				if s.flushWindow == 0 || pending >= 64 {
					flush()
					if ticker != nil {
						if !ticker.Stop() {
							select {
							case <-ticker.C:
							default:
							}
						}
						ticker.Reset(s.flushWindow)
					}
				}
			case <-sink.Context().Done():
				return
			case <-func() <-chan time.Time {
				if ticker != nil {
					return ticker.C
				}
				return make(chan time.Time)
			}():
				flush()
				if ticker != nil {
					ticker.Reset(s.flushWindow)
				}
			}
		}
	}()
	defer func() { close(outCh); wg.Wait() }()

	// Optional CEL filter
	cfilter, err := newCELFilter(opts.Filter)
	if err != nil {
		return err
	}
	eval := func(p int, seq uint64, header []byte, payload []byte) bool {
		return cfilter.Eval(p, seq, header, payload)
	}

	if parts <= 1 {
		log, err := s.OpenLogForRead(metaNS.Name, stream, 0)
		if err != nil {
			return err
		}
		// Resolve start token without groups/cursors
		start := s.resolveStartTokenSingle(log, "", opts, startToken)
		for {
			if err := sink.Context().Err(); err != nil {
				return err
			}
			items, _ := log.Read(eventlog.ReadOptions{Start: start, Limit: 128})
			if len(items) == 0 {
				if !log.WaitForAppend(50 * time.Millisecond) {
					if err := sink.Context().Err(); err != nil {
						return err
					}
				}
				continue
			}
			for _, it := range items {
				if eval(0, it.Seq, it.Header, it.Payload) {
					idTok := tokenFromSeq(it.Seq)
					id := make([]byte, len(idTok))
					copy(id, idTok[:])
					hdrs := s.extractHeadersFromHeader(it.Header)
					outCh <- SubscribeItem{ID: id, Payload: it.Payload, Headers: hdrs, Partition: 0, Seq: it.Seq}
				}
			}
			lastSeq := items[len(items)-1].Seq
			start = tokenFromSeq(lastSeq + 1)
		}
	}

	// Multi-partition fan-in without retries/cursors
	logs, toks, err := s.resolveStartTokensMulti(metaNS.Name, stream, "", parts, startToken, opts)
	if err != nil {
		return err
	}
	idx := 0
	messageCount := 0
	for {
		if err := sink.Context().Err(); err != nil {
			return err
		}
		// Check if we've reached the message limit
		if opts.Limit > 0 && messageCount >= opts.Limit {
			return nil
		}
		sent := 0
		type partBatch struct {
			p     int
			items []eventlog.Item
		}
		batches := make(chan partBatch, parts)
		var pfwg sync.WaitGroup
		pfwg.Add(parts)
		for i := 0; i < parts; i++ {
			p := (idx + i) % parts
			go func(p int) {
				defer pfwg.Done()
				if err := sink.Context().Err(); err != nil {
					return
				}
				items, _ := logs[p].Read(eventlog.ReadOptions{Start: toks[p], Limit: 64})
				if len(items) == 0 {
					return
				}
				batches <- partBatch{p: p, items: items}
			}(p)
		}
		pfwg.Wait()
		close(batches)
		for b := range batches {
			for _, it := range b.items {
				if eval(b.p, it.Seq, it.Header, it.Payload) {
					// Check if we've reached the limit before sending this message
					if opts.Limit > 0 && messageCount >= opts.Limit {
						return nil
					}
					idTok := tokenFromSeq(it.Seq)
					id := make([]byte, len(idTok))
					copy(id, idTok[:])
					hdrs := s.extractHeadersFromHeader(it.Header)
					outCh <- SubscribeItem{ID: id, Payload: it.Payload, Headers: hdrs, Partition: uint32(b.p), Seq: it.Seq}
					sent++
					messageCount++
				}
			}
			lastSeq := b.items[len(b.items)-1].Seq
			toks[b.p] = tokenFromSeq(lastSeq + 1)
		}
		if sent > 0 {
			idx = (idx + 1) % parts
			continue
		}
		if !logs[idx%parts].WaitForAppend(50 * time.Millisecond) {
			if err := sink.Context().Err(); err != nil {
				return err
			}
		}
		idx = (idx + 1) % parts
	}
}

// ListMessages returns up to limit messages from a specific partition starting at startToken.
func (s *Service) ListMessages(ctx context.Context, ns, stream string, partition uint32, startToken []byte, limit int, reverse bool) ([]MessageListItem, []byte, error) {
	if limit <= 0 {
		limit = 100
	}
	log, err := s.OpenLogForRead(ns, stream, partition)
	if err != nil {
		return nil, nil, err
	}
	var start eventlog.Token
	if len(startToken) == 8 {
		copy(start[:], startToken)
	}
	items, next := log.Read(eventlog.ReadOptions{Start: start, Limit: limit, Reverse: reverse})
	out := make([]MessageListItem, 0, len(items))
	for _, it := range items {
		idTok := tokenFromSeq(it.Seq)
		id := make([]byte, len(idTok))
		copy(id, idTok[:])
		out = append(out, MessageListItem{ID: id, Payload: it.Payload, Header: it.Header, Partition: partition, Seq: it.Seq})
	}
	var nextBytes []byte
	if next != (eventlog.Token{}) {
		nextBytes = make([]byte, 8)
		copy(nextBytes, next[:])
	}
	return out, nextBytes, nil
}

// Search returns up to Limit messages starting at the specified time anchor or from/earliest.
// Multi-partition fan-in with a simple round-robin scheduler; pagination token encodes per-partition cursors.
func (s *Service) Search(ctx context.Context, ns, stream string, nextToken []byte, opts SearchOptions) ([]MessageListItem, []byte, error) {
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	// Determine partitions
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return nil, nil, err
	}
	parts := metaNS.Partitions
	if cm, ok := s.readStreamMeta(metaNS.Name, stream); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}
	if parts <= 0 {
		parts = 1
	}

	// Open logs and resolve start tokens
	logs := make([]*eventlog.Log, 0, parts)
	toks := make([]eventlog.Token, parts)
	var rotateIdx int
	// Decode next_token if provided
	if len(nextToken) > 0 {
		// token format: base64(join of parts*8 bytes + 4 byte rotateIdx)
		raw := nextToken
		if len(raw) >= parts*8+4 {
			for i := 0; i < parts; i++ {
				copy(toks[i][:], raw[i*8:(i+1)*8])
			}
			rotateIdx = int(binary.BigEndian.Uint32(raw[parts*8:]))
		}
	}
	for p := 0; p < parts; p++ {
		l, err := s.OpenLogForRead(metaNS.Name, stream, uint32(p))
		if err != nil {
			return nil, nil, err
		}
		logs = append(logs, l)
		if (toks[p] == eventlog.Token{}) {
			if opts.AtMs > 0 {
				toks[p] = s.findStartTokenAt(l, opts.AtMs)
			} else if opts.From == "earliest" {
				toks[p] = eventlog.Token{}
			} else {
				items, _ := l.Read(eventlog.ReadOptions{Reverse: true, Limit: 1})
				if len(items) > 0 {
					toks[p] = tokenFromSeq(items[0].Seq + 1)
				}
			}
		}
	}
	// Optional CEL filter
	cfilter, err := newCELFilter(opts.Filter)
	if err != nil {
		return nil, nil, err
	}
	eval := func(p int, seq uint64, header []byte, payload []byte) bool {
		return cfilter.Eval(p, seq, header, payload)
	}

	// Read round-robin across partitions
	out := make([]MessageListItem, 0, opts.Limit)
	exhausted := make([]bool, parts) // Track which partitions are exhausted

	readOne := func(p int, quota int) int {
		if quota <= 0 || exhausted[p] {
			return 0
		}
		items, next := logs[p].Read(eventlog.ReadOptions{Start: toks[p], Limit: quota, Reverse: opts.Reverse})
		count := 0
		for _, it := range items {
			if eval(p, it.Seq, it.Header, it.Payload) {
				idTok := tokenFromSeq(it.Seq)
				id := make([]byte, len(idTok))
				copy(id, idTok[:])
				out = append(out, MessageListItem{ID: id, Payload: it.Payload, Header: it.Header, Partition: uint32(p), Seq: it.Seq})
				count++
				if count >= quota {
					break
				}
			}
		}
		if next != (eventlog.Token{}) {
			toks[p] = next
		} else {
			// No more messages in this partition
			exhausted[p] = true
		}
		return count
	}
	remaining := opts.Limit
	for remaining > 0 {
		progressed := 0
		for i := 0; i < parts && remaining > 0; i++ {
			p := (rotateIdx + i) % parts
			progressed += readOne(p, remaining)
			remaining = opts.Limit - len(out)
		}
		if progressed == 0 {
			break
		}
		rotateIdx = (rotateIdx + 1) % parts
	}
	// Encode next token: parts*8 bytes + 4 byte rotateIdx
	nextBytes := make([]byte, parts*8+4)
	for i := 0; i < parts; i++ {
		copy(nextBytes[i*8:(i+1)*8], toks[i][:])
	}
	binary.BigEndian.PutUint32(nextBytes[parts*8:], uint32(rotateIdx))
	// Compress to base64-like opaque? The HTTP layer already base64-encodes
	_ = base64.StdEncoding // referenced to avoid removal by tools
	return out, nextBytes, nil
}

// StreamStats returns first/last seq and count per partition for a stream.
func (s *Service) StreamStats(ctx context.Context, ns, stream string) ([]PartitionStat, uint64, error) {
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return nil, 0, err
	}
	parts := metaNS.Partitions
	if cm, ok := s.readStreamMeta(metaNS.Name, stream); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}
	if parts <= 0 {
		parts = 1
	}
	stats := make([]PartitionStat, 0, parts)
	var total uint64
	for p := 0; p < parts; p++ {
		l, err := s.OpenLogForRead(metaNS.Name, stream, uint32(p))
		if err != nil {
			return nil, 0, err
		}
		firstSeq, lastSeq, count, bytes, _ := l.Stats()
		// Use eventlog helper for last publish time
		lastPubMs := l.LastPublishMs()
		stats = append(stats, PartitionStat{Partition: uint32(p), FirstSeq: firstSeq, LastSeq: lastSeq, Count: count, Bytes: bytes, LastPublishMs: lastPubMs})
		total += count
	}
	return stats, total, nil
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

// findStartTokenAt returns the first token whose header timestamp >= atMs.
// Implementation: O(log N) binary search by sequence, using Read(Start=mid,Limit=1)
// and comparing the 8-byte ms timestamp stored in the record header. Safe under trims
// by deriving [lo,hi] from the current first/last entries via forward/reverse reads.
func (s *Service) findStartTokenAt(l *eventlog.Log, atMs int64) eventlog.Token {
	// Binary search by seq using Read(Start=mid,Limit=1) and header timestamps.
	firstItems, _ := l.Read(eventlog.ReadOptions{Limit: 1})
	if len(firstItems) == 0 {
		return eventlog.Token{}
	}
	lastItems, _ := l.Read(eventlog.ReadOptions{Reverse: true, Limit: 1})
	if len(lastItems) == 0 {
		return eventlog.Token{}
	}
	lo := firstItems[0].Seq
	hi := lastItems[0].Seq + 1 // exclusive upper bound
	for lo < hi {
		mid := lo + (hi-lo)/2
		midTok := tokenFromSeq(mid)
		items, _ := l.Read(eventlog.ReadOptions{Start: midTok, Limit: 1})
		if len(items) == 0 {
			// No item >= mid; clamp lo to hi
			lo = hi
			break
		}
		seq := items[0].Seq
		var ts int64
		if len(items[0].Header) >= 8 {
			ts = int64(binary.BigEndian.Uint64(items[0].Header[:8]))
		}
		if ts < atMs {
			lo = seq + 1
		} else {
			hi = seq
		}
	}
	return tokenFromSeq(lo)
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

// trackMessageDelivery records that a message was delivered to a group
func (s *Service) trackMessageDelivery(ns, stream, group string, id []byte) {
	if group == "" {
		return
	}
	key := deliveryKey(ns, stream, id, group)
	status := MessageDeliveryStatus{
		Group:        group,
		DeliveredAt:  time.Now().UnixMilli(),
		Acknowledged: false,
		Nacked:       false,
		RetryCount:   0,
	}
	if data, err := json.Marshal(status); err == nil {
		_ = s.rt.DB().Set(key, data)
	}
}

// updateDeliveryStatus updates the delivery status for a message/group
func (s *Service) updateDeliveryStatus(ns, stream, group string, id []byte, acked, nacked bool, retryCount uint32, errorMsg string) {
	key := deliveryKey(ns, stream, id, group)
	status := MessageDeliveryStatus{
		Group:        group,
		DeliveredAt:  time.Now().UnixMilli(),
		Acknowledged: acked,
		Nacked:       nacked,
		RetryCount:   retryCount,
		LastError:    errorMsg,
	}
	if data, err := json.Marshal(status); err == nil {
		_ = s.rt.DB().Set(key, data)
	}
}

// incrementPublishCounters increments per-bucket counters for publish_count and published_bytes
func (s *Service) incrementPublishCounters(ns, stream string, partition uint32, tsMs int64, payloadBytes int64) {
	// Best-effort; failures are ignored
	resList := []string{Res1m, Res5m, Res1h}
	// Compute bucket start per res
	for _, res := range resList {
		var step int64
		switch res {
		case Res1m:
			step = int64(time.Minute / time.Millisecond)
		case Res5m:
			step = int64(5 * time.Minute / time.Millisecond)
		case Res1h:
			step = int64(time.Hour / time.Millisecond)
		default:
			continue
		}
		bucket := (tsMs / step) * step
		// Aggregate keys
		kCount := metricsKeyAgg(ns, stream, string(MetricPublishCount), res, bucket)
		kBytes := metricsKeyAgg(ns, stream, "published_bytes", res, bucket)
		// Per-partition keys
		kpCount := metricsKeyPart(ns, stream, string(MetricPublishCount), res, partition, bucket)
		kpBytes := metricsKeyPart(ns, stream, "published_bytes", res, partition, bucket)
		// Increment via read-modify-write in a batch
		b := s.rt.DB().NewBatch()
		// count++
		s.incrementCounterInBatch(b, kCount, 1)
		s.incrementCounterInBatch(b, kpCount, 1)
		// bytes += payloadBytes
		s.incrementCounterInBatch(b, kBytes, payloadBytes)
		s.incrementCounterInBatch(b, kpBytes, payloadBytes)
		_ = s.rt.DB().CommitBatch(context.Background(), b)
		_ = b.Close()
	}
}

// incrementAckCounters increments per-bucket counters for ack_count (aggregate only for now)
func (s *Service) incrementAckCounters(ns, stream string, tsMs int64) {
	resList := []string{Res1m, Res5m, Res1h}
	for _, res := range resList {
		var step int64
		switch res {
		case Res1m:
			step = int64(time.Minute / time.Millisecond)
		case Res5m:
			step = int64(5 * time.Minute / time.Millisecond)
		case Res1h:
			step = int64(time.Hour / time.Millisecond)
		default:
			continue
		}
		bucket := (tsMs / step) * step
		k := metricsKeyAgg(ns, stream, string(MetricAckCount), res, bucket)
		b := s.rt.DB().NewBatch()
		s.incrementCounterInBatch(b, k, 1)
		_ = s.rt.DB().CommitBatch(context.Background(), b)
		_ = b.Close()
	}
}

// incrementCounterInBatch does read-modify-write of an int64 counter stored as big-endian 8 bytes
func (s *Service) incrementCounterInBatch(b *pebble.Batch, key []byte, delta int64) {
	cur, _ := s.rt.DB().Get(key)
	var val int64
	if len(cur) == 8 {
		val = int64(binary.BigEndian.Uint64(cur))
	}
	val += delta
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(val))
	_ = b.Set(key, buf[:], nil)
}

// GetMessageDeliveryStatus returns delivery status for a specific message across all groups
func (s *Service) GetMessageDeliveryStatus(ctx context.Context, ns, stream string, id []byte) ([]MessageDeliveryStatus, error) {
	prefix := deliveryPrefix(ns, stream, id)
	hi := append(append([]byte{}, prefix...), 0xFF)

	it, err := s.rt.DB().NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	var statuses []MessageDeliveryStatus
	for ok := it.First(); ok; ok = it.Next() {
		var status MessageDeliveryStatus
		if err := json.Unmarshal(it.Value(), &status); err == nil {
			statuses = append(statuses, status)
		}
	}
	return statuses, nil
}

// subscriber key helpers
func subKeyTotal(ns, stream string) string        { return ns + "|" + stream }
func subKeyGroup(ns, stream, group string) string { return ns + "|" + stream + "|" + group }

// ActiveSubscribersCount returns the number of active subscribers for a stream across all groups.
func (s *Service) ActiveSubscribersCount(ns, stream string) int {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	return s.activeSubs[subKeyTotal(ns, stream)]
}

func (s *Service) incSub(key string) {
	s.subsMu.Lock()
	s.activeSubs[key] = s.activeSubs[key] + 1
	s.subsMu.Unlock()
}

func (s *Service) decSub(key string) {
	s.subsMu.Lock()
	if v := s.activeSubs[key]; v > 1 {
		s.activeSubs[key] = v - 1
	} else {
		delete(s.activeSubs, key)
	}
	s.subsMu.Unlock()
}

// ActiveSubscribersCountGroup returns the number of active subscribers for a specific group on a stream.
func (s *Service) ActiveSubscribersCountGroup(ns, stream, group string) int {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	return s.activeSubs[subKeyGroup(ns, stream, group)]
}

// GetSubscriptions returns per-group cursor and lag information for a stream.
func (s *Service) GetSubscriptions(ctx context.Context, ns, stream string) ([]SubscriptionInfo, error) {
	// Determine partitions and latest seq per partition
	partsStats, _, err := s.StreamStats(ctx, ns, stream)
	if err != nil {
		return nil, err
	}
	// Discover groups with cursors and groups with retry/DLQ data, union them
	cursorGroups, err := s.GetWithCursorGroups(ctx, ns, stream)
	if err != nil {
		return nil, err
	}
	retryGroups := s.DiscoverGroupsWithRetryData(ns, stream)
	groupSet := make(map[string]bool)
	for _, g := range cursorGroups {
		groupSet[g] = true
	}
	for _, g := range retryGroups {
		groupSet[g] = true
	}
	groups := make([]string, 0, len(groupSet))
	for g := range groupSet {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	// Build info per group
	out := make([]SubscriptionInfo, 0, len(groups))
	for _, g := range groups {
		info := SubscriptionInfo{Group: g}
		info.ActiveSubscribers = s.ActiveSubscribersCountGroup(ns, stream, g)
		// Last delivered for group (best effort)
		if ts, _ := s.LastDeliveredMs(ctx, ns, stream); ts > 0 {
			info.LastDeliveredMs = ts
		}
		// Per-partition cursor and lag
		info.Partitions = make([]struct {
			Partition uint32 `json:"partition"`
			CursorSeq uint64 `json:"cursor_seq"`
			EndSeq    uint64 `json:"end_seq"`
			Lag       uint64 `json:"lag"`
		}, 0, len(partsStats))
		var totalLag uint64
		for _, ps := range partsStats {
			// Read cursor for this group/partition
			l, err := s.OpenLogForRead(ns, stream, ps.Partition)
			if err != nil {
				continue
			}
			var curSeq uint64
			if cur, ok := l.GetCursor(g); ok {
				curSeq = cur.Seq()
			} else {
				curSeq = 0
			}
			var lag uint64
			if ps.LastSeq >= curSeq {
				lag = ps.LastSeq - curSeq
			}
			totalLag += lag
			info.Partitions = append(info.Partitions, struct {
				Partition uint32 `json:"partition"`
				CursorSeq uint64 `json:"cursor_seq"`
				EndSeq    uint64 `json:"end_seq"`
				Lag       uint64 `json:"lag"`
			}{Partition: ps.Partition, CursorSeq: curSeq, EndSeq: ps.LastSeq, Lag: lag})
		}
		info.TotalLag = totalLag
		out = append(out, info)
	}
	return out, nil
}

// LastDeliveredMs returns the most recent delivery time (ms) for any group on the stream.
func (s *Service) LastDeliveredMs(ctx context.Context, ns, stream string) (uint64, error) {
	db := s.rt.DB()
	prefix := eventlog.KeyCursorLastDeliveredPrefix(ns, stream)
	hi := append(append([]byte{}, prefix...), 0xFF)
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer func() { _ = it.Close() }()
	var maxTs uint64
	for ok := it.First(); ok; ok = it.Next() {
		v := it.Value()
		if len(v) >= 8 {
			ts := binary.BigEndian.Uint64(v[:8])
			if ts > maxTs {
				maxTs = ts
			}
		}
	}
	return maxTs, nil
}

// seqToBytes converts a sequence number to 8-byte big-endian representation
func (s *Service) seqToBytes(seq uint64) []byte {
	bytes := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		bytes[i] = byte(seq >> ((7 - i) * 8))
	}
	return bytes
}

// DiscoverGroupsWithRetryData finds groups that have retry or DLQ data
func (s *Service) DiscoverGroupsWithRetryData(ns, stream string) []string {
	var groups []string
	// Eventlog key pattern: ns/{ns}/log/{topic}/{part_be4}/m
	// For retry/DLQ: ns/{ns}/log/{prefix}/{stream}/{group}/{part_be4}/m
	// Topic format: {prefix}/{stream}/{group}

	// Check both retry and dlq prefixes
	prefixes := []string{"retry", "dlq"}
	seen := make(map[string]bool)

	for _, prefix := range prefixes {
		searchPrefix := fmt.Sprintf("ns/%s/log/%s/%s/", ns, prefix, stream)

		it, err := s.rt.DB().NewIter(&pebble.IterOptions{
			LowerBound: []byte(searchPrefix),
			UpperBound: []byte(searchPrefix + "\xff"),
		})
		if err != nil {
			continue
		}

		for ok := it.First(); ok; ok = it.Next() {
			key := string(it.Key())
			// Extract group from key: ns/{ns}/log/{topic}/{part_be4}/m
			// Topic format: {prefix}/{stream}/{group}
			// Key format: ns/default/log/retry/demo/test-group/00000000/m
			parts := strings.Split(key, "/")
			// Key format: ns/{ns}/log/{prefix}/{stream}/{group}//{part_be4}/m
			// Expected parts: [ns, {ns}, log, {prefix}, {stream}, {group}, "", {part_be4}, m]
			if len(parts) >= 8 && parts[0] == "ns" && parts[1] == ns && parts[2] == "log" {
				// parts[3] = prefix, parts[4] = stream, parts[5] = group
				if parts[3] == prefix && parts[4] == stream && parts[5] != "" {
					group := parts[5]
					if !seen[group] {
						seen[group] = true
						groups = append(groups, group)
					}
				}
			}
		}
		_ = it.Close()
	}

	return groups
}

// ListRetryMessages returns messages from the retry queue
func (s *Service) ListRetryMessages(ctx context.Context, ns, stream, group string, startToken []byte, limit int, reverse bool) ([]RetryMessageItem, []byte, error) {
	if limit <= 0 {
		limit = 100
	}

	// Get all groups if group is empty
	groups := []string{group}
	if group == "" {
		// Get active groups and groups with retry data
		activeGroups, err := s.GetWithCursorGroups(ctx, ns, stream)
		if err != nil {
			return nil, nil, err
		}
		retryGroups := s.DiscoverGroupsWithRetryData(ns, stream)

		// Combine and deduplicate
		groupSet := make(map[string]bool)
		for _, g := range activeGroups {
			groupSet[g] = true
		}
		for _, g := range retryGroups {
			groupSet[g] = true
		}

		groups = make([]string, 0, len(groupSet))
		for g := range groupSet {
			groups = append(groups, g)
		}
	}

	var items []RetryMessageItem
	var nextToken []byte

	for _, g := range groups {
		retryTopic := "retry/" + stream + "/" + g
		log, err := s.rt.OpenLog(ns, retryTopic, 0)
		if err != nil {
			continue // Skip if retry topic doesn't exist
		}

		// Convert startToken to eventlog.Token
		var tok eventlog.Token
		if len(startToken) == 8 {
			copy(tok[:], startToken)
		}

		// Read retry messages
		retryItems, next := log.Read(eventlog.ReadOptions{
			Start:   tok,
			Limit:   limit,
			Reverse: reverse,
		})

		for _, item := range retryItems {
			// Get original message from main stream
			originalMsg, err := s.getOriginalMessage(ctx, ns, stream, item.Payload)
			if err != nil {
				continue
			}

			// Get retry count
			attempts := s.getRetryCount(ns, stream, g, item.Payload)

			// Get error message
			errorMsg := s.getErrorMessage(ns, stream, g, item.Payload)

			// Get headers and creation time from original message header
			headers := s.extractHeadersFromHeader(originalMsg.Header)
			createdAt := s.extractCreatedAtMsFromHeader(originalMsg.Header)

			// Calculate next retry time
			nextRetryAt := s.calculateNextRetryTime(attempts, createdAt)

			retryItem := RetryMessageItem{
				ID:            s.seqToBytes(item.Seq),
				Payload:       originalMsg.Payload,
				Headers:       headers,
				Partition:     originalMsg.Partition,
				Seq:           originalMsg.Seq,
				RetryCount:    attempts,
				MaxRetries:    s.policy.MaxAttempts,
				NextRetryAtMs: nextRetryAt,
				CreatedAtMs:   createdAt,
				LastError:     errorMsg,
				Group:         g,
			}
			items = append(items, retryItem)
		}

		if len(next) > 0 {
			nextToken = next[:]
		}
	}

	return items, nextToken, nil
}

// ListDLQMessages returns messages from the DLQ
func (s *Service) ListDLQMessages(ctx context.Context, ns, stream, group string, startToken []byte, limit int, reverse bool) ([]DLQMessageItem, []byte, error) {
	if limit <= 0 {
		limit = 100
	}

	// Get all groups if group is empty
	groups := []string{group}
	if group == "" {
		// Get active groups and groups with retry data
		activeGroups, err := s.GetWithCursorGroups(ctx, ns, stream)
		if err != nil {
			return nil, nil, err
		}
		retryGroups := s.DiscoverGroupsWithRetryData(ns, stream)

		// Combine and deduplicate
		groupSet := make(map[string]bool)
		for _, g := range activeGroups {
			groupSet[g] = true
		}
		for _, g := range retryGroups {
			groupSet[g] = true
		}

		groups = make([]string, 0, len(groupSet))
		for g := range groupSet {
			groups = append(groups, g)
		}
	}

	var items []DLQMessageItem
	var nextToken []byte

	for _, g := range groups {
		dlqTopic := "dlq/" + stream + "/" + g
		log, err := s.rt.OpenLog(ns, dlqTopic, 0)
		if err != nil {
			continue // Skip if DLQ topic doesn't exist
		}

		// Convert startToken to eventlog.Token
		var tok eventlog.Token
		if len(startToken) == 8 {
			copy(tok[:], startToken)
		}

		// Read DLQ messages
		dlqItems, next := log.Read(eventlog.ReadOptions{
			Start:   tok,
			Limit:   limit,
			Reverse: reverse,
		})

		for _, item := range dlqItems {
			// Get original message from main stream
			originalMsg, err := s.getOriginalMessage(ctx, ns, stream, item.Payload)
			if err != nil {
				continue
			}

			// Get final retry count
			attempts := s.getRetryCount(ns, stream, g, item.Payload)

			// Get error message
			errorMsg := s.getErrorMessage(ns, stream, g, item.Payload)

			// Get headers from original message header
			headers := s.extractHeadersFromHeader(originalMsg.Header)

			// Get actual failure time
			failedAtMs := s.getFailureTime(ns, stream, g, item.Payload)

			dlqItem := DLQMessageItem{
				ID:         s.seqToBytes(item.Seq),
				Payload:    originalMsg.Payload,
				Headers:    headers,
				Partition:  originalMsg.Partition,
				Seq:        originalMsg.Seq,
				RetryCount: attempts,
				MaxRetries: s.policy.MaxAttempts,
				FailedAtMs: failedAtMs,
				LastError:  errorMsg,
				Group:      g,
			}
			items = append(items, dlqItem)
		}

		if len(next) > 0 {
			nextToken = next[:]
		}
	}

	return items, nextToken, nil
}

// GetRetryDLQStats returns statistics for retry/DLQ data
func (s *Service) GetRetryDLQStats(ctx context.Context, ns, stream, group string) ([]RetryDLQStats, error) {
	// Get all groups if group is empty, otherwise use specific group
	groups := []string{group}
	if group == "" {
		// Hybrid discovery: union of cursor groups and groups with retry/DLQ data
		cursorGroups, err := s.GetWithCursorGroups(ctx, ns, stream)
		if err != nil {
			return nil, err
		}
		retryGroups := s.DiscoverGroupsWithRetryData(ns, stream)
		groupSet := make(map[string]bool)
		for _, g := range cursorGroups {
			groupSet[g] = true
		}
		for _, g := range retryGroups {
			groupSet[g] = true
		}
		groups = make([]string, 0, len(groupSet))
		for g := range groupSet {
			groups = append(groups, g)
		}
		sort.Strings(groups)
		if len(groups) == 0 {
			return []RetryDLQStats{}, nil
		}
	}

	var stats []RetryDLQStats
	for _, g := range groups {
		retryCount := s.countRetryMessages(ns, stream, g)
		dlqCount := s.countDLQMessages(ns, stream, g)
		totalRetries := s.getTotalRetries(ns, stream, g)

		// Calculate success rate
		totalMessages := retryCount + dlqCount
		successRate := 0.0
		if totalMessages > 0 {
			successRate = float64(dlqCount) / float64(totalMessages) * 100.0
		}

		stat := RetryDLQStats{
			Namespace:    ns,
			Stream:       stream,
			Group:        g,
			RetryCount:   retryCount,
			DLQCount:     dlqCount,
			TotalRetries: totalRetries,
			SuccessRate:  successRate,
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

// Helper methods

func (s *Service) getOriginalMessage(ctx context.Context, ns, stream string, id []byte) (MessageListItem, error) {
	return s.GetMessageByID(ctx, ns, stream, id)
}

func (s *Service) extractHeadersFromHeader(header []byte) map[string]string {
	// Parse headers from the message header bytes
	// The header format is: 8-byte timestamp + JSON headers
	if len(header) > 8 {
		headerBytes := header[8:] // Skip the 8-byte timestamp
		var headers map[string]string
		if err := json.Unmarshal(headerBytes, &headers); err == nil {
			return headers
		}
	}
	return map[string]string{}
}

func (s *Service) extractCreatedAtMsFromHeader(header []byte) int64 {
	// Extract creation time from the 8-byte timestamp in the header
	if len(header) >= 8 {
		return int64(binary.BigEndian.Uint64(header[:8]))
	}
	return time.Now().UnixMilli()
}

func (s *Service) getRetryCount(ns, stream, group string, id []byte) uint32 {
	key := attemptsKey(ns, stream, group, id)
	if b, err := s.rt.DB().Get(key); err == nil && len(b) == 4 {
		return binary.BigEndian.Uint32(b)
	}
	return 0
}

func (s *Service) getErrorMessage(ns, stream, group string, id []byte) string {
	key := errorKey(ns, stream, group, id)
	if b, err := s.rt.DB().Get(key); err == nil {
		return string(b)
	}
	return ""
}

func (s *Service) getFailureTime(ns, stream, group string, id []byte) int64 {
	key := failureTimeKey(ns, stream, group, id)
	if b, err := s.rt.DB().Get(key); err == nil && len(b) == 8 {
		return int64(binary.BigEndian.Uint64(b))
	}
	return time.Now().UnixMilli() // fallback to current time
}

func (s *Service) calculateNextRetryTime(attempts uint32, createdAtMs int64) int64 {
	// Exponential backoff: 200ms * (2^(attempts-1)) with max 30s
	baseDelay := int64(200) // ms
	delay := baseDelay * (1 << (attempts - 1))
	if delay > 30000 {
		delay = 30000
	}
	return createdAtMs + delay
}

func (s *Service) countRetryMessages(ns, stream, group string) uint32 {
	retryTopic := "retry/" + stream + "/" + group
	log, err := s.rt.OpenLog(ns, retryTopic, 0)
	if err != nil {
		return 0
	}
	_, _, count, _, _ := log.Stats()
	return uint32(count)
}

func (s *Service) countDLQMessages(ns, stream, group string) uint32 {
	dlqTopic := "dlq/" + stream + "/" + group
	log, err := s.rt.OpenLog(ns, dlqTopic, 0)
	if err != nil {
		return 0
	}
	_, _, count, _, _ := log.Stats()
	return uint32(count)
}

func (s *Service) getTotalRetries(ns, stream, group string) uint32 {
	// Count total retry attempts by scanning the attempts keyspace
	db := s.rt.DB()
	prefix := attemptsKey(ns, stream, group, []byte{})
	hi := append(append([]byte{}, prefix...), 0xFF)

	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return 0
	}
	defer func() { _ = it.Close() }()

	var totalRetries uint32
	for ok := it.First(); ok; ok = it.Next() {
		value := it.Value()
		if len(value) == 4 {
			attempts := binary.BigEndian.Uint32(value)
			totalRetries += attempts
		}
	}
	return totalRetries
}

// GetMessageByID retrieves a specific message by its ID from the main stream
func (s *Service) GetMessageByID(ctx context.Context, ns, stream string, id []byte) (MessageListItem, error) {
	if len(id) != 8 {
		return MessageListItem{}, fmt.Errorf("invalid message ID format")
	}

	// Convert ID to sequence number
	var token eventlog.Token
	copy(token[:], id)
	seq := token.Seq()

	// Determine partitions
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return MessageListItem{}, err
	}
	parts := metaNS.Partitions
	if cm, ok := s.readStreamMeta(metaNS.Name, stream); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}
	if parts <= 0 {
		parts = 1
	}

	// Search through all partitions to find the message
	for p := uint32(0); p < uint32(parts); p++ {
		log, err := s.OpenLogForRead(metaNS.Name, stream, p)
		if err != nil {
			continue
		}

		// Check if this partition has the message
		key := eventlog.KeyLogEntry(metaNS.Name, stream, p, seq)
		value, err := s.rt.DB().Get(key)
		if err == nil && len(value) > 0 {
			// Found the message, read it from the log
			items, _ := log.Read(eventlog.ReadOptions{
				Start: token,
				Limit: 1,
			})
			if len(items) > 0 {
				item := items[0]
				return MessageListItem{
					ID:        id,
					Payload:   item.Payload,
					Header:    item.Header,
					Partition: p,
					Seq:       item.Seq,
				}, nil
			}
		}
	}

	return MessageListItem{}, fmt.Errorf("message not found")
}

// GetWithCursorGroups returns the names of all consumer groups that have cursor keys for a stream.
func (s *Service) GetWithCursorGroups(ctx context.Context, ns, stream string) ([]string, error) {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}
	db := s.rt.DB()
	// prefix: ns/{ns}/cursor/{stream}/
	prefix := make([]byte, 0, len(ns)+len(stream)+16)
	prefix = append(prefix, 'n', 's', '/')
	prefix = append(prefix, ns...)
	prefix = append(prefix, '/', 'c', 'u', 'r', 's', 'o', 'r', '/')
	prefix = append(prefix, stream...)
	prefix = append(prefix, '/')
	hi := append(append([]byte{}, prefix...), 0xFF)
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	groups := make(map[string]bool)
	for ok := it.First(); ok; ok = it.Next() {
		key := string(it.Key())
		// Key format: ns/{ns}/cursor/{stream}/{group}/{part_be4}
		parts := strings.Split(key, "/")
		if len(parts) >= 5 && parts[0] == "ns" && parts[1] == ns && parts[2] == "cursor" && parts[3] == stream {
			group := parts[4]
			groups[group] = true
		}
	}

	// Convert map to sorted slice
	result := make([]string, 0, len(groups))
	for group := range groups {
		result = append(result, group)
	}
	sort.Strings(result)
	return result, nil
}

// GroupsCount returns the number of distinct consumer groups with cursors for a stream.
func (s *Service) GroupsCount(ctx context.Context, ns, stream string) (int, error) {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}
	db := s.rt.DB()
	// prefix: ns/{ns}/cursor/{stream}/
	prefix := make([]byte, 0, len(ns)+len(stream)+16)
	prefix = append(prefix, 'n', 's', '/')
	prefix = append(prefix, ns...)
	prefix = append(prefix, '/', 'c', 'u', 'r', 's', 'o', 'r', '/')
	prefix = append(prefix, stream...)
	prefix = append(prefix, '/')
	hi := append(append([]byte{}, prefix...), 0xFF)
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer func() { _ = it.Close() }()
	groups := map[string]struct{}{}
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) <= len(prefix) {
			continue
		}
		rest := k[len(prefix):]
		slash := bytes.IndexByte(rest, '/')
		if slash <= 0 {
			continue
		}
		groups[string(rest[:slash])] = struct{}{}
	}
	return len(groups), nil
}

// FlushStream removes all messages from a stream (or specific partition).
// If partition is nil, flushes all partitions for the stream.
// Returns the number of messages deleted.
func (s *Service) FlushStream(ctx context.Context, ns, stream string, partition *uint32) (uint64, error) {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}

	// Ensure namespace exists
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return 0, err
	}

	// Determine partitions to flush
	parts := metaNS.Partitions
	if cm, ok := s.readStreamMeta(metaNS.Name, stream); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}
	if parts <= 0 {
		parts = 1
	}

	var totalDeleted uint64

	// Flush specific partition or all partitions
	partitionsToFlush := []uint32{}
	if partition != nil {
		partitionsToFlush = append(partitionsToFlush, *partition)
	} else {
		for p := uint32(0); p < uint32(parts); p++ {
			partitionsToFlush = append(partitionsToFlush, p)
		}
	}

	for _, part := range partitionsToFlush {
		log, err := s.OpenLogForRead(metaNS.Name, stream, part)
		if err != nil {
			continue // Skip partitions that don't exist
		}

		// Get stats to know how many messages we're deleting
		_, _, count, _, _ := log.Stats()
		totalDeleted += count

		// Delete all entries by iterating through the log
		// We'll use a similar approach to the trim methods
		low := eventlog.KeyLogEntry(metaNS.Name, stream, part, 0)
		hi := eventlog.KeyLogEntry(metaNS.Name, stream, part, ^uint64(0))
		iter, err := s.rt.DB().NewIter(&pebble.IterOptions{LowerBound: low, UpperBound: append(hi, 0x00)})
		if err != nil {
			continue
		}
		defer func() { _ = iter.Close() }()

		batch := s.rt.DB().NewBatch()
		defer func() { _ = batch.Close() }()

		for ok := iter.First(); ok; ok = iter.Next() {
			if err := batch.Delete(iter.Key(), nil); err != nil {
				return totalDeleted, err
			}
		}

		// Commit the batch for this partition
		if err := s.rt.DB().CommitBatch(ctx, batch); err != nil {
			return totalDeleted, err
		}
	}

	return totalDeleted, nil
}

// DeleteMessage removes a specific message by ID from a stream.
func (s *Service) DeleteMessage(ctx context.Context, ns, stream string, messageID []byte) error {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}

	// Ensure namespace exists
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return err
	}

	// Determine partitions to search
	parts := metaNS.Partitions
	if cm, ok := s.readStreamMeta(metaNS.Name, stream); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}
	if parts <= 0 {
		parts = 1
	}

	// Convert message ID to sequence number
	var token eventlog.Token
	if len(messageID) == 8 {
		copy(token[:], messageID)
	} else {
		return fmt.Errorf("invalid message ID format")
	}

	seq := token.Seq()

	// Search through all partitions to find the message
	for p := uint32(0); p < uint32(parts); p++ {
		// Check if this partition has the message
		key := eventlog.KeyLogEntry(metaNS.Name, stream, p, seq)
		_, err := s.rt.DB().Get(key)
		if err == nil {
			// Found the message, delete it
			return s.rt.DB().Delete(key)
		}
	}

	return fmt.Errorf("message not found")
}

func (s *Service) Metrics(ctx context.Context, ns, ch string, metric Metric, startMs, endMs, stepMs int64, byPartition bool, fill string) ([]Series, error) {
	// Resolve namespace and partitions
	metaNS, err := s.EnsureNamespace(ctx, ns)
	if err != nil {
		return nil, err
	}
	parts := metaNS.Partitions
	if cm, ok := s.readStreamMeta(metaNS.Name, ch); ok && cm.Partitions > 0 {
		parts = cm.Partitions
	}
	if parts <= 0 {
		parts = 1
	}

	if endMs <= startMs || stepMs <= 0 {
		return []Series{{Label: "all", Points: [][2]float64{}}}, nil
	}

	// Compute bucket geometry
	bucketCount := int((endMs - startMs + stepMs - 1) / stepMs)
	if bucketCount <= 0 {
		bucketCount = 1
	}

	// Helper to materialize series points from bucket arrays
	buildPoints := func(buckets []float64) [][2]float64 {
		out := make([][2]float64, 0, len(buckets))
		for i, v := range buckets {
			// time at bucket start
			t := float64(startMs + int64(i)*stepMs)
			out = append(out, [2]float64{t, v})
		}
		return out
	}

	seconds := float64(stepMs) / 1000.0

	// Try reading from pre-aggregated counters when available
	res, ok := pickResolution(stepMs)
	if ok {
		series, ok := s.readCountersSeries(metaNS.Name, ch, metric, res, startMs, endMs, stepMs, byPartition, parts)
		if ok {
			return series, nil
		}
	}

	switch metric {
	case MetricPublishCount, MetricPublishRate, MetricBytesRate:
		// Initialize per-partition or aggregate buckets
		if byPartition {
			series := make([]Series, 0, parts)
			for p := 0; p < parts; p++ {
				buckets := make([]float64, bucketCount)
				// Open log for partition
				l, err := s.OpenLogForRead(metaNS.Name, ch, uint32(p))
				if err != nil {
					// If log missing, just emit zero series for this partition
					series = append(series, Series{Label: fmt.Sprintf("partition:%d", p), Points: buildPoints(buckets)})
					continue
				}
				// Find start token at startMs
				startTok := s.findStartTokenAt(l, startMs)
				var curTok = startTok
				for {
					items, next := l.Read(eventlog.ReadOptions{Start: curTok, Limit: 1024})
					if len(items) == 0 {
						break
					}
					for _, it := range items {
						var ts int64
						if len(it.Header) >= 8 {
							ts = int64(binary.BigEndian.Uint64(it.Header[:8]))
						}
						if ts < startMs {
							continue
						}
						if ts >= endMs {
							// We reached beyond the window; stop scanning this partition
							items = nil
							break
						}
						idx := int((ts - startMs) / stepMs)
						if idx < 0 || idx >= bucketCount {
							continue
						}
						switch metric {
						case MetricPublishCount:
							buckets[idx] += 1
						case MetricPublishRate:
							buckets[idx] += 1 // convert to rate later
						case MetricBytesRate:
							buckets[idx] += float64(len(it.Payload)) // convert to rate later
						}
					}
					if next == (eventlog.Token{}) {
						break
					}
					if next == curTok { // no progress safety
						break
					}
					curTok = next
				}
				// Convert to rate if required
				switch metric {
				case MetricPublishRate:
					for i := range buckets {
						buckets[i] = buckets[i] / seconds
					}
				case MetricBytesRate:
					for i := range buckets {
						buckets[i] = buckets[i] / seconds
					}
				}
				series = append(series, Series{Label: fmt.Sprintf("partition:%d", p), Points: buildPoints(buckets)})
			}
			return series, nil
		}

		// Aggregate across partitions into single series
		agg := make([]float64, bucketCount)
		for p := 0; p < parts; p++ {
			l, err := s.OpenLogForRead(metaNS.Name, ch, uint32(p))
			if err != nil {
				continue
			}
			startTok := s.findStartTokenAt(l, startMs)
			var curTok = startTok
			for {
				items, next := l.Read(eventlog.ReadOptions{Start: curTok, Limit: 1024})
				if len(items) == 0 {
					break
				}
				for _, it := range items {
					var ts int64
					if len(it.Header) >= 8 {
						ts = int64(binary.BigEndian.Uint64(it.Header[:8]))
					}
					if ts < startMs {
						continue
					}
					if ts >= endMs {
						items = nil
						break
					}
					idx := int((ts - startMs) / stepMs)
					if idx < 0 || idx >= bucketCount {
						continue
					}
					switch metric {
					case MetricPublishCount:
						agg[idx] += 1
					case MetricPublishRate:
						agg[idx] += 1
					case MetricBytesRate:
						agg[idx] += float64(len(it.Payload))
					}
				}
				if next == (eventlog.Token{}) {
					break
				}
				if next == curTok { // no progress
					break
				}
				curTok = next
			}
		}
		if metric == MetricPublishRate || metric == MetricBytesRate {
			for i := range agg {
				agg[i] = agg[i] / seconds
			}
		}
		return []Series{{Label: "all", Points: buildPoints(agg)}}, nil

	case MetricAckCount:
		// Aggregate acks from delivery status records. Partition breakdown is not available.
		// Key prefix: ns/{ns}/delivery/{stream}/
		lb := make([]byte, 0, len(metaNS.Name)+len(ch)+20)
		lb = append(lb, nsPrefix...)
		lb = append(lb, metaNS.Name...)
		lb = append(lb, deliverySeg...)
		lb = append(lb, ch...)
		lb = append(lb, sep)
		ub := append(append([]byte{}, lb...), 0xFF)

		buckets := make([]float64, bucketCount)
		it, err := s.rt.DB().NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: ub})
		if err != nil {
			// On iterator error, return empty series rather than failing the endpoint
			return []Series{{Label: "all", Points: buildPoints(buckets)}}, nil
		}
		defer func() { _ = it.Close() }()
		for ok := it.First(); ok; ok = it.Next() {
			var st MessageDeliveryStatus
			if err := json.Unmarshal(it.Value(), &st); err != nil {
				continue
			}
			if !st.Acknowledged {
				continue
			}
			ts := st.DeliveredAt
			if ts < startMs || ts >= endMs {
				continue
			}
			idx := int((ts - startMs) / stepMs)
			if idx >= 0 && idx < bucketCount {
				buckets[idx] += 1
			}
		}
		return []Series{{Label: "all", Points: buildPoints(buckets)}}, nil
	default:
		return nil, fmt.Errorf("unsupported metric: %s", string(metric))
	}
}

// SystemWideMetrics builds aggregated metrics across all streams in a namespace or all namespaces.
// If ns is empty, it aggregates across all namespaces.
func (s *Service) SystemWideMetrics(ctx context.Context, ns string, metric Metric, startMs, endMs, stepMs int64, byPartition bool, fill, unit string) (map[string]any, error) {
	var streams []StreamInfo
	var err error

	if ns == "" || ns == "*" {
		// Get all streams across all namespaces
		streams, err = s.ListAllStreams(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list all streams: %w", err)
		}
	} else {
		// Get streams for specific namespace
		streamNames, err := s.ListStreams(ctx, ns)
		if err != nil {
			return nil, fmt.Errorf("failed to list streams: %w", err)
		}
		streams = make([]StreamInfo, len(streamNames))
		for i, stream := range streamNames {
			streams[i] = StreamInfo{Namespace: ns, Stream: stream}
		}
	}

	if len(streams) == 0 {
		// Return empty response for namespace with no streams
		responseNamespace := ns
		if ns == "" || ns == "*" {
			responseNamespace = "*" // Wildcard for system-wide
		}
		return map[string]any{
			"namespace": responseNamespace,
			"stream":    "*", // Wildcard for system-wide
			"metric":    string(metric),
			"unit":      unit,
			"start_ms":  startMs,
			"end_ms":    endMs,
			"step_ms":   stepMs,
			"series":    [][2]float64{}, // Consistent schema
		}, nil
	}

	// Performance optimization: Limit number of streams to prevent system overload
	maxStreams := 100 // Configurable limit
	if len(streams) > maxStreams {
		// Sort by namespace/stream name for consistent results
		sort.Slice(streams, func(i, j int) bool {
			if streams[i].Namespace == streams[j].Namespace {
				return streams[i].Stream < streams[j].Stream
			}
			return streams[i].Namespace < streams[j].Namespace
		})
		streams = streams[:maxStreams]
	}

	// Calculate bucket count for time series
	bucketCount := int((endMs - startMs + stepMs - 1) / stepMs)
	if bucketCount <= 0 {
		bucketCount = 1
	}

	// Initialize aggregated buckets
	aggBuckets := make([]float64, bucketCount)

	// Process streams in parallel for better performance
	type streamResult struct {
		series []Series
		err    error
	}

	results := make(chan streamResult, len(streams))
	semaphore := make(chan struct{}, 10) // Limit concurrent requests

	for _, stream := range streams {
		go func(si StreamInfo) {
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			series, err := s.Metrics(ctx, si.Namespace, si.Stream, metric, startMs, endMs, stepMs, byPartition, fill)
			results <- streamResult{series: series, err: err}
		}(stream)
	}

	// Collect results and aggregate
	for i := 0; i < len(streams); i++ {
		result := <-results
		if result.err != nil {
			// Skip streams with errors, but log them
			continue
		}

		// Extract series data (should be single series for non-partition metrics)
		var streamSeries [][2]float64
		if len(result.series) > 0 {
			streamSeries = result.series[0].Points
		}

		// Add to aggregated buckets
		for i, point := range streamSeries {
			if i < len(aggBuckets) {
				aggBuckets[i] += point[1]
			}
		}
	}

	// Build aggregated series from buckets
	aggregatedSeries := make([][2]float64, 0, len(aggBuckets))
	for i, val := range aggBuckets {
		timestamp := float64(startMs + int64(i)*stepMs)
		aggregatedSeries = append(aggregatedSeries, [2]float64{timestamp, val})
	}

	// Determine namespace for response (empty for system-wide)
	responseNamespace := ns
	if ns == "" || ns == "*" {
		responseNamespace = "*" // Wildcard for system-wide
	}

	return map[string]any{
		"namespace": responseNamespace,
		"stream":    "*", // Wildcard for system-wide metrics
		"metric":    string(metric),
		"unit":      unit,
		"start_ms":  startMs,
		"end_ms":    endMs,
		"step_ms":   stepMs,
		"series":    aggregatedSeries, // Consistent schema with per-stream
	}, nil
}

// pickResolution chooses a stored resolution that matches stepMs closely.
func pickResolution(stepMs int64) (string, bool) {
	switch {
	case stepMs <= int64(time.Minute/time.Millisecond):
		return Res1m, true
	case stepMs <= int64(5*time.Minute/time.Millisecond):
		return Res5m, true
	case stepMs <= int64(time.Hour/time.Millisecond):
		return Res1h, true
	default:
		return "", false
	}
}

// readCountersSeries reads pre-aggregated counters from Pebble and converts to series.
func (s *Service) readCountersSeries(ns, stream string, metric Metric, res string, startMs, endMs, stepMs int64, byPartition bool, parts int) ([]Series, bool) {
	buckets := int((endMs - startMs + stepMs - 1) / stepMs)
	if buckets <= 0 {
		buckets = 1
	}

	// map requested metric to stored metric
	storedMetric := string(metric)
	isRate := false
	if metric == MetricPublishRate {
		storedMetric = string(MetricPublishCount)
		isRate = true
	} else if metric == MetricBytesRate {
		storedMetric = "published_bytes"
		isRate = true
	}

	// helper to convert float slices to points
	build := func(vals []float64) [][2]float64 {
		out := make([][2]float64, 0, len(vals))
		for i, v := range vals {
			t := float64(startMs + int64(i)*stepMs)
			out = append(out, [2]float64{t, v})
		}
		return out
	}

	secs := float64(stepMs) / 1000.0

	db := s.rt.DB()

	if byPartition {
		series := make([]Series, 0, parts)
		for p := 0; p < parts; p++ {
			vals := make([]float64, buckets)
			// iterate bucket timestamps
			for i := 0; i < buckets; i++ {
				bts := (startMs/stepMs + int64(i)) * stepMs
				key := metricsKeyPart(ns, stream, storedMetric, res, uint32(p), bts)
				if v, err := db.Get(key); err == nil && len(v) == 8 {
					c := float64(binary.BigEndian.Uint64(v))
					if isRate {
						c = c / secs
					}
					vals[i] = c
				}
			}
			series = append(series, Series{Label: fmt.Sprintf("partition:%d", p), Points: build(vals)})
		}
		return series, true
	}

	vals := make([]float64, buckets)
	for i := 0; i < buckets; i++ {
		bts := (startMs/stepMs + int64(i)) * stepMs
		key := metricsKeyAgg(ns, stream, storedMetric, res, bts)
		if v, err := db.Get(key); err == nil && len(v) == 8 {
			c := float64(binary.BigEndian.Uint64(v))
			if isRate {
				c = c / secs
			}
			vals[i] = c
		}
	}
	return []Series{{Label: "all", Points: build(vals)}}, true
}
