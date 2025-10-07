package streamsvc

import "strconv"

// Stream keys

// Keyspace (namespace-scoped):
// - ns/{ns}/stream/{stream}/error/{group}/{id}
// - ns/{ns}/stream/{stream}/failure/{group}/{id}
// - ns/{ns}/stream/{stream}/idem/{key}
// - ns/{ns}/stream/{stream}/meta
// - ns/{ns}/stream/{stream}/attempts/{group}/{id}
// - ns/{ns}/stream/{stream}/retry_next/{group}/{id}
// - ns/{ns}/delivery/{stream}/{id}/{group}
// - ns/{ns}/delivery/{stream}/{id}/ (prefix)

var (
	sep          = byte('/')
	nsPrefix     = []byte("ns/")
	streamSeg    = []byte("/stream/")
	groupCfgSeg  = []byte("/groupcfg/")
	errorSeg     = []byte("/error/")
	failureSeg   = []byte("/failure/")
	idemSeg      = []byte("/idem/")
	metaSuffix   = []byte("/meta")
	attemptsSeg  = []byte("/attempts/")
	retryNextSeg = []byte("/retry_next/")
	deliverySeg  = []byte("/delivery/")
)

func errorKey(ns, stream, group string, id []byte) []byte {
	// ns/{ns}/stream/{stream}/error/{group}/{id}
	b := make([]byte, 0, len(ns)+len(stream)+len(group)+25)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, streamSeg...)
	b = append(b, stream...)
	b = append(b, errorSeg...)
	b = append(b, group...)
	b = append(b, sep)
	b = append(b, id...)
	return b
}

func failureTimeKey(ns, stream, group string, id []byte) []byte {
	// ns/{ns}/stream/{stream}/failure/{group}/{id}
	b := make([]byte, 0, len(ns)+len(stream)+len(group)+25)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, streamSeg...)
	b = append(b, stream...)
	b = append(b, failureSeg...)
	b = append(b, group...)
	b = append(b, sep)
	b = append(b, id...)
	return b
}

func idemKey(ns, stream, key string) []byte {
	// ns/{ns}/stream/{stream}/idem/{key}
	b := make([]byte, 0, len(ns)+len(stream)+len(key)+20)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, streamSeg...)
	b = append(b, stream...)
	b = append(b, idemSeg...)
	b = append(b, key...)
	return b
}

func streamMetaKey(ns, stream string) []byte {
	// ns/{ns}/stream/{stream}/meta
	b := make([]byte, 0, len(ns)+len(stream)+10)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, streamSeg...)
	b = append(b, stream...)
	b = append(b, metaSuffix...)
	return b
}

func attemptsKey(ns, stream, group string, id []byte) []byte {
	// ns/{ns}/stream/{stream}/attempts/{group}/{id}
	b := make([]byte, 0, len(ns)+len(stream)+len(group)+30)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, streamSeg...)
	b = append(b, stream...)
	b = append(b, attemptsSeg...)
	b = append(b, group...)
	b = append(b, sep)
	b = append(b, id...)
	return b
}

// groupCfgKey is the per-group policy/config key: ns/{ns}/stream/{stream}/groupcfg/{group}
func groupCfgKey(ns, stream, group string) []byte {
	b := make([]byte, 0, len(ns)+len(stream)+len(group)+30)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, streamSeg...)
	b = append(b, stream...)
	b = append(b, groupCfgSeg...)
	b = append(b, group...)
	return b
}

// retryNextKey stores the next scheduled retry time (ms) for a given message id and group.
func retryNextKey(ns, stream, group string, id []byte) []byte {
	// ns/{ns}/stream/{stream}/retry_next/{group}/{id}
	b := make([]byte, 0, len(ns)+len(stream)+len(group)+30)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, streamSeg...)
	b = append(b, stream...)
	b = append(b, retryNextSeg...)
	b = append(b, group...)
	b = append(b, sep)
	b = append(b, id...)
	return b
}

func deliveryKey(ns, stream string, id []byte, group string) []byte {
	// ns/{ns}/delivery/{stream}/{id}/{group}
	b := make([]byte, 0, len(ns)+len(stream)+len(group)+30)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, deliverySeg...)
	b = append(b, stream...)
	b = append(b, sep)
	b = append(b, id...)
	b = append(b, sep)
	b = append(b, group...)
	return b
}

func deliveryPrefix(ns, stream string, id []byte) []byte {
	// ns/{ns}/delivery/{stream}/{id}/
	b := make([]byte, 0, len(ns)+len(stream)+25)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, deliverySeg...)
	b = append(b, stream...)
	b = append(b, sep)
	b = append(b, id...)
	b = append(b, sep)
	return b
}

// Metrics keys (aggregate and per-partition)
// Layout:
// - ns/{ns}/metrics/{stream}/{metric}/{res}/{bucket_ms}
// - ns/{ns}/metrics/{stream}/{metric}/{res}/{partition}/{bucket_ms}

var (
	metricsSeg = []byte("/metrics/")
)

func metricsKeyAgg(ns, stream, metric, res string, bucketMs int64) []byte {
	// ns/{ns}/metrics/{stream}/{metric}/{res}/{bucket_ms}
	b := make([]byte, 0, len(ns)+len(stream)+len(metric)+len(res)+40)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, metricsSeg...)
	b = append(b, stream...)
	b = append(b, sep)
	b = append(b, metric...)
	b = append(b, sep)
	b = append(b, res...)
	b = append(b, sep)
	b = append(b, []byte(strconv.FormatInt(bucketMs, 10))...)
	return b
}

func metricsKeyPart(ns, stream, metric, res string, partition uint32, bucketMs int64) []byte {
	// ns/{ns}/metrics/{stream}/{metric}/{res}/{partition}/{bucket_ms}
	b := make([]byte, 0, len(ns)+len(stream)+len(metric)+len(res)+40)
	b = append(b, nsPrefix...)
	b = append(b, ns...)
	b = append(b, metricsSeg...)
	b = append(b, stream...)
	b = append(b, sep)
	b = append(b, metric...)
	b = append(b, sep)
	b = append(b, res...)
	b = append(b, sep)
	b = append(b, []byte(strconv.FormatUint(uint64(partition), 10))...)
	b = append(b, sep)
	b = append(b, []byte(strconv.FormatInt(bucketMs, 10))...)
	return b
}
