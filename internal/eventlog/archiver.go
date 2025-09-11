package eventlog

// ArchiverHook is an optional callback invoked when trims delete ranges.
// Implementations may enqueue ranges to an internal archiver queue or emit metrics.
type ArchiverHook interface {
	EmitTrimRange(namespace, topic string, partition uint32, minSeq, maxSeq uint64)
}

type noopArchiver struct{}

func (noopArchiver) EmitTrimRange(string, string, uint32, uint64, uint64) {}
