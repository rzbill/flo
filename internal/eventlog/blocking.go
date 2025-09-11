package eventlog

import (
	"time"
)

// WaitForAppend blocks until either a new append occurs or timeout elapses.
// It returns true if woken by an append, false on timeout.
func (l *Log) WaitForAppend(timeout time.Duration) bool {
	ch := l.notifyCh
	if timeout <= 0 {
		<-ch
		return true
	}
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}
