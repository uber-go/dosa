package metrics

import (
	"time"
)

// Scope is a namespace wrapper around a stats reporter, ensuring that
// all emitted values have a given prefix or set of tags.
type Scope interface {
	// Counter returns the Counter object corresponding to the name.
	Counter(name string) Counter

	// Tagged returns a new child scope with the given tags and current tags.
	Tagged(tags map[string]string) Scope

	// SubScope returns a new child scope appending a further name prefix.
	SubScope(name string) Scope

	// Timer returns the Timer object corresponding to the name.
	Timer(name string) Timer
}

// Counter is the interface for emitting counter type metrics.
type Counter interface {
	// Inc increments the counter by a delta.
	Inc(delta int64)
}

// Timer is the interface for emitting timer metrics.
type Timer interface {
	// Start gives you back a specific point in time to report via Stop.
	Start() time.Time
	// Stop reports time elapsed since the timer start to the recorder.
	Stop()
}