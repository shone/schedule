package schedule

import (
	"time"
)

// Schedule.Start() accepts this Clock interface, allowing either a
// realtime clock (RealClock) or virtual clock (MockClock) to be used.
type Clock interface {
	NewTimer(time.Duration) Timer
	Now() time.Time
}

type Timer interface {
	Stop() bool
	Chan() <-chan time.Time
}

type RealClock struct {}

func (c *RealClock) NewTimer(d time.Duration) Timer {
	timer := time.NewTimer(d)
	return &RealTimer{timer: timer}
}

func (c *RealClock) Now() time.Time {
	return time.Now()
}

type RealTimer struct {
	timer *time.Timer
}

func (t *RealTimer) Stop() bool {
	return t.timer.Stop()
}

func (t *RealTimer) Chan() <-chan time.Time {
	return t.timer.C
}
