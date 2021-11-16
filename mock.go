package schedule

import (
	"time"
	"sync"
)

// Schedule.Start() can accept an instance of MockClock, allowing time to be arbitrarily skipped
// ahead. This is useful for testing.
type MockClock struct {
	now time.Time
	Timers map[*MockTimer]struct{}
	mutex sync.Mutex
}

func NewMockClock(now time.Time) *MockClock {
	return &MockClock{
		now: now,
		Timers: make(map[*MockTimer]struct{}),
	}
}

type MockTimer struct {
	TargetTime time.Time
	C chan time.Time
	clock *MockClock
}

func (c *MockClock) NewTimer(d time.Duration) Timer {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	timer := &MockTimer{
		TargetTime: c.now.Add(d),
		C: make(chan time.Time, 1),
		clock: c,
	}
	if c.now.UnixNano() >= timer.TargetTime.UnixNano() {
		timer.C <- c.now
	} else {
		c.Timers[timer] = struct{}{}
	}
	return timer
}

func (t *MockTimer) Chan() <-chan time.Time {
	return t.C
}

func (t *MockTimer) Stop() bool {
	t.clock.mutex.Lock()
	defer t.clock.mutex.Unlock()
	delete(t.clock.Timers, t)
	return true
}

func (c *MockClock) Now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.now
}

func (c *MockClock) AddTime(d time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.now = c.now.Add(d)
	for timer := range c.Timers {
		if c.now.UnixNano() >= timer.TargetTime.UnixNano() {
			timer.C <- c.now
			delete(c.Timers, timer)
		}
	}
}
