package schedule

import (
	"sync"
	"time"
)

// A Schedule is a queue of id/timestamp pairs (items). It it similar to a time.Timer, but where
// multiple times may be specified, each with a given id.
//
// Items pushed into the schedule will be emited at their specified times from
// the channel returned from Start(), according to the given Clock.
type Schedule struct {
	// Map of IDs to timestamps
	items      map[string]Item

	// The next earliest item (also present in items map), or nil if the schedule is empty.
	nextItem   Item

	// Notifies the goroutine in Start() that nextItem has changed.
	updates    chan Item

	isStarted  bool
	isStopped  bool
	stop       chan struct{}
	mutex      sync.RWMutex

	// A unique integer to be assigned to Items for each schedule update.
	// This allows the Start() goroutine to ensure that it only pops an item if the item hasn't
	// been changed in the meantime.
	counter    uint64
}

type Item struct {
	ID           string
	TimeUnixNano int64
	Counter      uint64
}

// A nil item can indicate an empty schedule or that there's nothing to wait for.
func (itm Item) IsNil() bool {
	return itm.ID == "" && itm.TimeUnixNano == 0
}

func NewSchedule() *Schedule {
	return &Schedule{
		items: make(map[string]Item),
		updates: make(chan Item, 1),
		stop: make(chan struct{}, 1),
	}
}

// Returns the item with the earliest timestamp, or a nil item if the schedule is empty.
func (s *Schedule) NextItem() Item {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.nextItem
}

// Returns a channel that emits schedule items when the given Clock reaches each item's timestamp.
// Items may be added, changed or deleted after Start() has been called.
// Start() may only be called once; a second call to Start() will panic.
//
// A schedule.RealClock may be passed, which operates in realtime.
// Alternatively, passing a schedule.MockClock allows directly advancing a virtual time by arbitrary
// durations. This is useful for testing.
func (s *Schedule) Start(clock Clock) chan Item {
	if s.isStarted {
		panic("attempted to start already started schedule")
	}
	s.isStarted = true

	// The returned channel is unbuffered, so this goroutine will block and
	// wait for each emitted item to be consumed before sleeping for the next.
	c := make(chan Item)

	go func() {
		defer close(c)

		var item Item

		loop:
		for {
			// Wait for schedule to be non-empty.
			for item.IsNil() {
				select {
				case item = <-s.updates:
				case <-s.stop:
					return
				}
			}

			// Wait for the item to be due.
			// (can be interrupted by a sooner item being pushed)
			timer := clock.NewTimer(time.Duration(item.TimeUnixNano - clock.Now().UnixNano()))
			select {
			case <-timer.Chan():
				// Slept until the item's timestamp
			case item = <-s.updates:
				// An earlier item has been pushed, so we should wait for that instead.
				timer.Stop()
				continue loop
			case <-s.stop:
				timer.Stop()
				return
			}

			// Wait for item to be handled
			// (can be interrupted by a sooner item being pushed)
			select {
			case c <- item:
				// Item has been consumed.
				// Pop the id from the schedule only if it hasn't been changed in the meantime by
				// another call to Push(). The counter is used to detect changes.
				item = s.pop(item.ID, item.Counter)
			case item = <-s.updates:
				// An earlier item has been pushed, so we should emit that instead.
				continue loop
			case <-s.stop:
				return
			}
		}
	}()

	return c
}

// Causes the channel returned by Start() to stop emitting items and to close.
// Does not wait for the channel to be drained.
func (s *Schedule) Stop() {
	if !s.isStopped {
		s.stop <- struct{}{}
		s.isStopped = true
	}
}

// Pushes an item to the schedule, which will be emitted from the channel returned by
// Start() when its timestamp is due according to the given Clock.
//
// If pushing the given item results in the schedule's next earliest item changing, the new
// earliest item is returned.
//
// If Push() is called before Start(), it may block until Start() is called.
// Push() may be called after Start(), which will cause the channel to emit items according
// to the updated schedule.
//
// The timestamp for an existing id in the schedule can be changed by calling Push() again
// with the same id.
func (s *Schedule) Push(id string, timeUnixNano int64) Item {
	if id == "" {
		panic("cannot push item to schedule with empty id")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Use a counter to uniquely identify which 'version' of the schedule the Start() goroutine is dealing with.
	s.counter++

	item := Item{id, timeUnixNano, s.counter}

	// Add item to schedule.
	s.items[id] = item

	var newNextItem Item

	// Determine if the schedule's next item has changed.
	if s.nextItem.IsNil() {
		// The schedule was empty, so this new item becomes the next item due.
		newNextItem = item
	} else if item.TimeUnixNano < s.nextItem.TimeUnixNano {
		// This new item comes before what was previously the earliest item, so
		// it becomes the new next item.
		newNextItem = item
	} else if s.nextItem.ID == item.ID && item.TimeUnixNano > s.nextItem.TimeUnixNano {
		// The next item has been moved forward in time, so another item may now supersede it.
		newNextItem = item
		for _, itm := range s.items {
			if itm.TimeUnixNano < newNextItem.TimeUnixNano {
				newNextItem = itm
			}
		}
	}

	if !newNextItem.IsNil() {
		s.nextItem = newNextItem
		// Notify the goroutine in Start() that it should wait on a different item now.
		s.updates <- newNextItem
		return newNextItem
	}

	return Item{}
}

// Deletes the given item from the schedule, causing it to no longer be emitted from
// the channel returned by Start().
// If the current earliest item is deleted, then the new next item (if any) is returned.
func (s *Schedule) Delete(id string) Item {
	if id == "" {
		panic("cannot delete schedule item with empty id")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.items, id)

	if s.nextItem.ID == id {
		s.nextItem = Item{}
		for _, itm := range s.items {
			if s.nextItem.IsNil() || itm.TimeUnixNano < s.nextItem.TimeUnixNano {
				s.nextItem = itm
			}
		}
		// Notify the goroutine in Start() that it should wait on a different item now.
		s.updates <- s.nextItem
		return s.nextItem
	}

	return Item{}
}

// Used internally by Start() to pop items off the schedule.
func (s *Schedule) pop(id string, counter uint64) Item {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	item, ok := s.items[id]
	if ok && item.Counter == counter {
		delete(s.items, id)
	}

	if s.nextItem.ID == id && s.nextItem.Counter == counter {
		s.nextItem = Item{}
		for _, itm := range s.items {
			if s.nextItem.IsNil() || itm.TimeUnixNano < s.nextItem.TimeUnixNano {
				s.nextItem = itm
			}
		}
	}

	return s.nextItem
}
