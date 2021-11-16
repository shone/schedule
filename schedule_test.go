package schedule_test

import (
	"henkel/libs/schedule"
	"testing"
	"runtime/debug"
	"time"
	"github.com/stretchr/testify/require"
)

func TestSchedule_NextItem(t *testing.T) {
	s := schedule.NewSchedule()
	_ = s.Start(schedule.NewMockClock(time.Date(2021, time.January, 01, 00, 00, 00, 0, time.UTC)))

	require.True(t, s.NextItem().IsNil(), "next item for newly created schedule should be nil")

	baseTime := time.Date(2021, time.January, 01, 00, 00, 00, 0, time.UTC)

	// push first item
	newNextItem := s.Push("1", baseTime.UnixNano())
	require.False(t, s.NextItem().IsNil())
	require.Equal(t, "1", s.NextItem().ID)
	require.Equal(t, baseTime.UnixNano(), s.NextItem().TimeUnixNano)
	require.False(t, newNextItem.IsNil())
	require.Equal(t, "1", newNextItem.ID)
	require.Equal(t, baseTime.UnixNano(), newNextItem.TimeUnixNano)

	// pushing the same item again should not change the next item due
	newNextItem = s.Push("1", baseTime.UnixNano())
	require.True(t, newNextItem.IsNil())

	// pushing the same timestamp again should not change the next item due, even if it's for a different id
	newNextItem = s.Push("2", baseTime.UnixNano())
	require.True(t, newNextItem.IsNil(), "")
	require.Equal(t, "1", s.NextItem().ID)
	require.Equal(t, baseTime.UnixNano(), s.NextItem().TimeUnixNano)

	// pushing a later timestamp should not affect the next item due
	newNextItem = s.Push("3", baseTime.Add(1 * time.Hour).UnixNano())
	require.True(t, newNextItem.IsNil())
	require.Equal(t, "1", s.NextItem().ID)
	require.Equal(t, baseTime.UnixNano(), s.NextItem().TimeUnixNano)

	// deleting the next item due should change the next item to the next soonest (or equal) timestamp
	newNextItem = s.Delete("1")
	require.Equal(t, "2", newNextItem.ID)
	require.Equal(t, baseTime.UnixNano(), newNextItem.TimeUnixNano)
	require.Equal(t, "2", s.NextItem().ID)
	require.Equal(t, baseTime.UnixNano(), s.NextItem().TimeUnixNano)

	// deleting the next item due should change the next item to the next soonest (or equal) timestamp
	newNextItem = s.Delete("2")
	require.Equal(t, "3", newNextItem.ID)
	require.Equal(t, baseTime.Add(1 * time.Hour).UnixNano(), newNextItem.TimeUnixNano)
	require.Equal(t, "3", s.NextItem().ID)
	require.Equal(t, baseTime.Add(1 * time.Hour).UnixNano(), s.NextItem().TimeUnixNano)

	newNextItem = s.Push("4", baseTime.Add(30 * time.Minute).UnixNano())
	require.Equal(t, "4", newNextItem.ID)
	require.Equal(t, baseTime.Add(30 * time.Minute).UnixNano(), newNextItem.TimeUnixNano)
	require.Equal(t, "4", s.NextItem().ID)
	require.Equal(t, baseTime.Add(30 * time.Minute).UnixNano(), s.NextItem().TimeUnixNano)

	// deleting a later item than the currently due one shouldn't change the currently due item
	newNextItem = s.Delete("3")
	require.True(t, newNextItem.IsNil())
	require.Equal(t, "4", s.NextItem().ID)
	require.Equal(t, baseTime.Add(30 * time.Minute).UnixNano(), s.NextItem().TimeUnixNano)

	// deleting the last item should set the currently due item to nil
	newNextItem = s.Delete("4")
	require.True(t, newNextItem.IsNil())
	require.True(t, s.NextItem().IsNil())
}

func TestSchedule_Chan(t *testing.T) {
	startTime := time.Date(2021, time.January, 01, 00, 00, 00, 0, time.UTC)

	s := schedule.NewSchedule()
	clock := schedule.NewMockClock(startTime)
	items := s.Start(clock)

	expectNoItem(t, items)

	s.Push("1", startTime.UnixNano())
	expectItem(t, items, "1", startTime.UnixNano())

	s.Push("1", startTime.Add(10 * time.Second).UnixNano())
	expectNoItem(t, items)
	clock.AddTime(5 * time.Second)
	expectNoItem(t, items)
	clock.AddTime(5 * time.Second)
	expectItem(t, items, "1", startTime.Add(10 * time.Second).UnixNano())

	s.Push("2", startTime.Add(20 * time.Second).UnixNano())
	s.Push("3", startTime.Add(30 * time.Second).UnixNano())
	expectNoItem(t, items)
	clock.AddTime(10 * time.Second)
	expectItem(t, items, "2", startTime.Add(20 * time.Second).UnixNano())
	clock.AddTime(10 * time.Second)
	expectItem(t, items, "3", startTime.Add(30 * time.Second).UnixNano())

	s.Push("5", startTime.Add(50 * time.Second).UnixNano())
	clock.AddTime(15 * time.Second)
	s.Push("4", startTime.Add(40 * time.Second).UnixNano())
	expectItem(t, items, "4", startTime.Add(40 * time.Second).UnixNano())
	s.Delete("5")
	clock.AddTime(10 * time.Second)
	expectNoItem(t, items)
}

func expectItem(t *testing.T, items <-chan schedule.Item, id string, timeUnixNano int64) {
	timeout := time.NewTimer(100 * time.Millisecond)
	select {
		case item, ok := <-items:
			if !ok {
				t.Fatalf("channel closed before receiving expected schedule item '%s' at %s, stack: %s", id, time.Unix(0, timeUnixNano).String(), debug.Stack())
			}
			if item.ID != id || item.TimeUnixNano != timeUnixNano {
				t.Fatalf("Got schedule item '%s' at %s instead of expected '%s' at %s, stack: %s", item.ID, time.Unix(0, item.TimeUnixNano).String(), id, time.Unix(0, timeUnixNano).String(), debug.Stack())
			}
			timeout.Stop()
		case <-timeout.C:
	}
}

func expectNoItem(t *testing.T, items <-chan schedule.Item) {
	timeout := time.NewTimer(100 * time.Millisecond)
	select {
		case item, ok := <-items:
			if ok {
				t.Fatalf("Expected no schedule item but instead got '%s' at %s, stack: %s", item.ID, time.Unix(0, item.TimeUnixNano).String(), debug.Stack())
			}
			timeout.Stop()
		case <-timeout.C:
	}
}
