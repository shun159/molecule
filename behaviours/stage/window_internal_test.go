package stage

import (
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
)

// TestWindowNext has the next window start as one ends, or at now if the
// stage is late by more than a window.
func TestWindowNext(t *testing.T) {
	t0 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	w := window{every: time.Second, start: t0}
	cases := []struct {
		now  time.Duration
		next time.Duration
	}{
		{1000 * time.Millisecond, time.Second},     // on time
		{1900 * time.Millisecond, time.Second},     // late, within the next
		{2000 * time.Millisecond, 2 * time.Second}, // a window missed
		{7300 * time.Millisecond, 7 * time.Second}, // several
	}
	for _, c := range cases {
		next, effs := w.next(t0.Add(c.now))
		if want := t0.Add(c.next); !next.start.Equal(want) {
			t.Fatalf("at %v: next at %v, want %v", c.now, next.start.Sub(t0), c.next)
		}
		timer := effs[0].(actor.StartTimer)
		if end := next.start.Add(time.Second); !timer.At.Equal(end) || timer.Msg != (windowEnd{start: next.start}) {
			t.Fatalf("at %v: timer %+v", c.now, timer)
		}
	}
}
