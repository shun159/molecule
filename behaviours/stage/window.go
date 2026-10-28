package stage

import (
	"time"

	"github.com/shun159/molecule/actor"
)

// Windowing is a consumer, or a producer-consumer, cutting time in
// windows of Window(), as the periodic triggers of Flow: HandleWindow is
// told of each window as it ends, with when it started, to emit, or keep,
// what the stage aggregated in it, and start afresh.
//
// The windows are of processing time, on the clock of the process,
// gensim's under simulation, and start at the multiples of Window() since
// the zero time: windows of a minute end at every minute. A stage told of
// a window late, by more than a window, is told of it once, as of one
// lasting until then: none of those it missed is told of apart.
type Windowing[S any] interface {
	Window() time.Duration
	HandleWindow(state S, start time.Time) (S, []actor.Effect)
}

// window is the window a stage is in.
type window struct {
	every time.Duration
	start time.Time
}

type (
	// windowKey is the key of the timer at the end of the window.
	windowKey struct{}
	// windowEnd is the message of that timer: the window of start ended.
	windowEnd struct{ start time.Time }
)

// windowOf returns the window of the stage b, if Windowing, at now, and
// the timer at its end.
func windowOf[S any](b any, now time.Time) (window, []actor.Effect) {
	w, ok := b.(Windowing[S])
	if !ok || w.Window() <= 0 {
		return window{}, nil
	}
	every := w.Window()
	return window{every: every, start: now.Truncate(every)}.timer()
}

// next returns the window after w, at now, and the timer at its end: the
// one starting as w ends, or that of now, if now is past it.
func (w window) next(now time.Time) (window, []actor.Effect) {
	w.start = w.start.Add(w.every)
	if now.Sub(w.start) >= w.every {
		w.start = now.Truncate(w.every)
	}
	return w.timer()
}

func (w window) timer() (window, []actor.Effect) {
	end := w.start.Add(w.every)
	return w, actor.Do(actor.StartTimer{Key: windowKey{}, At: end, Msg: windowEnd{start: w.start}})
}

// windowEnded tells h of the window that ended, and returns the next.
func windowEnded[S any](h Windowing[S], s S, w window, now time.Time) (S, []actor.Effect, window, []actor.Effect) {
	s, effs := h.HandleWindow(s, w.start)
	w, timer := w.next(now)
	return s, effs, w, timer
}

// clock returns now, or time.Now if nil: the clock of a stage.
func clock(now func() time.Time) func() time.Time {
	if now == nil {
		return time.Now
	}
	return now
}
