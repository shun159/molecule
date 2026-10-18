package genstatem

import (
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// BenchmarkHandle measures the adapter on the path of every event of every
// machine, without a process: a cast, an info, and an event timeout armed
// and fired.
func BenchmarkHandle(b *testing.B) {
	a := newAdapter[int, int](echoer{})
	m, _, _ := a.Init(proc.PID{}, nil)
	b.Run("cast", func(b *testing.B) {
		for b.Loop() {
			m, _ = a.Handle(m, actor.CastMsg{})
		}
	})
	b.Run("info", func(b *testing.B) {
		for b.Loop() {
			m, _ = a.Handle(m, gen.InfoMsg{Msg: "x"})
		}
	})
	arm := actor.CastMsg{Req: actor.Do(StartEventTimeout{After: time.Second, Msg: "t"})}
	b.Run("timeout", func(b *testing.B) {
		for b.Loop() {
			m, _ = a.Handle(m, arm)
			m, _ = a.Handle(m, gen.InfoMsg{Msg: fired{eventTimerKey{}}})
		}
	})
}
