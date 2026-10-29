package stage_test

import (
	"errors"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// TestStopAlone has a producer StopAlone wait for its consumers, go on
// when one of two leaves, and stop, normally, when the last does.
func TestStopAlone(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 10, Alone: true}))
	s.RunUntilIdle()
	if !s.Alive(src) {
		t.Fatal("stopped with no consumer yet")
	}
	first := spawn(t, s, stage.Consume(sink{From: src, Max: 1}))
	second := spawn(t, s, stage.Consume(sink{From: src, Max: 1}))
	s.RunUntilIdle()
	s.Send(first, actor.Do(stage.Cancel{Tag: "src"}))
	s.RunUntilIdle()
	if !s.Alive(src) {
		t.Fatal("stopped with a consumer left")
	}
	s.Exit(second, proc.Kill)
	s.RunUntilIdle()
	if reason, _ := exitOf(s, src); !errors.Is(reason, proc.Normal) {
		t.Fatalf("exited with %v", reason)
	}
}
