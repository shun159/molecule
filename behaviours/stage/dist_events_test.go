package stage_test

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// listed emits Events, as asked, and is then done.
type listed[T any] struct{ Events []T }

func (listed[T]) Init(proc.PID) (int, []actor.Effect, error) { return 0, nil, nil }

func (l listed[T]) HandleDemand(next, n int) (int, []actor.Effect) {
	k := min(n, len(l.Events)-next)
	out := actor.Do(stage.Emit[T]{Events: l.Events[next : next+k : next+k]})
	if next+k == len(l.Events) {
		out = append(out, stage.Done{})
	}
	return next + k, out
}

// gather keeps the events of its producer, and tells Done of them once
// its producer is done.
type gather[T any] struct {
	From actor.Dest
	Done actor.From
}

func (g gather[T]) Init(proc.PID) ([]T, []actor.Effect, error) {
	return nil, actor.Do(stage.Subscribe{To: g.From, Max: 7}), nil
}

func (gather[T]) HandleEvents(got []T, events []T, _ any) ([]T, []actor.Effect) {
	return append(got[:len(got):len(got)], events...), nil
}

func (g gather[T]) Terminate(got []T, _ error) []actor.Effect {
	return actor.Do(actor.Reply{To: g.Done, Value: got})
}

// across has events go from a node to another, and returns them as
// received.
func across[T any](t *testing.T, events []T) []T {
	t.Helper()
	nodes, _ := cluster(t, "a@events", "b@events")
	ctx := context.Background()
	src, err := stage.Start(ctx, nodes[0], stage.Produce(listed[T]{Events: events}))
	if err != nil {
		t.Fatal(err)
	}
	done := nodes[1].Alias()
	defer done.Release()
	if _, err := stage.Start(ctx, nodes[1], stage.Consume(gather[T]{From: src, Done: actor.From{Tag: done.Ref}})); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-done.C:
		got, _ := m.Msg.([]T)
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("no events")
		return nil
	}
}

// sample encodes itself.
type sample struct {
	At    int64
	Value float64
}

func (s sample) AppendBinary(b []byte) ([]byte, error) {
	b = binary.AppendVarint(b, s.At)
	return binary.BigEndian.AppendUint64(b, uint64(s.Value*1000)), nil
}

func (s *sample) DecodeBinary(b []byte) error {
	at, n := binary.Varint(b)
	if n <= 0 || len(b[n:]) != 8 {
		return errors.New("sample cut short")
	}
	s.At, s.Value = at, float64(binary.BigEndian.Uint64(b[n:]))/1000
	return nil
}

// record does not encode itself: its events go by gob.
type record struct {
	Key   string
	Count int
}

func init() {
	stage.Register[string]()
	stage.Register[sample]()
	stage.Register[record]()
}

// TestDistEvents has events of a basic type, of a type encoding itself,
// and of another, gob's, go across nodes.
func TestDistEvents(t *testing.T) {
	words := []string{"", "molecule", "ストリーム", "stage"}
	if got := across(t, words); !reflect.DeepEqual(got, words) {
		t.Errorf("words: %v", got)
	}
	samples := []sample{{1, 0.5}, {2, 1.25}, {3, 2}}
	if got := across(t, samples); !reflect.DeepEqual(got, samples) {
		t.Errorf("samples: %v", got)
	}
	records := []record{{"a", 1}, {"b", 2}}
	if got := across(t, records); !reflect.DeepEqual(got, records) {
		t.Errorf("records: %v", got)
	}
}
