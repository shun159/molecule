package kvs_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/molecule/proc"
)

// keeper is a server answering a put once it is durable, with its error.
type keeper struct{ tab *kvs.Table[string, int] }

type put struct {
	K string
	V int
}

func (keeper) Init(proc.PID) (struct{}, []actor.Effect, error) { return struct{}{}, nil, nil }

func (k keeper) HandleCall(s struct{}, req put, from genserver.From[error]) (struct{}, []actor.Effect) {
	if req.V < 0 {
		return s, actor.Do(k.tab.DeleteEffect(req.K, from))
	}
	return s, actor.Do(k.tab.PutEffect(req.K, req.V, from))
}

func (keeper) HandleCast(s struct{}, _ struct{}) (struct{}, []actor.Effect) { return s, nil }

func (keeper) HandleInfo(s struct{}, msg any) (struct{}, []actor.Effect) {
	if d, ok := msg.(kvs.Durable); ok {
		return s, actor.Do(d.Tag.(genserver.From[error]).Reply(d.Err))
	}
	return s, nil
}

func TestEffects(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[string, int]{})
	n := proc.NewNode("")
	ctx := context.Background()
	srv, err := genserver.Start(ctx, n, keeper{tab})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			if rep, err := srv.Call(ctx, n, put{fmt.Sprint(i), i}); rep != nil || err != nil {
				t.Errorf("put %d: %v, %v", i, rep, err)
			}
		})
	}
	wg.Wait()
	if rep, err := srv.Call(ctx, n, put{"0", -1}); rep != nil || err != nil {
		t.Fatalf("delete: %v, %v", rep, err)
	}
	info := tab.Info()
	if info.Size != 49 || info.DurableSeq != 51 {
		t.Fatalf("Info = %+v", info)
	}
	t.Logf("50 puts from 50 callers in %d syncs", info.Syncs)

	tab.Close()
	if rep, err := srv.Call(ctx, n, put{"x", 1}); !errors.Is(rep, kvs.ErrClosed) || err != nil {
		t.Fatalf("put after Close: %v, %v", rep, err)
	}
}

func TestEffectsInSim(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[string, int]{})
	defer tab.Close()
	s := gensim.New(1)
	pid, err := gensim.Spawn(s, genserver.Gen(keeper{tab}), nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		if rep, err := s.Call(pid, put{"k", i}); rep != nil || err != nil {
			t.Fatalf("put %d: %v, %v", i, rep, err)
		}
	}
	if v, _ := tab.Get("k"); v != 9 || tab.Info().DurableSeq != 10 {
		t.Fatalf("k = %d, Info = %+v", v, tab.Info())
	}
}

// batcher is a server making writes at once, answering once they are
// durable, with their error.
type batcher struct{ tab *kvs.Table[string, any] }

func (batcher) Init(proc.PID) (struct{}, []actor.Effect, error) { return struct{}{}, nil, nil }

func (b batcher) HandleCall(s struct{}, ws []kvs.Write[string, any], from genserver.From[error]) (struct{}, []actor.Effect) {
	return s, actor.Do(b.tab.WritesEffect(from, ws...))
}

func (batcher) HandleCast(s struct{}, _ struct{}) (struct{}, []actor.Effect) { return s, nil }

func (batcher) HandleInfo(s struct{}, msg any) (struct{}, []actor.Effect) {
	if d, ok := msg.(kvs.Durable); ok {
		return s, actor.Do(d.Tag.(genserver.From[error]).Reply(d.Err))
	}
	return s, nil
}

// Writes made at once are all made, with one answer; none, if one fails
// to encode, as a channel does.
func TestWritesEffect(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[string, any]{})
	defer tab.Close()
	n := proc.NewNode("")
	ctx := context.Background()
	srv, err := genserver.Start(ctx, n, batcher{tab})
	if err != nil {
		t.Fatal(err)
	}
	type W = kvs.Write[string, any]
	if rep, err := srv.Call(ctx, n, []W{{Key: "a", Val: 1}, {Key: "b", Val: 2}, {Key: "none", Delete: true}}); rep != nil || err != nil {
		t.Fatalf("writes: %v, %v", rep, err)
	}
	if a, _ := tab.Get("a"); a != 1 || tab.Len() != 2 {
		t.Fatalf("a %v, %d entries", a, tab.Len())
	}
	rep, err := srv.Call(ctx, n, []W{{Key: "x", Val: 1}, {Key: "y", Val: make(chan int)}, {Key: "a", Delete: true}})
	if rep == nil || err != nil {
		t.Fatalf("writes of a channel: %v, %v; want an error", rep, err)
	}
	if _, ok := tab.Get("x"); ok || tab.Len() != 2 {
		t.Errorf("writes failing made some: %d entries", tab.Len())
	}
	if rep, err := srv.Call(ctx, n, []W(nil)); rep != nil || err != nil {
		t.Errorf("no writes: %v, %v", rep, err)
	}
}
