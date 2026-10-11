package kvs_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/shun159/molecule"
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

func (keeper) Init(proc.PID) (struct{}, []molecule.Effect, error) { return struct{}{}, nil, nil }

func (k keeper) HandleCall(s struct{}, req put, from genserver.From[error]) (struct{}, []molecule.Effect) {
	if req.V < 0 {
		return s, molecule.Do(k.tab.DeleteEffect(req.K, from))
	}
	return s, molecule.Do(k.tab.PutEffect(req.K, req.V, from))
}

func (keeper) HandleCast(s struct{}, _ struct{}) (struct{}, []molecule.Effect) { return s, nil }

func (keeper) HandleInfo(s struct{}, msg any) (struct{}, []molecule.Effect) {
	if d, ok := msg.(kvs.Durable); ok {
		return s, molecule.Do(d.Tag.(genserver.From[error]).Reply(d.Err))
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
