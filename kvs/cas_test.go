package kvs_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/molecule/proc"
)

func TestPutIf(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			dir := t.TempDir()
			o := kvs.Options[string, []string]{ReadConcurrency: m.read, WriteConcurrency: m.write, Sync: kvs.SyncNone}
			tab := open(t, dir, o)
			if ok, err := tab.PutIf("a", []string{"1"}, 0); !ok || err != nil {
				t.Fatalf("PutIf absent: %v %v", ok, err)
			}
			if ok, _ := tab.PutIf("a", []string{"x"}, 0); ok {
				t.Fatal("PutIf rev 0 over an entry wrote")
			}
			_, rev, _ := tab.GetRev("a")
			if ok, _ := tab.PutIf("a", []string{"2"}, rev+1); ok {
				t.Fatal("PutIf with another rev wrote")
			}
			if ok, _ := tab.PutIf("a", []string{"2"}, rev); !ok {
				t.Fatal("PutIf with the rev did not write")
			}
			v, rev2, _ := tab.GetRev("a")
			if v[0] != "2" || rev2 <= rev {
				t.Fatalf("after PutIf: %v rev %d, was %d", v, rev2, rev)
			}
			if ok, _ := tab.DeleteIf("a", rev); ok {
				t.Fatal("DeleteIf with an old rev deleted")
			}

			// Revisions outlive a compaction and a reopening.
			tab.Put("b", []string{"b"})
			if err := tab.Compact(); err != nil {
				t.Fatal(err)
			}
			tab.Close()
			tab = open(t, dir, o)
			defer tab.Close()
			if _, r, _ := tab.GetRev("a"); r != rev2 {
				t.Fatalf("rev after reopening %d, want %d", r, rev2)
			}
			if ok, _ := tab.DeleteIf("a", rev2); !ok {
				t.Fatal("DeleteIf with the rev did not delete")
			}
			if _, ok := tab.Get("a"); ok {
				t.Fatal("a still there")
			}
		})
	}
}

func TestCompareAndSwap(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[string, int]{WriteConcurrency: true, Sync: kvs.SyncNone})
	defer tab.Close()
	if ok, _ := kvs.CompareAndSwap(tab, "n", 0, 1); ok {
		t.Fatal("CompareAndSwap of an absent key wrote")
	}
	tab.Put("n", 0)
	// Counting up from goroutines at once: each increment is a swap.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				for {
					v, _ := tab.Get("n")
					ok, err := kvs.CompareAndSwap(tab, "n", v, v+1)
					if err != nil {
						t.Error(err)
						return
					}
					if ok {
						break
					}
				}
			}
		})
	}
	wg.Wait()
	if v, _ := tab.Get("n"); v != 800 {
		t.Fatalf("n = %d, want 800", v)
	}
	if ok, _ := kvs.CompareAndDelete(tab, "n", 799); ok {
		t.Fatal("CompareAndDelete of another value deleted")
	}
	if ok, _ := kvs.CompareAndDelete(tab, "n", 800); !ok {
		t.Fatal("CompareAndDelete did not delete")
	}
}

// casKeeper puts what it is called with if the revision is as given, and
// answers with the error of its Durable.
type casKeeper struct{ tab *kvs.Table[string, int] }

type putIf struct {
	K   string
	V   int
	Rev uint64
}

func (casKeeper) Init(proc.PID) (struct{}, []molecule.Effect, error) { return struct{}{}, nil, nil }

func (k casKeeper) HandleCall(s struct{}, req putIf, from genserver.From[error]) (struct{}, []molecule.Effect) {
	return s, molecule.Do(k.tab.PutIfEffect(req.K, req.V, req.Rev, from))
}

func (casKeeper) HandleCast(s struct{}, _ struct{}) (struct{}, []molecule.Effect) { return s, nil }

func (casKeeper) HandleInfo(s struct{}, msg any) (struct{}, []molecule.Effect) {
	if d, ok := msg.(kvs.Durable); ok {
		return s, molecule.Do(d.Tag.(genserver.From[error]).Reply(d.Err))
	}
	return s, nil
}

func TestPutIfEffect(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[string, int]{})
	defer tab.Close()
	n := proc.NewNode("")
	ctx := context.Background()
	srv, err := genserver.Start(ctx, n, casKeeper{tab})
	if err != nil {
		t.Fatal(err)
	}
	if rep, err := srv.Call(ctx, n, putIf{"k", 1, 0}); rep != nil || err != nil {
		t.Fatalf("first: %v %v", rep, err)
	}
	if rep, _ := srv.Call(ctx, n, putIf{"k", 2, 0}); !errors.Is(rep, kvs.ErrConflict) {
		t.Fatalf("over an entry: %v", rep)
	}
	_, rev, _ := tab.GetRev("k")
	if rep, _ := srv.Call(ctx, n, putIf{"k", 3, rev}); rep != nil {
		t.Fatalf("with the rev: %v", rep)
	}
	if v, _ := tab.Get("k"); v != 3 {
		t.Fatalf("k = %d", v)
	}
	a, b := tab.PutIfEffect("k", 1, 7, "t"), tab.PutIfEffect("k", 1, 7, "t")
	if a != b {
		t.Fatalf("equal effects differ: %v %v", a, b)
	}
}
