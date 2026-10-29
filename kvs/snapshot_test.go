package kvs_test

import (
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/shun159/molecule/kvs"
)

// A snapshot reads as the table was when it was taken, written after, in
// every mode, of a Set and of an OrderedSet.
func TestSnapshot(t *testing.T) {
	for _, typ := range []kvs.Type{kvs.Set, kvs.OrderedSet} {
		for _, m := range modes {
			t.Run(m.name, func(t *testing.T) {
				o := kvs.Options[int, int]{ReadConcurrency: m.read, WriteConcurrency: m.write, Shards: 4, Type: typ}
				tab := open(t, t.TempDir(), o)
				for k := range 1000 {
					if err := tab.Put(k, k); err != nil {
						t.Fatal(err)
					}
				}
				_, rev, _ := tab.GetRev(500)
				snap := tab.Snapshot() // nothing ranged over before: no generation taken but its own
				want := map[int]int{}
				for k := range 1000 {
					want[k] = k
				}

				scramble(t, tab)
				if got := maps.Collect(snap.All()); !maps.Equal(got, want) {
					t.Fatalf("the snapshot has %d entries, changed", len(got))
				}
				if v, r, ok := snap.GetRev(500); !ok || v != 500 || r != rev || snap.Len() != 1000 {
					t.Fatalf("500: %d %d %v, of %d", v, r, ok, snap.Len())
				}
				if _, ok := snap.Get(5000); ok {
					t.Fatal("a key put after in the snapshot")
				}
				if maps.Equal(contents(tab), want) {
					t.Fatal("the table did not change")
				}
				if typ == kvs.OrderedSet {
					inOrder(t, snap)
				}
			})
		}
	}
}

// scramble writes over, deletes and adds keys.
func scramble(t *testing.T, tab *kvs.Table[int, int]) {
	t.Helper()
	r := rand.New(rand.NewPCG(2, 2))
	for i := range 3000 {
		k := r.IntN(6000)
		var err error
		if r.IntN(3) == 0 {
			err = tab.Delete(k)
		} else {
			err = tab.Put(k, -i)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// inOrder checks that the snapshot of 0 to 999 ranges in order.
func inOrder(t *testing.T, snap kvs.Snapshot[int, int]) {
	t.Helper()
	if ks := keysOf(snap.All()); !slices.IsSorted(ks) || len(ks) != 1000 {
		t.Fatalf("All: %d keys, sorted %v", len(ks), slices.IsSorted(ks))
	}
	if ks := keysOf(snap.Range(100, 110)); !slices.Equal(ks, []int{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}) {
		t.Fatalf("Range: %v", ks)
	}
}

func TestSnapshotRangeOfSet(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[int, int]{})
	defer func() {
		if recover() == nil {
			t.Fatal("Range of a Set")
		}
	}()
	tab.Snapshot().Range(0, 1)
}

// A snapshot takes all the shards at one moment: of keys written one
// after the other, it holds a prefix, never one without another before.
func TestSnapshotOneMoment(t *testing.T) {
	for _, typ := range []kvs.Type{kvs.Set, kvs.OrderedSet} {
		for _, m := range modes {
			t.Run(m.name, func(t *testing.T) {
				o := kvs.Options[int, int]{ReadConcurrency: m.read, WriteConcurrency: m.write, Shards: 8, Type: typ, Sync: kvs.SyncNone}
				tab := open(t, t.TempDir(), o)
				var wg sync.WaitGroup
				wg.Go(func() {
					for k := range 3000 {
						if err := tab.Put(k, k); err != nil {
							t.Error(err)
							return
						}
					}
				})
				for range 200 {
					snap := tab.Snapshot()
					ks := slices.Sorted(maps.Keys(maps.Collect(snap.All())))
					if len(ks) > 0 && ks[len(ks)-1] != len(ks)-1 {
						t.Fatalf("%d keys, up to %d", len(ks), ks[len(ks)-1])
					}
				}
				wg.Wait()
			})
		}
	}
}
