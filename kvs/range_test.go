package kvs_test

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"net/netip"
	"slices"
	"sync"
	"testing"

	"github.com/shun159/molecule/kvs"
)

// keysOf returns the keys of seq, in its order.
func keysOf[K comparable, V any](seq func(func(K, V) bool)) []K {
	var ks []K
	for k := range seq {
		ks = append(ks, k)
	}
	return ks
}

func between(want map[int]int, lo, hi int) []int {
	var ks []int
	for _, k := range slices.Sorted(maps.Keys(want)) {
		if k >= lo && k < hi {
			ks = append(ks, k)
		}
	}
	return ks
}

// An ordered table reads, writes and ranges as a map would, in every mode,
// and as well opened again and compacted.
func TestOrdered(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			dir := t.TempDir()
			o := kvs.Options[int, int]{ReadConcurrency: m.read, WriteConcurrency: m.write, Shards: 4, Type: kvs.OrderedSet}
			tab := open(t, dir, o)
			r := rand.New(rand.NewPCG(1, 1))
			want := map[int]int{}
			for i := range 5000 {
				k := r.IntN(1000)
				if r.IntN(4) == 0 {
					if err := tab.Delete(k); err != nil {
						t.Fatal(err)
					}
					delete(want, k)
					continue
				}
				if err := tab.Put(k, i); err != nil {
					t.Fatal(err)
				}
				want[k] = i
			}
			check := func(tab *kvs.Table[int, int]) {
				t.Helper()
				if got := contents(tab); !maps.Equal(got, want) {
					t.Fatalf("All = %v, want %v", got, want)
				}
				if got, all := keysOf(tab.All()), slices.Sorted(maps.Keys(want)); !slices.Equal(got, all) {
					t.Fatalf("All in the order %v, want %v", got, all)
				}
				for _, b := range [][2]int{{0, 1000}, {-5, 3}, {100, 200}, {500, 500}, {600, 400}, {998, 2000}} {
					if got, want := keysOf(tab.Range(b[0], b[1])), between(want, b[0], b[1]); !slices.Equal(got, want) {
						t.Fatalf("Range(%d, %d) = %v, want %v", b[0], b[1], got, want)
					}
				}
				for k, v := range want {
					if got, ok := tab.Get(k); !ok || got != v {
						t.Fatalf("Get(%d) = %d %v, want %d", k, got, ok, v)
					}
				}
				if tab.Len() != len(want) || tab.Info().Type != kvs.OrderedSet {
					t.Fatalf("Info = %+v, want %d entries", tab.Info(), len(want))
				}
			}
			check(tab)

			// Stopping early.
			n := 0
			for range tab.Range(0, 1000) {
				if n++; n == 3 {
					break
				}
			}

			if err := tab.Compact(); err != nil {
				t.Fatal(err)
			}
			check(tab)
			tab.Close()
			tab = open(t, dir, o)
			defer tab.Close()
			check(tab)

			// Conditional writes.
			_, rev, _ := tab.GetRev(10)
			if ok, err := tab.PutIf(10, -1, rev); !ok || err != nil {
				t.Fatalf("PutIf: %v %v", ok, err)
			}
			want[10] = -1
			if ok, _ := tab.PutIf(10, -2, rev); ok {
				t.Fatal("PutIf of a stale revision wrote")
			}
			check(tab)
		})
	}
}

// A range sees each shard as it was when it began, while writers go on,
// and may write to the table.
func TestRangeWhileWriting(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			o := kvs.Options[int, int]{ReadConcurrency: m.read, WriteConcurrency: m.write, Shards: 4, Type: kvs.OrderedSet, Sync: kvs.SyncNone}
			tab := open(t, t.TempDir(), o)
			defer tab.Close()
			for k := range 1000 {
				tab.Put(2*k, 0)
			}
			var wg sync.WaitGroup
			stop := make(chan struct{})
			for w := range 4 {
				wg.Go(func() {
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						k := 2*(i%1000) + 1 // odd keys come and go
						if i%2 == w%2 {
							tab.Put(k, i)
						} else {
							tab.Delete(k)
						}
						tab.Put(2*(i%1000), i) // even keys stay
					}
				})
			}
			for range 50 {
				prev, evens := -1, 0
				for k := range tab.Range(100, 1900) {
					if k <= prev {
						t.Fatalf("%d after %d", k, prev)
					}
					prev = k
					if k%2 == 0 {
						evens++
						tab.Put(k, -1) // writing during the range
					}
				}
				if evens != 900 {
					t.Fatalf("%d even keys, want 900", evens)
				}
			}
			close(stop)
			wg.Wait()
		})
	}
}

// Range orders addresses by netip.Addr.Compare, found for itself.
func TestRangeAddrs(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[netip.Addr, string]{Type: kvs.OrderedSet})
	defer tab.Close()
	for i := range 20 {
		ip := netip.AddrFrom4([4]byte{192, 168, 1, byte(250 - 10*i)})
		tab.Put(ip, fmt.Sprint(i))
	}
	got := keysOf(tab.Range(netip.MustParseAddr("192.168.1.100"), netip.MustParseAddr("192.168.1.200")))
	if len(got) != 10 || got[0].String() != "192.168.1.100" || got[9].String() != "192.168.1.190" {
		t.Fatalf("Range = %v", got)
	}
}

func TestRangeUnordered(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[int, int]{})
	defer tab.Close()
	defer func() {
		if recover() == nil {
			t.Fatal("Range of a Set did not panic")
		}
	}()
	tab.Range(0, 1)
}
