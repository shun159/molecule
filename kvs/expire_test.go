package kvs_test

import (
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/molecule/proc"
)

type expiring struct {
	IP     string
	Expiry time.Time // never, if zero
}

func expiringOptions(read, write bool) kvs.Options[string, expiring] {
	return kvs.Options[string, expiring]{
		ReadConcurrency: read, WriteConcurrency: write, Shards: 4,
		Expires: func(l expiring) time.Time { return l.Expiry },
	}
}

var t0 = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

func at(min int) time.Time { return t0.Add(time.Duration(min) * time.Minute) }

func keys[V any](m map[string]V) map[string]bool {
	ks := map[string]bool{}
	for k := range m {
		ks[k] = true
	}
	return ks
}

// Expire deletes the entries expired, as of the value written last, and
// those only; a table opened again knows when its entries expire.
func TestExpire(t *testing.T) {
	for _, m := range modes {
		for _, ordered := range []bool{false, true} {
			t.Run(fmt.Sprint(m.name, "/ordered=", ordered), func(t *testing.T) {
				dir := t.TempDir()
				o := expiringOptions(m.read, m.write)
				if ordered {
					o.Type = kvs.OrderedSet
				}
				tab := open(t, dir, o)
				put := func(k string, min int) {
					t.Helper()
					var exp time.Time
					if min >= 0 {
						exp = at(min)
					}
					if err := tab.Put(k, expiring{k, exp}); err != nil {
						t.Fatal(err)
					}
				}
				expire := func(min, want int, left ...string) {
					t.Helper()
					n, err := tab.Expire(at(min))
					if err != nil || n != want {
						t.Fatalf("Expire(%d) = %d %v, want %d", min, n, err, want)
					}
					wantLeft := map[string]bool{}
					for _, k := range left {
						wantLeft[k] = true
					}
					if got := keys(contents(tab)); !maps.Equal(got, wantLeft) {
						t.Fatalf("after Expire(%d): %v, want %v", min, got, wantLeft)
					}
				}
				put("a", 10)
				put("b", 20)
				put("c", 30)
				put("never", -1)
				put("renewed", 10)
				put("renewed", 40)
				put("shortened", 40)
				put("shortened", 15)
				put("deleted", 10)
				tab.Delete("deleted")
				if next, ok := tab.NextExpiry(); !ok || next.After(at(10)) {
					t.Fatalf("NextExpiry = %v %v", next, ok)
				}

				expire(5, 0, "a", "b", "c", "never", "renewed", "shortened")
				expire(10, 1, "b", "c", "never", "renewed", "shortened")
				expire(15, 1, "b", "c", "never", "renewed")

				tab.Close()
				tab = open(t, dir, o)
				defer tab.Close()
				if next, ok := tab.NextExpiry(); !ok || !next.Equal(at(20)) {
					t.Fatalf("reopened, NextExpiry = %v %v", next, ok)
				}
				expire(35, 2, "never", "renewed")
				expire(1000, 1, "never")
				if _, ok := tab.NextExpiry(); ok {
					t.Fatal("NextExpiry with nothing to expire")
				}
			})
		}
	}
}

// Entries written again and again leave out-of-date times in the heap,
// which is made again of the entries once large; the writes and expiries
// may go at once.
func TestExpireWhileWriting(t *testing.T) {
	tab := open(t, t.TempDir(), expiringOptions(true, true))
	defer tab.Close()
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range 5000 {
				k := fmt.Sprint(w, "-", i%10)
				tab.Put(k, expiring{k, at(i)}) // later and later
			}
		})
	}
	wg.Go(func() {
		for i := range 50 {
			if _, err := tab.Expire(at(i * 100)); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
	// Each entry expires at its last write, in 4990..4999.
	if n, err := tab.Expire(at(4989)); n != 0 || err != nil {
		t.Fatalf("Expire(4989) = %d %v", n, err)
	}
	if tab.Len() != 40 {
		t.Fatalf("Len = %d", tab.Len())
	}
	if n, _ := tab.Expire(at(4999)); n != 40 {
		t.Fatalf("Expire(4999) = %d", n)
	}
}

func TestExpireUnset(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[string, expiring]{})
	defer tab.Close()
	defer func() {
		if recover() == nil {
			t.Fatal("Expire of a table without Expires did not panic")
		}
	}()
	tab.Expire(t0)
}

// sweeper expires the entries of a table at the time it is called with,
// answering once the deletes are durable.
type sweeper struct{ tab *kvs.Table[string, expiring] }

func (sweeper) Init(proc.PID) (struct{}, []actor.Effect, error) { return struct{}{}, nil, nil }

func (s sweeper) HandleCall(st struct{}, now time.Time, from genserver.From[error]) (struct{}, []actor.Effect) {
	return st, actor.Do(s.tab.ExpireEffect(now, from))
}

func (sweeper) HandleCast(st struct{}, _ struct{}) (struct{}, []actor.Effect) { return st, nil }

func (sweeper) HandleInfo(st struct{}, msg any) (struct{}, []actor.Effect) {
	if d, ok := msg.(kvs.Durable); ok {
		return st, actor.Do(d.Tag.(genserver.From[error]).Reply(d.Err))
	}
	return st, nil
}

func TestExpireEffect(t *testing.T) {
	tab := open(t, t.TempDir(), expiringOptions(false, false))
	defer tab.Close()
	tab.Put("a", expiring{"a", at(1)})
	tab.Put("b", expiring{"b", at(2)})
	s := gensim.New(1)
	pid, err := gensim.Spawn(s, genserver.Gen(sweeper{tab}), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		min  int
		left int
	}{{0, 2}, {1, 1}, {5, 0}} {
		if rep, err := s.Call(pid, at(c.min)); rep != nil || err != nil {
			t.Fatalf("sweep at %d: %v %v", c.min, rep, err)
		}
		if tab.Len() != c.left || tab.Info().DurableSeq != tab.Info().LastSeq {
			t.Fatalf("swept at %d: Info = %+v", c.min, tab.Info())
		}
	}
}
