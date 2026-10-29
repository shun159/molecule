package kvs_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/molecule/proc"
)

// The benchmarks write to a directory made in $KVS_BENCH_DIR, if set, and
// to one of TempDir otherwise. A sync on tmpfs, which /tmp often is, costs
// nothing: to measure syncs, set KVS_BENCH_DIR to a directory on the disk.
//
//	KVS_BENCH_DIR=$HOME/tmp go test ./kvs -run '^$' -bench .

const (
	benchKeys    = 100_000
	benchValSize = 64
)

var benchVal = make([]byte, benchValSize)

func benchDir(b *testing.B) string {
	base := os.Getenv("KVS_BENCH_DIR")
	if base == "" {
		return b.TempDir()
	}
	dir, err := os.MkdirTemp(base, "kvs-bench-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func benchOptions(read, write bool, sync kvs.Sync) kvs.Options[uint64, []byte] {
	return kvs.Options[uint64, []byte]{ReadConcurrency: read, WriteConcurrency: write, Sync: sync}
}

// filled opens a table of benchKeys entries.
func filled(b *testing.B, read, write bool) *kvs.Table[uint64, []byte] {
	b.Helper()
	tab := open(b, benchDir(b), benchOptions(read, write, kvs.SyncNone))
	b.Cleanup(func() { tab.Close() })
	for k := range uint64(benchKeys) {
		if err := tab.Put(k, benchVal); err != nil {
			b.Fatal(err)
		}
	}
	return tab
}

func BenchmarkGet(b *testing.B) {
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			tab := filled(b, m.read, m.write)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), 0))
				for pb.Next() {
					if _, ok := tab.Get(r.Uint64N(benchKeys)); !ok {
						b.Error("missing")
						return
					}
				}
			})
		})
	}
	// Baselines: the maps alone, without a log.
	b.Run("baseline=rwmutex", func(b *testing.B) {
		var mu sync.RWMutex
		m := make(map[uint64][]byte, benchKeys)
		for k := range uint64(benchKeys) {
			m[k] = benchVal
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			r := rand.New(rand.NewPCG(rand.Uint64(), 0))
			for pb.Next() {
				mu.RLock()
				_ = m[r.Uint64N(benchKeys)]
				mu.RUnlock()
			}
		})
	})
	b.Run("baseline=sync.Map", func(b *testing.B) {
		var m sync.Map
		for k := range uint64(benchKeys) {
			m.Store(k, benchVal)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			r := rand.New(rand.NewPCG(rand.Uint64(), 0))
			for pb.Next() {
				m.Load(r.Uint64N(benchKeys))
			}
		})
	})
}

// BenchmarkPut puts from GOMAXPROCS goroutines, each waiting for its put
// to be durable.
func BenchmarkPut(b *testing.B) {
	for _, m := range modes {
		for _, s := range []struct {
			name string
			sync kvs.Sync
		}{{"none", kvs.SyncNone}, {"each", kvs.SyncEach}} {
			b.Run(m.name+"/sync="+s.name, func(b *testing.B) {
				tab := open(b, benchDir(b), benchOptions(m.read, m.write, s.sync))
				defer tab.Close()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					r := rand.New(rand.NewPCG(rand.Uint64(), 0))
					for pb.Next() {
						if err := tab.Put(r.Uint64N(benchKeys), benchVal); err != nil {
							b.Error(err)
							return
						}
					}
				})
				b.StopTimer()
				reportSyncs(b, tab)
			})
		}
	}
}

// BenchmarkPutDurable puts with SyncEach from a number of goroutines: the
// more wait together, the more share a sync.
func BenchmarkPutDurable(b *testing.B) {
	for _, n := range []int{1, 8, 64} {
		b.Run(fmt.Sprint("goroutines=", n), func(b *testing.B) {
			tab := open(b, benchDir(b), benchOptions(false, false, kvs.SyncEach))
			defer tab.Close()
			b.ResetTimer()
			parallel(b, n, func(r *rand.Rand) error { return tab.Put(r.Uint64N(benchKeys), benchVal) })
			b.StopTimer()
			reportSyncs(b, tab)
		})
	}
	// Baseline: a write and a sync of a file, for each put.
	b.Run("baseline=write+fsync", func(b *testing.B) {
		f, err := os.Create(filepath.Join(benchDir(b), "f"))
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		rec := make([]byte, 8+1+1+8+benchValSize+8)
		for b.Loop() {
			if _, err := f.Write(rec); err != nil {
				b.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// parallel runs f b.N times over n goroutines.
func parallel(b *testing.B, n int, f func(*rand.Rand) error) {
	var wg sync.WaitGroup
	var next sync.Mutex
	left := b.N
	for range n {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(rand.Uint64(), 0))
			for {
				next.Lock()
				if left == 0 {
					next.Unlock()
					return
				}
				left--
				next.Unlock()
				if err := f(r); err != nil {
					b.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

func reportSyncs(b *testing.B, tab *kvs.Table[uint64, []byte]) {
	if syncs := tab.Info().Syncs; syncs > 0 {
		b.ReportMetric(float64(b.N)/float64(syncs), "puts/sync")
	}
}

// BenchmarkMixed reads and writes from GOMAXPROCS goroutines, the writes
// not synced.
func BenchmarkMixed(b *testing.B) {
	for _, reads := range []int{90, 50} {
		for _, m := range modes {
			b.Run(fmt.Sprintf("reads=%d%%/%s", reads, m.name), func(b *testing.B) {
				tab := filled(b, m.read, m.write)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					r := rand.New(rand.NewPCG(rand.Uint64(), 0))
					for pb.Next() {
						k := r.Uint64N(benchKeys)
						if r.IntN(100) < reads {
							tab.Get(k)
						} else if err := tab.Put(k, benchVal); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		}
	}
}

// writer is a server putting what it is called with, and answering once
// the put is durable.
type writer struct{ tab *kvs.Table[uint64, []byte] }

func (writer) Init(proc.PID) (struct{}, []actor.Effect, error) { return struct{}{}, nil, nil }

func (w writer) HandleCall(s struct{}, k uint64, from genserver.From[error]) (struct{}, []actor.Effect) {
	return s, actor.Do(w.tab.PutEffect(k, benchVal, from))
}

func (writer) HandleCast(s struct{}, _ struct{}) (struct{}, []actor.Effect) { return s, nil }

func (writer) HandleInfo(s struct{}, msg any) (struct{}, []actor.Effect) {
	if d, ok := msg.(kvs.Durable); ok {
		return s, actor.Do(d.Tag.(genserver.From[error]).Reply(d.Err))
	}
	return s, nil
}

// BenchmarkPutEffect calls a server putting with PutEffect from
// GOMAXPROCS goroutines, each call answered once its put is durable: what
// a server answering only what is safe on disk pays.
func BenchmarkPutEffect(b *testing.B) {
	for _, s := range []struct {
		name string
		sync kvs.Sync
	}{{"none", kvs.SyncNone}, {"each", kvs.SyncEach}} {
		b.Run("sync="+s.name, func(b *testing.B) {
			tab := open(b, benchDir(b), benchOptions(false, false, s.sync))
			defer tab.Close()
			n := proc.NewNode("")
			ctx := context.Background()
			srv, err := genserver.Start(ctx, n, writer{tab})
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), 0))
				for pb.Next() {
					if rep, err := srv.Call(ctx, n, r.Uint64N(benchKeys)); rep != nil || err != nil {
						b.Error(rep, err)
						return
					}
				}
			})
			b.StopTimer()
			reportSyncs(b, tab)
		})
	}
}

// BenchmarkOpen opens a table whose log holds two writes of each of
// benchKeys keys.
func BenchmarkOpen(b *testing.B) {
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			dir := benchDir(b)
			o := benchOptions(m.read, m.write, kvs.SyncNone)
			o.CompactRatio = -1
			tab := open(b, dir, o)
			for range 2 {
				for k := range uint64(benchKeys) {
					tab.Put(k, benchVal)
				}
			}
			tab.Close()
			b.ResetTimer()
			for b.Loop() {
				tab := open(b, dir, o)
				tab.Close()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/(2*benchKeys), "ns/record")
		})
	}
}

// BenchmarkCompact compacts a table of benchKeys entries.
func BenchmarkCompact(b *testing.B) {
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			tab := filled(b, m.read, m.write)
			b.ResetTimer()
			for b.Loop() {
				if err := tab.Compact(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAll ranges over a table of benchKeys entries.
func BenchmarkAll(b *testing.B) {
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			tab := filled(b, m.read, m.write)
			b.ResetTimer()
			for b.Loop() {
				n := 0
				for range tab.All() {
					n++
				}
				if n != benchKeys {
					b.Fatal(n)
				}
			}
		})
	}
}

// BenchmarkPutSerial puts from one goroutine, unsynced: the latency of a
// put alone.
func BenchmarkPutSerial(b *testing.B) {
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			tab := open(b, benchDir(b), benchOptions(m.read, m.write, kvs.SyncNone))
			defer tab.Close()
			r := rand.New(rand.NewPCG(1, 2))
			for b.Loop() {
				if err := tab.Put(r.Uint64N(benchKeys), benchVal); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkOneWriter reads from GOMAXPROCS goroutines while one goroutine
// puts, unsynced, all along: the reads per op, the puts as a metric.
func BenchmarkOneWriter(b *testing.B) {
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			tab := filled(b, m.read, m.write)
			oneWriter(b, func(k uint64) { tab.Get(k) }, func(k uint64) { tab.Put(k, benchVal) })
		})
	}
	// Baselines: the maps alone, without a log.
	b.Run("baseline=rwmutex", func(b *testing.B) {
		var mu sync.RWMutex
		m := make(map[uint64][]byte, benchKeys)
		for k := range uint64(benchKeys) {
			m[k] = benchVal
		}
		oneWriter(b, func(k uint64) { mu.RLock(); _ = m[k]; mu.RUnlock() },
			func(k uint64) { mu.Lock(); m[k] = benchVal; mu.Unlock() })
	})
	b.Run("baseline=sync.Map", func(b *testing.B) {
		var m sync.Map
		for k := range uint64(benchKeys) {
			m.Store(k, benchVal)
		}
		oneWriter(b, func(k uint64) { m.Load(k) }, func(k uint64) { m.Store(k, benchVal) })
	})
}

func oneWriter(b *testing.B, get, put func(uint64)) {
	stop := make(chan struct{})
	puts := make(chan int)
	go func() {
		r := rand.New(rand.NewPCG(3, 4))
		n := 0
		for {
			select {
			case <-stop:
				puts <- n
				return
			default:
			}
			put(r.Uint64N(benchKeys))
			n++
		}
	}()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewPCG(rand.Uint64(), 0))
		for pb.Next() {
			get(r.Uint64N(benchKeys))
		}
	})
	b.StopTimer()
	close(stop)
	b.ReportMetric(float64(<-puts)/b.Elapsed().Seconds(), "puts/s")
}

// BenchmarkOrdered measures an OrderedSet of benchKeys entries:
// parallel gets, puts from one goroutine, unsynced, ranges of 100 entries,
// and opening, to compare with the benchmarks of the tables without.
func BenchmarkOrdered(b *testing.B) {
	ordered := func(m struct {
		name        string
		read, write bool
	}) kvs.Options[uint64, []byte] {
		o := benchOptions(m.read, m.write, kvs.SyncNone)
		o.Type = kvs.OrderedSet
		return o
	}
	filledOrdered := func(b *testing.B, o kvs.Options[uint64, []byte]) *kvs.Table[uint64, []byte] {
		tab := open(b, benchDir(b), o)
		b.Cleanup(func() { tab.Close() })
		for _, k := range rand.New(rand.NewPCG(1, 1)).Perm(benchKeys) {
			tab.Put(uint64(k), benchVal)
		}
		return tab
	}
	for _, m := range modes {
		b.Run("get/"+m.name, func(b *testing.B) {
			tab := filledOrdered(b, ordered(m))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), 0))
				for pb.Next() {
					if _, ok := tab.Get(r.Uint64N(benchKeys)); !ok {
						b.Error("missing")
						return
					}
				}
			})
		})
	}
	for _, m := range modes {
		b.Run("put/"+m.name, func(b *testing.B) {
			tab := filledOrdered(b, ordered(m))
			r := rand.New(rand.NewPCG(1, 2))
			b.ResetTimer()
			for b.Loop() {
				tab.Put(r.Uint64N(benchKeys), benchVal)
			}
		})
	}
	for _, m := range modes {
		b.Run("range100/"+m.name, func(b *testing.B) {
			tab := filledOrdered(b, ordered(m))
			r := rand.New(rand.NewPCG(1, 2))
			b.ResetTimer()
			for b.Loop() {
				lo := r.Uint64N(benchKeys - 100)
				n := 0
				for range tab.Range(lo, lo+100) {
					n++
				}
				if n != 100 {
					b.Fatal(n)
				}
			}
		})
	}
	for _, m := range modes {
		b.Run("open/"+m.name, func(b *testing.B) {
			dir := benchDir(b)
			o := ordered(m)
			tab := open(b, dir, o)
			for k := range uint64(benchKeys) {
				tab.Put(k, benchVal)
			}
			tab.Close()
			b.ResetTimer()
			for b.Loop() {
				tab := open(b, dir, o)
				tab.Close()
			}
		})
	}
}

// BenchmarkPutExpiring puts, unsynced, to a table with Expires, from one
// goroutine and from GOMAXPROCS, to compare with BenchmarkPutSerial and
// BenchmarkPut: the cost of noting when each entry expires.
func BenchmarkPutExpiring(b *testing.B) {
	o := func(m struct {
		name        string
		read, write bool
	}) kvs.Options[uint64, time.Time] {
		return kvs.Options[uint64, time.Time]{
			ReadConcurrency: m.read, WriteConcurrency: m.write, Sync: kvs.SyncNone,
			Expires: func(t time.Time) time.Time { return t },
		}
	}
	exp := time.Now().Add(time.Hour)
	for _, m := range modes {
		b.Run("serial/"+m.name, func(b *testing.B) {
			tab := open(b, benchDir(b), o(m))
			defer tab.Close()
			r := rand.New(rand.NewPCG(1, 2))
			for b.Loop() {
				tab.Put(r.Uint64N(benchKeys), exp)
			}
		})
	}
	for _, m := range modes {
		b.Run("parallel/"+m.name, func(b *testing.B) {
			tab := open(b, benchDir(b), o(m))
			defer tab.Close()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), 0))
				for pb.Next() {
					tab.Put(r.Uint64N(benchKeys), exp)
				}
			})
		})
	}
	b.Run("expire", func(b *testing.B) {
		m := modes[2]
		tab := open(b, benchDir(b), o(m))
		defer tab.Close()
		for b.Loop() {
			b.StopTimer()
			for k := range uint64(1000) {
				tab.Put(k, exp)
			}
			b.StartTimer()
			if n, _ := tab.Expire(exp); n != 1000 {
				b.Fatal(n)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/1000, "ns/entry")
	})
}

// BenchmarkPutExpiringFew rewrites a few entries that expire, in a table
// of benchKeys that do not: the times out of date it drops are of those
// few.
func BenchmarkPutExpiringFew(b *testing.B) {
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			tab := open(b, benchDir(b), expiringOptions(m.read, m.write))
			defer tab.Close()
			for k := range benchKeys {
				if err := tab.Put(fmt.Sprint(k), expiring{}); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			i := 0
			for b.Loop() {
				i++
				if err := tab.Put(fmt.Sprint("x", i%10), expiring{Expiry: at(i)}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSnapshot takes a snapshot of a table of 100000 entries, in
// each mode: little, keeping the trees or the tries; a copy of the maps
// of a Set without ReadConcurrency.
func BenchmarkSnapshot(b *testing.B) {
	for _, typ := range []kvs.Type{kvs.Set, kvs.OrderedSet} {
		for _, m := range modes {
			b.Run(fmt.Sprintf("%v/%s", typ, m.name), func(b *testing.B) {
				o := kvs.Options[int, int]{ReadConcurrency: m.read, WriteConcurrency: m.write, Shards: 8, Type: typ, Sync: kvs.SyncNone}
				tab := open(b, b.TempDir(), o)
				for k := range 100000 {
					if err := tab.Put(k, k); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					tab.Snapshot()
				}
			})
		}
	}
}
