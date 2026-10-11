package kvs_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/shun159/molecule"
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

func (writer) Init(proc.PID) (struct{}, []molecule.Effect, error) { return struct{}{}, nil, nil }

func (w writer) HandleCall(s struct{}, k uint64, from genserver.From[error]) (struct{}, []molecule.Effect) {
	return s, molecule.Do(w.tab.PutEffect(k, benchVal, from))
}

func (writer) HandleCast(s struct{}, _ struct{}) (struct{}, []molecule.Effect) { return s, nil }

func (writer) HandleInfo(s struct{}, msg any) (struct{}, []molecule.Effect) {
	if d, ok := msg.(kvs.Durable); ok {
		return s, molecule.Do(d.Tag.(genserver.From[error]).Reply(d.Err))
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
