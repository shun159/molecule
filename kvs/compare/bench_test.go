package compare

import (
	"math/rand/v2"
	"os"
	"testing"
)

// The benchmarks write to a directory made in $KVS_BENCH_DIR, if set, and
// to one of TempDir otherwise. A sync on tmpfs, which /tmp often is, costs
// nothing: to measure syncs, set KVS_BENCH_DIR to a directory on the disk.

func benchDir(b *testing.B) string {
	base := os.Getenv("KVS_BENCH_DIR")
	if base == "" {
		return b.TempDir()
	}
	dir, err := os.MkdirTemp(base, "compare-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func open(b *testing.B, e engine, dir string, durable bool) store {
	b.Helper()
	s, err := e.open(dir, durable)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

// fill puts every key once, to the page cache.
func fill(b *testing.B, s store) {
	b.Helper()
	for i := range uint64(numKeys) {
		if err := s.put(i, val); err != nil {
			b.Fatal(err)
		}
	}
}

func newRand() *rand.Rand { return rand.New(rand.NewPCG(rand.Uint64(), 0)) }

func BenchmarkGet(b *testing.B) {
	for _, e := range engines {
		s := open(b, e, benchDir(b), false)
		fill(b, s)
		b.Run("store="+e.name+"/mode=serial", func(b *testing.B) {
			r := newRand()
			for b.Loop() {
				if !s.get(r.Uint64N(numKeys)) {
					b.Fatal(errMissing)
				}
			}
		})
		b.Run("store="+e.name+"/mode=parallel", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				r := newRand()
				for pb.Next() {
					if !s.get(r.Uint64N(numKeys)) {
						b.Error(errMissing)
						return
					}
				}
			})
		})
		s.close()
	}
}

func BenchmarkPut(b *testing.B) {
	for _, e := range engines {
		for _, sync := range []string{"none", "each"} {
			b.Run("store="+e.name+"/sync="+sync, func(b *testing.B) {
				s := open(b, e, benchDir(b), sync == "each")
				defer s.close()
				r := newRand()
				for b.Loop() {
					if err := s.put(r.Uint64N(numKeys), val); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkPutDurable puts on disk from GOMAXPROCS goroutines: a store
// may sync the puts waiting together at once.
func BenchmarkPutDurable(b *testing.B) {
	for _, e := range engines {
		b.Run("store="+e.name, func(b *testing.B) {
			s := open(b, e, benchDir(b), true)
			defer s.close()
			b.RunParallel(func(pb *testing.PB) {
				r := newRand()
				for pb.Next() {
					if err := s.put(r.Uint64N(numKeys), val); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkOpen opens again a store closed after a put of each key.
func BenchmarkOpen(b *testing.B) {
	for _, e := range engines {
		b.Run("store="+e.name, func(b *testing.B) {
			dir := benchDir(b)
			s := open(b, e, dir, false)
			fill(b, s)
			if err := s.close(); err != nil {
				b.Fatal(err)
			}
			for b.Loop() {
				s := open(b, e, dir, false)
				if !s.get(numKeys - 1) {
					b.Fatal(errMissing)
				}
				if err := s.close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
