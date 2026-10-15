package pmap_test

import (
	"fmt"
	"maps"
	"testing"

	"github.com/shun159/molecule/pmap"
)

// BenchmarkSet sets a key of a map of size entries, keeping the map before
// as a pure state does: a Map, or a Go map cloned.
func BenchmarkSet(b *testing.B) {
	for _, size := range []int{1000, 100000, 1000000} {
		b.Run(fmt.Sprintf("pmap/%d", size), func(b *testing.B) {
			var m pmap.Map[int, int]
			for i := range size {
				m = m.Set(i, i)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				m = m.Set(i%size, i)
				i++
			}
		})
		if size > 100000 {
			continue // a clone of a million takes too long a while
		}
		b.Run(fmt.Sprintf("clone/%d", size), func(b *testing.B) {
			m := map[int]int{}
			for i := range size {
				m[i] = i
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				m = maps.Clone(m)
				m[i%size] = i
				i++
			}
		})
	}
}

// BenchmarkGet gets a key of a map of a million entries.
func BenchmarkGet(b *testing.B) {
	const size = 1000000
	m := pmap.Collect(func(yield func(int, int) bool) {
		for i := range size {
			if !yield(i, i) {
				return
			}
		}
	})
	i := 0
	for b.Loop() {
		m.Get(i % size)
		i++
	}
}
