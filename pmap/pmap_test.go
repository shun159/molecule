package pmap_test

import (
	"bytes"
	"encoding/gob"
	"maps"
	"math/rand/v2"
	"testing"

	"github.com/shun159/molecule/pmap"
)

// A map does as a Go map does, under sets and deletes at random, each
// version staying as it was made.
func TestMap(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var m pmap.Map[int, int]
	want := map[int]int{}
	type version struct {
		m    pmap.Map[int, int]
		want map[int]int
	}
	var versions []version
	for i := range 20000 {
		k := r.IntN(2000)
		if r.IntN(3) == 0 {
			m = m.Delete(k)
			delete(want, k)
		} else {
			m = m.Set(k, i)
			want[k] = i
		}
		if i%1000 == 0 {
			versions = append(versions, version{m, maps.Clone(want)})
		}
	}
	versions = append(versions, version{m, want})
	for i, v := range versions {
		if got := maps.Collect(v.m.All()); v.m.Len() != len(v.want) || !maps.Equal(got, v.want) {
			t.Fatalf("version %d: %d entries, %d listed, want %d", i, v.m.Len(), len(got), len(v.want))
		}
		for k, val := range v.want {
			if got, ok := v.m.Get(k); !ok || got != val {
				t.Fatalf("version %d: %d is %d, %v; want %d", i, k, got, ok, val)
			}
		}
		if _, ok := v.m.Get(-1); ok {
			t.Fatalf("version %d has -1", i)
		}
	}
}

// The zero map is empty, and deleting from it leaves it so.
func TestZero(t *testing.T) {
	var m pmap.Map[string, int]
	if _, ok := m.Get("a"); ok || m.Len() != 0 || m.Delete("a").Len() != 0 {
		t.Errorf("zero map: %v", maps.Collect(m.All()))
	}
	for range m.All() {
		t.Error("the zero map lists an entry")
	}
}

// Collect makes the map of a sequence, the last value of a key winning.
func TestCollect(t *testing.T) {
	m := pmap.Collect(func(yield func(string, int) bool) {
		for i, k := range []string{"a", "b", "a"} {
			if !yield(k, i) {
				return
			}
		}
	})
	if got := maps.Collect(m.All()); !maps.Equal(got, map[string]int{"a": 2, "b": 1}) || m.Len() != 2 {
		t.Errorf("collected %v, %d", got, m.Len())
	}
}

// A map goes through gob, in a struct, as in the state of a behaviour.
func TestGob(t *testing.T) {
	type state struct {
		Names pmap.Map[string, int]
		Empty pmap.Map[string, int]
	}
	var in state
	for i := range 100 {
		in.Names = in.Names.Set(string(rune('a'+i%26))+string(rune('a'+i/26)), i)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(in); err != nil {
		t.Fatal(err)
	}
	var out state
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(maps.Collect(out.Names.All()), maps.Collect(in.Names.All())) || out.Names.Len() != 100 || out.Empty.Len() != 0 {
		t.Errorf("decoded %d entries, %d empty", out.Names.Len(), out.Empty.Len())
	}
}
