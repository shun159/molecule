package main

import (
	"fmt"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/kvs"
)

const keys = 30000

func key(i int) string { return fmt.Sprintf("key%d", i) }

// Each node owns about its share of the keys.
func TestRingSpread(t *testing.T) {
	r := NewRing([]string{"a", "b", "c"}, 64)
	owned := map[string]int{}
	for i := range keys {
		owned[r.Owner(key(i))]++
	}
	for _, n := range []string{"a", "b", "c"} {
		if share := float64(owned[n]) / keys; share < 0.2 || share > 0.47 {
			t.Errorf("%s owns %.2f of the keys: %v", n, share, owned)
		}
	}
}

// A node added takes about 1/4 of the keys, all from the others to it,
// none between the others.
func TestRingAddNode(t *testing.T) {
	before := NewRing([]string{"a", "b", "c"}, 64)
	after := NewRing([]string{"a", "b", "c", "d"}, 64)
	moved := 0
	for i := range keys {
		was, is := before.Owner(key(i)), after.Owner(key(i))
		if was == is {
			continue
		}
		if is != "d" {
			t.Fatalf("%s moved from %s to %s", key(i), was, is)
		}
		moved++
	}
	if f := float64(moved) / keys; f < 0.15 || f > 0.35 {
		t.Errorf("%.2f of the keys moved", f)
	}
}

// TestStores puts keys and gets them back over three simulated nodes,
// each served by its node; with c crashed, the keys of a and b remain.
func TestStores(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	ring := NewRing(nodes, 64)
	s := gensim.New(1)
	for _, n := range nodes {
		dir := t.TempDir()
		if _, err := gensim.Start(s, supervisor.Child(kvringSup(n, dir)), gensim.On(n)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { // a node crashed, or the simulation ended, leaves it open
			if table, err := kvs.Opened[string, string](dir); err == nil {
				table.Close()
			}
		})
	}
	do := func(req request) (Reply, error) {
		v, err := s.Call(actor.Remote{Node: ring.Owner(req.key()), Name: string(storeName)}, req)
		r, _ := v.(Reply)
		return r, err
	}

	served := map[string]bool{}
	for i := range 50 {
		if r, err := do(putReq{Key: key(i), Value: fmt.Sprint(i)}); err != nil || r.Node != ring.Owner(key(i)) {
			t.Fatalf("put %s: %+v, %v", key(i), r, err)
		}
	}
	for i := range 50 {
		r, err := do(getReq{Key: key(i)})
		if err != nil || !r.Found || r.Value != fmt.Sprint(i) || r.Node != ring.Owner(key(i)) {
			t.Fatalf("get %s: %+v, %v", key(i), r, err)
		}
		served[r.Node] = true
	}
	if len(served) != 3 {
		t.Errorf("served by %v only", served)
	}

	s.Crash("c")
	for i := range 50 {
		_, err := do(getReq{Key: key(i)})
		if c := ring.Owner(key(i)) == "c"; c != (err != nil) {
			t.Errorf("get %s of %s with c crashed: %v", key(i), ring.Owner(key(i)), err)
		}
	}
}
