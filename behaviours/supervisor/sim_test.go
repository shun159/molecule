package supervisor

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// crasher is a worker crashing on a cast, its state the times it started.
type crasher struct{ genserver.Default[int] }

type crashNow struct{}

func (crasher) HandleCast(n int, _ crashNow) (int, []molecule.Effect) { panic("crash") }

func worker(id string, r Restart) ChildSpec {
	return ChildSpec{ID: id, Start: genserver.Child(crasher{}), Restart: r}
}

// tree is a simulation of a supervision tree.
type tree struct {
	t   *testing.T
	s   *gensim.Sim
	top proc.PID
}

func newTree(t *testing.T, seed uint64, spec Spec) *tree {
	t.Helper()
	s := gensim.New(seed)
	top, err := gensim.Start(s, Child(spec))
	if err != nil {
		t.Fatal(err)
	}
	return &tree{t, s, top}
}

// children returns the children of sup, by ID.
func (tr *tree) children(sup proc.PID) map[string]proc.PID {
	tr.t.Helper()
	v, err := tr.s.Call(sup, whichChildren{})
	if err != nil {
		tr.t.Fatal(err)
	}
	out := map[string]proc.PID{}
	for _, c := range v.([]ChildInfo) {
		out[c.ID] = c.PID
	}
	return out
}

func (tr *tree) crash(pid proc.PID) {
	tr.s.Cast(pid, crashNow{})
	tr.s.RunUntilIdle()
}

func TestSimTree(t *testing.T) {
	inner := Spec{Strategy: RestForOne, Intensity: 5, Children: []ChildSpec{
		worker("c", Permanent), worker("d", Permanent), worker("e", Permanent),
	}}
	tr := newTree(t, 1, Spec{Intensity: 5, Children: []ChildSpec{
		worker("a", Permanent),
		{ID: "inner", Start: Child(inner), Type: Supervisor},
		worker("b", Permanent),
	}})
	top := tr.children(tr.top)
	in := tr.children(top["inner"])

	// rest_for_one: d and e restart, c stays.
	tr.crash(in["d"])
	after := tr.children(top["inner"])
	if after["c"] != in["c"] || after["d"] == in["d"] || after["e"] == in["e"] {
		t.Errorf("inner after d crashed: %v, was %v", after, in)
	}
	for id, pid := range after {
		if !tr.s.Alive(pid) {
			t.Errorf("%s not running", id)
		}
	}
	// The top is untouched.
	if now := tr.children(tr.top); now["a"] != top["a"] || now["inner"] != top["inner"] || now["b"] != top["b"] {
		t.Errorf("top after d crashed: %v, was %v", now, top)
	}
}

func TestSimIntensity(t *testing.T) {
	tr := newTree(t, 1, Spec{Intensity: 2, Period: 5 * time.Second, Children: []ChildSpec{
		worker("a", Permanent), worker("b", Permanent),
	}})
	// Spread out, crashes stay within the limit.
	for range 4 {
		tr.crash(tr.children(tr.top)["b"])
		tr.s.Advance(3 * time.Second)
	}
	if !tr.s.Alive(tr.top) {
		t.Fatal("gave up on crashes spread out")
	}
	// In a row, they do not: the third is one too many.
	tr.s.Advance(10 * time.Second) // the window empties
	a := tr.children(tr.top)["a"]
	for i := range 3 {
		if !tr.s.Alive(tr.top) {
			t.Fatalf("gave up after %d crashes", i)
		}
		tr.crash(tr.children(tr.top)["b"])
	}
	if tr.s.Alive(tr.top) || tr.s.Alive(a) {
		t.Error("tree alive after too many restarts")
	}
}

func TestSimDynamic(t *testing.T) {
	tr := newTree(t, 1, Spec{Children: []ChildSpec{
		{ID: "pool", Start: DynamicChild(DynamicSpec{MaxChildren: 2, Intensity: 5}), Type: Supervisor},
	}})
	pool := tr.children(tr.top)["pool"]
	start := func() startResult {
		v, err := tr.s.Call(pool, startChild{ChildSpec{Start: genserver.Child(crasher{})}})
		if err != nil {
			t.Fatal(err)
		}
		return v.(startResult)
	}
	first, second := start(), start()
	if first.err != nil || second.err != nil {
		t.Fatalf("starts: %v, %v", first.err, second.err)
	}
	if third := start(); third.err != ErrMaxChildren {
		t.Errorf("third start: %v", third.err)
	}
	tr.crash(first.pid)
	v, _ := tr.s.Call(pool, countChildren{})
	if v != 2 || tr.s.Alive(first.pid) || !tr.s.Alive(second.pid) {
		t.Errorf("after a crash: %v children", v)
	}
}

// TestSimChaos crashes workers at random, for many seeds: once quiet, the
// tree either runs every permanent child, or has given up, all of it.
func TestSimChaos(t *testing.T) {
	for seed := range uint64(200) {
		inner := Spec{Strategy: Strategy(seed % 3), Intensity: 3, Period: time.Second, Children: []ChildSpec{
			worker("c", Permanent), worker("d", Transient), worker("e", Temporary),
		}}
		tr := newTree(t, seed, Spec{Strategy: Strategy((seed / 3) % 3), Intensity: 3, Period: time.Second, Children: []ChildSpec{
			worker("a", Permanent),
			{ID: "inner", Start: Child(inner), Type: Supervisor},
		}})
		r := rand.New(rand.NewPCG(seed, 3))
		var all []proc.PID
		for range 10 {
			if !tr.s.Alive(tr.top) {
				break
			}
			top := tr.children(tr.top)
			targets := []proc.PID{top["a"]}
			if tr.s.Alive(top["inner"]) {
				for _, pid := range tr.children(top["inner"]) {
					targets = append(targets, pid)
				}
			}
			all = append(all, targets...)
			if pid := targets[r.IntN(len(targets))]; !pid.IsZero() {
				tr.s.Cast(pid, crashNow{})
			}
			for range r.IntN(10) {
				tr.s.Step()
			}
			tr.s.Advance(time.Duration(r.IntN(600)) * time.Millisecond)
		}
		tr.s.RunUntilIdle()
		if !tr.s.Alive(tr.top) {
			for _, pid := range all {
				if tr.s.Alive(pid) {
					t.Fatalf("seed %d: %v alive, the tree given up", seed, pid)
				}
			}
			continue
		}
		top := tr.children(tr.top)
		if !tr.s.Alive(top["a"]) || !tr.s.Alive(top["inner"]) {
			t.Fatalf("seed %d: top children %v not all running", seed, top)
		}
		if c := tr.children(top["inner"])["c"]; !tr.s.Alive(c) {
			t.Fatalf("seed %d: permanent c not running\n%s", seed, fmt.Sprint(tr.children(top["inner"])))
		}
	}
}

func TestSimDeterministic(t *testing.T) {
	run := func() string {
		tr := newTree(t, 9, Spec{Strategy: OneForAll, Intensity: 5, Children: []ChildSpec{
			worker("a", Permanent), worker("b", Permanent), worker("c", Permanent),
		}})
		for range 3 {
			tr.crash(tr.children(tr.top)["b"])
		}
		return tr.s.TraceString()
	}
	if a, b := run(), run(); a != b {
		t.Error("same seed, different runs")
	}
}

func TestSimRestartCounts(t *testing.T) {
	tr := newTree(t, 1, Spec{Strategy: RestForOne, Intensity: 10, Children: []ChildSpec{
		worker("a", Permanent), worker("b", Permanent), worker("c", Permanent),
	}})
	tr.crash(tr.children(tr.top)["b"])
	tr.crash(tr.children(tr.top)["c"])
	v, _ := tr.s.Call(tr.top, whichChildren{})
	got := map[string]int{}
	for _, c := range v.([]ChildInfo) {
		got[c.ID] = c.Restarts
	}
	if want := map[string]int{"a": 0, "b": 1, "c": 2}; !maps.Equal(got, want) {
		t.Errorf("restarts %v, want %v", got, want)
	}
}
