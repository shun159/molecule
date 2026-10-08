package pg

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// member is a process to join groups.
type member struct{ genserver.Default[struct{}] }

type cluster struct {
	t      *testing.T
	s      *gensim.Sim
	scopes map[string]proc.PID
}

func newCluster(t *testing.T, seed uint64, nodes ...string) *cluster {
	c := &cluster{t: t, s: gensim.New(seed), scopes: map[string]proc.PID{}}
	for _, n := range nodes {
		c.startScope(n)
	}
	return c
}

func (c *cluster) startScope(node string) {
	c.t.Helper()
	pid, err := gensim.Spawn(c.s, genserver.Gen(Scope{Name: "pg"}), nil, gensim.On(node), gensim.Named("pg"))
	if err != nil {
		c.t.Fatal(err)
	}
	c.scopes[node] = pid
}

func (c *cluster) member(node string) proc.PID {
	c.t.Helper()
	pid, err := gensim.Spawn(c.s, genserver.Gen(member{}), nil, gensim.On(node))
	if err != nil {
		c.t.Fatal(err)
	}
	return pid
}

func (c *cluster) do(node string, req any) {
	c.s.Cast(c.scopes[node], req)
	c.s.RunUntilIdle()
}

// members returns the members of group as the scope of node sees them,
// sorted.
func (c *cluster) members(node, group string) []proc.PID {
	c.t.Helper()
	v, err := c.s.Call(c.scopes[node], MembersRequest{group})
	if err != nil {
		c.t.Fatal(err)
	}
	return sorted(v.([]proc.PID))
}

func sorted(pids []proc.PID) []proc.PID {
	return slices.SortedFunc(slices.Values(pids), func(a, b proc.PID) int { return cmp.Compare(a.String(), b.String()) })
}

func (c *cluster) want(group string, want []proc.PID, nodes ...string) {
	c.t.Helper()
	for _, n := range nodes {
		if got := c.members(n, group); !slices.Equal(got, sorted(want)) {
			c.t.Errorf("%s sees %s as %v, want %v", n, group, got, sorted(want))
		}
	}
}

func TestDistributed(t *testing.T) {
	c := newCluster(t, 1, "a", "b", "c")
	pa, pb, pc := c.member("a"), c.member("b"), c.member("c")
	c.do("a", join{"g", []proc.PID{pa}})
	c.do("b", join{"g", []proc.PID{pb, pb}})
	c.do("c", join{"h", []proc.PID{pc}})
	c.want("g", []proc.PID{pa, pb, pb}, "a", "b", "c")
	c.want("h", []proc.PID{pc}, "a", "b", "c")

	// Cut apart, each side sees its own.
	c.s.Partition([]string{"a"}, []string{"b", "c"})
	c.s.RunUntilIdle()
	c.want("g", []proc.PID{pa}, "a")
	c.want("g", []proc.PID{pb, pb}, "b", "c")
	c.do("a", leave{"g", []proc.PID{pa}})
	c.do("b", join{"h", []proc.PID{pb}})

	// Healed, all agree again.
	c.s.Heal()
	c.s.RunUntilIdle()
	c.want("g", []proc.PID{pb, pb}, "a", "b", "c")
	c.want("h", []proc.PID{pc, pb}, "a", "b", "c")

	// A node crashed, its members are gone.
	c.s.Crash("c")
	c.s.RunUntilIdle()
	c.want("h", []proc.PID{pb}, "a", "b")

	// A member exits: it leaves every group, everywhere.
	c.s.Exit(pb, proc.Kill)
	c.s.RunUntilIdle()
	c.want("g", nil, "a", "b")
	c.want("h", nil, "a", "b")

	// Restarted, the node takes part again.
	c.s.Restart("c")
	c.startScope("c")
	pc2 := c.member("c")
	c.do("c", join{"g", []proc.PID{pc2}})
	c.want("g", []proc.PID{pc2}, "a", "b", "c")
}

// TestConverges runs scopes through random joins, leaves, exits,
// partitions and crashes; once all heal, every scope sees, for each
// group, the members joined through the scopes alive.
func TestConverges(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	groups := []string{"g", "h"}
	for seed := range uint64(200) {
		c := newCluster(t, seed, nodes...)
		r := rand.New(rand.NewPCG(seed, 2))
		var members []proc.PID
		for range 40 {
			n := nodes[r.IntN(len(nodes))]
			switch x := r.IntN(10); {
			case x < 4 && c.s.Alive(c.scopes[n]):
				pid := c.member(n)
				members = append(members, pid)
				c.s.Cast(c.scopes[n], join{groups[r.IntN(2)], []proc.PID{pid}})
			case x < 5 && len(members) > 0:
				pid := members[r.IntN(len(members))]
				c.s.Cast(c.scopes[pid.Node()], leave{groups[r.IntN(2)], []proc.PID{pid}})
			case x < 6 && len(members) > 0:
				c.s.Exit(members[r.IntN(len(members))], proc.Kill)
			case x < 7:
				cut := slices.Clone(nodes)
				r.Shuffle(len(cut), func(i, j int) { cut[i], cut[j] = cut[j], cut[i] })
				c.s.Partition(cut[:1], cut[1:])
			case x < 8:
				c.s.Heal()
			case x < 9 && c.s.Alive(c.scopes[n]):
				c.s.Crash(n)
			default:
				if !c.s.Alive(c.scopes[n]) {
					c.s.Restart(n)
					c.startScope(n)
				}
			}
			for range r.IntN(20) {
				c.s.Step()
			}
		}
		c.s.Heal()
		for _, n := range nodes {
			if !c.s.Alive(c.scopes[n]) {
				c.s.Restart(n)
				c.startScope(n)
			}
		}
		c.s.RunUntilIdle()
		for _, g := range groups {
			var want []proc.PID
			for _, n := range nodes {
				s, _ := gensim.State[state](c.s, c.scopes[n])
				want = append(want, s.local.members[g]...)
			}
			for _, n := range nodes {
				if got := c.members(n, g); !slices.Equal(got, sorted(want)) {
					t.Fatalf("seed %d: %s sees %s as %v, want %v\n%s", seed, n, g, got, sorted(want), tail(c.s.TraceString()))
				}
			}
		}
	}
}

func tail(s string) string {
	if len(s) > 4000 {
		return "..." + s[len(s)-4000:]
	}
	return s
}

// TestLateScope starts a scope on a node already connected, and joins a
// member through it at once, before the scope knows the others: they learn
// of the member all the same, as the scope that discovers them is asked
// what it has.
func TestLateScope(t *testing.T) {
	for seed := range uint64(20) {
		c := newCluster(t, seed, "a", "b")
		// The node is connected, and told so, before it has a scope.
		pd := c.member("d")
		c.s.RunUntilIdle()
		pid, err := gensim.Spawn(c.s, genserver.Gen(Scope{Name: "pg"}), nil, gensim.On("d"), gensim.Named("pg"))
		if err != nil {
			t.Fatal(err)
		}
		c.scopes["d"] = pid
		c.s.Cast(pid, join{"g", []proc.PID{pd}})
		c.s.RunUntilIdle()
		c.want("g", []proc.PID{pd}, "a", "b", "d")
	}
}
