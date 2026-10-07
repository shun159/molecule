package main

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/genstatem"
	"github.com/shun159/molecule/proc"
)

// TestSimulation runs clusters of five servers through partitions,
// crashes, restarts and lost messages, each run decided by a seed, and
// checks the safety of Raft after every step:
//
//   - Election Safety: at most one leader in a term.
//   - State Machine Safety: servers never commit different entries at the
//     same index, and what is committed stays so.
//   - Leader Completeness: a leader has every entry committed in an
//     earlier term. A leader of an older term, cut off, may lack some.
//
// Then it heals all, and checks a leader is elected and commits.
func TestSimulation(t *testing.T) {
	seeds := uint64(100)
	if testing.Short() {
		seeds = 10
	}
	if n, err := strconv.ParseUint(os.Getenv("RAFT_SEEDS"), 10, 64); err == nil {
		seeds = n // RAFT_SEEDS=10000 go test ./examples/raft searches longer
	}
	for seed := range seeds {
		if err := simulate(seed); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

var nodes = []string{"n1", "n2", "n3", "n4", "n5"}

type cluster struct {
	s     *gensim.Sim
	seed  uint64
	pids  map[string]proc.PID
	disks map[string]Persistent // of the crashed servers

	leaders    map[uint64]string // the leader of each term seen
	committed  []Entry           // the longest committed log seen
	commitTerm []uint64          // the term each entry was seen committed in, at the latest
}

func simulate(seed uint64) error {
	c := &cluster{
		s:       gensim.New(seed),
		seed:    seed,
		pids:    map[string]proc.PID{},
		disks:   map[string]Persistent{},
		leaders: map[uint64]string{},
	}
	c.s.Loss = 0.05
	for _, n := range nodes {
		if err := c.start(n); err != nil {
			return err
		}
	}
	chaos := rand.New(rand.NewPCG(seed, 1))
	for i := range 100 {
		switch r := chaos.IntN(100); {
		case r < 40:
			target := c.leader()
			if target == "" {
				target = nodes[chaos.IntN(len(nodes))]
			}
			c.s.Cast(gen.Remote{Node: target, Name: raftName}, Propose{fmt.Sprintf("cmd %d", i)})
		case r < 50:
			shuffled := slices.Clone(nodes)
			chaos.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			cut := 1 + chaos.IntN(len(nodes)-1)
			c.s.Partition(shuffled[:cut], shuffled[cut:])
		case r < 60:
			c.s.Heal()
		case r < 65:
			if len(c.disks) < 2 {
				c.crash(nodes[chaos.IntN(len(nodes))])
			}
		case r < 75:
			if down := slices.Sorted(maps.Keys(c.disks)); len(down) > 0 {
				if err := c.restart(down[0]); err != nil {
					return err
				}
			}
		}
		if err := c.s.Run(100*time.Millisecond, c.check); err != nil {
			return err
		}
	}

	// All well again, a leader is elected, and commits.
	c.s.Heal()
	for _, n := range slices.Sorted(maps.Keys(c.disks)) {
		if err := c.restart(n); err != nil {
			return err
		}
	}
	c.s.Loss = 0
	if err := c.s.Run(3*time.Second, c.check); err != nil {
		return err
	}
	leader := c.leader()
	if leader == "" {
		return fmt.Errorf("no leader once healed")
	}
	c.s.Cast(gen.Remote{Node: leader, Name: raftName}, Propose{"last"})
	if err := c.s.Run(time.Second, c.check); err != nil {
		return err
	}
	for _, n := range nodes {
		d, _ := c.data(n)
		if !slices.ContainsFunc(d.Log[:d.Commit], func(e Entry) bool { return e.Cmd == "last" }) {
			return fmt.Errorf("%s has not committed the last command: %v", n, d.Log[:d.Commit])
		}
	}
	return nil
}

func (c *cluster) start(n string) error {
	srv := Server{
		Node:            n,
		Peers:           slices.DeleteFunc(slices.Clone(nodes), func(p string) bool { return p == n }),
		Seed:            c.seed,
		Disk:            c.disks[n],
		ElectionTimeout: 150 * time.Millisecond,
		Heartbeat:       50 * time.Millisecond,
	}
	pid, err := gensim.Spawn(c.s, genstatem.Gen[Role, Data](srv), nil, gensim.On(n), gensim.Named(raftName))
	c.pids[n] = pid
	return err
}

// crash crashes n, keeping what it has written.
func (c *cluster) crash(n string) {
	d, ok := c.data(n)
	if !ok {
		return
	}
	c.disks[n] = d.Persistent
	c.s.Crash(n)
}

func (c *cluster) restart(n string) error {
	c.s.Restart(n)
	err := c.start(n)
	delete(c.disks, n)
	return err
}

func (c *cluster) data(n string) (Data, bool) {
	m, ok := gensim.State[genstatem.Machine[Role, Data]](c.s, c.pids[n])
	return m.Data(), ok
}

// leader returns the leader of the highest term, if any.
func (c *cluster) leader() string {
	var leader string
	var term uint64
	for _, n := range nodes {
		m, ok := gensim.State[genstatem.Machine[Role, Data]](c.s, c.pids[n])
		if ok && m.State() == Leader && m.Data().Term >= term {
			leader, term = n, m.Data().Term
		}
	}
	return leader
}

// check checks the safety of Raft, at a point of the run.
func (c *cluster) check() error {
	for _, n := range nodes {
		m, ok := gensim.State[genstatem.Machine[Role, Data]](c.s, c.pids[n])
		if !ok {
			continue
		}
		d := m.Data()
		committed := d.Log[:d.Commit]
		k := min(len(committed), len(c.committed))
		if !slices.Equal(committed[:k], c.committed[:k]) {
			return fmt.Errorf("%s committed %v, others %v", n, committed, c.committed)
		}
		for i := len(c.committed); i < len(committed); i++ {
			c.committed = append(c.committed, committed[i])
			c.commitTerm = append(c.commitTerm, d.Term)
		}
		if m.State() != Leader {
			continue
		}
		if l, ok := c.leaders[d.Term]; ok && l != n {
			return fmt.Errorf("two leaders in term %d: %s and %s", d.Term, l, n)
		}
		c.leaders[d.Term] = n
		for i, e := range c.committed {
			if c.commitTerm[i] < d.Term && (i >= len(d.Log) || d.Log[i] != e) {
				return fmt.Errorf("leader %s of term %d lacks entry %d %v, committed in term %d", n, d.Term, i+1, e, c.commitTerm[i])
			}
		}
	}
	return nil
}
