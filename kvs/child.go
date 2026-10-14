package kvs

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// Child returns the owner of the table kept in dir, to start under a
// supervisor ahead of the processes that use it, which find it with
// Opened. It opens the table as it starts, failing to start if it cannot,
// and closes it as it stops. See Supervision in the package doc.
func Child[K comparable, V any](dir string, opts Options[K, V]) gen.Child {
	return genserver.Child(owner[K, V]{dir: clean(dir), opts: opts})
}

// Opened returns the table a Child keeps open in dir.
func Opened[K comparable, V any](dir string) (*Table[K, V], error) {
	v, ok := tables.Load(clean(dir))
	if !ok {
		return nil, fmt.Errorf("kvs: no table open in %s: its Child starts ahead of its users", dir)
	}
	t, ok := v.(*Table[K, V])
	if !ok {
		return nil, fmt.Errorf("kvs: the table open in %s is a %T", dir, v)
	}
	return t, nil
}

// tables are the tables open by owners, by directory.
var tables sync.Map

// owner holds the table open in dir while it runs, in tables: not in its
// state, which gensim fingerprints. It traps exits, for its supervisor
// stopping it to close the table.
type owner[K comparable, V any] struct {
	genserver.Default[struct{}]
	dir  string
	opts Options[K, V]
}

func (o owner[K, V]) Init(proc.PID) (struct{}, []actor.Effect, error) {
	t, err := Open(o.dir, o.opts) // fails if open already, by flock
	if err != nil {
		return struct{}{}, nil, err
	}
	tables.Store(o.dir, t)
	return struct{}{}, actor.Do(actor.TrapExit{On: true}), nil
}

func (o owner[K, V]) Terminate(struct{}, error) []actor.Effect {
	if t, ok := tables.LoadAndDelete(o.dir); ok {
		t.(*Table[K, V]).Close()
	}
	return nil
}

// clean returns dir as tables knows it, absolute if it can be.
func clean(dir string) string {
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return filepath.Clean(dir)
}
