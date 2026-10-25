package kvs

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
)

// TestCrashAt kills the process at each operation changing the directory
// of a run: opening a directory a crash left in a compaction, writes,
// compactions, and closing. What it leaves, as the page cache has it, and
// what a power loss then leaves of it, each hold the first S writes, S
// from those acked to those made, and take a write after.
func TestCrashAt(t *testing.T) {
	o := Options[string, int]{SegmentSize: 1 << 10, CompactRatio: -1}
	for n := 1; ; n++ {
		killed := true
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			killed = crashAt(t, o, n)
		})
		if !killed {
			return // the run ended before its nth operation
		}
	}
}

// crashAt runs TestCrashAt with the process killed at the nth operation,
// and reports whether it was.
func crashAt(t *testing.T, o Options[string, int], n int) bool {
	dir, h := crashedInCompaction(t, o)
	d := newDisk(t, dir)
	k := killAt(t, n)
	h, acked := runToCrash(t, dir, o, h)
	if !k.revive() {
		return false
	}
	l := loss{dir: t.TempDir(), acked: acked, issued: len(h)}
	if err := d.powerLoss(l.dir); err != nil {
		t.Fatal(err)
	}
	checkLoss(t, loss{dir: dir, acked: acked, issued: len(h)}, h, o)
	checkLoss(t, l, h, o)
	return true
}

// runToCrash opens dir, its writes h, and makes writes and compactions,
// then closes it, unless the process is killed first. It returns the
// writes made, and how many of them were acked.
func runToCrash(t *testing.T, dir string, o Options[string, int], h []write) ([]write, int) {
	acked := len(h)
	tab, err := Open(dir, o)
	if err != nil {
		if !errors.Is(err, errCrash) {
			t.Fatalf("open: %v", err)
		}
		return h, acked
	}
	for i := range 100 {
		if i%30 == 29 {
			tab.Compact() // failing, if killed, as the writes after
		}
		w := write{key: strconv.Itoa(i % 20), val: 1000 + i, del: i%5 == 4}
		h = append(h, w)
		if err := writeTo(tab, w); err != nil {
			crashClose(tab)
			return h, acked
		}
		acked++
	}
	tab.Close()
	return h, acked
}

// A kill kills the process at the nth operation changing the directory,
// a sync included: that one and all after fail with errCrash, undone.
type kill struct {
	mu   sync.Mutex
	n    int
	ops  int  // so far
	dead bool // killed
	done bool // revived: no operation fails
}

// killAt puts a kill at the nth operation under the operations of the
// package, while the test runs; over the disk model, if any, so that what
// it does not do, the model does not see.
func killAt(t *testing.T, n int) *kill {
	k := &kill{n: n}
	oCreate, oWrite, oTrunc, oRename, oRemove := createFile, writeAt, truncateFile, rename, remove
	oSyncFile, oSyncDir := syncFile, syncDir
	createFile = func(path string) (*os.File, error) {
		if k.dies() {
			return nil, errCrash
		}
		return oCreate(path)
	}
	writeAt = func(f *os.File, b []byte, off int64) (int, error) {
		if k.dies() {
			return 0, errCrash
		}
		return oWrite(f, b, off)
	}
	truncateFile = func(f *os.File, size int64) error { return k.do(func() error { return oTrunc(f, size) }) }
	rename = func(from, to string) error { return k.do(func() error { return oRename(from, to) }) }
	remove = func(path string) error { return k.do(func() error { return oRemove(path) }) }
	syncFile = func(f *os.File, dataOnly bool) error { return k.do(func() error { return oSyncFile(f, dataOnly) }) }
	syncDir = func(dir string) error { return k.do(func() error { return oSyncDir(dir) }) }
	t.Cleanup(func() {
		createFile, writeAt, truncateFile, rename, remove = oCreate, oWrite, oTrunc, oRename, oRemove
		syncFile, syncDir = oSyncFile, oSyncDir
	})
	return k
}

// dies counts an operation, and reports whether the process is dead at
// it.
func (k *kill) dies() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.done {
		return false
	}
	if k.ops++; k.ops == k.n {
		k.dead = true
	}
	return k.dead
}

// do does op, unless the process is dead at it.
func (k *kill) do(op func() error) error {
	if k.dies() {
		return errCrash
	}
	return op()
}

// revive lets the operations after succeed, for the test to reopen what
// the run left, and reports whether the process was killed.
func (k *kill) revive() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.done = true
	return k.dead
}
