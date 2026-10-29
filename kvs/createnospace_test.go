package kvs

import (
	"errors"
	"math/rand/v2"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
)

// TestCreateNoSpace fills the disk at each file a run of writes creates,
// a segment or a snapshot, until fullFor creates failed with ENOSPC; then
// frees it. A write failing, its segment not made, fails with ENOSPC,
// alone: the writes after go on once the disk is freed, as of a write
// taking space. What is left, and what a power loss leaves of it, keeping
// any of the changes to the directory not synced, hold the writes acked.
func TestCreateNoSpace(t *testing.T) {
	o := Options[string, int]{SegmentSize: 1 << 10, CompactMin: 1 << 10, CompactRatio: 1.2}
	for n := 1; ; n++ {
		filled := true
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			filled = createNoSpace(t, o, n)
		})
		if !filled {
			return // the run ended before its nth create
		}
	}
}

// createNoSpace runs TestCreateNoSpace with the disk full at the nth
// create, and reports whether it was.
func createNoSpace(t *testing.T, o Options[string, int], n int) bool {
	dir := t.TempDir()
	d := newDisk(t, dir)
	full := fullAtCreate(t, n)
	tab, err := Open(dir, o)
	if err != nil {
		if !errors.Is(err, syscall.ENOSPC) {
			t.Fatal(err)
		}
		return true // failed opening: nothing written
	}
	r := rand.New(rand.NewPCG(1, 2))
	var acked []write
	for i := range 150 {
		w := write{key: strconv.Itoa(r.IntN(100)), val: i, del: r.IntN(4) == 0}
		err := writeTo(tab, w)
		switch {
		case err == nil:
			acked = append(acked, w)
		case !errors.Is(err, syscall.ENOSPC):
			t.Fatalf("write %d: %v; want ENOSPC, of the disk full only", i, err)
		}
	}
	tab.Close() // telling of a compaction failed, if any
	full.ended.Store(true)
	if !full.filled.Load() {
		return false
	}
	if len(acked) < 150-fullFor {
		t.Errorf("%d writes acked of 150: the writes after the disk was freed failed", len(acked))
	}
	ops, err := d.unsynced()
	if err != nil {
		t.Fatal(err)
	}
	for _, keep := range keepings(len(ops)) {
		l := loss{dir: t.TempDir(), acked: len(acked), issued: len(acked)}
		if err := d.powerLossKeeping(l.dir, ops, keep); err != nil {
			t.Fatal(err)
		}
		checkLoss(t, l, acked, o)
	}
	checkLoss(t, loss{dir: dir, acked: len(acked), issued: len(acked)}, acked, o)
	return true
}

// A createFull is a disk filled at the nth create: that create and those
// after fail with ENOSPC, fullFor of them, the disk then freed. Once the
// run ended, it is filled no more.
type createFull struct {
	n       int64
	creates atomic.Int64
	failed  atomic.Int64
	filled  atomic.Bool
	ended   atomic.Bool
}

// fullAtCreate puts a createFull under the creates, while the test runs.
func fullAtCreate(t *testing.T, n int) *createFull {
	c := &createFull{n: int64(n)}
	orig := createFile
	createFile = func(path string) (*os.File, error) {
		if c.ended.Load() || c.creates.Add(1) < c.n || c.failed.Load() >= fullFor {
			return orig(path)
		}
		c.filled.Store(true)
		c.failed.Add(1)
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOSPC}
	}
	t.Cleanup(func() { createFile = orig })
	return c
}
