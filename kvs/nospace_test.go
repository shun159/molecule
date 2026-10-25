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

// fullFor is the writes of TestNoSpace failing, the disk full, before it
// is freed.
const fullFor = 3

// TestNoSpace fills the disk at each write taking space of a run, in
// segments and compactions, until fullFor writes failed; then frees it. A
// write failing fails with ENOSPC, alone: the writes after go on, those
// while full that fit in the space taken, and all once freed. Close tells
// of nothing; what a power loss then leaves holds the writes acked, and
// none of those failed.
func TestNoSpace(t *testing.T) {
	o := Options[string, int]{SegmentSize: 1 << 10, CompactMin: 1 << 10, CompactRatio: 1.2}
	for n := 1; ; n++ {
		filled := true
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			filled = noSpace(t, o, n)
		})
		if !filled {
			return // the run ended before its nth write taking space
		}
	}
}

// noSpace runs TestNoSpace with the disk full at the nth write taking
// space, and reports whether it was.
func noSpace(t *testing.T, o Options[string, int], n int) bool {
	dir := t.TempDir()
	d := newDisk(t, dir)
	disk := fillAt(t, n)
	tab, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(1, 2))
	var acked []write
	failed := 0
	for i := range 150 {
		w := write{key: strconv.Itoa(r.IntN(100)), val: i, del: r.IntN(4) == 0}
		wasFull := disk.full.Load()
		err := writeTo(tab, w)
		ofFull := wasFull || disk.full.Load() // filled by this write, or before
		switch {
		case err == nil:
			acked = append(acked, w)
		case !errors.Is(err, syscall.ENOSPC) || !ofFull:
			t.Fatalf("write %d: %v; want ENOSPC, of the disk full only", i, err)
		default:
			if failed++; failed == fullFor {
				disk.full.Store(false)
			}
		}
	}
	if err := tab.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	disk.ended.Store(true)
	if !disk.filled.Load() {
		return false
	}
	l := loss{dir: t.TempDir(), acked: len(acked), issued: len(acked)}
	if err := d.powerLoss(l.dir); err != nil {
		t.Fatal(err)
	}
	checkLoss(t, l, acked, o)
	return true
}

// A fullDisk is a disk filled at the nth write taking space, a write
// past the end of its file: that write and those after taking space write
// what fits in the file, and fail with ENOSPC, until full is unset. Once
// the run ended, it is filled no more.
type fullDisk struct {
	n      int64
	taking atomic.Int64 // the writes taking space so far
	filled atomic.Bool  // at the nth
	full   atomic.Bool
	ended  atomic.Bool
}

// fillAt puts a fullDisk filled at the nth write taking space under the
// writes, while the test runs.
func fillAt(t *testing.T, n int) *fullDisk {
	disk := &fullDisk{n: int64(n)}
	orig := writeAt
	writeAt = func(f *os.File, b []byte, off int64) (int, error) {
		return disk.write(orig, f, b, off)
	}
	t.Cleanup(func() { writeAt = orig })
	return disk
}

func (d *fullDisk) write(orig func(*os.File, []byte, int64) (int, error), f *os.File, b []byte, off int64) (int, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	fits := max(0, min(fi.Size()-off, int64(len(b))))
	if fits == int64(len(b)) || d.ended.Load() {
		return orig(f, b, off)
	}
	if d.taking.Add(1) == d.n {
		d.filled.Store(true)
		d.full.Store(true)
	}
	if !d.full.Load() {
		return orig(f, b, off)
	}
	m, err := orig(f, b[:fits], off)
	if err != nil {
		return m, err
	}
	return m, syscall.ENOSPC
}
