package kvs

import (
	"errors"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
)

// TestShortWrite has each write to a file of a run of writes, in segments
// and compactions, write a part of its bytes, and fail with EIO: one byte,
// half, or all but one. A write to the log failing so fails, and so does
// each write after: the log stays failed. What is left then, closed, and
// what a power loss leaves of it, keeping any of the changes to the
// directory not synced, hold the writes acked, and of those failed some
// first ones; the record cut short is taken for torn, and cut.
func TestShortWrite(t *testing.T) {
	o := Options[string, int]{SegmentSize: 1 << 10, CompactMin: 1 << 10, CompactRatio: 1.2}
	for _, part := range []struct {
		name string
		of   func(n int) int
	}{
		{"byte", func(int) int { return 1 }},
		{"half", func(n int) int { return n / 2 }},
		{"all but one", func(n int) int { return n - 1 }},
	} {
		t.Run(part.name, func(t *testing.T) {
			for n := 1; ; n++ {
				cut := true
				t.Run(strconv.Itoa(n), func(t *testing.T) {
					cut = shortWrite(t, o, n, part.of)
				})
				if !cut {
					return // the run ended before its nth write
				}
			}
		})
	}
}

// shortWrite runs TestShortWrite with the nth write cut to part of its
// bytes, and reports whether it was.
func shortWrite(t *testing.T, o Options[string, int], n int, part func(int) int) bool {
	dir := t.TempDir()
	d := newDisk(t, dir)
	s := cutAt(t, n, part)
	tab, err := Open(dir, o)
	if err != nil {
		if !errors.Is(err, syscall.EIO) {
			t.Fatal(err)
		}
		return true // failed opening: nothing written
	}
	h, writeErr := writeRun(tab)
	acked := len(h)
	if writeErr != nil {
		acked--
		if !errors.Is(writeErr, syscall.EIO) {
			t.Errorf("a write failed with %v, not the EIO of a write cut short", writeErr)
		}
		if err := writeTo(tab, write{key: "after", val: -1}); err == nil {
			t.Error("a write after the log failed succeeded")
		}
	}
	tab.Close() // telling of the failure, if any: of the log, or of a compaction
	s.ended.Store(true)
	if !s.cut.Load() {
		return false
	}
	ops, err := d.unsynced()
	if err != nil {
		t.Fatal(err)
	}
	for _, keep := range keepings(len(ops)) {
		l := loss{dir: t.TempDir(), acked: acked, issued: len(h)}
		if err := d.powerLossKeeping(l.dir, ops, keep); err != nil {
			t.Fatal(err)
		}
		checkLoss(t, l, h, o)
	}
	checkLoss(t, loss{dir: dir, acked: acked, issued: len(h)}, h, o)
	return true
}

// A shortWriter cuts the nth write to a file to part of its bytes, and
// fails it with EIO; the others write all. Once the run ended, it cuts no
// more.
type shortWriter struct {
	n      int64
	writes atomic.Int64
	cut    atomic.Bool
	ended  atomic.Bool
}

// cutAt puts a shortWriter cutting the nth write under the writes, while
// the test runs.
func cutAt(t *testing.T, n int, part func(int) int) *shortWriter {
	s := &shortWriter{n: int64(n)}
	orig := writeAt
	writeAt = func(f *os.File, b []byte, off int64) (int, error) {
		if s.ended.Load() || len(b) < 2 || s.writes.Add(1) != s.n {
			return orig(f, b, off)
		}
		s.cut.Store(true)
		m, err := orig(f, b[:part(len(b))], off)
		if err != nil {
			return m, err
		}
		return m, syscall.EIO
	}
	t.Cleanup(func() { writeAt = orig })
	return s
}
