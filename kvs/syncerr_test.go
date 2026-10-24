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

// TestSyncError fails each sync of a run of writes, in segments and
// compactions, once, with EIO; the syncs after succeed, as on Linux,
// which may drop the pages it failed to write and report them clean
// (fsyncgate). A write whose sync failed fails, and so does each write
// after: the log stays failed, though a sync would now succeed. Close
// tells of it. What a power loss then leaves holds the writes acked.
func TestSyncError(t *testing.T) {
	o := Options[string, int]{SegmentSize: 1 << 10, CompactMin: 1 << 10}
	for n := 1; ; n++ {
		failed := true
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			failed = syncError(t, o, n)
		})
		if !failed {
			return // the run ended before its nth sync
		}
	}
}

// syncError runs TestSyncError with the nth sync failed, and reports
// whether it was.
func syncError(t *testing.T, o Options[string, int], n int) bool {
	dir := t.TempDir()
	d := newDisk(t, dir)
	var syncs atomic.Int64 // of the writer and of compactions
	d.onSync = func(*os.File) error {
		if syncs.Add(1) == int64(n) {
			return syscall.EIO
		}
		return nil
	}
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
			t.Errorf("a write failed with %v, not the EIO of its sync", writeErr)
		}
		if err := writeTo(tab, write{key: "after", val: -1}); err == nil {
			t.Error("a write after the log failed succeeded")
		}
	}
	closeErr := tab.Close()
	if writeErr != nil && closeErr == nil {
		t.Error("Close told nothing of the log failed")
	}
	if syncs.Load() < int64(n) {
		return false
	}
	d.onSync = nil
	l := loss{dir: t.TempDir(), acked: acked, issued: len(h)}
	if err := d.powerLoss(l.dir); err != nil {
		t.Fatal(err)
	}
	checkLoss(t, l, h, o)
	return true
}

// writeRun makes a run of writes to tab, up to the first that fails, and
// returns them, that one last, and its error.
func writeRun(tab *Table[string, int]) ([]write, error) {
	r := rand.New(rand.NewPCG(1, 2))
	var h []write
	for i := range 150 {
		w := write{key: strconv.Itoa(r.IntN(100)), val: i, del: r.IntN(4) == 0}
		h = append(h, w)
		if err := writeTo(tab, w); err != nil {
			return h, err
		}
	}
	return h, nil
}

func writeTo(tab *Table[string, int], w write) error {
	if w.del {
		return tab.Delete(w.key)
	}
	return tab.Put(w.key, w.val)
}
