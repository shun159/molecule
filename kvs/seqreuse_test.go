package kvs

import (
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// A revision is of one write. A write is read before it is durable: lost
// at a power loss, its revision read, no write after is to have it, for a
// conditional write on it to fail, rather than write over an entry it did
// not read. The write lost is one of a run, or the first of a run all
// lost.
func TestRevNotReused(t *testing.T) {
	for _, c := range []struct {
		name       string
		newSession bool
	}{
		{"a write lost", false},
		{"a session lost", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			lost, rev := readThenLost(t, c.newSession)
			checkRevAfter(t, lost, rev)
		})
	}
}

// A crash at any operation of opening a log of a run all lost, the
// changes to the names of its directory kept or not, leaves it to go on
// past the seqs of that run: the stale segment it removes, empty, the last
// to tell them, is removed once the new one is in place.
func TestRevNotReusedOpening(t *testing.T) {
	lost, rev := readThenLost(t, true)
	for n := 1; ; n++ {
		killed := true
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			dir := copyDir(t, lost)
			d := newDisk(t, dir)
			k := killAt(t, n)
			tab, err := Open(dir, Options[string, string]{})
			if killed = k.revive(); !killed {
				if err != nil {
					t.Fatal(err)
				}
				tab.Close()
				return
			}
			ops, err := d.unsynced()
			if err != nil {
				t.Fatal(err)
			}
			for _, keep := range keepings(len(ops)) {
				dst := t.TempDir()
				if err := d.powerLossKeeping(dst, ops, keep); err != nil {
					t.Fatal(err)
				}
				checkRevAfter(t, dst, rev)
			}
			checkRevAfter(t, dir, rev)
		})
		if !killed {
			return // opened before its nth operation
		}
	}
}

// readThenLost puts a under k, then b, read before it is durable, then
// lost at a power loss: in the same run, or the first of a run after. It
// returns the disk the loss leaves, and the revision of b read.
func readThenLost(t *testing.T, newSession bool) (string, uint64) {
	t.Helper()
	dir := t.TempDir()
	d := newDisk(t, dir)
	lost := t.TempDir()
	var (
		armed atomic.Bool
		tab   *Table[string, string]
		read  string
		rev   uint64
		err   error
	)
	d.onSync = func(f *os.File) error {
		if f == nil || !armed.CompareAndSwap(true, false) {
			return nil
		}
		// The put of b is in the table, not yet durable: read, then lost.
		read, rev, _ = tab.GetRev("k")
		err = d.powerLoss(lost)
		return nil
	}
	if tab, err = Open(dir, Options[string, string]{}); err != nil {
		t.Fatal(err)
	}
	if err := tab.Put("k", "a"); err != nil {
		t.Fatal(err)
	}
	if newSession {
		tab.Close()
		if tab, err = Open(dir, Options[string, string]{}); err != nil {
			t.Fatal(err)
		}
	}
	armed.Store(true)
	if err := tab.Put("k", "b"); err != nil {
		t.Fatal(err)
	}
	tab.Close()
	if err != nil {
		t.Fatal(err)
	}
	if read != "b" {
		t.Fatalf("read %q before the sync, want b", read)
	}
	return lost, rev
}

// checkRevAfter opens dir, b of rev lost: a write after has a revision
// past rev, and a conditional write on rev fails.
func checkRevAfter(t *testing.T, dir string, rev uint64) {
	t.Helper()
	tab, err := Open(dir, Options[string, string]{})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	if v, _, _ := tab.GetRev("k"); v != "a" {
		t.Fatalf("after the loss, k is %q, want a", v)
	}
	if err := tab.Put("k", "c"); err != nil {
		t.Fatal(err)
	}
	if _, now, _ := tab.GetRev("k"); now <= rev {
		t.Errorf("c, written after b of rev %d lost, has rev %d", rev, now)
	}
	if ok, err := tab.PutIf("k", "d", rev); ok || err != nil {
		t.Errorf("PutIf on rev %d of b, lost: wrote %t, %v", rev, ok, err)
	}
}

// No more than maxLag writes are not durable: a write past them syncs
// them, the log syncing by itself in an hour only.
func TestLagBounded(t *testing.T) {
	tab, err := Open(t.TempDir(), Options[string, int]{Sync: SyncEvery(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	for i := range maxLag + 10 {
		if _, err := tab.put("k", i, nil); err != nil {
			t.Fatal(err)
		}
		if lag := tab.log.lastSeq() - tab.log.durable.Load(); lag > maxLag {
			t.Fatalf("write %d: %d writes not durable, more than %d", i, lag, maxLag)
		}
	}
	if tab.log.syncs.Load() == 0 {
		t.Error("no write synced those before it")
	}
}
