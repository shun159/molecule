//go:build realdisk

package kvs

import (
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

func TestRealDiskEIOExt4(t *testing.T)  { testRealEIO(t, "ext4") }
func TestRealDiskEIOXFS(t *testing.T)   { testRealEIO(t, "xfs") }
func TestRealDiskEIOBtrfs(t *testing.T) { testRealEIO(t, "btrfs") }

// Of the run of testRealEIO: puts before the writes of the device fail,
// those failing, and those after the table is opened again.
var (
	eioBefore = keys("a", 20)
	eioFailed = keys("f", 2)
	eioAfter  = keys("b", 20)
)

// testRealEIO fails the writes of the device under fs: a put fails with
// EIO, and so does each put after, the log failed. The device healed, the
// table is closed and opened again, as a supervisor would, the page cache
// kept: what it then holds is durable, though the writes failed may have
// left pages in the cache, clean, not on the device (fsyncgate). A power
// loss at any moment leaves the puts acked, and, once opened again, what
// it held then.
func testRealEIO(t *testing.T, fs string) {
	r := newRig(t, fs, true)
	dir := filepath.Join(r.mnt, "kvs")
	tab, err := Open(dir, Options[string, string]{})
	if err != nil {
		t.Fatal(err)
	}
	r.puts(tab, eioBefore)

	r.mark("eio")
	r.failWrites(true)
	for _, k := range eioFailed {
		r.mark("issue")
		if err := tab.Put(k, "v"); !errors.Is(err, syscall.EIO) {
			t.Errorf("put %s, the writes failing: %v, want EIO", k, err)
		}
	}
	r.failWrites(false)
	if err := tab.Close(); err == nil {
		t.Error("Close told nothing of the log failed")
	}

	if tab, err = Open(dir, Options[string, string]{}); err != nil {
		t.Fatal(err)
	}
	opened := maps.Collect(tab.All())
	r.mark("opened")
	t.Logf("opened again: %d entries, of those failed %v", len(opened), present(opened, eioFailed))
	r.puts(tab, eioAfter)
	if err := tab.Close(); err != nil {
		t.Fatal(err)
	}
	r.stop()

	l, err := readLogWrites(r.logImage)
	if err != nil {
		t.Fatal(err)
	}
	checkReplays(t, l, fs, func(dir string, marks map[string]int) {
		checkEIOLoss(t, dir, marks, opened)
	})
}

// puts puts each of ks, between a mark issue and a mark ack.
func (r *rig) puts(tab *Table[string, string], ks []string) {
	for _, k := range ks {
		r.mark("issue")
		if err := tab.Put(k, "v"); err != nil {
			r.t.Fatal(err)
		}
		r.mark("ack")
	}
}

// checkEIOLoss checks the table in dir, a power loss at marks: of the
// puts before the failure, the acked and some after; then those, and of
// the failed some first; once opened again, what it held then, and of
// the puts after, the acked and some after. A put after works.
func checkEIOLoss(t *testing.T, dir string, marks map[string]int, opened map[string]string) {
	t.Helper()
	tab, err := Open(dir, Options[string, string]{})
	if err != nil {
		t.Errorf("at %v: open: %v", marks, err)
		return
	}
	defer tab.Close()
	got := maps.Collect(tab.All())
	acked, issued := marks["ack"], marks["issue"]
	var ok bool
	switch {
	case marks["opened"] > 0:
		n := len(eioBefore) + len(eioFailed)
		ok = putsOf(got, opened, eioAfter, acked-len(eioBefore), issued-n)
	case marks["eio"] > 0:
		ok = putsOf(got, entriesOf(eioBefore), eioFailed, 0, issued-len(eioBefore))
	default:
		ok = putsOf(got, nil, eioBefore, acked, issued)
	}
	if !ok {
		t.Errorf("at %v: %d entries %v; opened again, it held %v", marks, len(got), slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(opened)))
	}
	if err := tab.Put("after", "v"); err != nil {
		t.Errorf("at %v: a put after: %v", marks, err)
	}
}

// putsOf reports whether got is base with the first s of ks put, for an
// s from lo to hi.
func putsOf(got, base map[string]string, ks []string, lo, hi int) bool {
	want := maps.Clone(base)
	if want == nil {
		want = map[string]string{}
	}
	for s := 0; s <= min(hi, len(ks)); s++ {
		if s >= lo && maps.Equal(got, want) {
			return true
		}
		if s < len(ks) {
			want[ks[s]] = "v"
		}
	}
	return false
}

func keys(prefix string, n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = fmt.Sprintf("%s%02d", prefix, i)
	}
	return ks
}

func entriesOf(ks []string) map[string]string {
	m := map[string]string{}
	for _, k := range ks {
		m[k] = "v"
	}
	return m
}

// present returns those of ks in m.
func present(m map[string]string, ks []string) []string {
	var in []string
	for _, k := range ks {
		if _, ok := m[k]; ok {
			in = append(in, k)
		}
	}
	return in
}
