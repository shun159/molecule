package kvs

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
)

// A disk is a model of what a power loss leaves of the directory of a
// table: of each file, what it held at its last sync; of the directory,
// the names it had at its last sync. A file created and not synced since
// is left empty; a name removed and not synced since is left. It sees the
// syncs of the package through syncFile and syncDir.
//
// It is right with SyncEach and SyncEvery, which write a file only under
// the lock a sync is under; not with SyncNone, whose writes to the mapping
// go on while a sync is read.
type disk struct {
	t   *testing.T
	dir string

	// onSync, if set, is called before each sync of a file, or of the
	// directory with f nil, and fails it with an error. It is set before
	// the table opens, not while it runs.
	onSync func(f *os.File) error

	mu    sync.Mutex
	files map[uint64][]byte // by inode, what the file held at its last sync
	names map[string]uint64 // the inode of each name, at the last sync of dir
	err   error             // the first failure to read what was synced
}

// newDisk models dir, all in it already on disk, while the test runs.
func newDisk(t *testing.T, dir string) *disk {
	t.Helper()
	dir, err := filepath.EvalSymlinks(dir) // as /proc names it
	if err != nil {
		t.Fatal(err)
	}
	d := &disk{t: t, dir: dir}
	d.scan()
	origFile, origDir := syncFile, syncDir
	syncFile = func(f *os.File, dataOnly bool) error {
		if err := d.before(f); err != nil {
			return err
		}
		if err := origFile(f, dataOnly); err != nil {
			return err
		}
		d.recordFile(f)
		return nil
	}
	syncDir = func(p string) error {
		if err := d.before(nil); err != nil {
			return err
		}
		if err := origDir(p); err != nil {
			return err
		}
		if p == dir {
			d.recordDir()
		}
		return nil
	}
	t.Cleanup(func() {
		syncFile, syncDir = origFile, origDir
		if d.err != nil {
			t.Errorf("disk model: %v", d.err)
		}
	})
	return d
}

func (d *disk) before(f *os.File) error {
	if d.onSync == nil {
		return nil
	}
	return d.onSync(f)
}

// fail notes err, under mu: a model missing a sync would lie quietly.
func (d *disk) fail(err error) {
	if d.err == nil {
		d.err = err
	}
}

func inoOf(fi os.FileInfo) uint64 { return fi.Sys().(*syscall.Stat_t).Ino }

// recordFile notes what f, just synced, holds, if it is in the directory.
// It reads f opened again by /proc: f may be write only, and its name may
// be another file's since a rename.
func (d *disk) recordFile(f *os.File) {
	d.mu.Lock()
	defer d.mu.Unlock()
	fd := fmt.Sprintf("/proc/self/fd/%d", f.Fd())
	path, err := os.Readlink(fd)
	if err != nil {
		d.fail(err)
		return
	}
	if filepath.Dir(path) != d.dir {
		return // a file of another table
	}
	fi, err := f.Stat()
	if err != nil {
		d.fail(err)
		return
	}
	b, err := os.ReadFile(fd)
	if err != nil {
		d.fail(err)
		return
	}
	d.files[inoOf(fi)] = b
}

// recordDir notes the names of the directory, just synced.
func (d *disk) recordDir() {
	d.mu.Lock()
	defer d.mu.Unlock()
	names, err := d.list()
	if err != nil {
		d.fail(err)
		return
	}
	for _, ino := range names {
		if _, ok := d.files[ino]; !ok {
			d.files[ino] = nil // created, never synced: on disk, empty
		}
	}
	d.names = names
}

// scan notes all in the directory as on disk: at first, and once the
// directory is what a power loss left.
func (d *disk) scan() {
	d.mu.Lock()
	defer d.mu.Unlock()
	names, err := d.list()
	if err != nil {
		d.fail(err)
		return
	}
	d.names, d.files = names, map[uint64][]byte{}
	for name, ino := range names {
		b, err := os.ReadFile(filepath.Join(d.dir, name))
		if err != nil {
			d.fail(err)
			return
		}
		d.files[ino] = b
	}
}

// list returns the inode of each name of the directory but LOCK, which
// holds nothing.
func (d *disk) list() (map[string]uint64, error) {
	ents, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, err
	}
	names := make(map[string]uint64, len(ents))
	for _, e := range ents {
		if e.Name() == "LOCK" {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			return nil, err
		}
		names[e.Name()] = inoOf(fi)
	}
	return names, nil
}

// powerLoss writes what a power loss now leaves into dst, empty: for a
// test to open it, many from one run. Into the directory itself, with its
// table closed, it rewinds it, and models it anew.
func (d *disk) powerLoss(dst string) error {
	d.mu.Lock()
	names := maps.Clone(d.names)
	files := make(map[string][]byte, len(names))
	for name, ino := range names {
		files[name] = d.files[ino]
	}
	d.mu.Unlock()
	inPlace := dst == d.dir
	if inPlace {
		ents, err := os.ReadDir(dst)
		if err != nil {
			return err
		}
		for _, e := range ents {
			if e.Name() != "LOCK" {
				os.Remove(filepath.Join(dst, e.Name()))
			}
		}
	}
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(dst, name), b, 0o644); err != nil {
			return err
		}
	}
	if inPlace {
		d.scan()
	}
	return nil
}

// logDir logs the files of dir and their sizes, to tell of a failure.
func logDir(t *testing.T, what, dir string) {
	t.Helper()
	ents, _ := os.ReadDir(dir)
	var list []string
	for _, e := range ents {
		if fi, err := e.Info(); err == nil {
			list = append(list, fmt.Sprintf("%s(%dB)", e.Name(), fi.Size()))
		}
	}
	slices.Sort(list)
	t.Logf("%s (%s): %v", what, dir, list)
}

// A write of a history of writes, to a table of string keys and int
// values.
type write struct {
	key string
	val int
	del bool
}

// stateAfter returns the entries the first n writes of h leave.
func stateAfter(h []write, n int) map[string]int {
	m := map[string]int{}
	for _, w := range h[:n] {
		apply(m, w)
	}
	return m
}

func apply(m map[string]int, w write) {
	if w.del {
		delete(m, w.key)
	} else {
		m[w.key] = w.val
	}
}

// A loss is the power lost at a moment of a run: the disk it left, the
// writes acked then, and the writes made, acked or not.
type loss struct {
	dir           string
	acked, issued int
}

// TestPowerLoss loses the power before each sync of a run of writes, in
// segments and compactions, and opens each disk it leaves: the writes
// acked are there, and of the others some first ones, none after; and the
// table goes on.
func TestPowerLoss(t *testing.T) {
	dir := t.TempDir()
	o := Options[string, int]{SegmentSize: 4 << 10, CompactMin: 2 << 10}
	d := newDisk(t, dir)
	var (
		mu      sync.Mutex
		history []write
		acked   int
		losses  []loss
		lossErr error
	)
	d.onSync = func(*os.File) error {
		mu.Lock()
		defer mu.Unlock()
		l := loss{dir: t.TempDir(), acked: acked, issued: len(history)}
		if err := d.powerLoss(l.dir); err != nil && lossErr == nil {
			lossErr = err
		}
		losses = append(losses, l)
		return nil
	}
	tab, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 600 {
		w := write{key: strconv.Itoa(r.IntN(50)), val: i, del: r.IntN(4) == 0}
		mu.Lock()
		history = append(history, w)
		mu.Unlock()
		if w.del {
			err = tab.Delete(w.key)
		} else {
			err = tab.Put(w.key, w.val)
		}
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		acked++
		mu.Unlock()
	}
	if err := tab.Close(); err != nil {
		t.Fatal(err)
	}
	d.onSync = nil
	if lossErr != nil {
		t.Fatal(lossErr)
	}
	compactions := tab.compactions.Load()
	for _, l := range losses {
		checkLoss(t, l, history, o)
	}
	t.Logf("%d power losses, over %d compactions", len(losses), compactions)
}

// checkLoss opens the disk of l: it must hold the entries of the first S
// writes of h, for an S from the writes acked to those made; and take a
// write after.
func checkLoss(t *testing.T, l loss, h []write, o Options[string, int]) {
	t.Helper()
	tab, err := Open(l.dir, o)
	if err != nil {
		t.Errorf("lost at %d writes acked: open: %v", l.acked, err)
		logDir(t, "the disk", l.dir)
		return
	}
	defer tab.Close()
	got := maps.Collect(tab.All())
	want := stateAfter(h, l.acked)
	for s := l.acked; !maps.Equal(got, want); s++ {
		if s == l.issued {
			t.Errorf("lost at %d writes acked, %d made: the entries of none of them", l.acked, l.issued)
			logDir(t, "the disk", l.dir)
			return
		}
		apply(want, h[s])
	}
	if err := tab.Put("after", -1); err != nil {
		t.Errorf("lost at %d writes acked: a write after: %v", l.acked, err)
	}
}
