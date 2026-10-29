package kvs

import (
	"errors"
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
// the names it had at its last sync, and of the changes to them since,
// files created, removed or renamed, each kept or lost on its own, as a
// test picks: see powerLossKeeping. A file created and not synced since is
// left empty. It sees the syncs of the package through syncFile and
// syncDir.
//
// A sync onSync fails with EIO drops what the file held that was not on
// disk: as Linux may mark the pages it failed to write clean, a sync
// after succeeds without them (fsyncgate). They stay in the page cache.
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

	mu      sync.Mutex
	files   map[uint64][]byte // by inode, what the file held at its last sync
	names   map[string]uint64 // the inode of each name, at the last sync of dir
	dropped map[uint64][]byte // by inode, what the file held as a sync failed
	pins    map[uint64]*os.File
	err     error // the first failure to read what was synced
}

// newDisk models dir, all in it already on disk, while the test runs.
func newDisk(t *testing.T, dir string) *disk {
	t.Helper()
	dir, err := filepath.EvalSymlinks(dir) // as /proc names it
	if err != nil {
		t.Fatal(err)
	}
	d := &disk{t: t, dir: dir, dropped: map[uint64][]byte{}, pins: map[uint64]*os.File{}}
	d.scan()
	origFile, origDir := syncFile, syncDir
	syncFile = func(f *os.File, dataOnly bool) error {
		if err := d.before(f); err != nil {
			if errors.Is(err, syscall.EIO) {
				d.drop(f) // a process killed before the sync drops nothing
			}
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
		for _, f := range d.pins {
			f.Close()
		}
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
	ino, b, ok := d.read(f)
	if !ok {
		return
	}
	if at, ok := d.dropped[ino]; ok {
		b = d.withoutDropped(ino, at, b)
	}
	d.files[ino] = b
}

// drop notes what f holds as a sync of it fails, to be dropped.
func (d *disk) drop(f *os.File) {
	if f == nil {
		return // a directory: what it lost is its names since its last sync
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if ino, b, ok := d.read(f); ok {
		d.dropped[ino] = b
	}
}

// withoutDropped returns b, just synced, but for the bytes the file still
// holds as at a sync of it failed, at: those are on disk as they were
// before it.
func (d *disk) withoutDropped(ino uint64, at, b []byte) []byte {
	b = slices.Clone(b)
	disk := d.files[ino]
	for i := range min(len(b), len(at)) {
		if b[i] != at[i] {
			continue // written again since: synced now
		}
		b[i] = 0
		if i < len(disk) {
			b[i] = disk[i]
		}
	}
	return b
}

// read returns the inode of f and what it holds, if it is in the
// directory, under mu.
func (d *disk) read(f *os.File) (uint64, []byte, bool) {
	fd := fmt.Sprintf("/proc/self/fd/%d", f.Fd())
	path, err := os.Readlink(fd)
	if err != nil {
		d.fail(err)
		return 0, nil, false
	}
	if filepath.Dir(path) != d.dir {
		return 0, nil, false // a file of another table
	}
	fi, err := f.Stat()
	if err != nil {
		d.fail(err)
		return 0, nil, false
	}
	b, err := os.ReadFile(fd)
	if err != nil {
		d.fail(err)
		return 0, nil, false
	}
	d.pin(inoOf(fi), fd)
	return inoOf(fi), b, true
}

// pin holds the file of ino open, by path, under mu: removed, its inode
// is not freed, for a file made after to take, as ext4 has it, and what
// the model notes of the inode stays of that file.
func (d *disk) pin(ino uint64, path string) {
	if _, ok := d.pins[ino]; ok {
		return
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return // removed meanwhile: noted of nothing after
	}
	if err != nil {
		d.fail(err)
		return
	}
	if fi, err := f.Stat(); err != nil || inoOf(fi) != ino {
		f.Close() // removed meanwhile, the name of another
		return
	}
	d.pins[ino] = f
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
	for name, ino := range names {
		if _, ok := d.files[ino]; !ok {
			d.files[ino] = nil // created, never synced: on disk, empty
			d.pin(ino, filepath.Join(d.dir, name))
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
		d.pin(ino, filepath.Join(d.dir, name))
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

// powerLoss writes what a power loss now leaves into dst, empty, the
// changes to the names of the directory not synced all lost: for a test
// to open it, many from one run. Into the directory itself, with its
// table closed, it rewinds it, and models it anew.
func (d *disk) powerLoss(dst string) error {
	return d.powerLossKeeping(dst, nil, func(int) bool { return false })
}

// unsynced returns the changes to the names of the directory since its
// last sync, as they are now.
func (d *disk) unsynced() ([]dirOp, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now, err := d.list()
	if err != nil {
		return nil, err
	}
	return dirOps(d.names, now), nil
}

// powerLossKeeping is powerLoss, but for the changes of ops, those not
// synced, that keep reports were kept: a power loss may keep any of
// them, a later one without one before.
func (d *disk) powerLossKeeping(dst string, ops []dirOp, keep func(i int) bool) error {
	d.mu.Lock()
	names := maps.Clone(d.names)
	for i, op := range ops {
		if keep(i) {
			op.applyTo(names)
		}
	}
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

// crashClose ends tab as its process dying would: nothing synced or cut,
// its files left as the page cache has them, its lock let go.
func crashClose[K comparable, V any](tab *Table[K, V]) {
	tab.closed.Store(true)

	tab.compactMu.Lock()
	defer tab.compactMu.Unlock()

	w := tab.log
	close(w.closing)
	<-w.done
	w.wmu.Lock()
	defer w.wmu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()

	if s := w.seg; s.f != nil { // nil: failed rotating, closed already
		if s.m != nil {
			munmap(s.m)
		}
		s.f.Close() // not s.close(): it would cut the file to its records
	}
	w.seg, w.err = &segment{}, ErrClosed // appends fail after
	unlockDir(tab.lock)
}

// TestCrashClose checks crashClose ends a table as a dying process: the
// lock let go, and a write copied, with SyncNone, still there.
func TestCrashClose(t *testing.T) {
	dir := t.TempDir()
	o := Options[string, int]{Sync: SyncNone}
	tab, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := tab.Put("a", 1); err != nil {
		t.Fatal(err)
	}
	crashClose(tab)
	if err := tab.Put("b", 2); err == nil {
		t.Error("a put after crashClose succeeded")
	}
	tab, err = Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	if v, ok := tab.Get("a"); !ok || v != 1 {
		t.Errorf("a = %d, %v after a crash; want 1", v, ok)
	}
}

// errCrash fails a sync, for the process to die before it.
var errCrash = errors.New("crash")

// recordsPerSegment is the records of TestCrashThenPowerLoss a segment
// holds, to the byte.
const recordsPerSegment = 10

// TestCrashThenPowerLoss kills the process before each sync of a run of
// writes, as the page cache holds them; opens the table, and writes once;
// then loses the power. What the open found stays, with the write after.
// The segments are filled to the byte, so that the last one, open, ends
// at the end of its file.
func TestCrashThenPowerLoss(t *testing.T) {
	o := Options[string, string]{SegmentSize: recordsPerSegment * recordSize(t)}
	for n := 1; ; n++ {
		crashed := true
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			crashed = crashThenPowerLoss(t, o, n)
		})
		if !crashed {
			return // the run ended before its nth sync
		}
	}
}

// crashThenPowerLoss runs TestCrashThenPowerLoss with the process killed
// before the nth sync, and reports whether it was.
func crashThenPowerLoss(t *testing.T, o Options[string, string], n int) bool {
	dir := t.TempDir()
	d := newDisk(t, dir)
	syncs := 0
	d.onSync = func(*os.File) error {
		if syncs++; syncs == n {
			return errCrash
		}
		return nil
	}
	tab, err := Open(dir, o)
	if err != nil {
		return errors.Is(err, errCrash) // killed opening: nothing to check
	}
	var h []write
	acked := 0
	for i := range 3*recordsPerSegment + 5 {
		w := write{key: fmt.Sprintf("k%03d", i%50), val: i}
		h = append(h, w)
		if err := tab.Put(w.key, value(w.val)); err != nil {
			break
		}
		acked++
	}
	if acked == len(h) {
		tab.Close()
		return false
	}
	crashClose(tab)
	d.onSync = nil

	// Opened after the crash: the first s writes, s from those acked to
	// those made.
	tab, err = Open(dir, o)
	if err != nil {
		t.Fatalf("killed at %d writes acked: open: %v", acked, err)
	}
	found := contents(tab)
	s := prefixOf(t, h, acked, found)
	if err := tab.Put("after", value(-1)); err != nil {
		t.Fatal(err)
	}
	crashClose(tab)

	// Opened after the power lost: what the first open found, and the
	// write after it.
	if err := d.powerLoss(dir); err != nil {
		t.Fatal(err)
	}
	tab, err = Open(dir, o)
	if err != nil {
		t.Fatalf("killed at %d writes acked, %d found: open after the power lost: %v", acked, s, err)
	}
	defer tab.Close()
	want := maps.Clone(found)
	want["after"] = value(-1)
	if got := contents(tab); !maps.Equal(got, want) {
		t.Errorf("killed at %d writes acked, %d found: after the power lost, %d entries, want %d", acked, s, len(got), len(want))
		logDir(t, "the disk", dir)
	}
	return true
}

// recordSize returns the size of a record in the log of a put of a key
// of 4 bytes and a value of 8: measured, not reckoned from the format.
func recordSize(t *testing.T) int64 {
	tab, err := Open(t.TempDir(), Options[string, string]{})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	if err := tab.Put("k000", value(0)); err != nil {
		t.Fatal(err)
	}
	tab.log.mu.Lock()
	defer tab.log.mu.Unlock()
	return tab.log.seg.off
}

// value is the value of 8 bytes of a write i.
func value(i int) string { return fmt.Sprintf("v%07d", i) }

func contents(tab *Table[string, string]) map[string]string { return maps.Collect(tab.All()) }

// prefixOf returns the s, from acked to len(h), whose first s writes leave
// got, failing the test if none does.
func prefixOf(t *testing.T, h []write, acked int, got map[string]string) int {
	t.Helper()
	want := map[string]string{}
	for _, w := range h[:acked] {
		want[w.key] = value(w.val)
	}
	for s := acked; ; s++ {
		if maps.Equal(got, want) {
			return s
		}
		if s == len(h) {
			t.Fatalf("killed at %d writes acked, %d made: the entries of none of them", acked, len(h))
		}
		want[h[s].key] = value(h[s].val)
	}
}
