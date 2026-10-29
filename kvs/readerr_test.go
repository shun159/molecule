package kvs

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"testing"
)

// TestOpenReadError fails the reads of opening with EIO, at each byte of
// the records of each file it reads and right after them, on a directory
// a crash left in a compaction: Open fails with EIO, changes nothing of
// the directory, and opening again, the fault gone, finds every write.
func TestOpenReadError(t *testing.T) {
	o := Options[string, int]{SegmentSize: 1 << 9}
	src, h := crashedInCompaction(t, o)
	before := dirBytes(t, src)
	want := stateAfter(h, len(h))
	fault := failReads(t)

	fault.read = []string{}
	tab, err := Open(copyDir(t, src), o)
	if err != nil {
		t.Fatal(err)
	}
	tab.Close()
	files := fault.read
	fault.read = nil
	t.Logf("opening reads %v, of %v", files, names(before))

	for _, name := range files {
		for at := 0; at <= recordsEnd(t, before[name]); at++ {
			dir := copyDir(t, src)
			fault.name, fault.at = name, at
			_, err := Open(dir, o)
			fault.name = ""
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("a read of %s failing at %d: %v; want EIO", name, at, err)
			}
			checkUnchanged(t, before, dirBytes(t, dir), name, at)
			if got := reopened(t, dir, o); !maps.Equal(got, want) {
				t.Fatalf("a read of %s failed at %d: open after: %d entries, want %d", name, at, len(got), len(want))
			}
		}
	}
}

// A readFault fails the reads of opening the file of name, at byte at,
// with EIO; none if name is "". It notes in read, if not nil, the files
// opening reads.
type readFault struct {
	name string
	at   int
	read []string
}

// failReads puts a readFault, failing nothing yet, before the reads of
// opening, while the test runs.
func failReads(t *testing.T) *readFault {
	fault := &readFault{}
	orig := openRead
	openRead = func(path string) (io.ReadCloser, error) {
		f, err := orig(path)
		name := filepath.Base(path)
		if err != nil || name != fault.name {
			if fault.read != nil {
				fault.read = append(fault.read, name)
			}
			return f, err
		}
		return failingFile{&failingReader{r: f, at: fault.at, err: syscall.EIO}, f}, nil
	}
	t.Cleanup(func() { openRead = orig })
	return fault
}

// A failingFile reads through a failingReader, and closes the file.
type failingFile struct {
	io.Reader
	io.Closer
}

// reopened returns the entries opening dir finds.
func reopened(t *testing.T, dir string, o Options[string, int]) map[string]int {
	t.Helper()
	tab, err := Open(dir, o)
	if err != nil {
		t.Fatalf("open after: %v", err)
	}
	defer tab.Close()
	return maps.Collect(tab.All())
}

// changedFiles returns the names of the files removed, changed or made
// between before and after.
func changedFiles(before, after map[string][]byte) []string {
	files := []string{}

	for name, b := range before {
		a, ok := after[name]
		if !ok || !slices.Equal(a, b) {
			files = append(files, name)
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			files = append(files, name)
		}
	}
	slices.Sort(files)
	return files
}

// checkUnchanged fails t if a failed Open, of a read of name failing at
// at, changed the directory: before is its files, by name, as Open found
// them; after, as Open left them.
func checkUnchanged(t *testing.T, before, after map[string][]byte, name string, at int) {
	t.Helper()
	if c := changedFiles(before, after); len(c) > 0 {
		t.Fatalf("a read of %s failing at %d: Open changed %v", name, at, c)
	}
}

// recordsEnd returns the offset after the records of the log file b: a
// read failing past it may not be reached, by opening stopping at the
// zeros after them.
func recordsEnd(t *testing.T, b []byte) int {
	t.Helper()
	none := func(uint64, byte, []byte, []byte, int64, uint64) error { return nil }
	n, err := readRecords(bytes.NewReader(b), none)
	if err != nil {
		t.Fatal(err)
	}
	return int(n)
}

// crashedInCompaction makes the directory of a table as a crash leaves it
// in a compaction, after the snapshot is in place: the snapshot before
// and the segments the new one holds are not yet removed; and the temp
// file of a compaction after is cut short. It returns the directory, and
// the writes made, all acked.
func crashedInCompaction(t *testing.T, o Options[string, int]) (string, []write) {
	dir := t.TempDir()
	tab, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	var h []write
	put := func(n int) {
		for range n {
			i := len(h)
			w := write{key: strconv.Itoa(i % 15), val: i, del: i%4 == 3}
			if err := writeTo(tab, w); err != nil {
				t.Fatal(err)
			}
			h = append(h, w)
		}
	}
	put(20)
	if err := tab.Compact(); err != nil {
		t.Fatal(err)
	}
	put(20)
	old := dirBytes(t, dir)
	if err := tab.Compact(); err != nil {
		t.Fatal(err)
	}
	put(20)
	crashClose(tab)

	now := dirBytes(t, dir)
	for name, b := range old {
		if _, ok := now[name]; !ok {
			writeFile(t, dir, name, b)
		}
	}
	_, snaps, _ := listLog(dir)
	latest := now[snaps[len(snaps)-1].name]
	writeFile(t, dir, snapName(uint64(len(h)))+".tmp", latest[:len(latest)/2])
	return dir, h
}

// dirBytes returns the files of dir, by name.
func dirBytes(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string][]byte{}
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		m[e.Name()] = b
	}
	return m
}

// copyDir copies the files of src to a new directory, and returns it.
func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	for name, b := range dirBytes(t, src) {
		writeFile(t, dst, name, b)
	}
	return dst
}

func writeFile(t *testing.T, dir, name string, b []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func names(m map[string][]byte) []string { return slices.Sorted(maps.Keys(m)) }
