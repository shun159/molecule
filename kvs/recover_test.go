package kvs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

// A last segment of records all in the snapshot, the new one after it
// gone, is deleted as the table opens, not counted.
func TestStaleLastSegment(t *testing.T) {
	dir := t.TempDir()
	tab, err := Open(dir, Options[string, int]{})
	if err != nil {
		t.Fatal(err)
	}
	tab.Put("a", 1)
	tab.Put("b", 2)
	segs, _, _ := listLog(dir)
	old := segs[len(segs)-1].name
	b, err := os.ReadFile(filepath.Join(dir, old))
	if err != nil {
		t.Fatal(err)
	}
	if err := tab.Compact(); err != nil {
		t.Fatal(err)
	}
	tab.Close()
	segs, _, _ = listLog(dir)
	for _, s := range segs {
		os.Remove(filepath.Join(dir, s.name))
	}
	if err := os.WriteFile(filepath.Join(dir, old), b, 0o644); err != nil {
		t.Fatal(err)
	}

	tab, err = Open(dir, Options[string, int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	if _, err := os.Stat(filepath.Join(dir, old)); !os.IsNotExist(err) {
		t.Errorf("%s, all in the snapshot, left: %v", old, err)
	}
	if info := tab.Info(); info.Segments != 1 || tab.Len() != 2 {
		t.Errorf("%d segments, %d entries; want the new one, 2", info.Segments, tab.Len())
	}
}

// Zeros after the records too few for a header end the records, as a
// segment synced and not yet cut to them leaves; a byte not zero there is
// a write cut short.
func TestZeroTailShorterThanHeader(t *testing.T) {
	dir := t.TempDir()
	tab, err := Open(dir, Options[string, int]{})
	if err != nil {
		t.Fatal(err)
	}
	tab.Put("a", 1)
	tab.Put("b", 2)
	tab.Close()
	segs, _, _ := listLog(dir)
	recs, err := os.ReadFile(filepath.Join(dir, segs[0].name))
	if err != nil {
		t.Fatal(err)
	}
	none := func(uint64, byte, []byte, []byte, int64) error { return nil }
	for k := 1; k < headerLen; k++ {
		tail := make([]byte, k)
		n, err := readRecords(bytes.NewReader(append(slices.Clone(recs), tail...)), none)
		if err != nil || n != int64(len(recs)) {
			t.Errorf("%d zeros after: %d, %v; want %d, nil", k, n, err, len(recs))
		}
		tail[k-1] = 1
		if _, err := readRecords(bytes.NewReader(append(slices.Clone(recs), tail...)), none); err != errTorn {
			t.Errorf("%d bytes after, not all zeros: %v; want errTorn", k, err)
		}
	}

	// A header whole, its body cut off, is a write cut short, though the
	// buffer it is read into holds zeros.
	if _, err := readRecords(bytes.NewReader(recs[:headerLen]), none); err != errTorn {
		t.Errorf("a header, its body cut off: %v; want errTorn", err)
	}
}

// A failingReader reads r up to at bytes, then fails with err: a disk
// that cannot read past at.
type failingReader struct {
	r   io.Reader
	at  int
	err error
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.at <= 0 {
		return 0, f.err
	}
	if len(p) > f.at {
		p = p[:f.at]
	}
	n, err := f.r.Read(p)
	f.at -= n
	return n, err
}

// A read failing, at any byte of the records or past them, is that
// failure: not a write cut short, which opening would cut off, nor the
// end of the records.
func TestReadError(t *testing.T) {
	dir := t.TempDir()
	tab, err := Open(dir, Options[string, int]{})
	if err != nil {
		t.Fatal(err)
	}
	tab.Put("a", 1)
	tab.Put("b", 2)
	tab.Close()
	segs, _, _ := listLog(dir)
	recs, err := os.ReadFile(filepath.Join(dir, segs[0].name))
	if err != nil {
		t.Fatal(err)
	}
	none := func(uint64, byte, []byte, []byte, int64) error { return nil }
	for at := 0; at <= len(recs); at++ {
		r := &failingReader{r: bytes.NewReader(recs), at: at, err: syscall.EIO}
		if _, err := readRecords(r, none); !errors.Is(err, syscall.EIO) {
			t.Errorf("a read failing at %d of %d: %v; want EIO", at, len(recs), err)
		}
	}
}
