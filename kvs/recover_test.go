package kvs

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
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
}
