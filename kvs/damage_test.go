package kvs

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
)

func TestClaimant(t *testing.T) {
	put := func(seq uint64, claim uint16) []byte {
		return appendRecord(nil, newRec(body(opPutClaim, "k", "v")), seq, claim)
	}
	old := appendRecord(nil, newRec(body(opPut, "k", "v")), 9, 0)
	garbage := bytes.Repeat([]byte{0xa5}, 13)
	// b begins by the record of 5, not whole: its bytes lost, zeros.
	torn := make([]byte, len(put(5, 1)))
	for _, c := range []struct {
		name string
		b    []byte
		want uint64 // the claimant's seq, 0 if none
	}{
		{"none after", nil, 0},
		{"zeros after", make([]byte, 100), 0},
		{"claiming it", put(7, 2), 7},
		{"claiming past it", put(7, 1), 7},
		{"of a seq past the records b holds", put(9, 4), 0},
		{"claiming before it", put(7, 3), 0},
		{"claiming nothing", put(7, 0), 0},
		{"of an op not claiming", old, 0},
		{"of a seq before it", put(5, 1), 0},
		{"claiming more than its seq", put(6, 9), 0},
		{"after garbage", append(garbage, put(8, 1)...), 8},
		{"after records not claiming it", append(put(6, 2), put(7, 1)...), 7},
		{"of a crc wrong", flip(put(7, 2), 5), 0},
	} {
		got, ok := claimant(append(slices.Clip(torn), c.b...), 5)
		if ok != (c.want != 0) || got != c.want {
			t.Errorf("%s: claimant = %d, %t; want %d", c.name, got, ok, c.want)
		}
	}
}

// flip returns b with a bit of its byte at i flipped.
func flip(b []byte, i int) []byte {
	b[i] ^= 0x10
	return b
}

// A record damaged, of a write a record after claims durable, fails Open
// with ErrCorrupt; the last, none claiming it, is taken for a write cut
// short, the writes before it there.
func TestDamageClaimed(t *testing.T) {
	const n = 6
	dir := t.TempDir()
	tab, err := Open(dir, Options[string, int]{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if err := tab.Put(strconv.Itoa(i), i); err != nil {
			t.Fatal(err)
		}
	}
	tab.Close()
	log, err := os.ReadFile(filepath.Join(dir, segName(1)))
	if err != nil {
		t.Fatal(err)
	}
	ends := recordEnds(t, log)
	for at := range ends[len(ends)-1] {
		k := recordAt(ends, at)
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, segName(1)), flip(bytes.Clone(log), at), 0o644); err != nil {
			t.Fatal(err)
		}
		tab, err := Open(dir, Options[string, int]{})
		if k < n-1 {
			if !errors.Is(err, ErrCorrupt) {
				t.Errorf("byte %d of record %d damaged, claimed: Open: %v, want ErrCorrupt", at, k, err)
			}
			if err == nil {
				tab.Close()
			}
			continue
		}
		if err != nil {
			t.Fatalf("byte %d of the last record damaged: Open: %v", at, err)
		}
		if tab.Len() != n-1 {
			t.Errorf("byte %d of the last record damaged: %d entries, want %d", at, tab.Len(), n-1)
		}
		tab.Close()
	}
}

// recordEnds returns the offset after each record of b.
func recordEnds(t *testing.T, b []byte) []int {
	t.Helper()
	var ends []int
	at := 0
	_, err := readRecords(bytes.NewReader(b), func(_ uint64, _ byte, _, _ []byte, size int64, _ uint64) error {
		at += int(size)
		ends = append(ends, at)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return ends
}

// recordAt returns the index of the record of ends at offset at.
func recordAt(ends []int, at int) int {
	for k, end := range ends {
		if at < end {
			return k
		}
	}
	return len(ends)
}

// A log of records claiming nothing, written before the claims, is read
// as it was: a record damaged is a write cut short, wherever in the last
// segment.
func TestDamageUnclaimed(t *testing.T) {
	const n = 5
	var log []byte
	for i := range n {
		log = appendRecord(log, newRec(body(opPut, strconv.Itoa(i), "v")), uint64(i+1), 0)
	}
	ends := recordEnds(t, log)
	for k := range n {
		dir := t.TempDir()
		at := ends[k] - seqLen - 1
		if err := os.WriteFile(filepath.Join(dir, segName(1)), flip(bytes.Clone(log), at), 0o644); err != nil {
			t.Fatal(err)
		}
		tab, err := Open(dir, Options[string, string]{})
		if err != nil {
			t.Fatalf("record %d damaged: Open: %v", k, err)
		}
		if tab.Len() != k {
			t.Errorf("record %d damaged: %d entries, want %d", k, tab.Len(), k)
		}
		tab.Close()
	}
}
