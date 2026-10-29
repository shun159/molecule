package kvs

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestDurableClaim(t *testing.T) {
	for _, c := range []struct {
		name         string
		seq, durable uint64
		synced       bool
		want         uint16
	}{
		{"some durable", 10, 7, true, 3},
		{"none durable", 10, 0, true, 10},
		{"never synced", 10, 7, false, 0},
		{"lag as far as it holds", 70000, 70000 - math.MaxUint16, true, math.MaxUint16},
		{"lag beyond", 70000, 70000 - math.MaxUint16 - 1, true, 0},
		{"none durable, seq beyond", 70000, 0, true, 0},
	} {
		if got := durableClaim(c.seq, c.durable, c.synced); got != c.want {
			t.Errorf("%s: durableClaim(%d, %d, %t) = %d, want %d", c.name, c.seq, c.durable, c.synced, got, c.want)
		}
	}
}

// claimed is a record as readRecords hands it.
type claimed struct {
	seq, durable uint64
	op           byte
	key, val     string
}

func readClaims(t *testing.T, b []byte) []claimed {
	t.Helper()
	var got []claimed
	n, err := readRecords(bytes.NewReader(b), func(seq uint64, op byte, k, v []byte, _ int64, durable uint64) error {
		got = append(got, claimed{seq: seq, durable: durable, op: op, key: string(k), val: string(v)})
		return nil
	})
	if err != nil || n != int64(len(b)) {
		t.Fatalf("read %d of %d bytes: %v", n, len(b), err)
	}
	return got
}

func body(op byte, k, v string) []byte {
	b := binary.AppendUvarint([]byte{op}, uint64(len(k)))
	return append(append(b, k...), v...)
}

// Records of the ops claiming read with what they claim, those of the ops
// before them as they were, claiming nothing: a log written before reads
// as it did, and one written on.
func TestRecordClaims(t *testing.T) {
	var b []byte
	b = appendRecord(b, newRec(body(opPut, "a", "1")), 1, 0)
	b = appendRecord(b, newRec(body(opDel, "a", "")), 2, 0)
	b = appendRecord(b, newRec(body(opPutClaim, "b", "2")), 3, 1)
	b = appendRecord(b, newRec(body(opDelClaim, "b", "")), 4, 2)
	b = appendRecord(b, newRec(body(opPutClaim, "c", "3")), 5, 0)
	b = appendRecord(b, newRec(body(opPut, "d", "4")), 6, 0)
	want := []claimed{
		{1, 0, opPut, "a", "1"},
		{2, 0, opDel, "a", ""},
		{3, 2, opPut, "b", "2"},
		{4, 2, opDel, "b", ""},
		{5, 0, opPut, "c", "3"},
		{6, 0, opPut, "d", "4"},
	}
	got := readClaims(t, b)
	if len(got) != len(want) {
		t.Fatalf("%d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d: %+v, want %+v", i, got[i], want[i])
		}
	}
}

// The crc of a record covers its claim: a claim changed is a record torn.
func TestClaimInCRC(t *testing.T) {
	b := appendRecord(nil, newRec(body(opPutClaim, "a", "1")), 9, 4)
	b[len(b)-seqLen-claimLen]++
	none := func(uint64, byte, []byte, []byte, int64, uint64) error { return nil }
	if _, err := readRecords(bytes.NewReader(b), none); err != errTorn {
		t.Errorf("a claim changed reads %v, want errTorn", err)
	}
}

// A write, each after the last is durable, claims it; of a table never
// synced, none claims.
func TestClaimsOfTable(t *testing.T) {
	for _, c := range []struct {
		name  string
		sync  Sync
		claim func(seq uint64) uint64
	}{
		{"each", SyncEach, func(seq uint64) uint64 { return seq - 1 }},
		{"none", SyncNone, func(uint64) uint64 { return 0 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			tab, err := Open(dir, Options[string, int]{Sync: c.sync})
			if err != nil {
				t.Fatal(err)
			}
			for i := range 5 {
				if err := tab.Put("k", i); err != nil {
					t.Fatal(err)
				}
			}
			if err := tab.Delete("k"); err != nil {
				t.Fatal(err)
			}
			tab.Close()
			segs, _, _ := listLog(dir)
			b, err := os.ReadFile(filepath.Join(dir, segs[len(segs)-1].name))
			if err != nil {
				t.Fatal(err)
			}
			got := readClaims(t, b[:recordsEnd(t, b)])
			if len(got) != 6 {
				t.Fatalf("%d records, want 6", len(got))
			}
			for _, r := range got {
				if want := c.claim(r.seq); r.durable != want {
					t.Errorf("seq %d claims %d durable, want %d", r.seq, r.durable, want)
				}
			}
		})
	}
}
