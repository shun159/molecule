package kvs

import (
	"testing"
	"time"
)

// A write made after a compaction cut the log, in a shard walked after,
// is in the snapshot: the snapshot is put in place once it is durable in
// the log too, not to bring back on a crash a write never durable.
func TestCompactionWaitsForWrites(t *testing.T) {
	tab, err := Open(t.TempDir(), Options[string, int]{Sync: SyncEvery(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	if _, err := tab.put("a", 1, nil); err != nil {
		t.Fatal(err)
	}
	cut := tab.log.cut()
	if _, err := tab.put("b", 2, nil); err != nil { // amid the walk
		t.Fatal(err)
	}
	if _, err := tab.writeSnapshot(snapName(cut.seq), cut.seq); err != nil {
		t.Fatal(err)
	}
	if d := tab.Info().DurableSeq; d < cut.seq+1 {
		t.Errorf("snapshot in place with b, seq %d, durable only to %d", cut.seq+1, d)
	}
}
