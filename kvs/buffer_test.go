package kvs

import "testing"

// A record of megabytes written, the log keeps no buffer it grew.
func TestLargeRecordLetGo(t *testing.T) {
	tab, err := Open(t.TempDir(), Options[string, []byte]{})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	if err := tab.Put("large", make([]byte, 4<<20)); err != nil {
		t.Fatal(err)
	}
	if err := tab.Put("small", []byte("x")); err != nil { // its batch takes the large's buffer, if kept
		t.Fatal(err)
	}
	tab.log.wmu.Lock()
	defer tab.log.wmu.Unlock()
	if p, s := cap(tab.log.pending), cap(tab.log.spare); max(p, s) > keptBuffer {
		t.Errorf("the log keeps buffers of %d and %d bytes", p, s)
	}
}
