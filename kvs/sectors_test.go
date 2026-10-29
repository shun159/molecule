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
	"testing"
)

// A disk writes a file a sector at a time, and keeps, of what was written
// and not synced, at a power loss, any of its sectors, in no order: a
// later one without one before.
const sectorSize = 512

// A sector is a sector of a file, written and not synced: off its offset.
type sector struct {
	name string
	ino  uint64
	off  int
}

// unsyncedSectors returns the sectors written and not synced of the files
// of the directory, as named at its last sync.
func (d *disk) unsyncedSectors() ([]sector, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var secs []sector
	for _, name := range slices.Sorted(maps.Keys(d.names)) {
		ino := d.names[name]
		live, err := d.live(ino)
		if err != nil {
			return nil, err
		}
		synced := d.files[ino]
		for off := 0; off < max(len(live), len(synced)); off += sectorSize {
			if !sameSector(synced, live, off) {
				secs = append(secs, sector{name: name, ino: ino, off: off})
			}
		}
	}
	return secs, nil
}

// live returns what the file of ino holds now, as the page cache has it,
// under mu.
func (d *disk) live(ino uint64) ([]byte, error) {
	f, ok := d.pins[ino]
	if !ok {
		return d.files[ino], nil // never synced, nor named at a sync: as noted
	}
	return os.ReadFile(fmt.Sprintf("/proc/self/fd/%d", f.Fd()))
}

// sameSector reports whether a and b hold the same of the sector at off,
// what is past the end of either being zeros.
func sameSector(a, b []byte, off int) bool {
	for i := off; i < off+sectorSize; i++ {
		if byteAt(a, i) != byteAt(b, i) {
			return false
		}
	}
	return true
}

func byteAt(b []byte, i int) byte {
	if i < len(b) {
		return b[i]
	}
	return 0
}

// powerLossSectors writes what a power loss leaves into dst, empty, as
// powerLoss, but for the sectors of secs, written and not synced, that
// keep reports kept: on disk, as written, the file as long as they need.
func (d *disk) powerLossSectors(dst string, secs []sector, keep func(i int) bool) error {
	d.mu.Lock()
	files := make(map[string][]byte, len(d.names))
	for name, ino := range d.names {
		files[name] = slices.Clone(d.files[ino])
	}
	for i, s := range secs {
		if !keep(i) {
			continue
		}
		live, err := d.live(s.ino)
		if err != nil {
			d.mu.Unlock()
			return err
		}
		files[s.name] = withSector(files[s.name], live, s.off)
	}
	d.mu.Unlock()
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(dst, name), b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// withSector returns b with the sector at off of live, b grown to hold it
// if it is shorter, but no longer than live.
func withSector(b, live []byte, off int) []byte {
	end := min(off+sectorSize, len(live))
	if end <= off {
		return b
	}
	if len(b) < end {
		b = append(b, make([]byte, end-len(b))...)
	}
	copy(b[off:end], live[off:end])
	return b
}

// TestPartialPersist loses the power before each sync of a run of writes,
// many a batch, keeping any of the sectors written and not synced, in no
// order, as keepings picks them: a later record on disk, maybe, without
// one before. Open takes it all for a write cut short, at the end of the
// log: the writes acked are there, and of the others some first ones; and
// the table goes on. It never takes it for damage.
func TestPartialPersist(t *testing.T) {
	dir := t.TempDir()
	o := Options[string, int]{SegmentSize: 8 << 10, CompactMin: 8 << 10}
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
		secs, err := d.unsyncedSectors()
		for _, keep := range keepings(len(secs)) {
			if err != nil {
				break
			}
			l := loss{dir: t.TempDir(), acked: acked, issued: len(history)}
			err = d.powerLossSectors(l.dir, secs, keep)
			losses = append(losses, l)
		}
		if err != nil && lossErr == nil {
			lossErr = err
		}
		return nil
	}
	tab, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(3, 4))
	for range 60 {
		var ws []Write[string, int]
		mu.Lock()
		for range 1 + r.IntN(40) {
			w := write{key: strconv.Itoa(r.IntN(200)), val: len(history), del: r.IntN(5) == 0}
			history = append(history, w)
			ws = append(ws, Write[string, int]{Key: w.key, Val: w.val, Delete: w.del})
		}
		mu.Unlock()
		if err := tab.Writes(ws...); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		acked += len(ws)
		mu.Unlock()
	}
	if err := tab.Close(); err != nil {
		t.Fatal(err)
	}
	d.onSync = nil
	if lossErr != nil {
		t.Fatal(lossErr)
	}
	for _, l := range losses {
		checkLoss(t, l, history, o)
	}
	t.Logf("%d power losses, of %d writes", len(losses), len(history))
}
