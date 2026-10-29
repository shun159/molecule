//go:build realdisk

package kvs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// cowSize is the room a snapshot of a device replayed has for the writes
// to it: those kept of a loss, and those of mounting and of the table.
const cowSize = 1 << 30

// checkReplays replays the log l, and checks the device as a power loss
// after the file system was made may leave it: at each flush, all written
// before it on the device; between two, any of the writes after the
// first, as keepings picks them, but those of FUA, on the device once
// written.
func checkReplays(t *testing.T, l *logWrites, h []write, fs string, o Options[string, int]) {
	r := newReplayer(t, l.sectorSize, h, fs, o)
	for _, e := range l.entries {
		r.entry(e)
	}
	t.Logf("%d devices checked, of %d entries, %d writes", r.checked, len(l.entries), len(h))
}

// A replayer replays a log of dm-log-writes into an image, checking as it
// goes the devices a power loss may leave: each a snapshot of the image,
// the writes the loss keeps written to it, mounted.
type replayer struct {
	t          *testing.T
	sectorSize int64
	base       *os.File  // the device as of the last flush
	since      []lwEntry // the writes since the last flush

	baseDev, cowDev string // loop devices of base, and of the writes to a snapshot of it
	snap            string // the name of the snapshot target
	mnt             string

	made          bool // the file system: the marks after it
	acked, issued int
	checked       int
	h             []write
	fs            string
	o             Options[string, int]
}

// newReplayer makes the image to replay into, and the loop devices of it
// and of the writes to its snapshots.
func newReplayer(t *testing.T, sectorSize int64, h []write, fs string, o Options[string, int]) *replayer {
	sh(t, "modprobe", "dm-snapshot")
	dir := t.TempDir()
	r := &replayer{t: t, sectorSize: sectorSize, h: h, fs: fs, o: o, snap: targetName("snap"), mnt: t.TempDir()}
	base := filepath.Join(dir, "base.img")
	r.baseDev = attachImage(t, base, filesystems[fs].size, t.Cleanup)
	r.cowDev = attachImage(t, filepath.Join(dir, "cow.img"), cowSize, t.Cleanup)
	f, err := os.OpenFile(base, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	r.base = f
	return r
}

// entry replays e.
func (r *replayer) entry(e lwEntry) {
	switch {
	case e.flags&lwMark != 0:
		r.mark(e.mark)
	case !r.made:
		r.replay(r.base, e)
	case e.flags&lwFlush != 0:
		r.checkSince()
		for _, w := range r.since {
			r.replay(r.base, w)
		}
		r.since = nil
		r.replay(r.base, e) // its write, if any, after the flush
		r.check(nil)
	default:
		r.since = append(r.since, e)
	}
}

// mark counts the mark of name.
func (r *replayer) mark(name string) {
	switch name {
	case "mkfs":
		r.made = true
	case "issue":
		r.issued++
	case "ack":
		r.acked++
	}
}

// checkSince checks the device with the writes since the last flush kept
// as keepings picks them, those of FUA all kept.
func (r *replayer) checkSince() {
	if len(r.since) == 0 {
		return
	}
	for _, keep := range keepings(len(r.since)) {
		var kept []lwEntry
		for i, w := range r.since {
			if keep(i) || w.flags&lwFUA != 0 {
				kept = append(kept, w)
			}
		}
		r.check(kept)
	}
}

// check checks a snapshot of the device, kept written to it, as of the
// marks so far: mounted, the table on it as checkLoss requires.
func (r *replayer) check(kept []lwEntry) {
	r.t.Helper()
	r.checked++
	table := fmt.Sprintf("0 %d snapshot %s %s N 8", filesystems[r.fs].size/512, r.baseDev, r.cowDev)
	sh(r.t, "dmsetup", "create", r.snap, "--table", table)
	defer undo(r.t, "dmsetup", "remove", r.snap)
	dev := "/dev/mapper/" + r.snap
	r.write(dev, kept)
	sh(r.t, "mount", "-t", r.fs, dev, r.mnt)
	defer undo(r.t, "umount", r.mnt)
	checkLoss(r.t, loss{dir: filepath.Join(r.mnt, "kvs"), acked: r.acked, issued: r.issued}, r.h, r.o)
}

// write writes the entries ws to the device at path.
func (r *replayer) write(path string, ws []lwEntry) {
	if len(ws) == 0 {
		return
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, w := range ws {
		r.replay(f, w)
	}
	if err := f.Close(); err != nil {
		r.t.Fatal(err)
	}
}

// replay writes e to f.
func (r *replayer) replay(f *os.File, e lwEntry) {
	if err := e.replay(f, r.sectorSize); err != nil {
		r.t.Fatal(err)
	}
}
