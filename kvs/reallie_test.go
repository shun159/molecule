//go:build realdisk

package kvs

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestRealDiskLostWritesExt4(t *testing.T)  { testRealLostWrites(t, "ext4") }
func TestRealDiskLostWritesXFS(t *testing.T)   { testRealLostWrites(t, "xfs") }
func TestRealDiskLostWritesBtrfs(t *testing.T) { testRealLostWrites(t, "btrfs") }

// testRealLostWrites has a disk lie: the writes of some puts, synced,
// dropped under dm-flakey, the syncs reported done; then puts after, on
// the disk. Mounted again, for nothing to come from the page cache, the
// records of the puts after claim those lost durable: Open fails with
// ErrCorrupt, rather than open without the puts acked; or, the file
// system checksumming its data, with EIO, the read of them failing.
func testRealLostWrites(t *testing.T, fs string) {
	f := newFlakey(t, fs)
	dir := filepath.Join(f.mnt, "kvs")
	tab, err := Open(dir, Options[string, string]{})
	if err != nil {
		t.Fatal(err)
	}
	val := strings.Repeat("v", 1000)
	put := func(ks []string) {
		for _, k := range ks {
			if err := tab.Put(k, val); err != nil {
				t.Fatal(err)
			}
		}
	}
	put(keys("a", 10))
	f.dropWrites(true)
	put(keys("d", 12)) // pages of their own, some: lost whole
	f.dropWrites(false)
	put(keys("b", 10))
	tab.Close()

	f.remount()
	tab, err = Open(dir, Options[string, string]{})
	caught := errors.Is(err, ErrCorrupt) || filesystems[fs].dataSums && errors.Is(err, syscall.EIO)
	if !caught {
		n := -1
		if err == nil {
			n = tab.Len()
			tab.Close()
		}
		t.Errorf("writes of puts acked lost, those after there: Open: %v, %d entries of 32; want ErrCorrupt", err, n)
	}
}

// A flakey is a file system on a loop device under dm-flakey, mounted:
// to drop its writes at will.
type flakey struct {
	t        *testing.T
	fs       string
	name     string // of the target
	loop     string
	dev, mnt string
	size     int64
}

// newFlakey makes fs on a dm-flakey over a loop device of an image,
// mounted.
func newFlakey(t *testing.T, fs string) *flakey {
	mountable(t, fs)
	sh(t, "modprobe", "dm-flakey")
	f := &flakey{t: t, fs: fs, name: targetName("flakey"), size: filesystems[fs].size, mnt: t.TempDir()}
	f.loop = attachImage(t, filepath.Join(t.TempDir(), "fs.img"), f.size, t.Cleanup)
	sh(t, "dmsetup", "create", f.name, "--table", f.table(false))
	t.Cleanup(func() { undo(t, "dmsetup", "remove", f.name) })
	f.dev = "/dev/mapper/" + f.name
	sh(t, append(filesystems[fs].mkfs, f.dev)...)
	sh(t, "mount", "-t", fs, f.dev, f.mnt)
	t.Cleanup(func() { undo(t, "umount", f.mnt) })
	return f
}

// table is the table of the target: up for an hour, or, dropping, its
// writes dropped for an hour, reported done.
func (f *flakey) table(dropping bool) string {
	if dropping {
		return fmt.Sprintf("0 %d flakey %s 0 0 3600 1 drop_writes", f.size/512, f.loop)
	}
	return fmt.Sprintf("0 %d flakey %s 0 3600 0", f.size/512, f.loop)
}

// dropWrites has the writes dropped, or, not dropping, go through again;
// the file system not frozen meanwhile.
func (f *flakey) dropWrites(dropping bool) {
	sh(f.t, "dmsetup", "suspend", "--nolockfs", f.name)
	sh(f.t, "dmsetup", "load", f.name, "--table", f.table(dropping))
	sh(f.t, "dmsetup", "resume", f.name)
}

// remount mounts the file system again: nothing of it then in the page
// cache.
func (f *flakey) remount() {
	sh(f.t, "umount", f.mnt)
	sh(f.t, "mount", "-t", f.fs, f.dev, f.mnt)
}
