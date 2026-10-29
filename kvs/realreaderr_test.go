//go:build realdisk

package kvs

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func TestRealDiskReadErrorExt4(t *testing.T)  { testRealReadError(t, "ext4") }
func TestRealDiskReadErrorXFS(t *testing.T)   { testRealReadError(t, "xfs") }
func TestRealDiskReadErrorBtrfs(t *testing.T) { testRealReadError(t, "btrfs") }

// testRealReadError puts records, then has a sector of them unreadable,
// under dm-dust, its file system mounted again for nothing to be read
// from the page cache: Open fails with EIO, rather than take the records
// from there on for a write cut short. The sector readable again, the
// table opens with all its records: the Open failed changed nothing.
func testRealReadError(t *testing.T, fs string) {
	d := newDusty(t, fs)
	dir := filepath.Join(d.mnt, "kvs")
	tab, err := Open(dir, Options[string, string]{})
	if err != nil {
		t.Fatal(err)
	}
	ks := keys("k", 30)
	for _, k := range ks {
		if err := tab.Put(k, strings.Repeat("v", 1000)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tab.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _, err := listLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	bad := d.sectorOf(filepath.Join(dir, segs[len(segs)-1].name), 8<<10)

	d.remount(func() { d.message("addbadblock", strconv.FormatInt(bad, 10)); d.message("enable") })
	if tab, err := Open(dir, Options[string, string]{}); !errors.Is(err, syscall.EIO) {
		t.Errorf("sector %d unreadable: Open: %v, want EIO", bad, err)
		if err == nil {
			tab.Close()
		}
	}
	d.remount(func() { d.message("disable") })
	tab, err = Open(dir, Options[string, string]{})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	if tab.Len() != len(ks) {
		t.Errorf("the sector readable again: %d entries, want %d", tab.Len(), len(ks))
	}
}

// A dusty is a file system on a loop device under dm-dust, mounted: of
// sectors to make unreadable at will.
type dusty struct {
	t        *testing.T
	fs       string
	name     string // of the target
	dev, mnt string
}

// newDusty makes fs on a dm-dust over a loop device of an image, mounted.
func newDusty(t *testing.T, fs string) *dusty {
	mountable(t, fs)
	sh(t, "modprobe", "dm-dust")
	size := filesystems[fs].size
	loop := attachImage(t, filepath.Join(t.TempDir(), "fs.img"), size, t.Cleanup)
	d := &dusty{t: t, fs: fs, name: targetName("dust"), mnt: t.TempDir()}
	sh(t, "dmsetup", "create", d.name, "--table", fmt.Sprintf("0 %d dust %s 0 512", size/512, loop))
	t.Cleanup(func() { undo(t, "dmsetup", "remove", d.name) })
	d.dev = "/dev/mapper/" + d.name
	sh(t, append(filesystems[fs].mkfs, d.dev)...)
	sh(t, "mount", "-t", fs, d.dev, d.mnt)
	t.Cleanup(func() { undo(t, "umount", d.mnt) })
	return d
}

// message sends dm-dust a message.
func (d *dusty) message(args ...string) {
	sh(d.t, append([]string{"dmsetup", "message", d.name, "0"}, args...)...)
}

// remount unmounts the file system, does f, and mounts it again: nothing
// of it then in the page cache.
func (d *dusty) remount(f func()) {
	sh(d.t, "umount", d.mnt)
	f()
	sh(d.t, "mount", "-t", d.fs, d.dev, d.mnt)
}

// sectorOf returns the sector of the device holding the byte at off of
// the file at path.
func (d *dusty) sectorOf(path string, off int64) int64 {
	at := fileExtentAt(d.t, path, off)
	if d.fs == "btrfs" { // a logical address of btrfs, to map to the device
		sh(d.t, "sync", "-f", path) // the extent in its tree on the device
		at = btrfsPhysical(d.t, d.dev, at)
	}
	return at / 512
}

var extentLine = regexp.MustCompile(`^\s*\d+:\s+(\d+)\.\.\s*(\d+):\s+(\d+)\.\.`)

// fileExtentAt returns where, in bytes, the file system holds the byte at
// off of the file at path, as filefrag tells it.
func fileExtentAt(t *testing.T, path string, off int64) int64 {
	t.Helper()
	for line := range strings.Lines(sh(t, "filefrag", "-v", "-b512", path)) {
		m := extentLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		lo, _ := strconv.ParseInt(m[1], 10, 64)
		hi, _ := strconv.ParseInt(m[2], 10, 64)
		phys, _ := strconv.ParseInt(m[3], 10, 64)
		if s := off / 512; lo <= s && s <= hi {
			return (phys+s-lo)*512 + off%512
		}
	}
	t.Fatalf("filefrag: no extent of %s at %d", path, off)
	return 0
}

var physicalOf = regexp.MustCompile(`physical (\d+)`)

// btrfsPhysical returns where on dev btrfs holds its logical address at.
func btrfsPhysical(t *testing.T, dev string, at int64) int64 {
	t.Helper()
	out := sh(t, "btrfs-map-logical", "-l", strconv.FormatInt(at, 10), dev)
	m := physicalOf.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("btrfs-map-logical: %s", out)
	}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	return n
}

// mountable skips the test unless it can make and mount fs.
func mountable(t *testing.T, fs string) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("needs root: see realdisk_test.go")
	}
	if _, err := exec.LookPath(filesystems[fs].mkfs[0]); err != nil {
		t.Skip(err)
	}
}
