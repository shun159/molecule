//go:build realdisk

package kvs

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func TestRealDiskNoSpaceExt4(t *testing.T)  { testRealNoSpace(t, "ext4") }
func TestRealDiskNoSpaceXFS(t *testing.T)   { testRealNoSpace(t, "xfs") }
func TestRealDiskNoSpaceBtrfs(t *testing.T) { testRealNoSpace(t, "btrfs") }

// testRealNoSpace fills fs, then puts until the disk is full: the put that
// finds it so fails with ENOSPC; on a file system writing in place, alone,
// the table going on once space is made. It opens again with the puts
// acked, none of those failed.
func testRealNoSpace(t *testing.T, fs string) {
	mnt := mountFresh(t, fs)
	dir := filepath.Join(mnt, "kvs")
	o := Options[string, string]{SegmentSize: 64 << 10, CompactRatio: -1}
	tab, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	filler := filepath.Join(mnt, "filler")
	fill(t, filler, 256<<10)

	val := strings.Repeat("v", 1000)
	var acked, failed []string
	for i := 0; len(failed) < 3 && i < 100000; i++ {
		k := strconv.Itoa(i)
		if err := tab.Put(k, val); err != nil {
			t.Logf("put %d: %v", i, err)
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("put %d, the disk full: %v, want ENOSPC", i, err)
			}
			failed = append(failed, k)
			continue
		}
		acked = append(acked, k)
	}
	if len(failed) == 0 {
		t.Fatal("the disk never full")
	}
	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	// Copying on write, the disk full may fail the log: see the package doc.
	if err := tab.Put("after", val); err != nil && filesystems[fs].inPlace {
		t.Errorf("a put after the space made: %v", err)
	}
	tab.Close()

	tab, err = Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	for _, k := range acked {
		if _, ok := tab.Get(k); !ok {
			t.Errorf("put %s, acked, lost", k)
		}
	}
	for _, k := range failed {
		if _, ok := tab.Get(k); ok {
			t.Errorf("put %s, failed, there", k)
		}
	}
	t.Logf("%d puts acked, then %d failed", len(acked), len(failed))
}

// mountFresh makes fs on a loop device of an image, mounted, and returns
// where.
func mountFresh(t *testing.T, fs string) string {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("needs root: see realdisk_test.go")
	}
	if _, err := exec.LookPath(filesystems[fs].mkfs[0]); err != nil {
		t.Skip(err)
	}
	dev := attachImage(t, filepath.Join(t.TempDir(), "fs.img"), filesystems[fs].size, t.Cleanup)
	sh(t, append(filesystems[fs].mkfs, dev)...)
	mnt := t.TempDir()
	sh(t, "mount", "-t", fs, dev, mnt)
	t.Cleanup(func() { undo(t, "umount", mnt) })
	return mnt
}

// fill writes the file at path until the disk is full, then cuts it by
// room, for so many bytes left.
func fill(t *testing.T, path string, room int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	chunk := make([]byte, 64<<10)
	var size int64
	for {
		n, err := f.Write(chunk)
		size += int64(n)
		if err != nil {
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatal(err)
			}
			break
		}
	}
	f.Sync() // the disk full, it may fail: the space is taken all the same
	if err := f.Truncate(max(size-room, 0)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}
