//go:build realdisk

package kvs

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The tests of this file run kvs on a real file system, on a loop device
// under dm-log-writes, then open it as a power loss may have left it: the
// disk model of the other tests checked against a kernel. They need root,
// and the modules dm-log-writes and dm-snapshot:
//
//	go test -c -tags realdisk -o kvs.test ./kvs && sudo ./kvs.test -test.run RealDisk -test.v
//
// They touch only the loop devices of the images they make, and the
// targets named kvs-test-*, and refuse to run beside another run.

// A filesystem is a file system tested: the least device it is made on,
// and how; and whether it writes over a file in place, the room a segment
// makes, in zeros, then the room of its records.
type filesystem struct {
	size    int64
	mkfs    []string
	inPlace bool
}

var filesystems = map[string]filesystem{
	"ext4":  {16 << 20, []string{"mkfs.ext4", "-q", "-E", "nodiscard"}, true},
	"xfs":   {300 << 20, []string{"mkfs.xfs", "-q", "-K"}, true},
	"btrfs": {128 << 20, []string{"mkfs.btrfs", "-q", "-K"}, false},
}

// logSize is the room of the log device: all the run writes, mkfs too.
const logSize = 4 << 30

func TestRealDiskExt4(t *testing.T)  { testRealDisk(t, "ext4") }
func TestRealDiskXFS(t *testing.T)   { testRealDisk(t, "xfs") }
func TestRealDiskBtrfs(t *testing.T) { testRealDisk(t, "btrfs") }

// testRealDisk runs writes and compactions on fs, each write made and
// acked between marks of the log, then opens the device as a power loss
// may have left it, at each flush and between: the writes acked are
// there, and of the others some first ones; and the table goes on.
func testRealDisk(t *testing.T, fs string) {
	o := Options[string, int]{SegmentSize: 1 << 10, CompactRatio: -1}
	r := newRig(t, fs, false)
	h := r.run(o, 60)
	r.stop()
	l, err := readLogWrites(r.logImage)
	if err != nil {
		t.Fatal(err)
	}
	checkReplays(t, l, fs, checkLossAt(t, h, o))
}

// A rig is a file system on a loop device under dm-log-writes, mounted;
// with a dm-flakey between, to fail the writes at will.
type rig struct {
	t        *testing.T
	name     string // of the target of the log
	logImage string // the file of the log device
	flakey   string // the target failing writes, if any
	size     int64
	mnt      string
	undos    []func() // to undo the setup, the last first
}

// newRig makes the images, their loop devices, the targets over them, and
// fs on them, mounted.
func newRig(t *testing.T, fs string, flakey bool) *rig {
	if os.Geteuid() != 0 {
		t.Skip("needs root: see realdisk_test.go")
	}
	if _, err := exec.LookPath(filesystems[fs].mkfs[0]); err != nil {
		t.Skip(err)
	}
	if out := sh(t, "dmsetup", "ls"); strings.Contains(out, "kvs-test-") {
		t.Fatalf("another run is on: %s", out)
	}
	sh(t, "modprobe", "dm-log-writes")
	dir := t.TempDir()
	size := filesystems[fs].size
	r := &rig{t: t, name: targetName("log"), logImage: filepath.Join(dir, "log.img"), size: size, mnt: t.TempDir()}
	t.Cleanup(r.stop)
	data := attachImage(t, filepath.Join(dir, "data.img"), size, r.later)
	log := attachImage(t, r.logImage, logSize, r.later)
	table := fmt.Sprintf("0 %d log-writes %s %s", size/512, data, log)
	r.do([]string{"dmsetup", "create", r.name, "--table", table}, []string{"dmsetup", "remove", r.name})
	dev := "/dev/mapper/" + r.name
	if flakey {
		dev = r.addFlakey(dev)
	}
	sh(t, append(filesystems[fs].mkfs, dev)...)
	r.mark("mkfs")
	r.do([]string{"mount", "-t", fs, dev, r.mnt}, []string{"umount", r.mnt})
	return r
}

// addFlakey makes a dm-flakey over dev, writing to it, and returns it.
func (r *rig) addFlakey(dev string) string {
	sh(r.t, "modprobe", "dm-flakey")
	r.flakey = targetName("flakey")
	r.do([]string{"dmsetup", "create", r.flakey, "--table", r.flakeyTable(dev, false)}, []string{"dmsetup", "remove", r.flakey})
	return "/dev/mapper/" + r.flakey
}

// flakeyTable is the table of the dm-flakey over dev: up for an hour, or,
// failing, its writes failing with EIO for an hour.
func (r *rig) flakeyTable(dev string, failing bool) string {
	if failing {
		return fmt.Sprintf("0 %d flakey %s 0 0 3600 1 error_writes", r.size/512, dev)
	}
	return fmt.Sprintf("0 %d flakey %s 0 3600 0", r.size/512, dev)
}

// failWrites has the writes to the device fail with EIO, or, not failing,
// go through again; the file system not frozen meanwhile, for nothing to
// be flushed.
func (r *rig) failWrites(failing bool) {
	table := r.flakeyTable("/dev/mapper/"+r.name, failing)
	sh(r.t, "dmsetup", "suspend", "--nolockfs", r.flakey)
	sh(r.t, "dmsetup", "load", r.flakey, "--table", table)
	sh(r.t, "dmsetup", "resume", r.flakey)
}

// do runs cmd, and has the rig run undoCmd once stopped.
func (r *rig) do(cmd, undoCmd []string) {
	sh(r.t, cmd...)
	r.later(func() { undo(r.t, undoCmd...) })
}

// later has the rig run f once stopped.
func (r *rig) later(f func()) { r.undos = append(r.undos, f) }

// stop undoes the setup, the last first, once: the log then whole.
func (r *rig) stop() {
	for len(r.undos) > 0 {
		f := r.undos[len(r.undos)-1]
		r.undos = r.undos[:len(r.undos)-1]
		f()
	}
}

// mark marks the log, as of the writes completed so far.
func (r *rig) mark(name string) {
	sh(r.t, "dmsetup", "message", r.name, "0", "mark", name)
}

// run makes n writes to a table on the rig, each between a mark issue and
// a mark ack, compacting it every 20, and returns them.
func (r *rig) run(o Options[string, int], n int) []write {
	tab, err := Open(filepath.Join(r.mnt, "kvs"), o)
	if err != nil {
		r.t.Fatal(err)
	}
	rnd := rand.New(rand.NewPCG(5, 6))
	var h []write
	for i := range n {
		w := write{key: strconv.Itoa(rnd.IntN(20)), val: i, del: rnd.IntN(4) == 0}
		h = append(h, w)
		r.mark("issue")
		if w.del {
			err = tab.Delete(w.key)
		} else {
			err = tab.Put(w.key, w.val)
		}
		if err != nil {
			r.t.Fatal(err)
		}
		r.mark("ack")
		if i%20 == 19 {
			if err := tab.Compact(); err != nil {
				r.t.Fatal(err)
			}
		}
	}
	if err := tab.Close(); err != nil {
		r.t.Fatal(err)
	}
	return h
}

// targetName returns the name of a target of this run, for role.
func targetName(role string) string {
	return "kvs-test-" + strconv.Itoa(os.Getpid()) + "-" + role
}

// attachImage makes an image of size at path, sparse, and a loop device
// of it, and returns the device; it hands the detaching of it to later,
// as t.Cleanup.
func attachImage(t *testing.T, path string, size int64, later func(func())) string {
	t.Helper()
	f, err := os.Create(path)
	if err == nil {
		err = f.Truncate(size)
		f.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	dev := sh(t, "losetup", "--find", "--show", path)
	later(func() { undo(t, "losetup", "-d", dev) })
	return dev
}

// sh runs a command, and returns its output; failing, the test fails.
func sh(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// undo runs a command undoing a setup; failing, the test fails, going on.
func undo(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
		t.Errorf("%s: %v\n%s", strings.Join(args, " "), err, out)
	}
}
