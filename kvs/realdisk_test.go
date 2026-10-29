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
// under dm-log-writes, then open it as each flush of the run left it: the
// disk model of the other tests checked against a kernel. They need root,
// and the module dm-log-writes:
//
//	go test -c -tags realdisk -o kvs.test ./kvs && sudo ./kvs.test -test.run RealDisk -test.v
//
// They touch only the loop devices of the images they make, and the
// targets named kvs-test-*, and refuse to run beside another run.

const devSize = 16 << 20

// mkfs makes each file system tested.
var mkfs = map[string][]string{
	"ext4": {"mkfs.ext4", "-q", "-E", "nodiscard"},
}

func TestRealDiskExt4(t *testing.T) { testRealDisk(t, "ext4") }

// testRealDisk runs writes on fs, each made and acked between marks of
// the log, then opens the device as each flush left it: the writes acked
// are there, and of the others some first ones; and the table goes on.
func testRealDisk(t *testing.T, fs string) {
	o := Options[string, int]{SegmentSize: 1 << 10, CompactRatio: -1}
	r := newRig(t, fs)
	h := r.run(o, 60)
	r.stop()
	l, err := readLogWrites(r.logImage)
	if err != nil {
		t.Fatal(err)
	}
	checkReplays(t, l, h, fs, o)
}

// A rig is a file system on a loop device under dm-log-writes, mounted.
type rig struct {
	t        *testing.T
	name     string // of the target
	logImage string // the file of the log device
	mnt      string
	stops    []func() error // to undo the setup, the last first
}

// newRig makes the images, their loop devices, the target over them, and
// fs on it, mounted.
func newRig(t *testing.T, fs string) *rig {
	if os.Geteuid() != 0 {
		t.Skip("needs root: see realdisk_test.go")
	}
	if out := sh(t, "dmsetup", "ls"); strings.Contains(out, "kvs-test-") {
		t.Fatalf("another run is on: %s", out)
	}
	sh(t, "modprobe", "dm-log-writes")
	dir := t.TempDir()
	r := &rig{t: t, name: "kvs-test-" + strconv.Itoa(os.Getpid()), logImage: filepath.Join(dir, "log.img"), mnt: t.TempDir()}
	t.Cleanup(r.stop)
	data := r.attach(filepath.Join(dir, "data.img"), devSize)
	log := r.attach(r.logImage, 64*devSize)
	table := fmt.Sprintf("0 %d log-writes %s %s", devSize/512, data, log)
	r.do([]string{"dmsetup", "create", r.name, "--table", table}, []string{"dmsetup", "remove", r.name})
	dev := "/dev/mapper/" + r.name
	sh(t, append(mkfs[fs], dev)...)
	r.mark("mkfs")
	r.do([]string{"mount", "-t", fs, dev, r.mnt}, []string{"umount", r.mnt})
	return r
}

// attach makes an image of size at path, and a loop device of it, and
// returns the device.
func (r *rig) attach(path string, size int64) string {
	f, err := os.Create(path)
	if err == nil {
		err = f.Truncate(size)
		f.Close()
	}
	if err != nil {
		r.t.Fatal(err)
	}
	dev := sh(r.t, "losetup", "--find", "--show", path)
	r.stops = append(r.stops, func() error { return run("losetup", "-d", dev) })
	return dev
}

// do runs cmd, and notes undo to undo it.
func (r *rig) do(cmd, undo []string) {
	sh(r.t, cmd...)
	r.stops = append(r.stops, func() error { return run(undo...) })
}

// stop undoes the setup, the last first, once: the log then whole.
func (r *rig) stop() {
	for len(r.stops) > 0 {
		undo := r.stops[len(r.stops)-1]
		r.stops = r.stops[:len(r.stops)-1]
		if err := undo(); err != nil {
			r.t.Error(err)
		}
	}
}

// mark marks the log, as of the writes completed so far.
func (r *rig) mark(name string) {
	sh(r.t, "dmsetup", "message", r.name, "0", "mark", name)
}

// run makes n writes to a table on the rig, each between a mark issue and
// a mark ack, and returns them.
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
	}
	if err := tab.Close(); err != nil {
		r.t.Fatal(err)
	}
	return h
}

// checkReplays replays the log l, and checks the device as each flush, or
// write of FUA, after the file system was made left it.
func checkReplays(t *testing.T, l *logWrites, h []write, fs string, o Options[string, int]) {
	dev := make([]byte, devSize)
	made := false
	var acked, issued, points int
	for _, e := range l.entries {
		e.replay(dev, l.sectorSize)
		switch e.mark {
		case "mkfs":
			made = true
		case "issue":
			issued++
		case "ack":
			acked++
		}
		if !made || !e.durable() {
			continue
		}
		points++
		checkDevice(t, dev, loss{acked: acked, issued: issued}, h, fs, o)
	}
	t.Logf("%d points of %d entries, %d writes", points, len(l.entries), len(h))
}

// checkDevice mounts a copy of dev, and checks the table on it, as of l.
func checkDevice(t *testing.T, dev []byte, l loss, h []write, fs string, o Options[string, int]) {
	t.Helper()
	img := filepath.Join(t.TempDir(), "replay.img")
	if err := os.WriteFile(img, dev, 0o600); err != nil {
		t.Fatal(err)
	}
	loop := sh(t, "losetup", "--find", "--show", img)
	defer run("losetup", "-d", loop)
	mnt := t.TempDir()
	sh(t, "mount", "-t", fs, loop, mnt)
	defer run("umount", mnt)
	l.dir = filepath.Join(mnt, "kvs")
	checkLoss(t, l, h, o)
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

// run runs a command, and returns its failure with its output.
func run(args ...string) error {
	out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return nil
}
