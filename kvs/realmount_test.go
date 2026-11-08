//go:build realdisk

package kvs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// unmount unmounts dir, trying again while it fails, for some 5s, as
// dmsetup remove --retry does: a process of the machine may look into a
// file system just mounted. Still failing, the error names the processes
// holding a file of it.
func unmount(dir string) error {
	var out []byte
	var err error
	for range 25 {
		if out, err = exec.Command("umount", dir).CombinedOutput(); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("umount %s: %v\n%s\nheld by: %s", dir, err, out, holders(dir))
}

// undoMount unmounts dir, undoing a setup; failing, the test fails, going
// on.
func undoMount(t *testing.T, dir string) {
	t.Helper()
	if err := unmount(dir); err != nil {
		t.Error(err)
	}
}

// holders lists the processes with a file under dir open, mapped, or as
// their directory: each pid, command and file.
func holders(dir string) string {
	var links []string
	for _, glob := range []string{"/proc/[0-9]*/cwd", "/proc/[0-9]*/root", "/proc/[0-9]*/fd/*", "/proc/[0-9]*/map_files/*"} {
		m, _ := filepath.Glob(glob)
		links = append(links, m...)
	}
	var held []string
	for _, l := range links {
		to, err := os.Readlink(l)
		if err != nil || (to != dir && !strings.HasPrefix(to, dir+"/")) {
			continue
		}
		pid := strings.Split(l, "/")[2]
		comm, _ := os.ReadFile("/proc/" + pid + "/comm")
		held = append(held, fmt.Sprintf("%s %s: %s", pid, strings.TrimSpace(string(comm)), to))
	}
	if len(held) == 0 {
		return "no process; a mount namespace, maybe"
	}
	return strings.Join(held, "; ")
}
