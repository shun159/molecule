package kvs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain runs the tests, then fails if a file of their directories is
// still open: a table not closed, or a file kvs did not close. A file
// nothing refers to any more may be closed by the GC before, unseen.
func TestMain(m *testing.M) {
	code := m.Run()
	if open := openTestFiles(); len(open) > 0 {
		fmt.Fprintf(os.Stderr, "files of the tests left open:\n\t%s\n", strings.Join(open, "\n\t"))
		code = 1
	}
	os.Exit(code)
}

// openTestFiles returns the files under the directories of the tests the
// process has open, each with its fd.
func openTestFiles() []string {
	fds, _ := filepath.Glob("/proc/self/fd/*")
	var open []string
	for _, fd := range fds {
		if to, err := os.Readlink(fd); err == nil && isTestFile(to) {
			open = append(open, filepath.Base(fd)+": "+to)
		}
	}
	return open
}

// isTestFile reports whether path is under the directory of a test, a
// benchmark or a fuzz test: of t.TempDir, named after it.
func isTestFile(path string) bool {
	for _, kind := range []string{"Test", "Benchmark", "Fuzz"} {
		if strings.HasPrefix(path, filepath.Join(os.TempDir(), kind)) {
			return true
		}
	}
	return false
}
