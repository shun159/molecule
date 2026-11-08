package kvs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// TestMain runs the tests, then fails if a file of their directories is
// still open: a table not closed, or a file kvs did not close. The files
// kvs opens are traced: held, for the GC not to close one left open, and
// named with where they were opened.
func TestMain(m *testing.M) {
	traceOpens()
	code := m.Run()
	if open := openTestFiles(); len(open) > 0 {
		fmt.Fprintf(os.Stderr, "files of the tests left open:\n\t%s\n", strings.Join(open, "\n\t"))
		code = 1
	}
	os.Exit(code)
}

// A tracedFile is a file kvs opened, and the stack of the open.
type tracedFile struct {
	f   *os.File
	pcs []uintptr
}

// traced holds the files kvs opened, by fd: the number of one is taken by
// another only once it is closed, so the file of an fd still open is the
// last opened with it.
var traced struct {
	sync.Mutex
	files map[uintptr]tracedFile
}

// traceOpens has openFile trace the files it opens.
func traceOpens() {
	traced.files = map[uintptr]tracedFile{}
	orig := openFile
	openFile = func(path string, flag int, perm os.FileMode) (*os.File, error) {
		f, err := orig(path, flag, perm)
		if err == nil {
			trace(f)
		}
		return f, err
	}
}

// trace holds f, with the stack of the caller of openFile.
func trace(f *os.File) {
	pcs := make([]uintptr, 32)
	pcs = pcs[:runtime.Callers(3, pcs)] // past Callers, trace and the hook
	traced.Lock()
	defer traced.Unlock()
	traced.files[f.Fd()] = tracedFile{f, pcs}
}

// openTestFiles returns the files under the directories of the tests the
// process has open, each with its fd, and where kvs opened it, if it did.
func openTestFiles() []string {
	fds, _ := filepath.Glob("/proc/self/fd/*")
	var open []string
	for _, fd := range fds {
		if to, err := os.Readlink(fd); err == nil && isTestFile(to) {
			open = append(open, filepath.Base(fd)+": "+to+openedAt(filepath.Base(fd)))
		}
	}
	return open
}

// openedAt returns where kvs opened the file of fd, still open, as lines
// of the stack; none, if kvs did not open it.
func openedAt(fd string) string {
	var n uintptr
	if _, err := fmt.Sscan(fd, &n); err != nil {
		return ""
	}
	traced.Lock()
	tf, ok := traced.files[n]
	traced.Unlock()
	if !ok || isClosed(tf.f) {
		return ""
	}
	var b strings.Builder
	b.WriteString(", opened by kvs at:")
	frames := runtime.CallersFrames(tf.pcs)
	for {
		fr, more := frames.Next()
		fmt.Fprintf(&b, "\n\t\t%s\n\t\t\t%s:%d", fr.Function, fr.File, fr.Line)
		if !more || fr.Function == "testing.tRunner" {
			return b.String()
		}
	}
}

// isClosed reports whether f is closed.
func isClosed(f *os.File) bool {
	_, err := f.Stat()
	return errors.Is(err, os.ErrClosed)
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
