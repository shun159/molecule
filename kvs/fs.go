package kvs

import "os"

// The operations changing the directory of a table, but its syncs (of
// sys_linux.go): variables, for a test to see each, or fail it, as a disk
// full or a crash would.
var (
	// createFile creates the file at path, or empties it.
	createFile = func(path string) (*os.File, error) {
		return os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	}
	// writeAt writes b to f at off.
	writeAt = func(f *os.File, b []byte, off int64) (int, error) { return f.WriteAt(b, off) }
	// truncateFile cuts or extends f to n bytes.
	truncateFile = (*os.File).Truncate
	rename       = os.Rename
	remove       = os.Remove
)
