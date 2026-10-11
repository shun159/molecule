//go:build !unix

package kvs

import (
	"os"
	"path/filepath"
)

// lockDir opens the file LOCK of dir, without locking it: two tables on
// one directory are not told apart here.
func lockDir(dir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
}

func unlockDir(f *os.File) error { return f.Close() }
