//go:build unix

package kvs

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// lockDir takes the lock of dir, a flock of its file LOCK: held by an open
// file, it goes with the process, if it dies.
func lockDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return f, nil
}

func unlockDir(f *os.File) error { return f.Close() }
