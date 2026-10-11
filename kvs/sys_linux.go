package kvs

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// madvPopulateWrite is MADV_POPULATE_WRITE, of Linux 5.14.
const madvPopulateWrite = 23

// mmap maps size bytes of f, shared, for writing.
func mmap(f *os.File, size int64) ([]byte, error) {
	return syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}

func munmap(m []byte) error { return syscall.Munmap(m) }

// populate maps the pages of m, of the file, ready for writing: the writes
// after take no fault. It is only an advice, ignored by older kernels.
func populate(m []byte) { syscall.Madvise(m, madvPopulateWrite) }

// datasync syncs the data of f, and of its metadata only what reading the
// data needs, such as its size.
func datasync(f *os.File) error { return syscall.Fdatasync(int(f.Fd())) }

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
