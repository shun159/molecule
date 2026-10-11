package kvs

import (
	"os"
	"syscall"
)

// datasync syncs the data of f, and of its metadata only what reading the
// data needs, such as its size.
func datasync(f *os.File) error { return syscall.Fdatasync(int(f.Fd())) }
