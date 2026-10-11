//go:build !linux

package kvs

import "os"

func datasync(f *os.File) error { return f.Sync() }
