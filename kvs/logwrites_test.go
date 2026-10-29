//go:build realdisk

package kvs

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// A log of dm-log-writes is the writes to a device, in the order they
// completed, the flushes and the marks among them: a device replayed up
// to a flush is as a power loss then may leave it.
//
// On the log device: a super block, then each entry in a sector of its
// own, the sectors it wrote after it.
type logWrites struct {
	sectorSize int64
	entries    []lwEntry
}

// An lwEntry is a write, a flush, a discard or a mark.
type lwEntry struct {
	sector, sectors int64 // where it wrote, in sectors of the device
	flags           uint64
	data            []byte // the sectors written
	mark            string
}

const (
	lwMagic   = 0x6a736677736872
	lwVersion = 1

	lwFlush   = 1 << 0
	lwFUA     = 1 << 1
	lwDiscard = 1 << 2
	lwMark    = 1 << 3

	lwSuperLen = 28 // magic, version, entries, sector size
	lwEntryLen = 32 // sector, sectors, flags, data length
)

// readLogWrites reads the log device of dm-log-writes, from its file.
func readLogWrites(path string) (*logWrites, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	super := make([]byte, lwSuperLen)
	if _, err := f.ReadAt(super, 0); err != nil {
		return nil, err
	}
	le := binary.LittleEndian
	if le.Uint64(super) != lwMagic || le.Uint64(super[8:]) != lwVersion {
		return nil, fmt.Errorf("%s: not a log of dm-log-writes", path)
	}
	l := &logWrites{sectorSize: int64(le.Uint32(super[24:]))}
	off := l.sectorSize
	for range le.Uint64(super[16:]) {
		e, next, err := l.entryAt(f, off)
		if err != nil {
			return nil, fmt.Errorf("%s: entry %d: %w", path, len(l.entries), err)
		}
		l.entries = append(l.entries, e)
		off = next
	}
	return l, nil
}

// entryAt reads the entry at off of the log f, and returns it, and the
// offset of the next.
func (l *logWrites) entryAt(f io.ReaderAt, off int64) (lwEntry, int64, error) {
	h := make([]byte, l.sectorSize)
	if _, err := f.ReadAt(h, off); err != nil {
		return lwEntry{}, 0, err
	}
	le := binary.LittleEndian
	e := lwEntry{sector: int64(le.Uint64(h)), sectors: int64(le.Uint64(h[8:])), flags: le.Uint64(h[16:])}
	off += l.sectorSize
	switch {
	case e.flags&lwMark != 0:
		n := le.Uint64(h[24:])
		if n > uint64(len(h)-lwEntryLen) {
			return lwEntry{}, 0, fmt.Errorf("a mark of %d bytes", n)
		}
		e.mark = string(h[lwEntryLen : lwEntryLen+n])
	case e.flags&lwDiscard == 0:
		e.data = make([]byte, e.sectors*l.sectorSize)
		if _, err := f.ReadAt(e.data, off); err != nil {
			return lwEntry{}, 0, err
		}
		off += int64(len(e.data))
	}
	return e, off, nil
}

// replay writes e to dev, the device replayed: a discard leaves zeros.
func (e lwEntry) replay(dev io.WriterAt, sectorSize int64) error {
	at := e.sector * sectorSize
	switch {
	case e.flags&lwMark != 0:
		return nil
	case e.flags&lwDiscard != 0:
		_, err := dev.WriteAt(make([]byte, e.sectors*sectorSize), at)
		return err
	}
	_, err := dev.WriteAt(e.data, at)
	return err
}
