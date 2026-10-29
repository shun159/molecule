//go:build realdisk

package kvs

import (
	"encoding/binary"
	"fmt"
	"os"
)

// A log of dm-log-writes is the writes to a device, in the order they
// completed, the flushes and the marks among them: a device replayed up
// to a flush, or a write of FUA, is as a power loss then may leave it.
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
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	le := binary.LittleEndian
	if len(b) < lwSuperLen || le.Uint64(b) != lwMagic || le.Uint64(b[8:]) != lwVersion {
		return nil, fmt.Errorf("%s: not a log of dm-log-writes", path)
	}
	l := &logWrites{sectorSize: int64(le.Uint32(b[24:]))}
	off := l.sectorSize
	for range le.Uint64(b[16:]) {
		e, next, err := l.entryAt(b, off)
		if err != nil {
			return nil, fmt.Errorf("%s: entry %d: %w", path, len(l.entries), err)
		}
		l.entries = append(l.entries, e)
		off = next
	}
	return l, nil
}

// entryAt returns the entry at off of the log b, and the offset of the
// next.
func (l *logWrites) entryAt(b []byte, off int64) (lwEntry, int64, error) {
	if off+l.sectorSize > int64(len(b)) {
		return lwEntry{}, 0, fmt.Errorf("past the end, at %d", off)
	}
	le := binary.LittleEndian
	h := b[off:]
	e := lwEntry{sector: int64(le.Uint64(h)), sectors: int64(le.Uint64(h[8:])), flags: le.Uint64(h[16:])}
	off += l.sectorSize
	switch {
	case e.flags&lwMark != 0:
		e.mark = string(h[lwEntryLen : lwEntryLen+le.Uint64(h[24:])])
	case e.flags&lwDiscard == 0:
		n := e.sectors * l.sectorSize
		if off+n > int64(len(b)) {
			return lwEntry{}, 0, fmt.Errorf("its %d sectors past the end", e.sectors)
		}
		e.data, off = b[off:off+n], off+n
	}
	return e, off, nil
}

// replay writes e to dev, the device replayed: a discard leaves zeros.
func (e lwEntry) replay(dev []byte, sectorSize int64) {
	at := e.sector * sectorSize
	switch {
	case e.flags&lwMark != 0:
	case e.flags&lwDiscard != 0:
		clear(dev[at : at+e.sectors*sectorSize])
	default:
		copy(dev[at:], e.data)
	}
}
