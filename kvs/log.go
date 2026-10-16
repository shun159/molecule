package kvs

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
)

// The log of a table is a directory of segments, files of records written
// one after another, and of at most one snapshot, the records of the
// entries of the table as they were at some seq.
//
// A record is
//
//	len   uint32  the length of what follows the crc
//	crc   uint32  CRC-32C of what follows it
//	body          op, uvarint length of the key, key, value
//	seq   uint64  its number in the order of writes
//
// in little endian. The seq comes last so that the CRC of the body is
// computed before the seq is known, outside the lock giving it.
//
// A segment is named by the seq of its first record, NNN.log, and a
// snapshot by the seq it covers, NNN.snap: the segments after it hold the
// writes since.
//
// The segment written to is mapped in memory, and filled with zeros ahead
// of its records, a chunk at a time; it is cut to its records once done
// with. A write within the zeros changes no metadata of the file, so that
// syncing it is syncing its data alone. A header of zeros ends a segment.

const (
	opPut byte = 1
	opDel byte = 2

	headerLen = 8
	seqLen    = 8
	maxRecord = 1 << 30

	zeroChunk = 1 << 20
)

// zeros fills segments ahead of their records.
var zeros = make([]byte, zeroChunk)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Errors of tables.
var (
	// ErrClosed: the table is closed.
	ErrClosed = errors.New("kvs: closed")
	// ErrLocked: another table has the directory open, in this process
	// or another.
	ErrLocked = errors.New("kvs: directory in use")
	// ErrCorrupt: a record of the log is damaged, elsewhere than at the
	// end of the last segment, where a write was cut short.
	ErrCorrupt = errors.New("kvs: corrupt log")
)

// rec is a record to write, but its seq.
type rec struct {
	body []byte
	crc  uint32 // of body
}

func newRec(body []byte) rec { return rec{body: body, crc: crc32.Checksum(body, castagnoli)} }

func (r rec) size() int64 { return int64(headerLen + len(r.body) + seqLen) }

// appendRecord appends r with seq to b.
func appendRecord(b []byte, r rec, seq uint64) []byte {
	var tail [seqLen]byte
	binary.LittleEndian.PutUint64(tail[:], seq)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(r.body)+seqLen))
	b = binary.LittleEndian.AppendUint32(b, crc32.Update(r.crc, castagnoli, tail[:]))
	b = append(b, r.body...)
	return append(b, tail[:]...)
}

// batch is the records written to the log together: done is closed once
// they are durable, or failed with err. The first writer waiting for it
// takes lead, and writes it out.
type batch struct {
	done chan struct{}
	err  error
	lead atomic.Bool
}

func newBatch() *batch { return &batch{done: make(chan struct{})} }

// finished is a batch already done, of no record.
var finished = func() *batch { b := newBatch(); close(b.done); return b }()

// rotated tells a compaction where the log was cut: the segments in old
// hold the records up to seq, and none after.
type rotated struct {
	seq uint64
	old []string
	err error
}

func segName(seq uint64) string  { return fmt.Sprintf("%016x.log", seq) }
func snapName(seq uint64) string { return fmt.Sprintf("%016x.snap", seq) }

// parseName returns the seq of a segment or snapshot file name.
func parseName(name, ext string) (uint64, bool) {
	s, ok := strings.CutSuffix(name, ext)
	if !ok || len(s) != 16 {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 16, 64)
	return n, err == nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

type logFile struct {
	name string
	seq  uint64
}

// listLog lists the segments and the snapshots of dir, in the order of
// their seq.
func listLog(dir string) (segs, snaps []logFile, err error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range ents {
		if seq, ok := parseName(e.Name(), ".log"); ok {
			segs = append(segs, logFile{e.Name(), seq})
		} else if seq, ok := parseName(e.Name(), ".snap"); ok {
			snaps = append(snaps, logFile{e.Name(), seq})
		}
	}
	bySeq := func(a, b logFile) int { return cmp.Compare(a.seq, b.seq) }
	slices.SortFunc(segs, bySeq)
	slices.SortFunc(snaps, bySeq)
	return segs, snaps, nil
}

// errTorn is a record cut short or damaged.
var errTorn = errors.New("kvs: torn record")

// readRecords reads the records of r, handing each to f with its size, and returns the
// offset after the last whole one. It stops at the first that is not
// whole, with errTorn.
func readRecords(r io.Reader, f func(seq uint64, op byte, key, val []byte, size int64) error) (int64, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	var off int64
	var hdr [headerLen]byte
	var buf []byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if err == io.EOF {
				return off, nil
			}
			return off, errTorn
		}
		n := binary.LittleEndian.Uint32(hdr[:4])
		if n == 0 && binary.LittleEndian.Uint32(hdr[4:]) == 0 {
			return off, nil // the zeros after the records
		}
		if n < seqLen+2 || n > maxRecord {
			return off, errTorn
		}
		if cap(buf) < int(n) {
			buf = make([]byte, n)
		}
		buf = buf[:n]
		if _, err := io.ReadFull(br, buf); err != nil {
			return off, errTorn
		}
		if crc32.Checksum(buf, castagnoli) != binary.LittleEndian.Uint32(hdr[4:]) {
			return off, errTorn
		}
		body, seq := buf[:n-seqLen], binary.LittleEndian.Uint64(buf[n-seqLen:])
		op := body[0]
		klen, m := binary.Uvarint(body[1:])
		knownOp := op == opPut || op == opDel
		keyFits := m > 0 && klen <= uint64(len(body)-1-m)
		if !knownOp || !keyFits {
			return off, errTorn
		}
		key := body[1+m : 1+m+int(klen)]
		if err := f(seq, op, key, body[1+m+int(klen):], int64(headerLen+n)); err != nil {
			return off, err
		}
		off += int64(headerLen + n)
	}
}
