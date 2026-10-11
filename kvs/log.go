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
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
// The segment written to is filled with zeros ahead of its records, a
// chunk at a time, and cut to its records once done with: a write within
// the zeros changes no metadata of the file, so that syncing it is syncing
// its data alone, fdatasync on Linux. A header of zeros ends a segment.

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

// wal appends records to the segments of a directory. Writers append to
// pending, and one of those waiting writes it out, syncs, and finishes the
// batch of those records, while the next pile up: a commit of a group.
// The goroutine of run writes for those not waiting, effects, and for
// SyncEvery, whose writers wait for its tick.
type wal struct {
	dir     string
	sync    Sync
	segSize int64
	after   func() // after each write, from run

	mu       sync.Mutex
	seq      uint64 // last given
	pending  []byte
	batch    *batch   // of pending
	last     *batch   // the last taken out of pending: batches finish in order
	unsynced []*batch // written, waiting for the sync of Sync.Every
	err      error    // set, appending fails

	kick    chan struct{}
	rotates chan chan rotated
	closing chan struct{}
	done    chan struct{}

	// Of the holder of wmu, writing.
	wmu      sync.Mutex
	seg      *os.File
	segBytes int64  // of records
	zeroed   int64  // the size of seg, zeros after its records
	written  uint64 // last seq written out
	spare    []byte
	failed   error

	durable  atomic.Uint64
	syncs    atomic.Uint64
	disk     atomic.Int64 // bytes of the segments and snapshot
	segments atomic.Int64
}

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

// startWAL starts the log of dir, its last seq being seq, in a new segment.
func startWAL(dir string, seq uint64, disk int64, segments int, o Sync, segSize int64, after func()) (*wal, error) {
	w := &wal{
		dir: dir, sync: o, segSize: segSize, after: after,
		seq: seq, written: seq, batch: newBatch(),
		kick:    make(chan struct{}, 1),
		rotates: make(chan chan rotated),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
	w.durable.Store(seq)
	w.disk.Store(disk)
	w.segments.Store(int64(segments))
	if err := w.newSegment(); err != nil {
		return nil, err
	}
	go w.run()
	return w, nil
}

// append appends r, and returns its seq and the batch it is in. The
// writer then waits for the batch, or kicks run to write it.
func (w *wal) append(r rec) (uint64, *batch, error) {
	w.mu.Lock()
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return 0, nil, err
	}
	w.seq++
	seq := w.seq
	w.pending = appendRecord(w.pending, r, seq)
	b := w.batch
	w.mu.Unlock()
	return seq, b, nil
}

// kickRun has run write what is pending.
func (w *wal) kickRun() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// wait returns once b is durable, writing it out if no one else does.
func (w *wal) wait(b *batch) error {
	select {
	case <-b.done:
		return b.err
	default:
	}
	if w.sync.every > 0 {
		w.kickRun()
	} else if b.lead.CompareAndSwap(false, true) {
		w.wmu.Lock()
		select {
		case <-b.done: // written by another
		default:
			w.flush()
		}
		w.wmu.Unlock()
	}
	<-b.done
	return b.err
}

// barrier returns the batch the records appended so far are durable with.
func (w *wal) barrier() (*batch, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return nil, w.err
	}
	if len(w.pending) > 0 {
		return w.batch, nil
	}
	if w.last != nil {
		return w.last, nil
	}
	return finished, nil
}

func (w *wal) run() {
	defer close(w.done)
	var tick <-chan time.Time
	if w.sync.every > 0 {
		t := time.NewTicker(w.sync.every)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-w.kick:
			w.wmu.Lock()
			w.flush()
			w.wmu.Unlock()
		case <-tick:
			w.wmu.Lock()
			w.syncUnsynced()
			w.wmu.Unlock()
		case reply := <-w.rotates:
			w.wmu.Lock()
			w.flush()
			r := w.rotate()
			w.wmu.Unlock()
			reply <- r
		case <-w.closing:
			w.wmu.Lock()
			w.flush()
			w.syncUnsynced()
			if w.seg != nil {
				if w.failed == nil {
					w.syncSeg()
				}
				w.closeSeg()
				w.seg = nil
			}
			w.wmu.Unlock()
			return
		}
	}
}

// flush writes out what is pending, under wmu.
func (w *wal) flush() {
	w.mu.Lock()
	buf, b, last := w.pending, w.batch, w.seq
	if len(buf) == 0 {
		w.mu.Unlock()
		return
	}
	w.pending, w.batch, w.last = w.spare[:0], newBatch(), b
	w.mu.Unlock()

	err := w.failed
	if err == nil && w.seg == nil {
		err = ErrClosed
	}
	if err == nil {
		err = w.write(buf)
		if err == nil {
			w.written = last
			w.segBytes += int64(len(buf))
			w.disk.Add(int64(len(buf)))
		}
	}
	w.spare = buf
	switch {
	case err != nil:
		w.finish(b, err)
	case w.sync.none:
		w.finish(b, nil)
	case w.sync.every > 0:
		w.mu.Lock()
		w.unsynced = append(w.unsynced, b)
		w.mu.Unlock()
	default:
		start := time.Now()
		err := w.syncSeg()
		took := time.Since(start)
		w.finish(b, err)
		w.gather(took)
	}
	if w.failed == nil && w.segBytes >= w.segSize {
		w.rotate()
	}
	if w.after != nil {
		w.after()
	}
}

// gather lets the writers just told their batch is durable write again
// before the next batch is taken, for a while bounded by a part of the
// sync before, took: without it, the writers settle in two groups, each
// writing while the other's batch is synced, and a batch has half of them.
func (w *wal) gather(took time.Duration) {
	deadline := time.Now().Add(min(took/8, 200*time.Microsecond))
	prev, still := w.pendingLen(), 0
	for still < 2 && time.Now().Before(deadline) {
		runtime.Gosched()
		n := w.pendingLen()
		if n == prev {
			still++
		} else {
			prev, still = n, 0
		}
	}
}

func (w *wal) pendingLen() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}

// write writes buf after the records of the segment, over its zeros.
func (w *wal) write(buf []byte) error {
	end := w.segBytes + int64(len(buf))
	for w.zeroed < end {
		if _, err := w.seg.WriteAt(zeros, w.zeroed); err != nil {
			return err
		}
		w.zeroed += zeroChunk
	}
	_, err := w.seg.WriteAt(buf, w.segBytes)
	return err
}

func (w *wal) syncSeg() error {
	w.syncs.Add(1)
	return datasync(w.seg)
}

// closeSeg cuts the segment to its records, and closes it.
func (w *wal) closeSeg() error {
	err := w.seg.Truncate(w.segBytes)
	if cerr := w.seg.Close(); err == nil {
		err = cerr
	}
	return err
}

func (w *wal) syncUnsynced() {
	w.mu.Lock()
	bs := w.unsynced
	w.unsynced = nil
	w.mu.Unlock()
	if len(bs) == 0 {
		return
	}
	err := w.failed
	if err == nil {
		err = w.syncSeg()
	}
	for _, b := range bs {
		w.finish(b, err)
	}
}

// finish ends b, with err; after an error, the log fails all writes.
func (w *wal) finish(b *batch, err error) {
	if err != nil && err != ErrClosed && w.failed == nil {
		w.failed = fmt.Errorf("kvs: log failed: %w", err)
		w.mu.Lock()
		w.err = w.failed
		w.mu.Unlock()
		err = w.failed
	}
	if err == nil {
		w.durable.Store(w.written)
	}
	b.err = err
	close(b.done)
}

// rotate closes the segment and starts the next.
func (w *wal) rotate() rotated {
	if w.failed != nil {
		return rotated{err: w.failed}
	}
	w.syncUnsynced()
	var err error
	if !w.sync.none {
		err = w.syncSeg()
	}
	if cerr := w.closeSeg(); err == nil {
		err = cerr
	}
	w.seg = nil
	if err == nil {
		err = w.newSegment()
	}
	if err != nil {
		w.finish(newBatch(), err) // fails the log
		return rotated{err: w.failed}
	}
	old, err := w.segmentsBefore(w.written + 1)
	return rotated{seq: w.written, old: old, err: err}
}

func (w *wal) newSegment() error {
	f, err := os.OpenFile(filepath.Join(w.dir, segName(w.written+1)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := syncDir(w.dir); err != nil {
		f.Close()
		return err
	}
	w.seg, w.segBytes, w.zeroed = f, 0, 0
	w.segments.Add(1)
	return nil
}

// segmentsBefore lists the segments starting before seq.
func (w *wal) segmentsBefore(seq uint64) ([]string, error) {
	segs, _, err := listLog(w.dir)
	if err != nil {
		return nil, err
	}
	var old []string
	for _, s := range segs {
		if s.seq < seq {
			old = append(old, s.name)
		}
	}
	return old, nil
}

// cut has run flush what is pending and start a new segment, and tells
// where the log was cut.
func (w *wal) cut() rotated {
	reply := make(chan rotated, 1)
	select {
	case w.rotates <- reply:
		return <-reply
	case <-w.done:
		return rotated{err: ErrClosed}
	}
}

func (w *wal) lastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

func (w *wal) close() {
	w.mu.Lock()
	if w.err == nil {
		w.err = ErrClosed
	}
	w.mu.Unlock()
	select {
	case <-w.closing:
	default:
		close(w.closing)
	}
	<-w.done
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
		if m <= 0 || (op != opPut && op != opDel) || klen > uint64(len(body)-1-m) {
			return off, errTorn
		}
		key := body[1+m : 1+m+int(klen)]
		if err := f(seq, op, key, body[1+m+int(klen):], int64(headerLen+n)); err != nil {
			return off, err
		}
		off += int64(headerLen + n)
	}
}
