package kvs

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// wal appends records to the segments of a directory.
//
// With SyncNone, the segment is mapped in memory: a write copies its
// record into the mapping, which is the page cache of the file, so that
// the record outlives the program once copied, without a system call.
//
// Otherwise, a write appends its record to pending, and a sync writes
// pending out, then syncs it with fdatasync: syncing pages written
// through a mapping costs more. The writers waiting for a batch, the
// records since the last sync began, elect one of them to sync it, while
// the next batch piles up: a commit of a group. The goroutine of run syncs
// for the writers not waiting, effects, and every period of SyncEvery.
type wal struct {
	dir     string
	sync    Sync
	segSize int64

	// Of appending.
	mu       sync.Mutex
	seq      uint64 // last given
	seg      *segment
	batch    *batch // of the records since the last sync began
	batchHas bool   // batch has records
	syncing  *batch // the batch a sync is writing out, if any
	pending  []byte // records to write out, at pendAt in seg
	pendAt   int64
	err      error // set, appending fails

	// Of syncing and rotating, taken before mu.
	wmu    sync.Mutex
	failed error
	spare  []byte

	kick    chan struct{}
	closing chan struct{}
	done    chan struct{}

	durable  atomic.Uint64
	syncs    atomic.Uint64
	disk     atomic.Int64 // bytes of records in the segments and snapshot
	segments atomic.Int64
}

// segment is a segment: its records, then zeros up to the size of the
// file, up to its size. Its mapping, if mapped, is not to be touched past
// the size of the file.
type segment struct {
	f      *os.File
	m      []byte // with SyncNone
	size   int64
	off    int64 // the end of the records
	zeroed int64 // the size of f
}

// startWAL starts the log of dir, its last seq being seq, in a new segment.
func startWAL(dir string, seq uint64, disk int64, segments int, o Sync, segSize int64) (*wal, error) {
	w := &wal{
		dir: dir, sync: o, segSize: segSize,
		seq: seq, batch: newBatch(),
		kick:    make(chan struct{}, 1),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
	w.durable.Store(seq)
	w.disk.Store(disk)
	w.segments.Store(int64(segments) + 1)
	seg, err := openSegment(dir, seq+1, segSize, o.none)
	if err != nil {
		return nil, err
	}
	w.seg = seg
	go w.run()
	return w, nil
}

func openSegment(dir string, first uint64, size int64, mapped bool) (*segment, error) {
	f, err := os.OpenFile(filepath.Join(dir, segName(first)), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	var m []byte
	if mapped {
		m, err = mmap(f, size)
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return &segment{f: f, m: m, size: size}, nil
}

// room fills the segment with zeros ahead of its records, for n bytes
// more of them.
func (s *segment) room(n int64) error {
	for s.zeroed < s.off+n {
		c := min(zeroChunk, s.size-s.zeroed)
		if _, err := s.f.WriteAt(zeros[:c], s.zeroed); err != nil {
			return err
		}
		if s.m != nil {
			populate(s.m[s.zeroed : s.zeroed+c])
		}
		s.zeroed += c
	}
	return nil
}

// close cuts the segment to its records, and closes it.
func (s *segment) close() error {
	var err error
	if s.m != nil {
		err = munmap(s.m)
	}
	if terr := s.f.Truncate(s.off); err == nil {
		err = terr
	}
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// append appends r, and returns its seq and the batch it is in, done
// already with SyncNone. A writer then waits for the batch, or kicks run
// to sync it.
func (w *wal) append(r rec) (uint64, *batch, error) {
	n := r.size()
	w.mu.Lock()
	for w.err == nil && w.seg.off+n > w.seg.size {
		w.mu.Unlock()
		w.wmu.Lock()
		w.rotate(n)
		w.wmu.Unlock()
		w.mu.Lock()
	}
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return 0, nil, err
	}
	s := w.seg
	if err := s.room(n); err != nil {
		w.fail(err)
		err = w.err
		w.mu.Unlock()
		return 0, nil, err
	}
	w.seq++
	seq := w.seq
	if s.m != nil {
		appendRecord(s.m[s.off:s.off], r, seq)
	} else {
		if len(w.pending) == 0 {
			w.pendAt = s.off
		}
		w.pending = appendRecord(w.pending, r, seq)
	}
	s.off += n
	b := w.batch
	if w.sync.none {
		w.durable.Store(seq)
		b = finished
	} else {
		w.batchHas = true
	}
	w.mu.Unlock()
	w.disk.Add(n)
	return seq, b, nil
}

// kickRun has run sync what is not yet.
func (w *wal) kickRun() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// wait returns once b is durable, syncing it if no one else does.
func (w *wal) wait(b *batch) error {
	select {
	case <-b.done:
		return b.err
	default:
	}
	if w.sync.every == 0 && b.lead.CompareAndSwap(false, true) {
		w.wmu.Lock()
		select {
		case <-b.done: // synced by another
		default:
			w.syncNow()
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
	switch {
	case w.err != nil:
		return nil, w.err
	case w.batchHas:
		return w.batch, nil
	case w.syncing != nil:
		return w.syncing, nil
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
		case <-tick:
		case <-w.closing:
			return
		}
		w.wmu.Lock()
		w.syncNow()
		w.wmu.Unlock()
	}
}

// syncNow syncs the batch, under wmu.
func (w *wal) syncNow() {
	w.mu.Lock()
	if w.err != nil || !w.batchHas {
		w.mu.Unlock()
		return
	}
	b, last, s := w.batch, w.seq, w.seg
	buf, at := w.pending, w.pendAt
	w.batch, w.batchHas, w.syncing = newBatch(), false, b
	w.pending = w.spare[:0]
	w.mu.Unlock()

	start := time.Now()
	_, err := s.f.WriteAt(buf, at)
	w.spare = buf
	if err == nil {
		w.syncs.Add(1)
		err = datasync(s.f)
	}
	took := time.Since(start)
	w.mu.Lock()
	if err != nil {
		w.fail(err)
		err = w.err
	}
	w.syncing = nil
	w.mu.Unlock()
	w.complete(b, last, err)
	if w.sync.every == 0 {
		w.gather(took)
	}
}

// gather lets the writers just told their batch is durable write again
// before the next batch is synced, for a while bounded by a part of the
// sync before, took: without it, the writers settle in two groups, each
// writing while the other's batch is synced, and a batch has half of them.
func (w *wal) gather(took time.Duration) {
	deadline := time.Now().Add(min(took/8, 200*time.Microsecond))
	prev, still := w.lastSeq(), 0
	for still < 2 && time.Now().Before(deadline) {
		runtime.Gosched()
		n := w.lastSeq()
		if n == prev {
			still++
		} else {
			prev, still = n, 0
		}
	}
}

// complete ends b, its records up to last, with err.
func (w *wal) complete(b *batch, last uint64, err error) {
	if err == nil {
		w.durable.Store(last)
	}
	b.err = err
	close(b.done)
}

// fail makes the log fail every write from now on, under mu, after err.
// The batch piling up fails with it.
func (w *wal) fail(err error) {
	if w.err != nil {
		return
	}
	w.failed = fmt.Errorf("kvs: log failed: %w", err)
	w.err = w.failed
	b := w.batch
	w.batch, w.batchHas = newBatch(), false
	w.complete(b, 0, w.err)
}

// rotate goes on in a new segment, under wmu, if the records of n bytes
// do not fit in the current; with n < 0, in any case. It returns the last
// seq of the segments before.
func (w *wal) rotate(n int64) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	if n >= 0 && w.seg.off+n <= w.seg.size || n < 0 && w.seg.off == 0 {
		return w.seq, nil // rotated by another, or nothing to cut
	}
	had := w.seg.off > 0
	err := w.writePending()
	if err == nil && !w.sync.none {
		w.syncs.Add(1)
		err = datasync(w.seg.f)
	}
	if w.batchHas {
		b := w.batch
		w.batch, w.batchHas = newBatch(), false
		w.complete(b, w.seq, err)
	}
	if cerr := w.seg.close(); err == nil {
		err = cerr
	}
	var seg *segment
	if err == nil {
		seg, err = openSegment(w.dir, w.seq+1, max(w.segSize, n), w.sync.none)
	}
	if err != nil {
		w.seg = &segment{} // nothing fits: appends rotate, and fail
		w.fail(err)
		return 0, w.err
	}
	w.seg = seg
	if had {
		w.segments.Add(1)
	} // else the new one has its name, and replaces it
	return w.seq, nil
}

// writePending writes out what is pending, under wmu and mu.
func (w *wal) writePending() error {
	if len(w.pending) == 0 {
		return nil
	}
	_, err := w.seg.f.WriteAt(w.pending, w.pendAt)
	w.pending = w.pending[:0]
	return err
}

// cut goes on in a new segment, and tells where the log was cut.
func (w *wal) cut() rotated {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	seq, err := w.rotate(-1)
	if err != nil {
		return rotated{err: err}
	}
	segs, _, err := listLog(w.dir)
	if err != nil {
		return rotated{err: err}
	}
	var old []string
	for _, s := range segs {
		if s.seq <= seq {
			old = append(old, s.name)
		}
	}
	return rotated{seq: seq, old: old}
}

func (w *wal) lastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// failure returns the error the log failed with, if any.
func (w *wal) failure() error {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failed
}

// close syncs the log and closes it.
func (w *wal) close() {
	close(w.closing)
	<-w.done
	w.wmu.Lock()
	defer w.wmu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seg.f == nil {
		return // failed rotating, closed already
	}
	var err error
	if w.err == nil {
		err = w.writePending()
		if err == nil {
			w.syncs.Add(1)
			err = datasync(w.seg.f)
		}
		w.err = ErrClosed
	}
	if w.batchHas {
		w.complete(w.batch, w.seq, err)
		w.batchHas = false
	}
	if cerr := w.seg.close(); err == nil {
		err = cerr
	}
	if err != nil && w.failed == nil {
		w.failed = fmt.Errorf("kvs: log failed: %w", err)
	}
	w.seg = &segment{}
}
