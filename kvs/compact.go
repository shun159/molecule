package kvs

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Compact rewrites the log as a snapshot of the entries, and the writes
// made while it is written: the segments before go. Writes go on during
// it.
func (t *Table[K, V]) Compact() error {
	t.compactMu.Lock()
	defer t.compactMu.Unlock()
	if t.closed.Load() {
		return ErrClosed
	}
	return t.compact()
}

// maybeCompact starts a compaction if the log has grown large enough.
func (t *Table[K, V]) maybeCompact() {
	if t.opts.CompactRatio < 0 {
		return
	}
	disk := t.log.disk.Load()
	if disk < t.opts.CompactMin || float64(disk) < t.opts.CompactRatio*float64(t.live.Load()) {
		return
	}
	if !t.compacting.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer t.compacting.Store(false)
		t.compactMu.Lock()
		defer t.compactMu.Unlock()
		if t.closed.Load() {
			return
		}
		if err := t.compact(); err != nil {
			t.compactErr.Store(&err)
		}
	}()
}

// compact snapshots the table as of a cut of the log, and removes the
// segments before the cut and the snapshots before this one.
func (t *Table[K, V]) compact() error {
	cut := t.log.cut()
	if cut.err != nil {
		return cut.err
	}
	name := snapName(cut.seq)
	size, err := t.writeSnapshot(name, cut.seq)
	if err != nil {
		return err
	}
	t.log.disk.Add(size)
	for _, s := range cut.old {
		t.removeFile(s)
		t.log.segments.Add(-1)
	}
	t.removeSnapshotsBut(name)
	t.compactions.Add(1)
	t.lastCompact.Store(time.Now().UnixNano())
	return nil
}

// removeSnapshotsBut removes the snapshots of the table but the one of
// name.
func (t *Table[K, V]) removeSnapshotsBut(name string) {
	_, snaps, err := listLog(t.dir)
	if err != nil {
		return
	}
	for _, s := range snaps {
		if s.name != name {
			t.removeFile(s.name)
		}
	}
}

// removeFile removes the file of name from the directory of the table, and
// its size from the size on disk, if it was there.
func (t *Table[K, V]) removeFile(name string) {
	path := filepath.Join(t.dir, name)
	if fi, err := os.Stat(path); err == nil && remove(path) == nil {
		t.log.disk.Add(-fi.Size())
	}
}

// writeSnapshot writes the entries to the snapshot name, and returns its
// size. Each record has the seq of the write that set its entry, so that
// revisions outlive compactions.
// writeSnapshot writes the entries to the snapshot name, of the log up to
// seq: and of writes after, to the shards walked after them, which the
// snapshot holds too. It is put in place once those are durable in the
// log: else a crash losing them from the log would find them in it, never
// durable, and maybe without others before them, in shards walked before.
func (t *Table[K, V]) writeSnapshot(name string, seq uint64) (int64, error) {
	path := filepath.Join(t.dir, name)
	f, err := createFile(path + ".tmp")
	if err != nil {
		return 0, err
	}
	defer remove(path + ".tmp") // if left
	w := bufio.NewWriterSize(&fileWriter{f: f}, 1<<20)
	size, last, werr := t.writeEntries(w, seq)
	if werr == nil {
		werr = w.Flush()
	}
	if werr == nil {
		werr = syncFile(f, false)
	}
	if err := f.Close(); werr == nil {
		werr = err
	}
	if werr == nil && last > seq {
		werr = t.log.syncWritten()
	}
	if werr == nil {
		werr = rename(path+".tmp", path)
	}
	if werr == nil {
		werr = syncDir(t.dir)
	}
	return size, werr
}

// A fileWriter writes to f through writeAt, from its start.
type fileWriter struct {
	f   *os.File
	off int64
}

func (w *fileWriter) Write(b []byte) (int, error) {
	n, err := writeAt(w.f, b, w.off)
	w.off += int64(n)
	return n, err
}

// writeEntries writes a record of each entry to w, as the shards hold them
// once locked, and returns their size and the latest write among them, or
// since, if later.
func (t *Table[K, V]) writeEntries(w io.Writer, since uint64) (size int64, last uint64, err error) {
	last = since
	var buf, out []byte
	for shard := range t.idx.byShard(true) {
		for l := range shard {
			body, err := t.encode(&buf, opPutClaim, l.Key, l.Val)
			if err != nil {
				return size, last, err
			}
			// A snapshot claims nothing, synced whole before it is in place;
			// its records are as long as those of the log, as LiveBytes counts.
			out = appendRecord(out[:0], newRec(body), l.Meta.rev, 0)
			if _, err := w.Write(out); err != nil {
				return size, last, err
			}
			last = max(last, l.Meta.rev)
			size += int64(len(out))
		}
	}
	return size, last, nil
}
