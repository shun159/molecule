package kvs

import (
	"encoding/binary"
	"errors"
	"iter"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Options configure a table.
type Options[K comparable, V any] struct {
	// ReadConcurrency makes reads take no lock: the entries are a
	// persistent trie, whose root each write replaces. A write costs
	// more, allocating the path to what it changes; a range over the
	// table copies nothing.
	ReadConcurrency bool
	// WriteConcurrency splits the table in Shards, each with a lock of
	// its own, so that writes to keys of different shards go at once.
	// They still append to one log, under a lock held briefly.
	WriteConcurrency bool
	// Shards is the number of shards with WriteConcurrency, rounded up
	// to a power of two: 64 if zero.
	Shards int

	// Type is Set, the default, or OrderedSet.
	Type Type
	// Compare orders the keys of an OrderedSet, as cmp.Compare does,
	// reporting 0 for equal keys only. If nil, the order of K: by its
	// method Compare(K) int, if any, as for netip.Addr and time.Time, or
	// that of its strings, numbers, or arrays of bytes.
	Compare func(a, b K) int

	// Expires tells when the entry of a value expires, if ever, with the
	// zero time: if set, Expire deletes the entries expired.
	Expires func(V) time.Time

	// Sync says when a write is durable: SyncEach if zero.
	Sync Sync
	// SegmentSize is the size past which the log goes on in a new
	// segment: 64 MiB if zero.
	SegmentSize int64
	// The log is compacted by itself when it is CompactRatio times as
	// large as the records of the entries, 2 if zero, and at least
	// CompactMin bytes, 16 MiB if zero. A negative CompactRatio leaves
	// compaction to Compact.
	CompactRatio float64
	CompactMin   int64

	// KeyCodec and ValueCodec encode the keys and the values for the
	// log: DefaultCodec if nil.
	KeyCodec   Codec[K]
	ValueCodec Codec[V]
}

// Sync says when the writes to a table are durable: on disk, or, with
// SyncNone, in the hands of the system.
type Sync struct {
	none  bool
	every time.Duration
}

var (
	// SyncEach syncs the log after each batch of writes, the writes made
	// while the previous batch was written and synced: a write is
	// durable once on disk, at the cost of a sync shared with its batch.
	SyncEach = Sync{}
	// SyncNone never syncs but on Close: a write is durable once copied
	// into the page cache, through a mapping of the log, without a
	// system call. It outlives the program, not the machine.
	SyncNone = Sync{none: true}
)

// SyncEvery syncs the log every d: a write is durable at the sync after
// it.
func SyncEvery(d time.Duration) Sync { return Sync{every: d} }

// Table is a table of entries, each a value under a key, in memory and in
// a log on disk. Its methods may be called from any goroutine.
//
// Values are not copied: a value put is shared with those who get it, and
// is not to be changed after.
type Table[K comparable, V any] struct {
	dir  string
	opts Options[K, V]
	kc   Codec[K]
	vc   Codec[V]
	idx  index[K, V]
	log  *wal
	live atomic.Int64
	lock *os.File
	bufs sync.Pool

	closed      atomic.Bool
	compactMu   sync.Mutex
	compacting  atomic.Bool
	compactions atomic.Uint64
	lastCompact atomic.Int64 // unix nanoseconds
	compactErr  atomic.Pointer[error]

	exp *expiries[K, V] // with Expires
}

// Get returns the value under k.
func (t *Table[K, V]) Get(k K) (V, bool) {
	v, _, ok := t.idx.get(k)
	return v, ok
}

// Len returns the number of entries.
func (t *Table[K, V]) Len() int { return t.idx.len() }

// Put puts v under k, and returns once the write is durable. Readers see
// it at once, before.
func (t *Table[K, V]) Put(k K, v V) error {
	b, err := t.put(k, v, nil)
	if err != nil {
		return err
	}
	return t.log.wait(b)
}

// Delete deletes the entry under k, if any, and returns once the delete
// is durable.
func (t *Table[K, V]) Delete(k K) error {
	b, err := t.delete(k, nil)
	if err != nil {
		return err
	}
	return t.log.wait(b)
}

func (t *Table[K, V]) put(k K, v V, match matcher[V]) (*batch, error) {
	bp := t.buf()
	defer t.putBuf(bp)
	body, err := t.encode(bp, opPutClaim, k, v)
	if err != nil {
		return nil, err
	}
	b, err := t.idx.put(t.log, k, v, newRec(body), match)
	if err == nil {
		t.noteExpiry(k, v)
		t.maybeCompact()
	}
	return b, err
}

func (t *Table[K, V]) delete(k K, match matcher[V]) (*batch, error) {
	bp := t.buf()
	defer t.putBuf(bp)
	var zero V
	body, err := t.encode(bp, opDelClaim, k, zero)
	if err != nil {
		return nil, err
	}
	b, err := t.idx.del(t.log, k, newRec(body), match)
	if err == nil {
		t.maybeCompact()
	}
	return b, err
}

// keptBuffer is the most a buffer kept for the next write holds: one a
// large record grew is let go.
const keptBuffer = 1 << 20

// putBuf keeps bp for the next encoding, unless a large record grew it.
func (t *Table[K, V]) putBuf(bp *[]byte) {
	if cap(*bp) <= keptBuffer {
		t.bufs.Put(bp)
	}
}

func (t *Table[K, V]) buf() *[]byte {
	if bp, ok := t.bufs.Get().(*[]byte); ok {
		return bp
	}
	b := make([]byte, 0, 256)
	return &b
}

// encode returns the body of the record of op on k and v, made in *bp.
// The key goes after room for its length, written before it once known.
func (t *Table[K, V]) encode(bp *[]byte, op byte, k K, v V) ([]byte, error) {
	const pre = 1 + binary.MaxVarintLen64
	b, err := t.kc.Append(append((*bp)[:0], make([]byte, pre)...), k)
	if err != nil {
		return nil, err
	}
	var l [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(l[:], uint64(len(b)-pre))
	start := pre - n - 1
	b[start] = op
	copy(b[start+1:pre], l[:n])
	if base, _ := unclaimed(op); base == opPut {
		if b, err = t.vc.Append(b, v); err != nil {
			return nil, err
		}
	}
	*bp = b
	return b[start:], nil
}

// All returns the entries, in the order of their keys in an OrderedSet.
// Each shard is ranged over as it was at a moment: in a Set without
// ReadConcurrency, as a copy of it. The table may be written during the
// range.
func (t *Table[K, V]) All() iter.Seq2[K, V] {
	return t.Select(nil)
}

// Select returns the entries for which keep, if not nil, reports true,
// as All does.
func (t *Table[K, V]) Select(keep func(K, V) bool) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		// kept yields l, unless left out, and reports whether to go on.
		kept := func(l *leaf[K, V]) bool {
			if keep != nil && !keep(l.Key, l.Val) {
				return true
			}
			return yield(l.Key, l.Val)
		}
		if x, ok := t.idx.(*treeIndex[K, V]); ok {
			x.ascend(nil, nil, kept) // in order, merging the shards
			return
		}
		for shard := range t.idx.byShard(false) {
			for l := range shard {
				if !kept(l) {
					return
				}
			}
		}
	}
}

// Range returns the entries of the keys from lo, up to hi but not hi, in
// order, as All does. It panics if the table is not an OrderedSet.
//
// With WriteConcurrency, it merges the entries of the shards: a range of
// few entries costs a search in each shard.
func (t *Table[K, V]) Range(lo, hi K) iter.Seq2[K, V] {
	x, ok := t.idx.(*treeIndex[K, V])
	if !ok {
		panic("kvs: Range of a Set")
	}
	return func(yield func(K, V) bool) {
		x.ascend(&lo, &hi, func(l *leaf[K, V]) bool { return yield(l.Key, l.Val) })
	}
}

// Info is what Table.Info tells of a table.
type Info struct {
	Dir              string
	ReadConcurrency  bool
	WriteConcurrency bool
	Shards           int
	Type             Type

	// Size is the number of entries; LiveBytes, the size of the records
	// that wrote them, in the log.
	Size      int
	LiveBytes int64
	// DiskBytes is the size of the log, Segments its number of segments.
	DiskBytes int64
	Segments  int

	// LastSeq is the seq of the last write, DurableSeq of the last
	// durable: the writes between are not yet.
	LastSeq    uint64
	DurableSeq uint64
	// Syncs is the number of syncs of the log since Open.
	Syncs uint64

	// Compactions is the number of compactions since Open, the last at
	// LastCompaction; CompactionErr is the error of the last that ran by
	// itself and failed, if any.
	Compactions    uint64
	LastCompaction time.Time
	CompactionErr  error
}

// Info returns the size of the table and the state of its log.
func (t *Table[K, V]) Info() Info {
	shards := 1
	if t.opts.WriteConcurrency {
		shards = 1
		for shards < t.opts.Shards {
			shards <<= 1
		}
	}
	i := Info{
		Dir:              t.dir,
		ReadConcurrency:  t.opts.ReadConcurrency,
		WriteConcurrency: t.opts.WriteConcurrency,
		Shards:           shards,
		Type:             t.opts.Type,
		Size:             t.idx.len(),
		LiveBytes:        t.live.Load(),
		DiskBytes:        t.log.disk.Load(),
		Segments:         int(t.log.segments.Load()),
		LastSeq:          t.log.lastSeq(),
		DurableSeq:       t.log.durable.Load(),
		Syncs:            t.log.syncs.Load(),
		Compactions:      t.compactions.Load(),
	}
	if ns := t.lastCompact.Load(); ns != 0 {
		i.LastCompaction = time.Unix(0, ns)
	}
	if err := t.compactErr.Load(); err != nil {
		i.CompactionErr = *err
	}
	return i
}

// Close makes the writes durable, closes the log, and lets go of the
// directory. Writes after fail with ErrClosed; reads go on, of the
// entries as they are. Closing again does nothing.
func (t *Table[K, V]) Close() error {
	if t.closed.Swap(true) {
		return nil
	}
	t.compactMu.Lock()
	defer t.compactMu.Unlock()
	t.log.close()
	return errors.Join(t.log.failure(), unlockDir(t.lock))
}
