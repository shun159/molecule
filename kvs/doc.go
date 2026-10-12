// Package kvs is a table of keys and values kept on disk: the entries are
// in memory, for reads, and every write is appended to a log, read back
// into memory when the table opens again. It is for state a program keeps
// across restarts, small enough to hold in memory, such as the leases of
// a DHCP server.
//
//	leases, err := kvs.Open[string, Lease](dir, kvs.Options[string, Lease]{})
//	err = leases.Put(mac, lease)
//	lease, ok := leases.Get(mac)
//
// # Writes
//
// Put and Delete return once the write is durable; readers see it at once,
// before. Sync says what durable is: on disk, each write synced with the
// others of its batch, SyncEach, the default; on disk at the next sync of
// a period, SyncEvery; in the page cache, SyncNone, where a write copies
// its record into a mapping of the log and Put returns at once: the write
// outlives the program, not the machine.
//
// A behaviour writes with [Table.PutEffect] and [Table.DeleteEffect]: the
// runtime of the behaviour performs the write, see gen.Performer, and a
// [Durable] with the tag of the effect arrives when the write is durable.
// A server answers once what it answered is safe:
//
//	case kvs.Durable:
//		return s, molecule.Do(molecule.Reply{To: m.Tag.(molecule.From), Value: ack})
//
// A conditional write writes only if the entry is as it requires.
// [Table.PutIf] and [Table.DeleteIf] compare the revision of the entry,
// the seq of the write that set it, which [Table.GetRev] returns, as etcd
// compares the mod revision of a key; [CompareAndSwap] and
// [CompareAndDelete] compare its value, if comparable.
//
// Once a write to the log fails, the table fails every write after: the
// entries in memory may hold writes the log has not. The program closes
// it and opens it again, as a supervisor would restart a process.
//
// # Concurrency
//
// The methods of a table may be called from any goroutine, by any number
// of processes. Its Options choose how it bears them, like the
// read_concurrency and write_concurrency of an ETS table:
//
//	(neither)         a map under a RWMutex
//	ReadConcurrency   a persistent trie, read without lock; a write
//	                  replaces its root, under a Mutex
//	WriteConcurrency  Shards maps, each under a RWMutex of its own
//	both              Shards tries, each written under a Mutex of its own
//
// A range over the table, All or Select, sees each shard as it was at a
// moment, and may write to the table; without ReadConcurrency, it copies
// each shard to do so.
//
// # Order
//
// A table is a [Set], by default, or an [OrderedSet], as an ETS table is
// a set or an ordered_set. An OrderedSet holds its entries in the order
// of their keys, in a B-tree, copied on write, in place of the map or the
// trie: All goes in that order, and [Table.Range] goes over the keys of an
// interval. A range copies nothing, in any mode. The order is that of K,
// see Options.Compare, or Compare.
//
//	leases, err := kvs.Open(dir, kvs.Options[netip.Addr, Lease]{Type: kvs.OrderedSet})
//	for ip, lease := range leases.Range(lo, hi) {
//
// It costs more than a map: a get searches the tree, and a write with
// ReadConcurrency copies the nodes of its path. With WriteConcurrency, a
// range merges the shards, and searches each.
//
// # The log
//
// The log is the files of a directory, which the table holds alone while
// open: segments of records, and a snapshot of the entries. A record has
// a checksum; one cut short at the end of the last segment, by a crash,
// is dropped on opening.
//
// A compaction writes a snapshot of the entries, and removes the segments
// it holds. It runs by itself once the log is large enough and mostly
// writes since overwritten, see Options, or on Compact. Writes go on
// during it.
//
// kvs runs on Linux.
//
// Keys and values are encoded for the log by a [Codec]: by default, plainly
// for strings, byte slices and integers, by MarshalBinary for types with
// it, and by gob for others.
package kvs
