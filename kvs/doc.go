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
//		return s, actor.Do(actor.Reply{To: m.Tag.(actor.From), Value: ack})
//
// A conditional write writes only if the entry is as it requires.
// [Table.PutIf] and [Table.DeleteIf] compare the revision of the entry,
// the seq of the write that set it, which [Table.GetRev] returns, as etcd
// compares the mod revision of a key; [CompareAndSwap] and
// [CompareAndDelete] compare its value, if comparable.
//
// Once a write to the log fails, the table fails every write after: the
// entries in memory may hold writes the log has not. The program closes
// it and opens it again, as a supervisor would restart a process. A write
// finding the disk full fails alone, with ENOSPC, before anything of it
// is in the log or in memory: the writes after go on once there is space.
//
// # Supervision
//
// [Child] makes a table a child of a supervision tree, which opens it as
// it starts and closes it as it stops. The processes after it find it by
// its directory, with [Opened]:
//
//	Strategy: supervisor.OneForAll,
//	Children: []supervisor.ChildSpec{
//		{ID: "table", Start: kvs.Child(dir, kvs.Options[string, Lease]{})},
//		{ID: "server", Start: genserver.Child(Server{Dir: dir})},
//	},
//	...
//	leases, err := kvs.Opened[string, Lease](s.Dir)
//
// Under OneForAll, the table and its users restart together: a user that
// stops on a write failed has the table opened again, and a table opened
// again has its users find it anew.
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
// A [Snapshot] is the table at one moment, all its shards together, to
// read as long as needed, the table written meanwhile: of an OrderedSet,
// or with ReadConcurrency, it costs little to take, whatever the size of
// the table, keeping the trees, or the tries, of the moment; of a Set
// without ReadConcurrency, it copies the entries.
//
//	snap := leases.Snapshot()
//	for ip, lease := range snap.Range(lo, hi) {
//
// A [Ranger] serves a range of a snapshot as a stage, to a client of this
// node or another, as it asks for it: no more of a range of any size in
// memory than it asked for.
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
// # Expiry
//
// A table with Expires in its Options knows when the entry of each value
// expires, as a lease does, and [Table.Expire] deletes the entries expired
// at a time, as conditional deletes: an entry written again meanwhile
// stays. The table deletes nothing by itself, and reads see an entry
// expired until then. A behaviour expires them with [Table.ExpireEffect]
// at the time of its clock, on a timer set by [Table.NextExpiry]:
//
//	Expires: func(l Lease) time.Time { return l.Expiry },
//	...
//	case tick:
//		return s, actor.Do(s.leases.ExpireEffect(s.now(), nil))
//
// # Costs
//
// For a table of N entries, in S shards with WriteConcurrency, 1 without,
// a range of K entries, and E entries that expire:
//
//	                  Get, Put, Delete  Len   All, Select  Range
//	Set               O(1)              O(S)  O(N)         -
//	  ReadConcurrency O(log32(N/S))     O(S)  O(N)         -
//	OrderedSet        O(log(N/S))       O(S)  O(N log S)   O(S log(N/S) + K log S)
//
// With one shard, an OrderedSet goes over All in O(N), and a Range in
// O(log N + K). A get takes the read lock of its shard, or no lock with
// ReadConcurrency. A write appends its record to the log too, which costs
// more than any of these, and with SyncEach waits for a sync.
//
// All and Select copy each shard of a Set without ReadConcurrency, O(N/S)
// at a time. Elsewhere they copy nothing; but in an OrderedSet without
// ReadConcurrency, the writes after copy the nodes they change, each node
// once, as every write with ReadConcurrency does.
//
// A write of an entry that expires adds O(log E) to note when, amortized:
// the times out of date are dropped by looking at those E entries alone,
// under one lock for the table, once they double. Expire costs O(log E)
// for each entry it finds expired, with the delete, or written again since.
//
// Opening reads the snapshot and the segments after it, in time of their
// records.
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
