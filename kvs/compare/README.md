# kvs compared

kvs against stores of its kind, on one machine: an index in memory over
a log on disk, as Bitcask, and bbolt for reference.

| store | what | durable put |
|---|---|---|
| kvs | Go; shards of maps, each under a RWMutex, over a log (WriteConcurrency) | pwrite + fdatasync, segments zero-filled ahead |
| kvs-rc | kvs with ReadConcurrency: a trie read without lock, written under a Mutex | as kvs |
| [rosedb](https://github.com/rosedblabs/rosedb) v2.3.3 | Go; Bitcask, a B-tree index over a WAL; a get reads the file | write + fsync |
| [bitcask-go](https://git.mills.io/prologic/bitcask) v2.1.5 | Go; Bitcask, a radix tree over data files; a get reads the file | write + fsync |
| [bbolt](https://github.com/etcd-io/bbolt) v1.5.0 | Go; a B+tree in a mapped file, nothing to rebuild on open | a transaction: 2 fdatasyncs |
| ETS | Erlang; in memory only, the baseline of a read | - |
| DETS | Erlang; a hash table in a file | `dets:sync` |
| [bitcask](https://github.com/basho/bitcask) | Erlang, the original, its NIFs doing the I/O; optional, see below | `bitcask:sync`: 2 fsyncs |

Every store holds 100k keys of 8 bytes with values of 64 bytes, and the
benchmarks pick keys at random among them (doc.go lists them).

## Running

Syncs cost nothing on tmpfs: give a directory on the disk.

    cd kvs/compare
    export KVS_BENCH_DIR=$HOME/tmp
    for i in 1 2 3 4 5; do
        go test -run '^$' -bench . -benchmem >> go.txt
        escript erlang/bench.escript >> erl.txt
    done
    go run golang.org/x/perf/cmd/benchstat@latest \
        -ignore goos,goarch,pkg,cpu -col /store go.txt erl.txt

The Erlang Bitcask runs if on the code path: `erlang/build-bitcask.sh`
builds it and prints the `ERL_LIBS` to run the script with. Its puts sync
by `bitcask:sync`, not the sync_strategy o_sync: the NIF opening a file
clears O_SYNC when it reads the option create, which comes after.

## Results

Medians of 5 runs, Go and Erlang alternating, 2026-10-25: a laptop, i7-1165G7
(4 cores, 8 threads), ext4 on NVMe, Linux 7.2, Go 1.27, OTP 29. Time per
operation; with 8 goroutines, the time of all over the operations of all.

| | kvs | kvs-rc | rosedb | bitcask-go | bitcask | bbolt | ETS | ETS rw | DETS |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Get, 1 goroutine | **76 ns** | 118 ns | 4.0 µs | 2.4 µs | 1.7 µs | 1.0 µs | 249 ns | 333 ns | 7.8 µs |
| Get, 8 goroutines | 28 ns | **25 ns** | 1.5 µs | 1.5 µs | 2.1 µs | 1.4 µs | 147 ns | 113 ns | 12.9 µs |
| Put, sync=none | 438 ns | 1.16 µs | 2.6 µs | 11.3 µs | 2.5 µs | 15.9 µs | 267 ns | 307 ns | 24.3 µs |
| Put, sync=each | **1.24 ms** | 1.33 ms | 3.65 ms | 1.86 ms | 3.57 ms | 3.50 ms | - | - | 3.99 ms |
| PutDurable, 8 goroutines | **170 µs** | **170 µs** | 3.82 ms | 1.88 ms | 4.11 ms | 3.51 ms | - | - | 585 µs |
| Open, 100k puts | 27 ms | 34 ms | 261 ms | 591 ms | 56 ms | 29 µs | - | - | 135 µs |

Allocations of the Go stores:

| allocs/op | kvs | kvs-rc | rosedb | bitcask-go | bbolt |
|---|--:|--:|--:|--:|--:|
| Get | 0 | 0 | 6 | 13 | 9 |
| Put, sync=none | 0 | 4 | 14 | 74 | 61 |
| Put, sync=each | 2 | 5 | 14 | 70 | 52 |
| Open | 201k | 270k | 1.41M | 3.28M | 63 |

## Reading them

- A get of kvs or ETS reads memory alone; one of the Bitcasks reads the
  value from the file, a syscall. kvs returns the value it holds, ETS
  copies it to the heap of the process.
- A put of kvs with sync=none copies its record into a mapping of the
  log, no syscall; the others write(2) it.
- With sync=each, kvs and bbolt fdatasync, the Bitcasks fsync. kvs
  writes over segments zero-filled ahead, so that a sync does not change
  the size of the file.
- In PutDurable, kvs syncs the puts waiting together at once. DETS does
  as much by chance: a `dets:sync` finds the inserts queued meanwhile.
  The Erlang bitcask puts in the process that opened it, for the others.
- ETS puts faster than kvs with sync=none: it has no log to write.
- Reading without lock gains nothing here: no put runs meanwhile, and the
  RWMutexes of kvs are many, one a shard, so the readers seldom share
  one. A get of kvs-rc walks a trie, more misses than a map; a put
  copies the nodes of its path.
- bbolt and DETS keep their entries in a file they read on demand, with
  nothing to rebuild on opening; the others read their log, or the Erlang
  bitcask its hint files.
- The loop of the Erlang script costs about 15 ns a get and 30 ns a put,
  against 2 ns in Go.

## Checking the syncs

The syncs of a durable put, counted with `strace -f -c`, against what a
sync costs on the disk alone, by `dd bs=128 oflag=dsync`: 0.93 ms over a
file allocated ahead, 1.81 ms at its end, where the size changes.

| store | syncs a put | a put |
|---|--:|--:|
| kvs | 1 fdatasync, file allocated ahead | 1.24 ms |
| bitcask-go | 1 fsync at the end of the file | 1.86 ms |
| bbolt | 2 fdatasyncs | 3.50 ms |
| rosedb | 2 fsyncs at the end of the file | 3.65 ms |
| kvs, 8 writers | 261 fdatasyncs for 2000 puts | 170 µs |
