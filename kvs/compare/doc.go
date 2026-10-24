// Package compare measures kvs against stores of its kind, on the same
// machine and the same workloads: an index in memory over a log on disk,
// as Bitcask, and bbolt for reference, a B+tree on disk.
//
// The workloads are those of kvs/bench_test.go: 100k keys of 8 bytes,
// values of 64 bytes, random keys among them.
//
//	Get/store=S/mode=serial    a get, from one goroutine
//	Get/store=S/mode=parallel  a get, from GOMAXPROCS goroutines
//	Put/store=S/sync=none      a put to the page cache, from one goroutine
//	Put/store=S/sync=each      a put on disk, from one goroutine
//	PutDurable/store=S         a put on disk, from GOMAXPROCS goroutines
//	Open/store=S               opening a log of 100k puts again
//
// erlang/bench.escript prints the same benchmarks for ETS, DETS and, if
// on its code path, Bitcask, so that benchstat compares them all. See
// README.md.
package compare
