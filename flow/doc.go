// Package flow runs work on the events of stages as a plan of operations,
// as Elixir's Flow does on GenStage: map, filter and the like are fused in
// one function, run by workers on the cores, each event going through all
// of them at once, with no stage, nor slice, between two operations.
//
//	lines := flow.From[string](tail)                  // Flow[string, string]
//	recs := flow.Map(lines, parseLine)                // Flow[string, record]
//	good := flow.Filter(recs, record.valid)           // Flow[string, record]
//	for r, err := range flow.Run(ctx, node, good, flow.Workers(4)) {
//
// A Flow[S, T] takes events of type S, of its sources, and gives events of
// type T: Map and the others return another, the operation fused at its
// end. Go methods having no type parameters of their own, they are
// functions, around the flow they change.
//
// # Layers
//
// [Partition] ends a layer of workers, and starts another, of as many
// workers as it says, each event going to the worker of its key: the
// events of a key all to one, for a Reduce to have them together. A flow
// runs in as many layers as it has partitions, and one.
//
//	byPath := flow.Partition(recs, 4, func(r record) string { return r.Path })
//
// # Reducing
//
// [Reduce] folds the events of each worker of a layer into an accumulator
// of its own, and lets it out once the sources are done; [ReduceEvery],
// at the end of each window of time as well, and starts afresh, for a
// stream with no end; [ReduceByKey], by key, one [KV] for each key at the
// end. After a Partition of the same key, a worker has all the events of
// its keys:
//
//	counts := flow.ReduceByKey(byPath, record.path, zero, count)
//
// The operations after a Reduce are on what it lets out; a layer reduces
// once.
//
// # Running
//
// Run starts its workers, producer-consumers on the node given, those of
// the first layer subscribing to every source, which gives each event to
// one of them, the one wanting most, those of each other to every worker
// of the layer before, for their partition; and ranges over what the last
// give. The workers subscribe held, and ask once all have subscribed: no
// worker is late to a source, or to another worker, done before it. The sources done,
// the workers handle what they hold, and stop; the range ends. Breaking
// off the range stops the workers, and an Ephemeral source with them.
//
// The events come in no order but that of each worker: of several, those
// of a source come mixed.
package flow
