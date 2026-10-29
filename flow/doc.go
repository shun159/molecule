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
// # Running
//
// Run starts its workers, producer-consumers on the node given, each
// subscribing to every source, which gives each event to one of them, the
// one wanting most; and ranges over what they all give. The sources done,
// the workers handle what they hold, and stop; the range ends. Breaking
// off the range stops the workers, and an Ephemeral source with them.
//
// The events come in no order but that of each worker: of several, those
// of a source come mixed.
package flow
