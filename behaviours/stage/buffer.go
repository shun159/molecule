package stage

import (
	"errors"
	"log/slog"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Buffer bounds the events a producer, or a producer-consumer, holds
// beyond the demand of its consumers: those it emits beyond, or while it
// has none. A stage implementing Buffering has another than the default,
// of DefaultBuffer events dropping the oldest.
type Buffer struct {
	// Size is how many events it holds at most: DefaultBuffer if zero, no
	// bound if negative.
	Size int
	// Overflow is what becomes of the events beyond Size.
	Overflow Overflow
}

// Buffering is a producer or a producer-consumer telling its Buffer.
type Buffering interface {
	Buffer() Buffer
}

// Overflow is what becomes of the events beyond a Buffer.
type Overflow int

const (
	// DropOldest drops the events held longest, as GenStage's
	// buffer_keep: :last.
	DropOldest Overflow = iota
	// DropNewest drops the events emitted last, as buffer_keep: :first.
	DropNewest
	// StopOnFull stops the stage with ErrBufferFull: for events none may
	// be lost of.
	StopOnFull
)

// DefaultBuffer is the Size of a Buffer without one.
const DefaultBuffer = 10000

// ErrBufferFull stops a stage holding more events than its Buffer, with
// StopOnFull.
var ErrBufferFull = errors.New("stage: the buffer is full")

// Dropped is the event, see proc.WithEvents, of a stage dropping Events
// for its Buffer is full, which it logs as well.
type Dropped struct {
	Stage  proc.PID
	Events int
}

// bufferOf returns the Buffer of the stage b, its size made a bound: -1
// for none.
func bufferOf(b any) Buffer {
	var buf Buffer
	if x, ok := b.(Buffering); ok {
		buf = x.Buffer()
	}
	switch {
	case buf.Size == 0:
		buf.Size = DefaultBuffer
	case buf.Size < 0:
		buf.Size = -1
	}
	return buf
}

// bound holds the events of e to its buffer, dropping those beyond or
// stopping the stage, as the Buffer says.
func (e *emitting) bound(out []actor.Effect) []actor.Effect {
	if e.buffer.Size < 0 || e.to.held() <= e.buffer.Size {
		return out
	}
	if e.buffer.Overflow == StopOnFull {
		return append(out, actor.Stop{Reason: ErrBufferFull})
	}
	n := e.change().trim(e.buffer.Size, e.buffer.Overflow == DropNewest)
	e.dropped += uint64(n)
	return append(out,
		gen.Log{Level: slog.LevelWarn, Msg: "stage: buffer full, events dropped", Attrs: []any{"stage", e.self, "dropped", n}},
		gen.Emit{Event: Dropped{Stage: e.self, Events: n}},
	)
}
