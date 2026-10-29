package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// Tail emits the lines of the file at Path as they are written to it, as
// tail -F does: from its end when it starts, unless FromStart, and from
// its start again if it is cut shorter. It reads in an Async no more
// lines than asked for, and, at the end, looks again every Poll.
type Tail struct {
	Path      string
	Poll      time.Duration
	FromStart bool
}

// tailing is where Tail is in its file.
type tailing struct {
	offset int64 // where the next read starts; -1, at the end, not known yet
	want   int   // lines asked for and not emitted
	busy   bool  // a read is running, or waiting for its Poll
}

type (
	// read is what a read got: lines, complete, and where it stopped.
	read struct {
		lines  []string
		offset int64
	}
	// poll has Tail look again at the end of its file.
	poll struct{}
)

func (t Tail) Init(proc.PID) (tailing, []actor.Effect, error) {
	if t.FromStart {
		return tailing{}, nil, nil
	}
	return tailing{offset: -1}, nil, nil
}

func (t Tail) HandleDemand(s tailing, n int) (tailing, []actor.Effect) {
	s.want += n
	return t.read(s)
}

func (t Tail) HandleInfo(s tailing, msg any) (tailing, []actor.Effect) {
	switch m := msg.(type) {
	case actor.AsyncResult:
		return t.got(s, m)
	case poll:
		s.busy = false
		return t.read(s)
	}
	return s, nil
}

// got emits the lines read, and reads on: at once if it got some, after
// Poll at the end of the file, or if it failed.
func (t Tail) got(s tailing, m actor.AsyncResult) (tailing, []actor.Effect) {
	r, _ := m.Value.(read)
	if m.Err == nil {
		s.offset = r.offset
	}
	s.want -= len(r.lines)
	out := actor.Do(stage.Emit[string]{Events: r.lines})
	if len(r.lines) > 0 && m.Err == nil {
		s.busy = false
		s, more := t.read(s)
		return s, append(out, more...)
	}
	return s, append(out, actor.StartTimer{Key: poll{}, After: t.Poll, Msg: poll{}})
}

// read reads the lines wanted, unless a read is on.
func (t Tail) read(s tailing) (tailing, []actor.Effect) {
	if s.busy || s.want == 0 {
		return s, nil
	}
	s.busy = true
	path, offset, n := t.Path, s.offset, s.want
	return s, actor.Do(actor.Async{Key: "read", Run: func(context.Context) (any, error) {
		return readLines(path, offset, n)
	}})
}

// readLines reads n lines at most of the file at path, from offset, or
// from its end if -1, or from its start if it is shorter: complete lines
// only, a line being written read once it is.
func readLines(path string, offset int64, n int) (read, error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return read{offset: 0}, nil // not there yet
	}
	if err != nil {
		return read{}, err
	}
	defer f.Close()
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return read{}, err
	}
	if offset < 0 {
		return read{offset: size}, nil
	}
	if size < offset {
		offset = 0 // cut shorter: written anew
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return read{}, err
	}
	r := bufio.NewReader(f)
	got := read{offset: offset}
	for len(got.lines) < n {
		line, err := r.ReadString('\n')
		if err != nil {
			break // the end, or a line not complete yet
		}
		got.offset += int64(len(line))
		got.lines = append(got.lines, line[:len(line)-1])
	}
	return got, nil
}
