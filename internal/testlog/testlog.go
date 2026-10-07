// Package testlog records slog output for tests to check.
package testlog

import (
	"context"
	"log/slog"
	"sync"
)

// Record is a logged record, its attributes flattened to strings.
type Record struct {
	Level   slog.Level
	Message string
	Attrs   map[string]string
}

// Recorder is a slog.Handler keeping what is logged.
type Recorder struct {
	mu      sync.Mutex
	records []Record
}

// New returns a recorder and a logger writing to it.
func New() (*Recorder, *slog.Logger) {
	r := &Recorder{}
	return r, slog.New(r)
}

func (r *Recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *Recorder) Handle(_ context.Context, rec slog.Record) error {
	attrs := make(map[string]string)
	rec.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, Record{Level: rec.Level, Message: rec.Message, Attrs: attrs})
	return nil
}

func (r *Recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *Recorder) WithGroup(string) slog.Handler      { return r }

// Records returns the records with message msg, all of them if msg is "".
func (r *Recorder) Records(msg string) []Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Record
	for _, rec := range r.records {
		if msg == "" || rec.Message == msg {
			out = append(out, rec)
		}
	}
	return out
}
