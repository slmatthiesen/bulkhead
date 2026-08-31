// Package logbuf makes the control plane's own log readable from the console.
//
// It exists because killing a process is invisible. The run stops advancing, and that is all an
// audience sees -- there is no way to tell a crash from a slow step, which is exactly the
// distinction the demo is about. Streaming the log puts the death on screen: the last line before
// the process goes is whatever it was doing, and the first lines after are the restart finding the
// abandoned run and claiming it back.
//
// The buffer dies with the process, and that is correct rather than unfortunate. A log that
// survived the crash would be a log from somewhere else.
package logbuf

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Line is one log record, already formatted for display.
type Line struct {
	ID    int64  `json:"id"`
	At    string `json:"at"`
	Level string `json:"level"`
	Text  string `json:"text"`
}

// Buffer is an slog.Handler that keeps the recent past and notifies subscribers.
type Buffer struct {
	inner slog.Handler

	mu      sync.Mutex
	next    int64
	lines   []Line
	limit   int
	waiters map[chan struct{}]struct{}
}

func New(inner slog.Handler, limit int) *Buffer {
	return &Buffer{inner: inner, limit: limit, waiters: map[chan struct{}]struct{}{}}
}

func (b *Buffer) Enabled(ctx context.Context, l slog.Level) bool { return b.inner.Enabled(ctx, l) }
func (b *Buffer) WithAttrs(a []slog.Attr) slog.Handler           { return &child{b, b.inner.WithAttrs(a)} }
func (b *Buffer) WithGroup(n string) slog.Handler                { return &child{b, b.inner.WithGroup(n)} }

func (b *Buffer) Handle(ctx context.Context, r slog.Record) error {
	b.record(r)
	return b.inner.Handle(ctx, r)
}

// child keeps attributes flowing to stderr while still recording into the one shared buffer, so a
// logger derived with .With() does not quietly stop appearing in the console.
type child struct {
	root  *Buffer
	inner slog.Handler
}

func (c *child) Enabled(ctx context.Context, l slog.Level) bool { return c.inner.Enabled(ctx, l) }
func (c *child) WithAttrs(a []slog.Attr) slog.Handler           { return &child{c.root, c.inner.WithAttrs(a)} }
func (c *child) WithGroup(n string) slog.Handler                { return &child{c.root, c.inner.WithGroup(n)} }
func (c *child) Handle(ctx context.Context, r slog.Record) error {
	c.root.record(r)
	return c.inner.Handle(ctx, r)
}

func (b *Buffer) record(r slog.Record) {
	var attrs strings.Builder
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&attrs, " %s=%v", a.Key, a.Value)
		return true
	})

	b.mu.Lock()
	b.next++
	b.lines = append(b.lines, Line{
		ID:    b.next,
		At:    r.Time.Format("15:04:05.000"),
		Level: r.Level.String(),
		Text:  r.Message + attrs.String(),
	})
	if len(b.lines) > b.limit {
		b.lines = b.lines[len(b.lines)-b.limit:]
	}
	for w := range b.waiters {
		close(w)
		delete(b.waiters, w)
	}
	b.mu.Unlock()
}

// Clear empties the buffer without resetting the id counter, so a client still holding an old
// Last-Event-ID just sees nothing until the next real line rather than colliding with a reused id.
func (b *Buffer) Clear() {
	b.mu.Lock()
	b.lines = nil
	b.mu.Unlock()
}

// Since returns lines newer than the given id, waiting up to timeout for one to arrive rather than
// returning empty immediately -- so a tailing client is a blocked read, not a busy loop.
func (b *Buffer) Since(ctx context.Context, after int64, timeout time.Duration) []Line {
	if lines := b.after(after); len(lines) > 0 {
		return lines
	}

	wake := make(chan struct{})
	b.mu.Lock()
	b.waiters[wake] = struct{}{}
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-wake:
	case <-timer.C:
	case <-ctx.Done():
	}

	b.mu.Lock()
	delete(b.waiters, wake)
	b.mu.Unlock()
	return b.after(after)
}

func (b *Buffer) after(id int64) []Line {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Line, 0, 8)
	for _, l := range b.lines {
		if l.ID > id {
			out = append(out, l)
		}
	}
	return out
}
