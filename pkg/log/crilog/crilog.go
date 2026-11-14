// Package crilog reads and writes sandbox output in the CRI log format
// used by the kubelet:
//
//	2016-10-06T00:17:09.669794202Z stdout F full line
//	2016-10-06T00:17:09.669794202Z stdout P partial line without newline
//
// Each record holds one line or a partial line of one stream, so the
// reader can reproduce the original byte stream exactly and the kubelet
// can read the file directly once Tidelet runs as a node.
package crilog

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Stream identifies stdout or stderr.
type Stream string

// Streams.
const (
	Stdout Stream = "stdout"
	Stderr Stream = "stderr"
)

// Entry is one decoded record. Data ends with '\n' for full lines and
// has no trailing newline for partial ones.
type Entry struct {
	Time   time.Time
	Stream Stream
	Data   []byte
}

// Writer appends records to a log file. It is safe for concurrent use.
type Writer struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// NewWriter opens path for appending, creating it if needed.
func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, w: bufio.NewWriterSize(f, 64<<10)}, nil
}

// Write records data for stream, splitting it into lines.
func (w *Writer) Write(stream Stream, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		var line []byte
		tag := "F"
		if i < 0 {
			line, data, tag = data, nil, "P"
		} else {
			line, data = data[:i], data[i+1:]
		}
		if _, err := fmt.Fprintf(w.w, "%s %s %s ", ts, stream, tag); err != nil {
			return err
		}
		if _, err := w.w.Write(line); err != nil {
			return err
		}
		if err := w.w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return w.w.Flush()
}

// Close flushes and closes the file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// ReadOptions control Read.
type ReadOptions struct {
	// Follow keeps reading until Done reports true and the file is drained,
	// or ctx ends.
	Follow bool
	// Done reports whether the writer has finished. Used with Follow.
	Done func() bool
	// TailLines limits output to the last N records. Zero means all.
	TailLines int64
	// Since skips records older than this time.
	Since time.Time
	// PollInterval is how often Follow checks for new data.
	PollInterval time.Duration
}

// Read decodes the file at path and calls fn for each entry. A missing
// file is treated as empty.
func Read(ctx context.Context, path string, opts ReadOptions, fn func(Entry) error) error {
	if opts.PollInterval == 0 {
		opts.PollInterval = 100 * time.Millisecond
	}
	f, err := openOrWait(ctx, path, opts)
	if err != nil || f == nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if opts.TailLines > 0 {
		var buf []Entry
		n := int(opts.TailLines)
		if err := readAll(ctx, f, opts.Since, func(e Entry) error {
			buf = append(buf, e)
			if len(buf) > n {
				buf = buf[1:]
			}
			return nil
		}); err != nil {
			return err
		}
		for _, e := range buf {
			if err := fn(e); err != nil {
				return err
			}
		}
		if !opts.Follow {
			return nil
		}
	}
	for {
		if err := readAll(ctx, f, opts.Since, fn); err != nil {
			return err
		}
		if !opts.Follow {
			return nil
		}
		if opts.Done != nil && opts.Done() {
			// One last drain in case the writer flushed after Done flipped.
			return readAll(ctx, f, opts.Since, fn)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(opts.PollInterval):
		}
	}
}

func openOrWait(ctx context.Context, path string, opts ReadOptions) (*os.File, error) {
	for {
		f, err := os.Open(path)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if !opts.Follow || (opts.Done != nil && opts.Done()) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(opts.PollInterval):
		}
	}
}

// readAll consumes complete records from f's current offset. A trailing
// record without a newline is left for the next call.
func readAll(ctx context.Context, f *os.File, since time.Time, fn func(Entry) error) error {
	start, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	r := bufio.NewReaderSize(f, 64<<10)
	consumed := start
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Rewind to the start of the incomplete line so it is
				// re-read whole once the writer finishes it.
				_, serr := f.Seek(consumed, io.SeekStart)
				return serr
			}
			return err
		}
		consumed += int64(len(line))
		e, ok := parse(line[:len(line)-1])
		if !ok || (!since.IsZero() && e.Time.Before(since)) {
			continue
		}
		if err := fn(e); err != nil {
			return err
		}
	}
}

func parse(line []byte) (Entry, bool) {
	// <ts> <stream> <F|P> <data>
	sp1 := bytes.IndexByte(line, ' ')
	if sp1 < 0 {
		return Entry{}, false
	}
	rest := line[sp1+1:]
	sp2 := bytes.IndexByte(rest, ' ')
	if sp2 < 0 {
		return Entry{}, false
	}
	stream := Stream(rest[:sp2])
	rest = rest[sp2+1:]
	if len(rest) < 2 || rest[1] != ' ' {
		return Entry{}, false
	}
	tag := rest[0]
	data := rest[2:]
	ts, err := time.Parse(time.RFC3339Nano, string(line[:sp1]))
	if err != nil {
		return Entry{}, false
	}
	out := make([]byte, 0, len(data)+1)
	out = append(out, data...)
	if tag == 'F' {
		out = append(out, '\n')
	}
	return Entry{Time: ts, Stream: stream, Data: out}, true
}
