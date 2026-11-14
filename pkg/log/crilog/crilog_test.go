package crilog

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func collect(t *testing.T, path string, opts ReadOptions) (string, []Entry) {
	t.Helper()
	var all []Entry
	var out []byte
	err := Read(context.Background(), path, opts, func(e Entry) error {
		all = append(all, e)
		out = append(out, e.Data...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(out), all
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []struct {
		s Stream
		d string
	}{
		{Stdout, "hello "}, {Stdout, "world\nsecond"}, {Stderr, "err\n"}, {Stdout, " line\n\nlast"},
	}
	for _, c := range chunks {
		if err := w.Write(c.s, []byte(c.d)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, entries := collect(t, path, ReadOptions{})
	want := "hello world\nseconderr\n line\n\nlast"
	if got != want {
		t.Fatalf("round trip = %q, want %q", got, want)
	}
	if entries[0].Stream != Stdout || entries[3].Stream != Stderr {
		t.Fatalf("streams = %+v", entries)
	}
	last := entries[len(entries)-1].Data
	if last[len(last)-1] == '\n' {
		t.Fatal("partial last record should not end in newline")
	}
	got, _ = collect(t, path, ReadOptions{TailLines: 2})
	if got != "\nlast" {
		t.Fatalf("tail = %q", got)
	}
}

func TestFollow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.log")
	var done atomic.Bool
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for i := range 3 {
			_ = w.Write(Stdout, []byte{byte('a' + i), '\n'})
			time.Sleep(20 * time.Millisecond)
		}
		_ = w.Close()
		done.Store(true)
	}()
	got, _ := collect(t, path, ReadOptions{Follow: true, Done: done.Load, PollInterval: 5 * time.Millisecond})
	if got != "a\nb\nc\n" {
		t.Fatalf("follow = %q", got)
	}
}

func TestMissingFile(t *testing.T) {
	got, _ := collect(t, filepath.Join(t.TempDir(), "nope.log"), ReadOptions{})
	if got != "" {
		t.Fatal(got)
	}
}
