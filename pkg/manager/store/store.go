// Package store persists the manager's sandbox table so that sandboxes
// survive a daemon restart.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Record is what the manager persists per sandbox.
type Record struct {
	Sandbox sandbox.Sandbox `json:"sandbox"`
	// Cgroup is the enforced cgroup configuration, kept so the manager can
	// rebuild its view after a restart.
	Cgroup json.RawMessage `json:"cgroup,omitempty"`
	// StartRequested records that a start was asked for while queued.
	StartRequested bool `json:"startRequested,omitempty"`
}

// Store persists Records.
type Store interface {
	Save(rec Record) error
	Delete(id string) error
	List() ([]Record, error)
}

// Memory is an in-memory Store for tests.
type Memory struct {
	mu   sync.Mutex
	recs map[string]Record
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{recs: map[string]Record{}} }

// Save implements Store.
func (m *Memory) Save(rec Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs[rec.Sandbox.ID] = rec
	return nil
}

// Delete implements Store.
func (m *Memory) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.recs, id)
	return nil
}

// List implements Store.
func (m *Memory) List() ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, 0, len(m.recs))
	for _, r := range m.recs {
		out = append(out, r)
	}
	return out, nil
}

// Dir stores one JSON file per sandbox in a directory. Writes are atomic
// (write to a temp file, then rename).
type Dir struct {
	path string
	mu   sync.Mutex
}

// NewDir creates the directory if needed.
func NewDir(path string) (*Dir, error) {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &Dir{path: path}, nil
}

func (d *Dir) file(id string) string { return filepath.Join(d.path, id+".json") }

// Save implements Store.
func (d *Dir) Save(rec Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(d.path, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func(err error) error {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return cleanup(err)
	}
	if err := tmp.Sync(); err != nil {
		return cleanup(err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, d.file(rec.Sandbox.ID))
}

// Delete implements Store.
func (d *Dir) Delete(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := os.Remove(d.file(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// List implements Store.
func (d *Dir) List() ([]Record, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	var out []Record
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(d.path, e.Name()))
		if err != nil {
			return nil, err
		}
		var rec Record
		if err := json.Unmarshal(data, &rec); err != nil {
			return nil, fmt.Errorf("decode %s: %w", e.Name(), err)
		}
		out = append(out, rec)
	}
	return out, nil
}
