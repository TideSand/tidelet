package containerd

import (
	"errors"
	"io"
	"sync"

	"github.com/TideSand/tidelet/pkg/log/crilog"
)

// errSlowSubscriber is returned to an attach client that cannot keep up
// with the sandbox's output. Output is never dropped from the log file;
// only the live feed to that client is cut.
var errSlowSubscriber = errors.New("attach client too slow, detached")

// subscriberBuffer is how many output frames a subscriber may fall behind.
const subscriberBuffer = 4096

type ioFrame struct {
	stream crilog.Stream
	data   []byte
}

type ioSub struct {
	ch      chan ioFrame
	dropped bool
}

// ioHub owns a sandbox's standard streams on the daemon side. It receives
// stdout and stderr from the containerd FIFOs, records them in the CRI
// log file, and fans them out to attached clients. It also holds the
// writer side of the process's stdin when the sandbox was created with
// stdin enabled.
type ioHub struct {
	id  string
	tty bool
	log *crilog.Writer

	mu          sync.Mutex
	subs        map[*ioSub]struct{}
	closed      bool
	stdinR      *io.PipeReader
	stdinW      *io.PipeWriter
	stdinClosed bool
}

func newIOHub(id, logPath string, tty, stdin bool) (*ioHub, error) {
	w, err := crilog.NewWriter(logPath)
	if err != nil {
		return nil, err
	}
	h := &ioHub{id: id, tty: tty, log: w, subs: map[*ioSub]struct{}{}}
	if stdin {
		h.stdinR, h.stdinW = io.Pipe()
	}
	return h, nil
}

// streams returns the reader and writers to hand to containerd's cio.
func (h *ioHub) streams() (stdin io.Reader, stdout, stderr io.Writer) {
	if h.stdinR != nil {
		stdin = h.stdinR
	}
	return stdin, &hubWriter{h: h, stream: crilog.Stdout}, &hubWriter{h: h, stream: crilog.Stderr}
}

type hubWriter struct {
	h      *ioHub
	stream crilog.Stream
}

func (w *hubWriter) Write(p []byte) (int, error) {
	return len(p), w.h.write(w.stream, p)
}

func (h *ioHub) write(stream crilog.Stream, p []byte) error {
	if err := h.log.Write(stream, p); err != nil {
		return err
	}
	data := make([]byte, len(p))
	copy(data, p)
	frame := ioFrame{stream: stream, data: data}
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		select {
		case s.ch <- frame:
		default:
			s.dropped = true
			delete(h.subs, s)
			close(s.ch)
		}
	}
	return nil
}

// subscribe registers a live output consumer. If the hub is already
// closed the returned channel is closed.
func (h *ioHub) subscribe() *ioSub {
	s := &ioSub{ch: make(chan ioFrame, subscriberBuffer)}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		close(s.ch)
		return s
	}
	h.subs[s] = struct{}{}
	return s
}

func (h *ioHub) unsubscribe(s *ioSub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		close(s.ch)
	}
}

func (h *ioHub) hasStdin() bool { return h.stdinW != nil }

// writeStdin forwards data to the process's stdin.
func (h *ioHub) writeStdin(p []byte) (int, error) {
	h.mu.Lock()
	w, closed := h.stdinW, h.stdinClosed
	h.mu.Unlock()
	if w == nil {
		return 0, errors.New("sandbox has no stdin")
	}
	if closed {
		return 0, io.ErrClosedPipe
	}
	// Pipe writes block until cio has copied the data into the FIFO, which
	// is the back-pressure we want; do not hold the lock meanwhile.
	return w.Write(p)
}

// closeStdin signals EOF to the process. It is idempotent.
func (h *ioHub) closeStdin() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stdinW == nil || h.stdinClosed {
		return false
	}
	h.stdinClosed = true
	_ = h.stdinW.Close()
	return true
}

// close ends the hub after the process has exited: subscribers see EOF
// and the log file is flushed. It is idempotent.
func (h *ioHub) close() {
	h.closeStdin()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for s := range h.subs {
		delete(h.subs, s)
		close(s.ch)
	}
	_ = h.log.Close()
}
