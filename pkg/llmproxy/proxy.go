// Package llmproxy is tideletd's pass-through proxy for the model API.
//
// A sandbox reaches its model through the proxy (ANTHROPIC_BASE_URL on the
// bridge address). The proxy forwards every request to the sandbox's
// upstream exactly as received, and forwards the response exactly as the
// upstream sent it: bytes, headers, status, and a stream's chunks as they
// arrived. It changes nothing in either direction. What it adds is
// knowledge and timing: it reads a copy of each model response to learn
// whether it carries tool calls, reports the call to the manager as
// policy.ModelEvents, and, when the policy answers a ResponseReady event
// with a Hold, keeps back the response from its first tool-call event (or
// its final piece, when there is none) until the policy's Release or the
// maximum defer time, whichever is first. Everything before that is
// forwarded as it arrives, so a streamed response keeps flowing; but the
// agent cannot start a tool it has not received, so its tool calls start
// when the scheduler lets them, rather than the moment the model answered.
//
// The sandbox is identified by the source address of the connection, which
// is the sandbox's IP on the bridge. Requests from addresses the manager
// does not know, and requests to paths that are not model calls (token
// counting, model listing), are forwarded without events or holds.
package llmproxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TideSand/tidelet/pkg/policy"
)

// Resolver is what the proxy needs from the manager.
type Resolver interface {
	// LookupSandbox maps a client IP on the bridge to the sandbox ID and
	// its upstream (nil for the proxy's default).
	LookupSandbox(ip string) (id string, upstream *url.URL, ok bool)
	// OnModelEvent reports a model event to the policy. For a ResponseReady
	// event it says whether the response is to be held.
	OnModelEvent(ev policy.ModelEvent) (hold bool)
	// BeforeDelivery is called with a response carrying tool calls, after
	// any hold and before the agent gets it, so that the sandbox can be
	// checkpointed first. It returns the time that took, which counts as
	// held.
	BeforeDelivery(ev policy.ModelEvent) time.Duration
	// BeforeRequest is called with a model request as it is forwarded, so
	// that the sandbox can be checkpointed while the agent waits on the
	// model and has nothing of the response yet. It returns a function
	// that waits for that checkpoint, or nil; the proxy waits before
	// writing the first byte of the response, so the agent's state at the
	// checkpoint is always "request sent, nothing received".
	BeforeRequest(ev policy.ModelEvent) func()
}

// Config configures a Proxy.
type Config struct {
	// Upstream is the default model API base, e.g. https://api.anthropic.com.
	Upstream *url.URL
	// MaxDefer bounds how long a held response waits for a Release.
	MaxDefer time.Duration
	Resolver Resolver
	Logger   *slog.Logger
	// Transport, when set, replaces the default HTTP transport (tests).
	Transport http.RoundTripper
	// HoldWhole keeps every byte of a model response's body back until the policy has
	// decided (and any checkpoint before delivery is taken). Whole-sandbox checkpoints need
	// it: an agent restored to a checkpoint that already had part of the response sees a
	// stream that broke off, and asks the model to resume it (Claude Code: "Your response
	// above was cut off mid-stream"), a request no recording answers; with nothing received
	// it simply sends the same request again. The headers still go out at once.
	HoldWhole bool
}

// KeepaliveInterval is how often a held event stream gets a comment line.
var KeepaliveInterval = 20 * time.Second

// Proxy is the pass-through model proxy.
type Proxy struct {
	cfg    Config
	log    *slog.Logger
	client *http.Client
	calls  atomic.Uint64

	mu       sync.Mutex
	held     map[string]chan struct{} // call -> closed on Release
	released map[string]time.Time     // calls released before their hold was registered
}

// New builds a proxy.
func New(cfg Config) (*Proxy, error) {
	if cfg.Upstream == nil {
		return nil, errors.New("llmproxy: Upstream is required")
	}
	if cfg.Resolver == nil {
		return nil, errors.New("llmproxy: Resolver is required")
	}
	if cfg.MaxDefer <= 0 {
		cfg.MaxDefer = 60 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	transport := cfg.Transport
	if transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.DisableCompression = true // forward the upstream's encoding as it is; never ask for one on the client's behalf
		t.ResponseHeaderTimeout = 0
		transport = t
	}
	return &Proxy{
		cfg:      cfg,
		log:      cfg.Logger.With("component", "llmproxy"),
		client:   &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		held:     map[string]chan struct{}{},
		released: map[string]time.Time{},
	}, nil
}

// Serve listens on addr until ctx is done.
func (p *Proxy) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: p, ReadHeaderTimeout: 30 * time.Second}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	p.log.Info("llm proxy listening", "addr", ln.Addr().String(), "upstream", p.cfg.Upstream.String(), "maxDefer", p.cfg.MaxDefer)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Release delivers a held response. It reports whether the call was held.
func (p *Proxy) Release(call string) bool {
	p.mu.Lock()
	ch, ok := p.held[call]
	if ok {
		delete(p.held, call)
	} else {
		// The policy's Release can run before the response's hold is registered
		// (the hold is decided in one event and the release sent from another, a
		// few ms apart): remember it, so the hold is not taken at all.
		now := time.Now()
		for c, at := range p.released {
			if now.Sub(at) > 5*time.Minute {
				delete(p.released, c)
			}
		}
		p.released[call] = now
	}
	p.mu.Unlock()
	if ok {
		close(ch)
	}
	return ok
}

// Held reports how many responses are being held back.
func (p *Proxy) Held() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.held)
}

// ServeHTTP implements http.Handler.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	sandboxID, upstream, known := p.cfg.Resolver.LookupSandbox(ip)
	if upstream == nil {
		upstream = p.cfg.Upstream
	}
	modelCall := known && isModelCall(r.URL.Path)
	call := ""
	var bodyHash hash.Hash
	var ckMu sync.Mutex
	var waitCheckpoint func()
	if modelCall {
		call = fmt.Sprintf("%s-%d", sandboxID, p.calls.Add(1))
		started := policy.ModelEvent{Kind: policy.ModelRequestStarted, Time: time.Now(), Sandbox: sandboxID, Call: call}
		p.cfg.Resolver.OnModelEvent(started)
		// The checkpoint is taken once the agent has sent the whole request: a checkpoint
		// pauses the sandbox, and one taken while the body is still on its way would stall
		// the body, and so the upstream's clock, for the checkpoint's duration.
		checkpoint := func() {
			ckMu.Lock()
			defer ckMu.Unlock()
			if waitCheckpoint == nil {
				waitCheckpoint = p.cfg.Resolver.BeforeRequest(started)
			}
		}
		// The body streams through untouched; its digest lets the call be joined with the
		// upstream's record of the same request.
		bodyHash = sha256.New()
		if r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0 {
			r.Body = struct {
				io.Reader
				io.Closer
			}{&eofNotifier{Reader: io.TeeReader(r.Body, bodyHash), onEOF: checkpoint}, r.Body}
		} else {
			checkpoint()
		}
	}

	resp, err := p.forward(r, upstream)
	if err != nil {
		p.log.Warn("upstream request failed", "sandbox", sandboxID, "path", r.URL.Path, "err", err)
		http.Error(w, "tidelet llm proxy: upstream unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// The response goes out as it comes in, up to the point where the agent could act on it.
	// For an event stream that is the first tool-call event: an agent runs a tool as soon as
	// that block is complete, before the stream's message_stop, so everything from that event
	// on is kept back until the policy's decision (text before it keeps flowing, and the
	// client sees a live stream). A stream without tool calls, or a plain body, flows except
	// for its last piece, which is enough to keep the agent from moving on before the decision.
	for k, vv := range resp.Header {
		if hopByHop[k] {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	flusher, _ := w.(http.Flusher)
	write := func(c []byte) bool {
		ckMu.Lock()
		wait := waitCheckpoint
		waitCheckpoint = nil
		ckMu.Unlock()
		if wait != nil {
			wait() // the checkpoint taken at the request must not include any response byte
		}
		if _, err := w.Write(c); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	gate := newGate(modelCall && isEventStream(resp))
	if modelCall && p.cfg.HoldWhole {
		gate.closed = true // keep everything until the decision
	}
	var chunks [][]byte
	buf := make([]byte, 64<<10)
	var readErr error
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			c := append([]byte(nil), buf[:n]...)
			chunks = append(chunks, c)
			if out := gate.push(c); len(out) > 0 {
				write(out)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			readErr = err
			break
		}
	}
	if readErr != nil {
		p.log.Warn("upstream response truncated", "sandbox", sandboxID, "call", call, "err", readErr)
	}
	lastByte := time.Now()

	var held time.Duration
	forced := false
	var info responseInfo
	body := ""
	if modelCall {
		body = hex.EncodeToString(bodyHash.Sum(nil))
		info = inspect(resp, chunks)
		ready := policy.ModelEvent{Kind: policy.ModelResponseReady, Time: time.Now(), Sandbox: sandboxID, Call: call, Status: resp.StatusCode,
			ToolCalls: info.toolCalls, ToolNames: info.toolNames, ToolInputs: info.toolInputs, StopReason: info.stopReason, Body: body}
		if p.cfg.Resolver.OnModelEvent(ready) {
			// An event stream held mid-way is silent, and the agent's HTTP client gives up a
			// silent stream after some minutes (Claude Code: five). An SSE comment line, which
			// every event-stream parser skips, keeps it alive between the whole events the
			// gate forwards; it changes no event. A plain body gets nothing.
			var keepalive func() bool
			// With HoldWhole not even a comment goes out: the agent must hold no byte of the
			// body if it is checkpointed before delivery (its own idle timeouts must allow the
			// hold, as the replay harness sets them).
			if gate.stream && !p.cfg.HoldWhole {
				keepalive = func() bool { return write([]byte(": keepalive\n\n")) }
			}
			held, forced = p.hold(r.Context(), call, keepalive)
		}
		if info.toolCalls > 0 {
			// A checkpoint before the tool calls run is the policy's business, not the
			// agent's time: it is reported as part of the hold.
			ready.Time, ready.Held = time.Now(), held
			held += p.cfg.Resolver.BeforeDelivery(ready)
		}
	}
	if rest := gate.rest(); len(rest) > 0 {
		write(rest)
	}
	if modelCall {
		delivered := time.Now()
		p.cfg.Resolver.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseDelivered, Time: delivered, Sandbox: sandboxID, Call: call, Status: resp.StatusCode,
			ToolCalls: info.toolCalls, ToolNames: info.toolNames, ToolInputs: info.toolInputs, StopReason: info.stopReason, Held: held, Forced: forced, Body: body})
		// One line per call, so a run's timing can be reconstructed from the log: when the
		// agent got the response, and how much of the gap since the model answered was the hold.
		// overheadMs is the proxy's own time between the upstream's last byte and the delivery,
		// the hold excluded: inspection, the policy's decision and the write.
		p.log.Info("model response delivered", "sandbox", sandboxID, "call", call, "body", body, "status", resp.StatusCode,
			"toolCalls", info.toolCalls, "heldMs", held.Milliseconds(), "overheadMs", (delivered.Sub(lastByte) - held).Milliseconds(), "forced", forced, "truncated", readErr != nil)
	}
}

// hold waits for the call's Release, the maximum defer time, or the client
// going away. It returns how long it waited and whether the wait ended by
// the deadline rather than a Release.
func (p *Proxy) hold(ctx context.Context, call string, keepalive func() bool) (time.Duration, bool) {
	ch := make(chan struct{})
	p.mu.Lock()
	if _, early := p.released[call]; early {
		delete(p.released, call)
		p.mu.Unlock()
		return 0, false // released already: not held at all
	}
	p.held[call] = ch
	p.mu.Unlock()
	start := time.Now()
	timer := time.NewTimer(p.cfg.MaxDefer)
	defer timer.Stop()
	var tick <-chan time.Time
	if keepalive != nil {
		ticker := time.NewTicker(KeepaliveInterval)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		select {
		case <-ch:
			return time.Since(start), false
		case <-tick:
			if keepalive() {
				continue
			}
		case <-timer.C:
		case <-ctx.Done():
		}
		break
	}
	p.mu.Lock()
	delete(p.held, call)
	p.mu.Unlock()
	return time.Since(start), true
}

// forward sends the request to the upstream unchanged.
func (p *Proxy) forward(r *http.Request, upstream *url.URL) (*http.Response, error) {
	target := *upstream
	target.Path = strings.TrimSuffix(upstream.Path, "/") + r.URL.Path
	target.RawQuery = r.URL.RawQuery
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), r.Body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = r.ContentLength
	for k, vv := range r.Header {
		if hopByHop[k] {
			continue
		}
		req.Header[k] = append([]string(nil), vv...)
	}
	req.Host = target.Host
	return p.client.Do(req)
}

// eofNotifier calls onEOF once, when the reader is exhausted.
type eofNotifier struct {
	io.Reader
	onEOF func()
	done  bool
}

func (n *eofNotifier) Read(b []byte) (int, error) {
	k, err := n.Reader.Read(b)
	if err == io.EOF && !n.done {
		n.done = true
		n.onEOF()
	}
	return k, err
}

var hopByHop = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true,
}

// isEventStream says whether the response is a server-sent-event stream the
// proxy can read event by event (an encoded body cannot be).
func isEventStream(resp *http.Response) bool {
	if enc := strings.ToLower(resp.Header.Get("Content-Encoding")); enc != "" && enc != "identity" {
		return false
	}
	return strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

// gate decides, piece by piece, which bytes of a response may go to the
// client before the policy's decision. For an event stream it forwards whole
// events as they complete, always keeping the latest one back, and once an
// event opens a tool call it keeps everything from that event on. For any
// other body it forwards every piece but the latest.
type gate struct {
	stream    bool
	toolOpen  bool   // an Anthropic tool_use block is streaming: its stop event closes the gate
	closed    bool   // the agent could act on the next event: nothing more goes out
	unscanned []byte // bytes of the stream not yet split into complete events
	kept      []byte // bytes held back for after the decision, in order
}

func newGate(stream bool) *gate { return &gate{stream: stream} }

// eventType is the "type" of an Anthropic server-sent event's data, "" for
// anything else.
func eventType(ev []byte) string {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		var e struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(bytes.TrimSpace(line[len("data:"):]), &e) == nil {
			return e.Type
		}
	}
	return ""
}

// closes says whether the gate must close at this event: the agent acts on
// a tool call as soon as its block is complete, so an Anthropic tool_use
// block's input deltas may stream (the agent parses them meanwhile) but
// whatever follows them, its content_block_stop first of all, is kept back;
// a tool call in any other format closes the gate where it opens.
func (g *gate) closes(ev []byte) bool {
	if g.toolOpen {
		return eventType(ev) != "content_block_delta"
	}
	if !eventOpensToolCall(ev) {
		return false
	}
	if eventType(ev) == "content_block_start" {
		g.toolOpen = true
		return false
	}
	return true
}

// push takes the next piece of the body and returns the bytes that may go
// out now.
func (g *gate) push(c []byte) []byte {
	if g.closed {
		g.kept = append(g.kept, c...)
		return nil
	}
	if !g.stream {
		out := g.kept
		g.kept = append([]byte(nil), c...)
		return out
	}
	g.unscanned = append(g.unscanned, c...)
	// Complete events end with a blank line; the tail after the last one stays unscanned.
	complete := bytes.LastIndex(g.unscanned, []byte("\n\n"))
	if complete < 0 {
		return nil
	}
	complete += 2
	events := g.unscanned[:complete]
	// Look for the first event the agent could act on (see closes); everything from it
	// on is kept until the decision.
	for off := 0; off < len(events); {
		next := bytes.Index(events[off:], []byte("\n\n"))
		if next < 0 {
			break
		}
		ev := events[off : off+next+2]
		if g.closes(ev) {
			out := append(g.kept, events[:off]...)
			g.kept = append([]byte(nil), g.unscanned[off:]...)
			g.unscanned = nil
			g.closed = true
			return out
		}
		off += next + 2
	}
	// No tool call yet: everything complete goes out except the latest event, which waits
	// for the next one (or the decision).
	last := bytes.LastIndex(events[:len(events)-2], []byte("\n\n"))
	var out []byte
	if last >= 0 {
		out = append(g.kept, events[:last+2]...)
		g.kept = append([]byte(nil), events[last+2:]...)
	} else {
		out = g.kept
		g.kept = append([]byte(nil), events...)
	}
	g.unscanned = append([]byte(nil), g.unscanned[complete:]...)
	return out
}

// rest returns everything still held back, to go out after the decision.
func (g *gate) rest() []byte {
	out := append(g.kept, g.unscanned...)
	g.kept, g.unscanned = nil, nil
	return out
}

// eventOpensToolCall says whether a server-sent event starts a tool call:
// Anthropic's content_block_start of a tool block, an OpenAI chat delta that
// names a tool call, or an OpenAI responses output item of a function call.
func eventOpensToolCall(ev []byte) bool {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		var e struct {
			Type         string                `json:"type"`
			ContentBlock struct{ Type string } `json:"content_block"`
			Item         struct{ Type string } `json:"item"`
			Choices      []struct {
				Delta struct {
					ToolCalls []json.RawMessage `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(bytes.TrimSpace(line[5:]), &e); err != nil {
			continue
		}
		switch e.Type {
		case "content_block_start":
			if t := e.ContentBlock.Type; t == "tool_use" || t == "server_tool_use" || t == "mcp_tool_use" {
				return true
			}
		case "response.output_item.added", "response.output_item.done":
			if e.Item.Type == "function_call" {
				return true
			}
		}
		for _, c := range e.Choices {
			if len(c.Delta.ToolCalls) > 0 {
				return true
			}
		}
	}
	return false
}

// isModelCall says whether a path is a model call worth scheduling on:
// Anthropic's /v1/messages (not count_tokens), OpenAI's chat completions
// and responses.
func isModelCall(path string) bool {
	path = strings.TrimSuffix(path, "/")
	return strings.HasSuffix(path, "/v1/messages") || strings.HasSuffix(path, "/v1/chat/completions") || strings.HasSuffix(path, "/v1/responses")
}

type responseInfo struct {
	toolCalls  int
	toolNames  []string
	toolInputs []string // each tool call's input as JSON, "" when unknown
	stopReason string
}

// add records one tool call.
func (i *responseInfo) add(name string, input []byte) {
	i.toolCalls++
	i.toolNames = append(i.toolNames, name)
	i.toolInputs = append(i.toolInputs, string(input))
}

// inspect reads a copy of the response for tool calls and the stop reason.
// Both Anthropic's Messages shape and OpenAI's chat completions are
// understood, as a body or as a server-sent-event stream; anything else
// yields zero tool calls.
func inspect(resp *http.Response, chunks [][]byte) responseInfo {
	body := bytes.Join(chunks, nil)
	switch strings.ToLower(resp.Header.Get("Content-Encoding")) {
	case "gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return responseInfo{}
		}
		if body, err = io.ReadAll(zr); err != nil {
			return responseInfo{}
		}
	case "", "identity":
	default:
		return responseInfo{}
	}
	if strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") || bytes.HasPrefix(bytes.TrimSpace(body), []byte("event:")) || bytes.HasPrefix(bytes.TrimSpace(body), []byte("data:")) {
		return inspectStream(body)
	}
	return inspectBody(body)
}

func inspectBody(body []byte) responseInfo {
	var msg struct {
		Content []struct {
			Type, Name string
			Input      json.RawMessage `json:"input"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
		Choices    []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				ToolCalls []struct {
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
		Output []struct {
			Type, Name string
			Arguments  string `json:"arguments"`
		} `json:"output"`
	}
	if err := json.Unmarshal(body, &msg); err != nil {
		return responseInfo{}
	}
	info := responseInfo{stopReason: msg.StopReason}
	for _, b := range msg.Content {
		if b.Type == "tool_use" || b.Type == "server_tool_use" || b.Type == "mcp_tool_use" {
			info.add(b.Name, b.Input)
		}
	}
	for _, c := range msg.Choices {
		info.stopReason = c.FinishReason
		for _, t := range c.Message.ToolCalls {
			info.add(t.Function.Name, []byte(t.Function.Arguments))
		}
	}
	for _, o := range msg.Output {
		if o.Type == "function_call" {
			info.add(o.Name, []byte(o.Arguments))
		}
	}
	return info
}

func inspectStream(body []byte) responseInfo {
	var info responseInfo
	// Anthropic streams a tool call's input as input_json_delta pieces of the block's index;
	// OpenAI as argument deltas of the call's index. Both are joined per tool call.
	blocks := map[int]int{}      // Anthropic content block index -> tool call number
	openaiCalls := map[int]int{} // OpenAI tool call index -> tool call number
	var inputs []*bytes.Buffer
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		var ev struct {
			Type         string `json:"type"`
			Index        int    `json:"index"`
			ContentBlock struct {
				Type, Name string
				Input      json.RawMessage `json:"input"`
			} `json:"content_block"`
			Delta struct {
				Type        string `json:"type"`
				PartialJSON string `json:"partial_json"`
				StopReason  string `json:"stop_reason"`
			} `json:"delta"`
			Item struct {
				Type, Name string
				Arguments  string `json:"arguments"`
			} `json:"item"` // OpenAI responses: response.output_item.done
			Choices []struct {
				FinishReason string `json:"finish_reason"`
				Delta        struct {
					ToolCalls []struct {
						Index    int `json:"index"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(bytes.TrimSpace(line[5:]), &ev); err != nil {
			continue
		}
		switch ev.Type {
		case "content_block_start":
			if t := ev.ContentBlock.Type; t == "tool_use" || t == "server_tool_use" || t == "mcp_tool_use" {
				blocks[ev.Index] = info.toolCalls
				info.add(ev.ContentBlock.Name, nil)
				inputs = append(inputs, &bytes.Buffer{})
			}
		case "content_block_delta":
			if n, ok := blocks[ev.Index]; ok && ev.Delta.Type == "input_json_delta" {
				inputs[n].WriteString(ev.Delta.PartialJSON)
			}
		case "message_delta":
			if ev.Delta.StopReason != "" {
				info.stopReason = ev.Delta.StopReason
			}
		case "response.output_item.done":
			if ev.Item.Type == "function_call" {
				info.add(ev.Item.Name, []byte(ev.Item.Arguments))
				inputs = append(inputs, nil)
			}
		}
		for _, c := range ev.Choices {
			if c.FinishReason != "" {
				info.stopReason = c.FinishReason
			}
			for _, t := range c.Delta.ToolCalls {
				if t.Function.Name != "" { // the first delta of a call names it; later deltas carry arguments only
					openaiCalls[t.Index] = info.toolCalls
					info.add(t.Function.Name, nil)
					inputs = append(inputs, &bytes.Buffer{})
				}
				if n, ok := openaiCalls[t.Index]; ok && inputs[n] != nil {
					inputs[n].WriteString(t.Function.Arguments)
				}
			}
		}
	}
	for n, b := range inputs {
		if b != nil && b.Len() > 0 {
			info.toolInputs[n] = b.String()
		}
	}
	return info
}

// Stats is a snapshot for status output.
type Stats struct {
	Calls uint64
	Held  int
}

// Stats implements status reporting.
func (p *Proxy) Stats() Stats {
	return Stats{Calls: p.calls.Load(), Held: p.Held()}
}
