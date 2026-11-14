package llmproxy

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TideSand/tidelet/pkg/policy"
)

// fakeResolver knows one sandbox at 127.0.0.1 and records every event.
type fakeResolver struct {
	mu       sync.Mutex
	events   []policy.ModelEvent
	hold     bool
	upstream *url.URL
}

func (r *fakeResolver) LookupSandbox(ip string) (string, *url.URL, bool) {
	if ip == "127.0.0.1" {
		return "sb1", r.upstream, true
	}
	return "", nil, false
}

func (r *fakeResolver) BeforeDelivery(policy.ModelEvent) time.Duration { return 0 }
func (r *fakeResolver) BeforeRequest(policy.ModelEvent) func()         { return nil }

func (r *fakeResolver) OnModelEvent(ev policy.ModelEvent) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	return ev.Kind == policy.ModelResponseReady && r.hold
}

func (r *fakeResolver) kinds() []policy.ModelEventKind {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]policy.ModelEventKind, len(r.events))
	for i, e := range r.events {
		out[i] = e.Kind
	}
	return out
}

const stream = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"content\":[]}}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"Bash\",\"input\":{}}}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_2\",\"name\":\"Read\",\"input\":{}}}\n\n" +
	"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":7}}\n\n" +
	"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

// upstream serves the stream in three flushed pieces and echoes what it got.
func upstream(t *testing.T, got *http.Request, gotBody *[]byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got = *r
		b, _ := io.ReadAll(r.Body)
		*gotBody = b
		if strings.HasSuffix(r.URL.Path, "/count_tokens") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"input_tokens":3}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Upstream", "yes")
		w.Header().Set("Request-Id", "req_123")
		w.WriteHeader(200)
		f := w.(http.Flusher)
		for _, piece := range []string{stream[:80], stream[80:300], stream[300:]} {
			_, _ = io.WriteString(w, piece)
			f.Flush()
			time.Sleep(5 * time.Millisecond)
		}
	}))
}

func newProxy(t *testing.T, res *fakeResolver, up string, maxDefer time.Duration) *httptest.Server {
	u, _ := url.Parse(up)
	p, err := New(Config{Upstream: u, MaxDefer: maxDefer, Resolver: res})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(p)
}

func TestForwardsUnchangedAndReportsToolCalls(t *testing.T) {
	var got http.Request
	var gotBody []byte
	up := upstream(t, &got, &gotBody)
	defer up.Close()
	res := &fakeResolver{}
	px := newProxy(t, res, up.URL, time.Second)
	defer px.Close()

	body := `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req, _ := http.NewRequest("POST", px.URL+"/v1/messages?beta=true", strings.NewReader(body))
	req.Header.Set("Anthropic-Beta", "x,y")
	req.Header.Set("X-Api-Key", "secret")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(out) != stream {
		t.Fatalf("response body changed:\n%s", out)
	}
	if resp.Header.Get("X-Upstream") != "yes" || resp.Header.Get("Request-Id") != "req_123" || resp.Header.Get("Content-Type") != "text/event-stream" || resp.StatusCode != 200 {
		t.Fatalf("response headers changed: %v %d", resp.Header, resp.StatusCode)
	}
	if string(gotBody) != body || got.URL.Path != "/v1/messages" || got.URL.RawQuery != "beta=true" || got.Header.Get("Anthropic-Beta") != "x,y" || got.Header.Get("X-Api-Key") != "secret" {
		t.Fatalf("request changed: %s %s %v", got.URL, gotBody, got.Header)
	}
	kinds := res.kinds()
	if len(kinds) != 3 || kinds[0] != policy.ModelRequestStarted || kinds[1] != policy.ModelResponseReady || kinds[2] != policy.ModelResponseDelivered {
		t.Fatalf("events = %v", kinds)
	}
	ready := res.events[1]
	if ready.ToolCalls != 2 || strings.Join(ready.ToolNames, ",") != "Bash,Read" || ready.StopReason != "tool_use" || ready.Sandbox != "sb1" || ready.Status != 200 {
		t.Fatalf("ready event = %+v", ready)
	}
	if want := fmt.Sprintf("%x", sha256.Sum256([]byte(body))); res.events[2].Body != want || ready.Body != want {
		t.Fatalf("body digest = %q / %q, want %q", ready.Body, res.events[2].Body, want)
	}
	if res.events[2].Held != 0 || res.events[2].Forced {
		t.Fatalf("delivered event = %+v", res.events[2])
	}
}

func TestHoldReleaseAndForcedDelivery(t *testing.T) {
	var got http.Request
	var gotBody []byte
	up := upstream(t, &got, &gotBody)
	defer up.Close()
	res := &fakeResolver{hold: true}
	u, _ := url.Parse(up.URL)
	p, _ := New(Config{Upstream: u, MaxDefer: 300 * time.Millisecond, Resolver: res})
	px := httptest.NewServer(p)
	defer px.Close()

	// Held, then released by the policy. While held, everything but the stream's last chunk
	// has already reached the client.
	done := make(chan *http.Response, 1)
	var partial atomic.Int64
	go func() {
		resp, err := http.Post(px.URL+"/v1/messages", "application/json", strings.NewReader(`{}`))
		if err != nil {
			done <- nil
			return
		}
		buf := make([]byte, 4096)
		var body []byte
		for {
			n, err := resp.Body.Read(buf)
			body = append(body, buf[:n]...)
			partial.Store(int64(len(body)))
			if err != nil {
				break
			}
		}
		resp.Body = io.NopCloser(bytes.NewReader(body))
		done <- resp
	}()
	deadline := time.Now().Add(2 * time.Second)
	for p.Held() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if p.Held() != 1 {
		t.Fatal("response not held")
	}
	select {
	case <-done:
		t.Fatal("delivered while held")
	case <-time.After(50 * time.Millisecond):
	}
	// Everything before the first tool block's stop flowed, its input included; the stop waits.
	if want := int64(messageEnd(stream)); partial.Load() != want {
		t.Fatalf("while held the client had %d bytes, want %d (up to the first tool block's stop)", partial.Load(), want)
	}
	res.mu.Lock()
	call := res.events[1].Call
	res.mu.Unlock()
	if !p.Release(call) {
		t.Fatal("release found no held call")
	}
	resp := <-done
	out, _ := io.ReadAll(resp.Body)
	if string(out) != stream {
		t.Fatal("held response body changed")
	}
	res.mu.Lock()
	delivered := res.events[2]
	res.mu.Unlock()
	if delivered.Kind != policy.ModelResponseDelivered || delivered.Forced || delivered.Held < 50*time.Millisecond {
		t.Fatalf("delivered = %+v", delivered)
	}
	if p.Release(call) {
		t.Fatal("second release should find nothing")
	}

	// Held and never released: delivered by the deadline, reported as forced.
	start := time.Now()
	resp2, err := http.Post(px.URL+"/v1/messages", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	out2, _ := io.ReadAll(resp2.Body)
	if string(out2) != stream || time.Since(start) < 300*time.Millisecond {
		t.Fatalf("forced delivery wrong: %d bytes after %v", len(out2), time.Since(start))
	}
	res.mu.Lock()
	last := res.events[len(res.events)-1]
	res.mu.Unlock()
	if !last.Forced || last.Held < 300*time.Millisecond {
		t.Fatalf("forced event = %+v", last)
	}
	if p.Held() != 0 {
		t.Fatal("held table not cleared")
	}
}

func TestNonModelCallsAndUnknownClientsPassThrough(t *testing.T) {
	var got http.Request
	var gotBody []byte
	up := upstream(t, &got, &gotBody)
	defer up.Close()
	res := &fakeResolver{hold: true}
	px := newProxy(t, res, up.URL, time.Second)
	defer px.Close()
	resp, err := http.Post(px.URL+"/v1/messages/count_tokens", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(out, []byte(`{"input_tokens":3}`)) || len(res.kinds()) != 0 {
		t.Fatalf("count_tokens: body %s, events %v", out, res.kinds())
	}
}

func TestInspectBodies(t *testing.T) {
	info := inspectBody([]byte(`{"content":[{"type":"text","text":"x"},{"type":"tool_use","name":"Bash"}],"stop_reason":"tool_use"}`))
	if info.toolCalls != 1 || info.toolNames[0] != "Bash" || info.stopReason != "tool_use" {
		t.Fatalf("anthropic body: %+v", info)
	}
	info = inspectBody([]byte(`{"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"function":{"name":"shell"}},{"function":{"name":"read"}}]}}]}`))
	if info.toolCalls != 2 || info.stopReason != "tool_calls" {
		t.Fatalf("openai body: %+v", info)
	}
	info = inspectStream([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"shell\"}}]}}]}\n\ndata: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n"))
	if info.toolCalls != 1 || info.stopReason != "tool_calls" {
		t.Fatalf("openai stream: %+v", info)
	}
	if info := inspectBody([]byte(`not json`)); info.toolCalls != 0 {
		t.Fatal("garbage counted as tool calls")
	}
}
