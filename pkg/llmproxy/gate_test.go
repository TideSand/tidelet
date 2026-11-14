package llmproxy

import (
	"bytes"
	"strings"
	"testing"
)

// messageEnd is where the kept part of the stream begins: the first event
// after the first tool_use block's start that is not one of its input deltas.
func messageEnd(s string) int {
	start := strings.Index(s, "\"content_block\":{\"type\":\"tool_use\"")
	off := start + strings.Index(s[start:], "\n\n") + 2
	for off < len(s) {
		next := strings.Index(s[off:], "\n\n")
		if next < 0 || !strings.Contains(s[off:off+next], "content_block_delta") {
			return off
		}
		off += next + 2
	}
	return off
}

func TestGateKeepsStreamFromTheFirstToolBlockStop(t *testing.T) {
	// Pieces cut in the middle of events, with the tool_use event split across two pieces:
	// the tool call's input streams; its stop event waits for the decision.
	cut := strings.Index(stream, "\"index\":1")
	pieces := [][]byte{[]byte(stream[:40]), []byte(stream[40:cut]), []byte(stream[cut:])}
	g := newGate(true)
	var out []byte
	for _, p := range pieces {
		out = append(out, g.push(p)...)
	}
	want := stream[:messageEnd(stream)]
	if string(out) != want {
		t.Fatalf("forwarded before the decision:\n%q\nwant:\n%q", out, want)
	}
	if string(out)+string(g.rest()) != stream {
		t.Fatal("bytes were lost or reordered")
	}
}

func TestGateTextOnlyStreamKeepsLastEvent(t *testing.T) {
	s := "event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"text\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\"}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	g := newGate(true)
	var out []byte
	for i := 0; i < len(s); i += 7 { // small pieces
		end := min(i+7, len(s))
		out = append(out, g.push([]byte(s[i:end]))...)
	}
	last := strings.LastIndex(s[:len(s)-2], "\n\n") + 2
	if string(out) != s[:last] {
		t.Fatalf("forwarded %q, want everything but the last event", out)
	}
	if string(out)+string(g.rest()) != s {
		t.Fatal("bytes were lost or reordered")
	}
}

func TestGatePlainBodyKeepsLastPiece(t *testing.T) {
	g := newGate(false)
	var out []byte
	for _, p := range []string{"{\"a\":", "1,\"b\"", ":2}"} {
		out = append(out, g.push([]byte(p))...)
	}
	if string(out) != "{\"a\":1,\"b\"" || string(g.rest()) != ":2}" {
		t.Fatalf("forwarded %q, rest %q", out, g.rest())
	}
}

func TestEventOpensToolCall(t *testing.T) {
	for _, c := range []struct {
		ev   string
		want bool
	}{
		{"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"Bash\"}}\n\n", true},
		{"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"text\"}}\n\n", false},
		{"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"f\"}}]}}]}\n\n", true},
		{"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n", false},
		{"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\"}}\n\n", true},
		{"data: [DONE]\n\n", false},
	} {
		if got := eventOpensToolCall([]byte(c.ev)); got != c.want {
			t.Errorf("%q: got %v, want %v", bytes.TrimSpace([]byte(c.ev)), got, c.want)
		}
	}
}
