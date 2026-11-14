// Package client is a Go client for the tideletd gRPC API with helpers
// that adapt the streaming exec and attach RPCs to io.Reader and
// io.Writer.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/TideSand/tidelet/api/tidelet/v1alpha1"
)

// Client talks to tideletd.
type Client struct {
	conn      *grpc.ClientConn
	Sandboxes pb.SandboxServiceClient
	Node      pb.NodeServiceClient
}

// New connects to "unix:///path", "/path" or "tcp://host:port".
func New(addr string) (*Client, error) {
	var target string
	switch {
	case strings.HasPrefix(addr, "tcp://"):
		target = "dns:///" + strings.TrimPrefix(addr, "tcp://")
	case strings.HasPrefix(addr, "unix://"):
		target = "unix://" + strings.TrimPrefix(addr, "unix://")
	default:
		target = "unix://" + addr
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	return &Client{conn: conn, Sandboxes: pb.NewSandboxServiceClient(conn), Node: pb.NewNodeServiceClient(conn)}, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

// ExecOptions parameterise Exec.
type ExecOptions struct {
	Sandbox    string
	Command    []string
	Env        map[string]string
	WorkingDir string
	TTY        bool
	// Stdin, when non-nil, is pumped to the process; on EOF the process's
	// stdin is closed.
	Stdin        io.Reader
	Stdout       io.Writer
	Stderr       io.Writer
	TerminalSize *pb.TerminalSize
	Resize       <-chan *pb.TerminalSize
}

// Exec runs a command in a sandbox and returns its exit code.
func (c *Client) Exec(ctx context.Context, opts ExecOptions) (int32, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.Sandboxes.Exec(ctx)
	if err != nil {
		return -1, err
	}
	send := &lockedSender[*pb.ExecRequest]{send: stream.Send}
	if err := send.Send(&pb.ExecRequest{Msg: &pb.ExecRequest_Start{Start: &pb.ExecStart{
		Sandbox:      opts.Sandbox,
		Command:      opts.Command,
		Env:          opts.Env,
		WorkingDir:   opts.WorkingDir,
		Tty:          opts.TTY,
		Stdin:        opts.Stdin != nil,
		TerminalSize: opts.TerminalSize,
	}}}); err != nil {
		return -1, err
	}
	if opts.Stdin != nil {
		go pump(ctx, opts.Stdin, func(b []byte) error {
			return send.Send(&pb.ExecRequest{Msg: &pb.ExecRequest_Stdin{Stdin: b}})
		}, func() error {
			return send.Send(&pb.ExecRequest{Msg: &pb.ExecRequest_CloseStdin{CloseStdin: true}})
		})
	}
	if opts.Resize != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case sz, ok := <-opts.Resize:
					if !ok {
						return
					}
					_ = send.Send(&pb.ExecRequest{Msg: &pb.ExecRequest_Resize{Resize: sz}})
				}
			}
		}()
	}
	stdout, stderr := orDiscard(opts.Stdout), orDiscard(opts.Stderr)
	for {
		msg, err := stream.Recv()
		if err != nil {
			return -1, err
		}
		switch m := msg.GetMsg().(type) {
		case *pb.ExecResponse_Stdout:
			if _, err := stdout.Write(m.Stdout); err != nil {
				return -1, err
			}
		case *pb.ExecResponse_Stderr:
			if _, err := stderr.Write(m.Stderr); err != nil {
				return -1, err
			}
		case *pb.ExecResponse_Exit:
			_ = stream.CloseSend()
			return m.Exit.GetCode(), nil
		}
	}
}

// AttachOptions parameterise Attach.
type AttachOptions struct {
	Sandbox string
	// Stdin, when non-nil, is pumped to the process; on EOF the process's
	// stdin is closed.
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	// Attached is called once the server confirms output is being
	// forwarded.
	Attached     func()
	TerminalSize *pb.TerminalSize
	Resize       <-chan *pb.TerminalSize
}

// Attach connects to a sandbox's main process. It returns the exit code
// if the process exited while attached, or nil if the stream ended for
// another reason (such as ctx being cancelled).
func (c *Client) Attach(ctx context.Context, opts AttachOptions) (*int32, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.Sandboxes.Attach(ctx)
	if err != nil {
		return nil, err
	}
	send := &lockedSender[*pb.AttachRequest]{send: stream.Send}
	if err := send.Send(&pb.AttachRequest{Msg: &pb.AttachRequest_Start{Start: &pb.AttachStart{
		Sandbox:      opts.Sandbox,
		Stdin:        opts.Stdin != nil,
		TerminalSize: opts.TerminalSize,
	}}}); err != nil {
		return nil, err
	}
	if opts.Stdin != nil {
		go pump(ctx, opts.Stdin, func(b []byte) error {
			return send.Send(&pb.AttachRequest{Msg: &pb.AttachRequest_Stdin{Stdin: b}})
		}, func() error {
			return send.Send(&pb.AttachRequest{Msg: &pb.AttachRequest_CloseStdin{CloseStdin: true}})
		})
	}
	if opts.Resize != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case sz, ok := <-opts.Resize:
					if !ok {
						return
					}
					_ = send.Send(&pb.AttachRequest{Msg: &pb.AttachRequest_Resize{Resize: sz}})
				}
			}
		}()
	}
	stdout, stderr := orDiscard(opts.Stdout), orDiscard(opts.Stderr)
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil, nil
			}
			return nil, err
		}
		switch m := msg.GetMsg().(type) {
		case *pb.AttachResponse_Attached:
			if opts.Attached != nil {
				opts.Attached()
			}
		case *pb.AttachResponse_Stdout:
			if _, err := stdout.Write(m.Stdout); err != nil {
				return nil, err
			}
		case *pb.AttachResponse_Stderr:
			if _, err := stderr.Write(m.Stderr); err != nil {
				return nil, err
			}
		case *pb.AttachResponse_Exit:
			_ = stream.CloseSend()
			code := m.Exit.GetCode()
			return &code, nil
		}
	}
}

// LogsOptions parameterise Logs.
type LogsOptions struct {
	Follow    bool
	TailLines int64
	Since     time.Time
}

// Logs streams recorded output, calling fn for each chunk.
func (c *Client) Logs(ctx context.Context, sb string, opts LogsOptions, fn func(*pb.LogsResponse) error) error {
	req := &pb.LogsRequest{Sandbox: sb, Follow: opts.Follow, TailLines: opts.TailLines}
	if !opts.Since.IsZero() {
		req.Since = timestamppb.New(opts.Since)
	}
	stream, err := c.Sandboxes.Logs(ctx, req)
	if err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := fn(msg); err != nil {
			return err
		}
	}
}

// Watch streams lifecycle events to fn until ctx ends.
func (c *Client) Watch(ctx context.Context, fn func(*pb.WatchResponse) error) error {
	stream, err := c.Sandboxes.Watch(ctx, &pb.WatchRequest{})
	if err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := fn(msg); err != nil {
			return err
		}
	}
}

type lockedSender[T any] struct {
	mu   sync.Mutex
	send func(T) error
}

func (s *lockedSender[T]) Send(m T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.send(m)
}

// pump copies r into send until EOF, then calls closeFn.
func pump(ctx context.Context, r io.Reader, send func([]byte) error, closeFn func() error) {
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if serr := send(data); serr != nil {
				return
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) && ctx.Err() == nil {
				_ = closeFn()
			}
			return
		}
	}
}

func orDiscard(w io.Writer) io.Writer {
	if w == nil {
		return io.Discard
	}
	return w
}
