// Package server exposes the manager over gRPC, with a REST/JSON gateway
// generated from the same proto for the non-bidirectional methods.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	gw "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/TideSand/tidelet/api/tidelet/v1alpha1"
	"github.com/TideSand/tidelet/pkg/log/crilog"
	"github.com/TideSand/tidelet/pkg/manager"
	"github.com/TideSand/tidelet/pkg/manager/metrics"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
	"github.com/TideSand/tidelet/pkg/version"
)

// DefaultSocket is the daemon's default gRPC unix socket path.
const DefaultSocket = "/run/tidelet/tidelet.sock"

// DefaultHTTPAddress is the default REST gateway listen address.
const DefaultHTTPAddress = "127.0.0.1:7474"

// Server serves the API.
type Server struct {
	mgr      *manager.Manager
	log      *slog.Logger
	sb       *sandboxServer
	nd       *nodeServer
	registry *prometheus.Registry
}

// New builds a server around a manager.
func New(mgr *manager.Manager, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "server")
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.New(mgr), collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return &Server{
		mgr:      mgr,
		log:      logger,
		sb:       &sandboxServer{mgr: mgr, log: logger},
		nd:       &nodeServer{mgr: mgr},
		registry: reg,
	}
}

// Register adds the services to a gRPC server.
func (s *Server) Register(g *grpc.Server) {
	pb.RegisterSandboxServiceServer(g, s.sb)
	pb.RegisterNodeServiceServer(g, s.nd)
}

// GatewayHandler returns the REST gateway handler. It dials the gRPC
// endpoint rather than routing in-process so that server-streaming
// methods (Logs, Watch) work over HTTP as newline-delimited JSON.
func (s *Server) GatewayHandler(ctx context.Context, grpcAddr string) (http.Handler, error) {
	mux := gw.NewServeMux()
	target := grpcTarget(grpcAddr)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := pb.RegisterSandboxServiceHandlerFromEndpoint(ctx, mux, target, opts); err != nil {
		return nil, err
	}
	if err := pb.RegisterNodeServiceHandlerFromEndpoint(ctx, mux, target, opts); err != nil {
		return nil, err
	}
	return mux, nil
}

// grpcTarget converts a listen address into a gRPC dial target.
func grpcTarget(addr string) string {
	if strings.HasPrefix(addr, "tcp://") {
		return "dns:///" + strings.TrimPrefix(addr, "tcp://")
	}
	return "unix://" + strings.TrimPrefix(addr, "unix://")
}

// Listen opens a listener for "unix:///path", "/path" or "tcp://host:port".
func Listen(addr string) (net.Listener, error) {
	if strings.HasPrefix(addr, "tcp://") {
		return net.Listen("tcp", strings.TrimPrefix(addr, "tcp://"))
	}
	path := strings.TrimPrefix(addr, "unix://")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o660); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

// Serve runs the gRPC server on grpcAddr and, if httpAddr is not empty,
// the REST gateway on httpAddr, until ctx is done.
func (s *Server) Serve(ctx context.Context, grpcAddr, httpAddr string) error {
	grpcLn, err := Listen(grpcAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", grpcAddr, err)
	}
	g := grpc.NewServer(grpc.UnaryInterceptor(s.unaryLog), grpc.StreamInterceptor(s.streamLog))
	s.Register(g)

	errCh := make(chan error, 2)
	go func() { errCh <- g.Serve(grpcLn) }()
	s.log.Info("grpc listening", "addr", grpcAddr)

	var httpSrv *http.Server
	if httpAddr != "" {
		h, err := s.GatewayHandler(ctx, grpcAddr)
		if err != nil {
			g.Stop()
			return err
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
		// /debug/whole?op=checkpoint|rollback|restore&id=<sandbox>: drive the whole-sandbox
		// checkpoint mechanism by hand, for testing it.
		mux.HandleFunc("/debug/whole", func(w http.ResponseWriter, r *http.Request) {
			out, err := s.mgr.DebugWhole(r.Context(), r.URL.Query().Get("op"), r.URL.Query().Get("id"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(out + "\n"))
		})
		mux.Handle("/", h)
		httpLn, err := net.Listen("tcp", httpAddr)
		if err != nil {
			g.Stop()
			return fmt.Errorf("listen on %s: %w", httpAddr, err)
		}
		httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() { errCh <- httpSrv.Serve(httpLn) }()
		s.log.Info("http gateway listening", "addr", httpAddr)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			s.log.Error("server failed", "err", err)
		}
	}
	if httpSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = httpSrv.Shutdown(shutdownCtx)
		cancel()
	}
	stopped := make(chan struct{})
	go func() { g.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		g.Stop()
	}
	return nil
}

func (s *Server) unaryLog(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	s.log.Debug("rpc", "method", info.FullMethod, "code", status.Code(err), "dur", time.Since(start))
	return resp, err
}

func (s *Server) streamLog(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, ss)
	s.log.Debug("stream", "method", info.FullMethod, "code", status.Code(err), "dur", time.Since(start))
	return err
}

// toStatus maps manager and runtime errors to gRPC status codes.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	code := codes.Internal
	switch {
	case errors.Is(err, manager.ErrNotFound), errors.Is(err, runtime.ErrNotFound):
		code = codes.NotFound
	case errors.Is(err, manager.ErrInvalid), errors.Is(err, runtime.ErrNoStdin):
		code = codes.InvalidArgument
	case errors.Is(err, manager.ErrConflict), errors.Is(err, manager.ErrAmbiguous), errors.Is(err, runtime.ErrNotRunning):
		code = codes.FailedPrecondition
	case errors.Is(err, manager.ErrRejected):
		code = codes.ResourceExhausted
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	}
	return status.Error(code, err.Error())
}

type sandboxServer struct {
	pb.UnimplementedSandboxServiceServer
	mgr *manager.Manager
	log *slog.Logger
}

func (s *sandboxServer) CreateSandbox(ctx context.Context, req *pb.CreateSandboxRequest) (*pb.CreateSandboxResponse, error) {
	if req.GetSpec() == nil {
		return nil, status.Error(codes.InvalidArgument, "spec is required")
	}
	sb, err := s.mgr.Create(ctx, req.GetName(), fromSpec(req.GetSpec()))
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.CreateSandboxResponse{Sandbox: toSandbox(*sb)}, nil
}

func (s *sandboxServer) StartSandbox(ctx context.Context, req *pb.StartSandboxRequest) (*pb.StartSandboxResponse, error) {
	sb, err := s.mgr.StartSandbox(ctx, req.GetSandbox())
	if err != nil {
		return nil, toStatus(err)
	}
	// Return the post-start state.
	if cur, gerr := s.mgr.Get(sb.ID); gerr == nil {
		sb = cur
	}
	return &pb.StartSandboxResponse{Sandbox: toSandbox(*sb)}, nil
}

func (s *sandboxServer) GetSandbox(_ context.Context, req *pb.GetSandboxRequest) (*pb.GetSandboxResponse, error) {
	sb, err := s.mgr.Get(req.GetSandbox())
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.GetSandboxResponse{Sandbox: toSandbox(*sb)}, nil
}

func (s *sandboxServer) ListSandboxes(context.Context, *pb.ListSandboxesRequest) (*pb.ListSandboxesResponse, error) {
	list := s.mgr.List()
	out := make([]*pb.Sandbox, 0, len(list))
	for _, sb := range list {
		out = append(out, toSandbox(sb))
	}
	return &pb.ListSandboxesResponse{Sandboxes: out}, nil
}

func (s *sandboxServer) StopSandbox(ctx context.Context, req *pb.StopSandboxRequest) (*pb.StopSandboxResponse, error) {
	sb, err := s.mgr.Stop(ctx, req.GetSandbox(), time.Duration(req.GetTimeoutSeconds())*time.Second)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.StopSandboxResponse{Sandbox: toSandbox(*sb)}, nil
}

func (s *sandboxServer) DeleteSandbox(ctx context.Context, req *pb.DeleteSandboxRequest) (*pb.DeleteSandboxResponse, error) {
	if err := s.mgr.Delete(ctx, req.GetSandbox(), req.GetForce()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.DeleteSandboxResponse{}, nil
}

func (s *sandboxServer) GetStats(ctx context.Context, req *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	st, err := s.mgr.Stats(ctx, req.GetSandbox())
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.GetStatsResponse{Stats: toStats(st)}, nil
}

func (s *sandboxServer) Logs(req *pb.LogsRequest, stream pb.SandboxService_LogsServer) error {
	opts := manager.LogsOptions{Follow: req.GetFollow(), TailLines: req.GetTailLines()}
	if req.GetSince() != nil {
		opts.Since = req.GetSince().AsTime()
	}
	err := s.mgr.Logs(stream.Context(), req.GetSandbox(), opts, func(e crilog.Entry) error {
		ls := pb.LogStream_LOG_STREAM_STDOUT
		if e.Stream == crilog.Stderr {
			ls = pb.LogStream_LOG_STREAM_STDERR
		}
		return stream.Send(&pb.LogsResponse{Stream: ls, Data: e.Data, Timestamp: timestamppb.New(e.Time)})
	})
	if err != nil && stream.Context().Err() == nil {
		return toStatus(err)
	}
	return nil
}

func (s *sandboxServer) Watch(_ *pb.WatchRequest, stream pb.SandboxService_WatchServer) error {
	ch, cancel := s.mgr.Subscribe()
	defer cancel()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(toEvent(ev)); err != nil {
				return err
			}
		}
	}
}

// sender serialises Send calls on a stream.
type sender[T any] struct {
	mu   sync.Mutex
	send func(T) error
}

func (s *sender[T]) Send(m T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.send(m)
}

// chunkWriter turns io.Writer calls into stream messages. Large writes are
// split so that a message never exceeds the gRPC default limit.
type chunkWriter struct {
	send func([]byte) error
}

const maxChunk = 1 << 20

func (w chunkWriter) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		c := p
		if len(c) > maxChunk {
			c = p[:maxChunk]
		}
		if err := w.send(c); err != nil {
			return n, err
		}
		n += len(c)
		p = p[len(c):]
	}
	return n, nil
}

// errDetached is used to end a stdin pipe when the client goes away
// without closing stdin; the sandbox's own stdin stays open.
var errDetached = errors.New("client detached")

func (s *sandboxServer) Exec(stream pb.SandboxService_ExecServer) error {
	first, err := stream.Recv()
	if err != nil {
		return toStatus(err)
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first message must be start")
	}
	if len(start.GetCommand()) == 0 {
		return status.Error(codes.InvalidArgument, "command is required")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	out := &sender[*pb.ExecResponse]{send: stream.Send}
	var stdinR *io.PipeReader
	var stdinW *io.PipeWriter
	if start.GetStdin() {
		stdinR, stdinW = io.Pipe()
	}
	resize := make(chan sandbox.TerminalSize, 16)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				if stdinW != nil {
					_ = stdinW.CloseWithError(errDetached)
				}
				if !errors.Is(err, io.EOF) {
					cancel()
				}
				return
			}
			switch m := msg.GetMsg().(type) {
			case *pb.ExecRequest_Stdin:
				if stdinW != nil {
					if _, err := stdinW.Write(m.Stdin); err != nil {
						return
					}
				}
			case *pb.ExecRequest_Resize:
				select {
				case resize <- *termSize(m.Resize):
				default:
				}
			case *pb.ExecRequest_CloseStdin:
				if stdinW != nil {
					_ = stdinW.Close()
				}
			}
		}
	}()

	opts := runtime.ExecOptions{
		Command:      start.GetCommand(),
		Env:          start.GetEnv(),
		WorkingDir:   start.GetWorkingDir(),
		Stdout:       chunkWriter{func(b []byte) error { return out.Send(&pb.ExecResponse{Msg: &pb.ExecResponse_Stdout{Stdout: b}}) }},
		Stderr:       chunkWriter{func(b []byte) error { return out.Send(&pb.ExecResponse{Msg: &pb.ExecResponse_Stderr{Stderr: b}}) }},
		TTY:          start.GetTty(),
		TerminalSize: termSize(start.GetTerminalSize()),
		Resize:       resize,
	}
	if stdinR != nil {
		opts.Stdin = stdinR
	}
	res, err := s.mgr.Exec(ctx, start.GetSandbox(), opts)
	if stdinW != nil {
		_ = stdinW.CloseWithError(errDetached)
	}
	if err != nil {
		if stream.Context().Err() != nil {
			return nil
		}
		return toStatus(err)
	}
	return out.Send(&pb.ExecResponse{Msg: &pb.ExecResponse_Exit{Exit: &pb.Exit{Code: res.ExitCode}}})
}

func (s *sandboxServer) Attach(stream pb.SandboxService_AttachServer) error {
	first, err := stream.Recv()
	if err != nil {
		return toStatus(err)
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first message must be start")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	out := &sender[*pb.AttachResponse]{send: stream.Send}
	var stdinR *io.PipeReader
	var stdinW *io.PipeWriter
	if start.GetStdin() {
		stdinR, stdinW = io.Pipe()
	}
	resize := make(chan sandbox.TerminalSize, 16)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				if stdinW != nil {
					// Detach without closing the sandbox's stdin.
					_ = stdinW.CloseWithError(errDetached)
				}
				if !errors.Is(err, io.EOF) {
					cancel()
				}
				return
			}
			switch m := msg.GetMsg().(type) {
			case *pb.AttachRequest_Stdin:
				if stdinW != nil {
					if _, err := stdinW.Write(m.Stdin); err != nil {
						return
					}
				}
			case *pb.AttachRequest_Resize:
				select {
				case resize <- *termSize(m.Resize):
				default:
				}
			case *pb.AttachRequest_CloseStdin:
				if stdinW != nil {
					_ = stdinW.Close()
				}
			}
		}
	}()

	opts := runtime.AttachOptions{
		Stdout:       chunkWriter{func(b []byte) error { return out.Send(&pb.AttachResponse{Msg: &pb.AttachResponse_Stdout{Stdout: b}}) }},
		Stderr:       chunkWriter{func(b []byte) error { return out.Send(&pb.AttachResponse{Msg: &pb.AttachResponse_Stderr{Stderr: b}}) }},
		Attached:     func() { _ = out.Send(&pb.AttachResponse{Msg: &pb.AttachResponse_Attached{Attached: &pb.Attached{}}}) },
		TerminalSize: termSize(start.GetTerminalSize()),
		Resize:       resize,
	}
	if stdinR != nil {
		opts.Stdin = stdinR
	}
	err = s.mgr.Attach(ctx, start.GetSandbox(), opts)
	if stdinW != nil {
		_ = stdinW.CloseWithError(errDetached)
	}
	if err != nil {
		if stream.Context().Err() != nil {
			return nil
		}
		return toStatus(err)
	}
	// The process exited; report its code.
	sb, err := s.mgr.Get(start.GetSandbox())
	if err != nil || sb.Status.ExitCode == nil {
		return nil
	}
	return out.Send(&pb.AttachResponse{Msg: &pb.AttachResponse_Exit{Exit: &pb.Exit{Code: *sb.Status.ExitCode}}})
}

type nodeServer struct {
	pb.UnimplementedNodeServiceServer
	mgr *manager.Manager
}

func (s *nodeServer) GetNode(context.Context, *pb.GetNodeRequest) (*pb.GetNodeResponse, error) {
	n := s.mgr.Node()
	return &pb.GetNodeResponse{Node: &pb.NodeStatus{
		Policy:       n.Policy,
		Runtime:      n.Runtime,
		Capacity:     toResourceList(n.Capacity),
		Allocatable:  toResourceList(n.Allocatable),
		Allocated:    toResourceList(n.Allocated),
		SandboxCount: int32(n.Sandboxes),
		QueuedCount:  int32(n.Queued),
	}}, nil
}

func (s *nodeServer) GetVersion(context.Context, *pb.GetVersionRequest) (*pb.GetVersionResponse, error) {
	return &pb.GetVersionResponse{Version: version.Version}, nil
}
