package graceful

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// freeAddr returns a localhost address with a currently-free port.
// There is an inherent TOCTOU race; acceptable for tests.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func waitUp(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not start in time")
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	s := New(http.NotFoundHandler())
	if s.srv.Addr != DefaultAddr {
		t.Errorf("Addr = %q, want %q", s.srv.Addr, DefaultAddr)
	}
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"ReadHeaderTimeout", s.srv.ReadHeaderTimeout, DefaultReadHeaderTimeout},
		{"ReadTimeout", s.srv.ReadTimeout, DefaultReadTimeout},
		{"WriteTimeout", s.srv.WriteTimeout, DefaultWriteTimeout},
		{"IdleTimeout", s.srv.IdleTimeout, DefaultIdleTimeout},
		{"shutdownTimeout", s.shutdownTimeout, DefaultShutdownTimeout},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestOptions(t *testing.T) {
	t.Parallel()
	baseCtx := func(net.Listener) context.Context { return context.Background() }
	s := New(http.NotFoundHandler(),
		WithAddr("127.0.0.1:9999"),
		WithReadHeaderTimeout(time.Second),
		WithReadTimeout(2*time.Second),
		WithWriteTimeout(3*time.Second),
		WithIdleTimeout(4*time.Second),
		WithShutdownTimeout(5*time.Second),
		WithMaxHeaderBytes(12345),
		WithBaseContext(baseCtx),
	)

	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "addr", got: s.srv.Addr, want: "127.0.0.1:9999"},
		{name: "read header timeout", got: s.srv.ReadHeaderTimeout, want: time.Second},
		{name: "read timeout", got: s.srv.ReadTimeout, want: 2 * time.Second},
		{name: "write timeout", got: s.srv.WriteTimeout, want: 3 * time.Second},
		{name: "idle timeout", got: s.srv.IdleTimeout, want: 4 * time.Second},
		{name: "shutdown timeout", got: s.shutdownTimeout, want: 5 * time.Second},
		{name: "max header bytes", got: s.srv.MaxHeaderBytes, want: 12345},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.got != tt.want {
				t.Errorf("got %v, want %v", tt.got, tt.want)
			}
		})
	}
	if s.srv.BaseContext == nil {
		t.Error("BaseContext was not applied")
	}
}

func TestWithBaseContext_NilKeepsDefault(t *testing.T) {
	t.Parallel()
	s := New(http.NotFoundHandler(), WithBaseContext(nil))
	if s.srv.BaseContext != nil {
		t.Error("nil BaseContext should keep the standard library default")
	}
}

func TestRun_ServesRequests(t *testing.T) {
	addr := freeAddr(t)
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}), WithAddr(addr))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	waitUp(t, addr)

	resp, err := http.Get("http://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRun_DrainsInflightRequests(t *testing.T) {
	addr := freeAddr(t)
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond) // simulate real work
		w.Write([]byte("done"))
	}), WithAddr(addr), WithShutdownTimeout(5*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	waitUp(t, addr)

	// Fire a request, then cancel while it is in flight.
	respCh := make(chan string, 1)
	go func() {
		resp, err := http.Get("http://" + addr)
		if err != nil {
			respCh <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		respCh <- string(body)
	}()
	time.Sleep(100 * time.Millisecond) // ensure the handler is sleeping
	cancel()

	select {
	case body := <-respCh:
		if body != "done" {
			t.Errorf("in-flight request got %q, want %q (was killed mid-flight)", body, "done")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed")
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after drain")
	}
}

func TestRun_HooksRunInOrder(t *testing.T) {
	addr := freeAddr(t)
	var order []int
	var hookCtxHasDeadline atomic.Bool
	s := New(http.NotFoundHandler(), WithAddr(addr), WithOnShutdown(
		func(ctx context.Context) error {
			_, hasDeadline := ctx.Deadline()
			hookCtxHasDeadline.Store(hasDeadline)
			order = append(order, 1)
			return nil
		},
		func(ctx context.Context) error {
			order = append(order, 2)
			return nil
		},
	))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	waitUp(t, addr)
	cancel()

	if err := <-runErr; err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Errorf("hooks ran in order %v, want [1 2]", order)
	}
	if !hookCtxHasDeadline.Load() {
		t.Error("hook context had no deadline")
	}
}

// TestRun_HookErrorPropagates locks the v0.2.0 behavior change: hook errors
// used to be logged and swallowed. Now every hook still runs, and Run
// returns the hook error.
func TestRun_HookErrorPropagates(t *testing.T) {
	addr := freeAddr(t)
	hookErr := errors.New("test hook error")
	var secondRan atomic.Bool
	s := New(http.NotFoundHandler(), WithAddr(addr), WithOnShutdown(
		func(ctx context.Context) error { return hookErr },
		func(ctx context.Context) error { secondRan.Store(true); return nil },
	))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	waitUp(t, addr)
	cancel()

	err := <-runErr
	if !errors.Is(err, hookErr) {
		t.Errorf("Run returned %v, want it to wrap the hook error", err)
	}
	if !secondRan.Load() {
		t.Error("second hook did not run after first hook failed")
	}
}

func TestRun_TLSConfigError(t *testing.T) {
	t.Parallel()
	s := New(http.NotFoundHandler(), WithTLS("cert.pem", ""))
	if err := s.Run(context.Background()); err == nil {
		t.Error("expected error for cert-without-key, got nil")
	}
}

func TestHTTPServerAccessor(t *testing.T) {
	t.Parallel()
	s := New(http.NotFoundHandler(), WithAddr("127.0.0.1:12345"))
	if s.HTTPServer() != s.srv {
		t.Error("HTTPServer() did not return the underlying server")
	}
	if s.HTTPServer().Addr != "127.0.0.1:12345" {
		t.Errorf("Addr = %q", s.HTTPServer().Addr)
	}
}

func TestWithLogger(t *testing.T) {
	t.Parallel()
	def := New(http.NotFoundHandler(), WithLogger(nil))
	if def.logger == nil {
		t.Error("nil logger replaced the default")
	}
	custom := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(http.NotFoundHandler(), WithLogger(custom))
	if s.logger != custom {
		t.Error("custom logger was not applied")
	}
}

func TestRun_BadAddress(t *testing.T) {
	s := New(http.NotFoundHandler(), WithAddr("127.0.0.1:notaport"))
	if err := s.Run(context.Background()); err == nil {
		t.Error("expected error for bad listen address, got nil")
	}
}

func TestShutdown_Standalone(t *testing.T) {
	addr := freeAddr(t)
	var hookRan atomic.Bool
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}), WithAddr(addr), WithOnShutdown(func(ctx context.Context) error {
		hookRan.Store(true)
		return nil
	}))

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.serve() }()
	waitUp(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned %v", err)
	}
	if !hookRan.Load() {
		t.Error("shutdown hook did not run")
	}
	if err := <-serveErr; err != nil {
		t.Errorf("serve returned %v after Shutdown, want nil", err)
	}
}

// TestRun_ShutdownTimeoutForceClose is the regression test for the old Run
// shutdown path. A handler that outlives ShutdownTimeout must not keep Run
// waiting. Run has to close the server by force and return the shutdown
// error within a bounded time.
func TestRun_ShutdownTimeoutForceClose(t *testing.T) {
	addr := freeAddr(t)
	handlerStarted := make(chan struct{})
	var startedOnce sync.Once
	var hookRan atomic.Bool
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() { close(handlerStarted) })
		select {
		case <-r.Context().Done():
		case <-time.After(30 * time.Second):
		}
	}), WithAddr(addr),
		WithShutdownTimeout(300*time.Millisecond),
		WithOnShutdown(func(ctx context.Context) error {
			hookRan.Store(true)
			return nil
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	waitUp(t, addr)

	go func() {
		resp, err := http.Get("http://" + addr)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()

	select {
	case <-handlerStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never started")
	}

	start := time.Now()
	cancel()
	select {
	case err := <-runErr:
		if err == nil {
			t.Error("Run returned nil, want the shutdown timeout error")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Run error = %v, want it to wrap context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Errorf("Run took %v after cancel, want under 3s", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s of cancel; shutdown timeout did not force close")
	}
	if !hookRan.Load() {
		t.Error("shutdown hook did not run on the timeout path")
	}
}

func TestServe_OnListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("via listener"))
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Serve(ctx, ln) }()
	waitUp(t, addr)

	resp, err := http.Get("http://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "via listener" {
		t.Errorf("body = %q, want %q", body, "via listener")
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

func TestServe_NilListener(t *testing.T) {
	t.Parallel()
	s := New(http.NotFoundHandler())
	if err := s.Serve(context.Background(), nil); err == nil {
		t.Error("expected error for nil listener, got nil")
	}
}

func TestServe_BaseContextValueReachesHandler(t *testing.T) {
	type ctxKey struct{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, _ := r.Context().Value(ctxKey{}).(string)
		w.Write([]byte(v))
	}), WithBaseContext(func(net.Listener) context.Context {
		return context.WithValue(context.Background(), ctxKey{}, "from base")
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Serve(ctx, ln) }()
	waitUp(t, addr)

	resp, err := http.Get("http://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "from base" {
		t.Errorf("body = %q, want %q", body, "from base")
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Errorf("Serve returned %v, want nil", err)
	}
}

func TestServe_MaxHeaderBytesRejectsOversizedHeader(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}), WithMaxHeaderBytes(2048))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Serve(ctx, ln) }()
	waitUp(t, addr)

	req, err := http.NewRequest(http.MethodGet, "http://"+addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Big", strings.Repeat("a", 8192))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusRequestHeaderFieldsTooLarge)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Errorf("Serve returned %v, want nil", err)
	}
}

func TestShutdown_CalledTwiceRunsHooksOnce(t *testing.T) {
	addr := freeAddr(t)
	var hookCalls atomic.Int32
	s := New(http.NotFoundHandler(), WithAddr(addr), WithOnShutdown(
		func(ctx context.Context) error {
			hookCalls.Add(1)
			return nil
		},
	))

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.serve() }()
	waitUp(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("first Shutdown returned %v", err)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown returned %v, want the first result (nil)", err)
	}
	if got := hookCalls.Load(); got != 1 {
		t.Errorf("hook ran %d times, want 1", got)
	}
	if err := <-serveErr; err != nil {
		t.Errorf("serve returned %v after Shutdown, want nil", err)
	}
}

func TestShutdown_ConcurrentCallsShareResult(t *testing.T) {
	addr := freeAddr(t)
	hookErr := errors.New("pool close failed")
	var hookCalls atomic.Int32
	s := New(http.NotFoundHandler(), WithAddr(addr), WithOnShutdown(
		func(ctx context.Context) error {
			hookCalls.Add(1)
			time.Sleep(50 * time.Millisecond)
			return hookErr
		},
	))

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.serve() }()
	waitUp(t, addr)

	const callers = 5
	results := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			results[i] = s.Shutdown(ctx)
		}()
	}
	wg.Wait()

	for i, err := range results {
		if !errors.Is(err, hookErr) {
			t.Errorf("caller %d got %v, want the shared hook error", i, err)
		}
	}
	if got := hookCalls.Load(); got != 1 {
		t.Errorf("hook ran %d times, want 1", got)
	}
	if err := <-serveErr; err != nil {
		t.Errorf("serve returned %v after Shutdown, want nil", err)
	}
}

func TestShutdown_HookErrorsJoined(t *testing.T) {
	errA := errors.New("flush failed")
	errB := errors.New("pool close failed")
	s := New(http.NotFoundHandler(), WithOnShutdown(
		func(ctx context.Context) error { return errA },
		func(ctx context.Context) error { return errB },
	))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.Shutdown(ctx)
	if !errors.Is(err, errA) {
		t.Errorf("Shutdown error %v does not wrap first hook error", err)
	}
	if !errors.Is(err, errB) {
		t.Errorf("Shutdown error %v does not wrap second hook error", err)
	}
}

// writeTestCert generates a self-signed cert/key pair in t.TempDir.
func writeTestCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func TestRun_TLS(t *testing.T) {
	certFile, keyFile := writeTestCert(t)
	addr := freeAddr(t)
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("secure"))
	}), WithAddr(addr), WithTLS(certFile, keyFile))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	waitUp(t, addr)

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} // test-only self-signed cert
	resp, err := client.Get("https://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "secure" {
		t.Errorf("body = %q, want %q", body, "secure")
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
