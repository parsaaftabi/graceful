# graceful

[![CI](https://github.com/parsaaftabi/graceful/actions/workflows/ci.yml/badge.svg)](https://github.com/parsaaftabi/graceful/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/parsaaftabi/graceful.svg)](https://pkg.go.dev/github.com/parsaaftabi/graceful)
[![Go Report Card](https://goreportcard.com/badge/github.com/parsaaftabi/graceful)](https://goreportcard.com/report/github.com/parsaaftabi/graceful)
[![codecov](https://codecov.io/gh/parsaaftabi/graceful/graph/badge.svg)](https://codecov.io/gh/parsaaftabi/graceful)
[![Release](https://img.shields.io/github/v/release/parsaaftabi/graceful)](https://github.com/parsaaftabi/graceful/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

graceful runs an `http.Server` with timeouts set, signal handling wired, in-flight requests drained on shutdown, and hooks for cleanup. It is one file, 346 lines, and it uses only the standard library.

```go
mux := http.NewServeMux()
mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
})

srv := graceful.New(mux,
    graceful.WithAddr(":8080"),
    graceful.WithOnShutdown(func(ctx context.Context) error {
        return db.Close()
    }),
)
if err := srv.Run(context.Background()); err != nil {
    log.Fatal(err)
}
```

## Install

```bash
go get github.com/parsaaftabi/graceful@v0.2.0
```

Requires Go 1.24 or later.

## Why

A raw `http.Server` leaves every timeout at 0. Zero means no timeout. One slow client can hold a connection open for as long as it wants, and nothing in the standard setup reacts to Ctrl+C in a terminal or SIGTERM from Kubernetes. Killing the process mid-request drops the request.

graceful sets the timeouts, handles SIGINT and SIGTERM, drains in-flight requests before exit, runs cleanup hooks in order, and puts a deadline on the shutdown itself.

## Defaults

| Setting | Default | Notes |
|---|---|---|
| Addr | `:8080` | Used by Run. Serve takes a listener instead. |
| ReadHeaderTimeout | 5s | Headers must arrive fast. This is the slowloris guard. |
| ReadTimeout | 10s | Bounds the full request read, body included. |
| WriteTimeout | 30s | Bounds the full response write, handler time included. |
| IdleTimeout | 120s | How long a keep-alive connection may sit idle. |
| ShutdownTimeout | 20s | In-flight requests and hooks must finish within this. |
| MaxHeaderBytes | 0 | 0 keeps the Go default of 1 MB. |

Every default has a `With...` option. If you run a slow endpoint, an export or a stream, raise WriteTimeout. It is the default people get wrong most often.

## Run and Serve

`Run(ctx)` binds the address from WithAddr and blocks. It returns when the context is cancelled or the process gets SIGINT or SIGTERM, after shutdown completes.

`Serve(ctx, listener)` does the same work on a listener you created. Create the listener yourself when the address has to be `:0` in a test, when the socket is a unix socket, or when the service manager passes the socket in, as with systemd socket activation.

```go
ln, err := net.Listen("tcp", "127.0.0.1:0")
if err != nil {
    log.Fatal(err)
}
srv := graceful.New(mux)
if err := srv.Serve(context.Background(), ln); err != nil {
    log.Fatal(err)
}
```

Shutdown closes the listener. Do not reuse it after Serve returns.

Two more options wrap plain `http.Server` fields:

- `WithMaxHeaderBytes(n)` caps request header size. Oversized headers get a 431 response.
- `WithBaseContext(fn)` sets the function that builds the base context for every request. Put values in there that all handlers need.

## Shutdown timeout and force close

ShutdownTimeout bounds the drain and the hooks together. If a handler is still running when it expires, the drain stops waiting. Since v0.2.0, Run and Serve then close the server by force and return an error that wraps `context.DeadlineExceeded`. The process exits. It does not sit there until the handler decides to finish.

Pick a ShutdownTimeout that covers your slowest normal request plus your hooks. 20s is the default because most services fit in it. If your slowest request takes 60s, set 70s and accept the slower exit.

## Shutdown hooks

Hooks run in order, after the listener stops accepting new connections. Each hook gets the shutdown context. Hooks are the place to close a database pool, flush buffered telemetry, and close files.

All hooks run, even when one returns an error. A failed drain does not skip the hooks either.

Behavior change in v0.2.0. Hook errors used to be logged and then dropped, so Run returned nil and the process reported a clean exit even when cleanup had failed. Now the errors are logged, collected with `errors.Join`, and returned from Shutdown and from the shutdown path of Run and Serve. Check them with `errors.Is`. Code that treated a nil return from Run as proof that cleanup worked will now see the failures it used to miss.

## Calling Shutdown yourself

`Shutdown(ctx)` shuts the server down outside Run and Serve. It is safe to call twice. It is safe to call from several goroutines at once. The first call does the work, later calls wait for it and return the same result, and hooks run once. This matters in practice because main and a signal handler and a health endpoint can all reach for shutdown at the same time.

## Examples

- [examples/basic](examples/basic) is a small service with `/healthz` and a `/slow` endpoint that sleeps for 3 seconds. Start it, request `/slow`, press Ctrl+C mid-request, and the request finishes before the process exits.

  ```bash
  cd examples/basic && go run .
  ```

- Runnable examples also live on [pkg.go.dev](https://pkg.go.dev/github.com/parsaaftabi/graceful#pkg-examples).

## Behavior notes

- Run and Serve return nil on a clean shutdown. `http.ErrServerClosed` counts as clean.
- WithTLS needs both a cert file and a key file. Passing only one is an error from Run and Serve, before anything listens.
- `HTTPServer()` returns the underlying `*http.Server` for the fields that have no option, such as ConnState.

## License

MIT. See [LICENSE](LICENSE).
