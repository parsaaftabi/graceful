# Changelog

## v0.2.0 (2026-11-14)

Fixed a shutdown path that could leave Run waiting. When the shutdown context ran out, Run closed nothing and waited on the serve goroutine. Run and Serve now close the server by force on that path and return the error. A handler stuck past ShutdownTimeout no longer holds the process open. There is a regression test with a handler blocked for 30s, a 300ms timeout, and a 3s bound on Run returning.

Added Serve(ctx, net.Listener). Run still binds WithAddr itself. Serve takes a listener you made, for port 0 in tests, unix sockets, and sockets passed in by systemd.

Added WithMaxHeaderBytes and WithBaseContext. Both set the http.Server field of the same name.

Changed hook error handling. Hooks still run in order and all of them run. The difference is the return value. In v0.1.0 a hook error was logged and shutdown reported success. Now hook errors are logged, joined with errors.Join, and returned from Shutdown and from Run and Serve. If you checked Run for nil to mean cleanup worked, this release will show you failures that were hidden before.

Hooks now run even when draining the HTTP server fails. Before, a failed drain returned early and skipped them.

Shutdown is safe to call more than once now. Concurrent calls wait for the first one and return its result. Hooks run once.

CI runs the tests on Go 1.24 and Go 1.27.

## v0.1.0

First release.

New builds a Server with timeout defaults: ReadHeaderTimeout 5s, ReadTimeout 10s, WriteTimeout 30s, IdleTimeout 120s, ShutdownTimeout 20s, Addr :8080.

Run binds the address, handles SIGINT and SIGTERM, drains in-flight requests, and runs shutdown hooks in order. Shutdown does the same without Run. WithTLS serves HTTPS and errors when only one of cert and key is set. HTTPServer exposes the wrapped http.Server.

Hook errors were logged only in this release. Shutdown reported success either way. Fixed in v0.2.0.
