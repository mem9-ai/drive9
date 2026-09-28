// Package main runs a loopback-only Drive9 fault proxy for the X1-X5 cases.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var errDropResponse = errors.New("drop successful upstream response")

type eventLog struct {
	mu  sync.Mutex
	out *os.File
}

func (e *eventLog) write(mode, event string, count int, r *http.Request, status int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = json.NewEncoder(e.out).Encode(map[string]any{
		"at":   time.Now().UTC().Format(time.RFC3339Nano),
		"mode": mode, "event": event, "count": count,
		"method": r.Method, "path": r.URL.Path, "upstream_status": status,
	})
	_ = e.out.Sync()
}

type faultProxy struct {
	mode    string
	method  string
	path    string
	delay   time.Duration
	hold    time.Duration
	mu      sync.Mutex
	count   int
	dropped bool
	log     *eventLog
	proxy   *httputil.ReverseProxy
}

func (f *faultProxy) match(r *http.Request) bool {
	return (f.method == "" || r.Method == f.method) && strings.Contains(r.URL.Path, f.path)
}

func (f *faultProxy) next() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
	return f.count
}

func (f *faultProxy) claimResponseDrop() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dropped {
		return false
	}
	f.dropped = true
	return true
}

func reset(w http.ResponseWriter) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "connection reset unavailable", http.StatusBadGateway)
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		return
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}

func (f *faultProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/__fault_proxy_healthz" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if !f.match(r) {
		f.proxy.ServeHTTP(w, r)
		return
	}
	count := f.next()
	f.log.write(f.mode, "matched", count, r, 0)
	switch f.mode {
	case "x1":
		if count == 1 {
			f.log.write(f.mode, "hang_start", count, r, 0)
			time.Sleep(f.hold)
			f.log.write(f.mode, "hang_end_reset", count, r, 0)
			reset(w)
			return
		}
	case "x2":
		time.Sleep(f.delay)
		f.log.write(f.mode, "delayed", count, r, 0)
		if count == 2 {
			f.log.write(f.mode, "reset_after_delay", count, r, 0)
			reset(w)
			return
		}
	case "x3":
		if count == 1 {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			f.log.write(f.mode, "injected_429", count, r, http.StatusTooManyRequests)
			return
		}
	case "x4":
		if count == 1 {
			f.log.write(f.mode, "reset_during_mount", count, r, 0)
			reset(w)
			return
		}
	}
	f.proxy.ServeHTTP(w, r)
}

func newFaultProxy(target *url.URL, mode, method, path string, delay, hold time.Duration, log *eventLog) *faultProxy {
	f := &faultProxy{mode: mode, method: method, path: path, delay: delay, hold: hold, log: log}
	proxy := httputil.NewSingleHostReverseProxy(target)
	oldDirector := proxy.Director
	proxy.Director = func(r *http.Request) {
		oldDirector(r)
		r.Host = target.Host
	}
	proxy.FlushInterval = -1 // SSE must not wait for the response body to finish.
	proxy.ModifyResponse = func(response *http.Response) error {
		if f.mode == "x5" && response.Request != nil && response.StatusCode >= 200 &&
			response.StatusCode < 300 && f.match(response.Request) && f.claimResponseDrop() {
			f.log.write(f.mode, "upstream_success_response_dropped", 1,
				response.Request, response.StatusCode)
			return errDropResponse
		}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, errDropResponse) {
			reset(w)
			return
		}
		f.log.write(f.mode, "upstream_error", 0, r, 0)
		http.Error(w, "upstream error", http.StatusBadGateway)
	}
	f.proxy = proxy
	return f
}

func main() {
	listen := flag.String("listen", "127.0.0.1:18765", "loopback HTTP listener")
	upstream := flag.String("upstream", "", "HTTPS PSC upstream")
	mode := flag.String("mode", "", "fault case: x1, x2, x3, x4, x5")
	method := flag.String("match-method", "PUT", "HTTP method to fault; empty matches any")
	path := flag.String("match-path", "", "required path substring")
	events := flag.String("events", "", "exclusive JSONL evidence path")
	delay := flag.Duration("delay", 250*time.Millisecond, "x2 per-request latency")
	hold := flag.Duration("hold", 5*time.Second, "x1 blackhole duration")
	flag.Parse()
	if *upstream == "" || *events == "" || *path == "" {
		panic("upstream, events and match-path are required")
	}
	if !strings.HasPrefix(*listen, "127.0.0.1:") {
		panic("proxy must listen on 127.0.0.1")
	}
	switch *mode {
	case "x1", "x2", "x3", "x4", "x5":
	default:
		panic("unknown mode")
	}
	target, err := url.Parse(*upstream)
	if err != nil || target.Scheme != "https" || target.Host == "" {
		panic("upstream must be HTTPS")
	}
	logFile, err := os.OpenFile(*events, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()
	f := newFaultProxy(target, *mode, *method, *path, *delay, *hold, &eventLog{out: logFile})
	server := &http.Server{Addr: *listen, Handler: f, ReadHeaderTimeout: 15 * time.Second}
	fmt.Fprintln(os.Stderr, "fault proxy listening on", *listen)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
