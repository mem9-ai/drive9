package fuse

import (
	"context"
	"fmt"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

// ProfilingOptions configures optional runtime profiling for a FUSE mount.
type ProfilingOptions struct {
	CPUProfilePath      string
	HeapProfilePath     string
	ProfileDir          string
	HeapProfileInterval time.Duration
	PprofAddr           string
	PerfSamplesPath     string
	PerfSampleInterval  time.Duration
	PerfMaxSamples      int
}

// Profiler owns the profiling resources for one mount process.
type Profiler struct {
	opts ProfilingOptions

	mu      sync.Mutex
	cpuFile *os.File
	cpuPath string
	server  *http.Server
	ln      net.Listener

	stopCh chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
}

// StartProfiler starts optional CPU, heap, and HTTP pprof profiling.
func StartProfiler(opts ProfilingOptions) (*Profiler, error) {
	p := &Profiler{
		opts:   opts,
		stopCh: make(chan struct{}),
	}
	if opts.CPUProfilePath == "" &&
		opts.HeapProfilePath == "" &&
		opts.ProfileDir == "" &&
		opts.HeapProfileInterval <= 0 &&
		opts.PprofAddr == "" {
		return p, nil
	}

	if opts.HeapProfileInterval > 0 && opts.ProfileDir == "" {
		return nil, fmt.Errorf("heap profile interval requires profile dir")
	}
	if opts.CPUProfilePath != "" {
		if err := p.StartCPUProfile(opts.CPUProfilePath); err != nil {
			return nil, fmt.Errorf("start cpu profile %s: %w", opts.CPUProfilePath, err)
		}
	}
	if opts.ProfileDir != "" {
		if err := os.MkdirAll(opts.ProfileDir, 0o755); err != nil {
			p.Stop()
			return nil, fmt.Errorf("create profile dir %s: %w", opts.ProfileDir, err)
		}
	}
	if opts.HeapProfileInterval > 0 {
		p.wg.Add(1)
		go p.heapLoop()
	}
	if opts.PprofAddr != "" {
		ln, err := net.Listen("tcp", opts.PprofAddr)
		if err != nil {
			p.Stop()
			return nil, fmt.Errorf("listen pprof %s: %w", opts.PprofAddr, err)
		}
		p.ln = ln
		p.server = &http.Server{Handler: p.newPprofMux()}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			err := p.server.Serve(ln)
			if err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "drive9: pprof server failed: %v\n", err)
			}
		}()
		fmt.Fprintf(os.Stderr, "drive9: pprof listening on %s\n", ln.Addr().String())
	}
	return p, nil
}

// Stop stops active profilers and writes the final heap profile when requested.
func (p *Profiler) Stop() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.stopCh != nil {
			close(p.stopCh)
		}
		if p.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = p.server.Shutdown(ctx)
			cancel()
		}
		p.wg.Wait()
		p.stopCPUProfile()
		if p.opts.HeapProfilePath != "" {
			if err := writeHeapProfile(p.opts.HeapProfilePath); err != nil {
				fmt.Fprintf(os.Stderr, "drive9: write heap profile %s: %v\n", p.opts.HeapProfilePath, err)
			}
		}
	})
}

// StartCPUProfile starts CPU profiling into path. It is safe to call through
// the control endpoint while the mount is running.
func (p *Profiler) StartCPUProfile(path string) error {
	if p == nil {
		return fmt.Errorf("profiler is nil")
	}
	if path == "" {
		path = p.defaultCPUProfilePath()
	}
	if path == "" {
		return fmt.Errorf("cpu profile path is required")
	}
	if err := ensureParentDir(path); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create cpu profile %s: %w", path, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cpuFile != nil {
		_ = f.Close()
		return fmt.Errorf("cpu profile already running: %s", p.cpuPath)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return err
	}
	p.cpuFile = f
	p.cpuPath = path
	return nil
}

func (p *Profiler) stopCPUProfile() string {
	p.mu.Lock()
	f := p.cpuFile
	path := p.cpuPath
	p.cpuFile = nil
	p.cpuPath = ""
	p.mu.Unlock()

	if f != nil {
		pprof.StopCPUProfile()
		_ = f.Close()
	}
	return path
}

func (p *Profiler) defaultCPUProfilePath() string {
	if p.opts.CPUProfilePath != "" {
		return p.opts.CPUProfilePath
	}
	if p.opts.ProfileDir != "" {
		return filepath.Join(p.opts.ProfileDir, "cpu.pprof")
	}
	return ""
}

func (p *Profiler) heapLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.opts.HeapProfileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case t := <-ticker.C:
			name := fmt.Sprintf("heap-%s.pprof", t.UTC().Format("20060102-150405"))
			path := filepath.Join(p.opts.ProfileDir, name)
			if err := writeHeapProfile(path); err != nil {
				fmt.Fprintf(os.Stderr, "drive9: write heap profile %s: %v\n", path, err)
			}
		}
	}
}

func writeHeapProfile(path string) error {
	if err := ensureParentDir(path); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create heap profile %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		return fmt.Errorf("write heap profile %s: %w", path, err)
	}
	return nil
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create profile parent %s: %w", dir, err)
	}
	return nil
}

func (p *Profiler) newPprofMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	mux.HandleFunc("/debug/drive9/profile/cpu/start", p.handleStartCPUProfile)
	mux.HandleFunc("/debug/drive9/profile/cpu/stop", p.handleStopCPUProfile)
	return mux
}

func (p *Profiler) handleStartCPUProfile(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if err := p.StartCPUProfile(path); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if path == "" {
		path = p.defaultCPUProfilePath()
	}
	_, _ = fmt.Fprintf(w, "started cpu profile: %s\n", path)
}

func (p *Profiler) handleStopCPUProfile(w http.ResponseWriter, _ *http.Request) {
	path := p.stopCPUProfile()
	if path == "" {
		http.Error(w, "cpu profile is not running", http.StatusConflict)
		return
	}
	_, _ = fmt.Fprintf(w, "stopped cpu profile: %s\n", path)
}
