// Command drive9-create-bench benchmarks Drive9 tenant provisioning.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	reportSchema      = "drive9-provision-bench/v2"
	envServer         = "DRIVE9_SERVER"
	envPublicKey      = "DRIVE9_PUBLIC_KEY"
	envPrivateKey     = "DRIVE9_PRIVATE_KEY"
	maxBodyBytes      = 1 << 20
	histogramBarWidth = 50
)

type benchConfig struct {
	Server         string
	Out            string
	Inventory      string
	SampleOut      string
	SampleSeed     string
	PublicKey      string
	PrivateKey     string
	SpendingLimit  *int64
	Total          int
	SampleSize     int
	Concurrency    int
	RPS            float64
	WaitReady      bool
	PollInterval   time.Duration
	Timeout        time.Duration
	ReportInterval time.Duration
}

type reportConfig struct {
	Server         string        `json:"server"`
	Mode           string        `json:"mode"`
	Inventory      string        `json:"inventory,omitempty"`
	SampleOut      string        `json:"sample_out,omitempty"`
	SampleSeed     string        `json:"sample_seed,omitempty"`
	SpendingLimit  *int64        `json:"tidbcloud_spending_limit,omitempty"`
	Total          int           `json:"total"`
	SampleSize     int           `json:"sample_size,omitempty"`
	Concurrency    int           `json:"concurrency"`
	RPS            float64       `json:"rps"`
	WaitReady      bool          `json:"wait_ready"`
	PollInterval   time.Duration `json:"poll_interval_ns"`
	Timeout        time.Duration `json:"timeout_ns"`
	ReportInterval time.Duration `json:"report_interval_ns"`
}

type benchmarkReport struct {
	SchemaVersion string           `json:"schema_version"`
	StartedAt     time.Time        `json:"started_at"`
	FinishedAt    time.Time        `json:"finished_at"`
	Config        reportConfig     `json:"config"`
	Summary       benchmarkSummary `json:"summary"`
	Inventory     inventorySummary `json:"inventory"`
	Failures      []tenantFailure  `json:"failure_samples,omitempty"`
}

type inventorySummary struct {
	Path            string `json:"path,omitempty"`
	Records         int    `json:"records"`
	Active          int    `json:"active"`
	SampleOutput    string `json:"sample_output,omitempty"`
	SampleRequested int    `json:"sample_requested,omitempty"`
	SampleWritten   int    `json:"sample_written,omitempty"`
}

type benchmarkSummary struct {
	Requested         int            `json:"requested"`
	Completed         int            `json:"completed"`
	Success           int            `json:"success"`
	Failed            int            `json:"failed"`
	ProvisionRequests int            `json:"provision_requests"`
	StatusRequests    int            `json:"status_requests"`
	ElapsedSeconds    float64        `json:"elapsed_seconds"`
	TenantsPerMinute  float64        `json:"tenants_per_minute"`
	ProvisionLatency  latencySummary `json:"provision_latency_seconds"`
	ReadyLatency      latencySummary `json:"ready_latency_seconds"`
}

type tenantResult struct {
	Index             int     `json:"index"`
	TenantID          string  `json:"tenant_id,omitempty"`
	InitialStatus     string  `json:"initial_status,omitempty"`
	FinalStatus       string  `json:"final_status,omitempty"`
	ProvisionAccepted bool    `json:"provision_accepted"`
	Success           bool    `json:"success"`
	Error             string  `json:"error,omitempty"`
	ProvisionSeconds  float64 `json:"provision_seconds,omitempty"`
	ReadySeconds      float64 `json:"ready_seconds,omitempty"`
	StatusRequests    int     `json:"status_requests"`
	ProvisionRequests int     `json:"-"`
	readyMeasured     bool
	APIKey            string    `json:"-"`
	CreatedAt         time.Time `json:"-"`
}

type provisionResponse struct {
	TenantID string `json:"tenant_id"`
	APIKey   string `json:"api_key"`
	Status   string `json:"status"`
}

type tenantStatusResponse struct {
	Status string `json:"status"`
}

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type optionalInt64 struct {
	set   bool
	value int64
}

func (v *optionalInt64) String() string {
	if !v.set {
		return ""
	}
	return strconv.FormatInt(v.value, 10)
}

func (v *optionalInt64) Set(raw string) error {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return fmt.Errorf("must be a non-negative integer")
	}
	v.set = true
	v.value = value
	return nil
}

func main() {
	os.Exit(runMain(
		os.Args[1:],
		os.Getenv,
		os.UserHomeDir,
		os.Stdout,
		os.Stderr,
	))
}

func runMain(
	args []string,
	getenv func(string) string,
	userHomeDir func() (string, error),
	stdout, stderr io.Writer,
) int {
	if len(args) > 0 && args[0] == "sample" {
		return runSampleMain(args[1:], stdout, stderr)
	}

	homeDir, err := userHomeDir()
	if err != nil {
		_, _ = fmt.Fprintf(
			stderr,
			"drive9-create-bench: determine home directory: %v\n",
			err,
		)
		return 2
	}
	cfg, err := parseConfig(args, getenv, homeDir, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "drive9-create-bench: %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	progressLogger := log.New(stdout, "", log.LstdFlags)
	errorLogger := log.New(stderr, "", log.LstdFlags)
	client := newBenchHTTPClient(cfg)
	defer client.CloseIdleConnections()
	report, runErr := runBenchmark(ctx, cfg, client, progressLogger, errorLogger)
	if reportErr := writeReport(cfg.Out, report); reportErr != nil {
		errorLogger.Printf("write report: %v", reportErr)
		return 1
	}
	printSummary(stdout, report)
	if runErr != nil {
		errorLogger.Printf("benchmark stopped: %v", runErr)
		return 1
	}
	if report.Summary.Failed > 0 {
		return 1
	}
	return 0
}

func newBenchHTTPClient(cfg benchConfig) *http.Client {
	var transport *http.Transport
	if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = defaultTransport.Clone()
	} else {
		transport = &http.Transport{Proxy: http.ProxyFromEnvironment}
	}
	workerConnections := int64(cfg.Concurrency) * 2
	connectionLimit := int(min(int64(10_000), max(int64(100), workerConnections)))
	transport.MaxIdleConns = connectionLimit
	transport.MaxIdleConnsPerHost = connectionLimit
	transport.MaxConnsPerHost = connectionLimit
	return &http.Client{Transport: transport}
}

func parseConfig(
	args []string,
	getenv func(string) string,
	homeDir string,
	output io.Writer,
) (benchConfig, error) {
	configFile := ""
	if strings.TrimSpace(homeDir) != "" {
		configFile = filepath.Join(homeDir, ".drive9", "bench", "config.json")
	}
	cfg := benchConfig{
		Total:          1,
		Concurrency:    1,
		RPS:            1,
		WaitReady:      true,
		PollInterval:   5 * time.Second,
		Timeout:        10 * time.Minute,
		SampleSeed:     "drive9-create-bench",
		ReportInterval: 10 * time.Second,
	}
	var spendingLimit optionalInt64
	fs := flag.NewFlagSet("drive9-create-bench", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&configFile, "config", configFile, "optional benchmark input config file (mode 0600)")
	fs.StringVar(&cfg.Server, "server", cfg.Server, "Drive9 server URL (or DRIVE9_SERVER)")
	fs.StringVar(&cfg.Out, "out", "", "JSON report output path (required)")
	fs.StringVar(&cfg.Inventory, "inventory", "", "append-only credential inventory JSONL output path")
	fs.IntVar(&cfg.Total, "total", cfg.Total, "total tenants to provision")
	fs.IntVar(&cfg.SampleSize, "sample-size", 0, "active spaces to select from this run")
	fs.StringVar(&cfg.SampleSeed, "sample-seed", cfg.SampleSeed, "deterministic active-space selection seed")
	fs.StringVar(&cfg.SampleOut, "sample-out", "", "drive9-space-bench JSON snapshot output path")
	fs.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "concurrent workers")
	fs.Float64Var(&cfg.RPS, "rps", cfg.RPS, "maximum provision requests per second")
	fs.BoolVar(&cfg.WaitReady, "wait-ready", cfg.WaitReady, "wait for every tenant to become active")
	fs.DurationVar(&cfg.PollInterval, "poll-interval", cfg.PollInterval, "tenant status polling interval")
	fs.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "per-tenant provision and readiness timeout")
	fs.DurationVar(&cfg.ReportInterval, "report-interval", cfg.ReportInterval, "progress log interval")
	fs.Var(&spendingLimit, "tidbcloud-spending-limit", "monthly TiDB Cloud spending limit")
	fs.Usage = func() {
		_, _ = fmt.Fprintf(
			output,
			"Usage: drive9-create-bench [flags]\n"+
				"       drive9-create-bench sample [flags]\n\n",
		)
		fs.PrintDefaults()
		_, _ = fmt.Fprintf(output,
			"\nConfiguration precedence: flags > non-empty environment variables > "+
				"config file > built-in defaults.\n"+
				"Credentials are read from the config file, %s, or %s; "+
				"both must be set together.\n",
			envPublicKey, envPrivateKey)
	}
	if err := fs.Parse(args); err != nil {
		return benchConfig{}, err
	}
	if fs.NArg() != 0 {
		return benchConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	configFile = strings.TrimSpace(configFile)
	fileCfg, err := loadCreateBenchFileConfig(configFile, flagProvided(fs, "config"))
	if err != nil {
		return benchConfig{}, err
	}
	if !flagProvided(fs, "server") {
		if value := strings.TrimSpace(getenv(envServer)); value != "" {
			cfg.Server = value
		} else if fileCfg.Server != nil {
			cfg.Server = strings.TrimSpace(*fileCfg.Server)
		}
	}
	if value := strings.TrimSpace(getenv(envPublicKey)); value != "" {
		cfg.PublicKey = value
	} else if fileCfg.PublicKey != nil {
		cfg.PublicKey = strings.TrimSpace(*fileCfg.PublicKey)
	}
	if value := strings.TrimSpace(getenv(envPrivateKey)); value != "" {
		cfg.PrivateKey = value
	} else if fileCfg.PrivateKey != nil {
		cfg.PrivateKey = strings.TrimSpace(*fileCfg.PrivateKey)
	}

	cfg.Server = strings.TrimRight(strings.TrimSpace(cfg.Server), "/")
	cfg.Out = strings.TrimSpace(cfg.Out)
	cfg.Inventory = strings.TrimSpace(cfg.Inventory)
	cfg.SampleOut = strings.TrimSpace(cfg.SampleOut)
	cfg.SampleSeed = strings.TrimSpace(cfg.SampleSeed)
	if err := validateConfig(cfg, spendingLimit); err != nil {
		return benchConfig{}, err
	}
	if spendingLimit.set {
		cfg.SpendingLimit = &spendingLimit.value
	}
	return cfg, nil
}

func validateConfig(cfg benchConfig, spendingLimit optionalInt64) error {
	if cfg.Server == "" {
		return fmt.Errorf(
			"server is required; pass --server, set %s, or use --config",
			envServer,
		)
	}
	u, err := url.Parse(cfg.Server)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("server must be an absolute http or https URL")
	}
	if u.User != nil {
		return fmt.Errorf("server URL must not contain credentials")
	}
	if strings.ContainsAny(cfg.Server, "?#") {
		return fmt.Errorf("server URL must not contain a query or fragment")
	}
	if cfg.Out == "" {
		return fmt.Errorf("out is required")
	}
	if cfg.Inventory != "" && samePath(cfg.Inventory, cfg.Out) {
		return fmt.Errorf("out and inventory must use different paths")
	}
	if cfg.Total <= 0 {
		return fmt.Errorf("total must be positive")
	}
	if cfg.SampleSize < 0 {
		return fmt.Errorf("sample-size must not be negative")
	}
	if cfg.SampleSize > cfg.Total {
		return fmt.Errorf("sample-size must not exceed total")
	}
	if cfg.SampleSize > 0 {
		if cfg.Inventory == "" {
			return fmt.Errorf("sample-size requires inventory")
		}
		if cfg.SampleOut == "" {
			return fmt.Errorf("sample-size requires sample-out")
		}
		if cfg.SampleSeed == "" {
			return fmt.Errorf("sample-size requires a non-empty sample-seed")
		}
	}
	if cfg.SampleSize == 0 && cfg.SampleOut != "" {
		return fmt.Errorf("sample-out requires a positive sample-size")
	}
	if cfg.SampleOut != "" &&
		(samePath(cfg.SampleOut, cfg.Out) || samePath(cfg.SampleOut, cfg.Inventory)) {
		return fmt.Errorf("out, inventory, and sample-out must use different paths")
	}
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be positive")
	}
	if cfg.RPS <= 0 || math.IsNaN(cfg.RPS) || math.IsInf(cfg.RPS, 0) {
		return fmt.Errorf("rps must be positive and finite")
	}
	intervalNanos := float64(time.Second) / cfg.RPS
	if intervalNanos < 1 || intervalNanos > float64(math.MaxInt64) {
		return fmt.Errorf("rps must produce an interval representable by time.Duration")
	}
	if cfg.PollInterval <= 0 {
		return fmt.Errorf("poll-interval must be positive")
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if cfg.ReportInterval <= 0 {
		return fmt.Errorf("report-interval must be positive")
	}
	if (cfg.PublicKey == "") != (cfg.PrivateKey == "") {
		return fmt.Errorf("%s and %s must be set together", envPublicKey, envPrivateKey)
	}
	if spendingLimit.set && cfg.PublicKey == "" {
		return fmt.Errorf("--tidbcloud-spending-limit requires %s and %s", envPublicKey, envPrivateKey)
	}
	return nil
}

func flagProvided(flags *flag.FlagSet, name string) bool {
	provided := false
	flags.Visit(func(current *flag.Flag) {
		if current.Name == name {
			provided = true
		}
	})
	return provided
}

func samePath(left, right string) bool {
	if left == "" || right == "" {
		return false
	}
	leftAbsolute, leftErr := filepath.Abs(left)
	rightAbsolute, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return filepath.Clean(left) == filepath.Clean(right)
	}
	return filepath.Clean(leftAbsolute) == filepath.Clean(rightAbsolute)
}

func runBenchmark(
	ctx context.Context,
	cfg benchConfig,
	client httpDoer,
	progressLogger *log.Logger,
	errorLogger *log.Logger,
) (benchmarkReport, error) {
	started := time.Now().UTC()
	if progressLogger == nil {
		progressLogger = log.New(io.Discard, "", 0)
	}
	if errorLogger == nil {
		errorLogger = log.New(io.Discard, "", 0)
	}
	accumulator := newBenchmarkAccumulator(cfg.Total)
	inventoryState := inventorySummary{
		Path:            cfg.Inventory,
		SampleOutput:    cfg.SampleOut,
		SampleRequested: cfg.SampleSize,
	}
	finishReport := func() benchmarkReport {
		finished := time.Now().UTC()
		summary, failures := accumulator.Snapshot(finished.Sub(started))
		return benchmarkReport{
			SchemaVersion: reportSchema,
			StartedAt:     started,
			FinishedAt:    finished,
			Config:        safeReportConfig(cfg),
			Summary:       summary,
			Inventory:     inventoryState,
			Failures:      failures,
		}
	}

	if cfg.SampleSize > 0 {
		if err := ensureFileDoesNotExist(cfg.SampleOut); err != nil {
			return finishReport(), err
		}
		if err := ensurePrivateDirectory(
			filepath.Dir(cfg.SampleOut),
			"selected snapshot",
		); err != nil {
			return finishReport(), err
		}
	}
	var inventory *inventoryWriter
	if cfg.Inventory != "" {
		var err error
		inventory, err = openInventoryWriter(cfg.Inventory)
		if err != nil {
			return finishReport(), err
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := min(cfg.Concurrency, cfg.Total)
	jobs := make(chan int)
	results := make(chan tenantResult, workers)
	limiter := newPacedLimiter(cfg.RPS)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if err := limiter.Wait(runCtx); err != nil {
					results <- tenantResult{Index: index, Error: err.Error()}
					continue
				}
				results <- runTenant(runCtx, cfg, client, index, errorLogger)
			}
		}()
	}

	go func() {
		defer close(jobs)
		for index := 0; index < cfg.Total; index++ {
			select {
			case jobs <- index:
			case <-runCtx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	reportInterval := cfg.ReportInterval
	if reportInterval <= 0 {
		reportInterval = 10 * time.Second
	}
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()
	sampler := newSpaceSampler(cfg.SampleSize, cfg.SampleSeed)
	window := newProgressWindow()
	windowStarted := time.Now()
	var runErr error
	inventoryHealthy := true
	for results != nil {
		select {
		case result, open := <-results:
			if !open {
				results = nil
				continue
			}
			accumulator.Record(result)
			window.Record(result)
			if !result.Success {
				logTenantFailure(errorLogger, result)
			}
			if inventory == nil || !result.ProvisionAccepted || !inventoryHealthy {
				continue
			}
			record := inventoryRecordFromResult(cfg, result)
			if err := inventory.Append(record); err != nil {
				runErr = fmt.Errorf("persist inventory: %w", err)
				inventoryHealthy = false
				cancel()
				continue
			}
			inventoryState.Records++
			if record.Active {
				inventoryState.Active++
			}
			sampler.Offer(record)
		case reportedAt := <-ticker.C:
			logProgressWindow(
				progressLogger,
				window.SnapshotAndReset(reportedAt.Sub(windowStarted)),
				accumulator,
				reportedAt.Sub(started),
				inventoryState.Active,
			)
			windowStarted = reportedAt
		}
	}
	if window.completed > 0 {
		reportedAt := time.Now()
		logProgressWindow(
			progressLogger,
			window.SnapshotAndReset(reportedAt.Sub(windowStarted)),
			accumulator,
			reportedAt.Sub(started),
			inventoryState.Active,
		)
	}

	if inventory != nil {
		if err := inventory.Close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("close inventory: %w", err)
		}
	}
	if runErr == nil && ctx.Err() != nil {
		runErr = ctx.Err()
	}
	if runErr == nil && cfg.SampleSize > 0 {
		spaces := sampler.Spaces()
		if len(spaces) != cfg.SampleSize {
			runErr = fmt.Errorf(
				"only %d active spaces available; sample-size requires %d",
				len(spaces),
				cfg.SampleSize,
			)
		} else if err := writeSelectedSnapshot(cfg.SampleOut, cfg.Server, spaces); err != nil {
			runErr = err
		} else {
			inventoryState.SampleWritten = len(spaces)
		}
	}
	return finishReport(), runErr
}

func logProgressWindow(
	logger *log.Logger,
	window progressWindowSnapshot,
	accumulator *benchmarkAccumulator,
	elapsed time.Duration,
	activeInventory int,
) {
	total, _ := accumulator.Snapshot(elapsed)
	logger.Printf(
		"progress: window_seconds=%.3f window_completed=%d window_success=%d "+
			"window_failed=%d total_completed=%d/%d total_success=%d "+
			"total_failed=%d active_inventory=%d window_tenants_per_minute=%.2f "+
			"total_tenants_per_minute=%.2f",
		window.ElapsedSeconds,
		window.Completed,
		window.Success,
		window.Failed,
		total.Completed,
		total.Requested,
		total.Success,
		total.Failed,
		activeInventory,
		window.TenantsPerMinute,
		total.TenantsPerMinute,
	)
	logWindowLatency(logger, "provision", window.ProvisionLatency)
	logWindowLatency(logger, "ready", window.ReadyLatency)
}

func logWindowLatency(
	logger *log.Logger,
	name string,
	latency windowLatencySummary,
) {
	logger.Printf(
		"%s_latency_window: samples=%d average_seconds=%.6f "+
			"p50_seconds=%.6f p90_seconds=%.6f p95_seconds=%.6f "+
			"p99_seconds=%.6f max_seconds=%.6f",
		name,
		latency.SampleCount,
		latency.Average,
		latency.P50,
		latency.P90,
		latency.P95,
		latency.P99,
		latency.Max,
	)
}

func logTenantFailure(logger *log.Logger, result tenantResult) {
	logger.Printf(
		"tenant failure: index=%d tenant=%q provision_accepted=%t "+
			"initial_status=%q final_status=%q status_requests=%d "+
			"provision_seconds=%.6f ready_seconds=%.6f error=%q",
		result.Index,
		result.TenantID,
		result.ProvisionAccepted,
		result.InitialStatus,
		result.FinalStatus,
		result.StatusRequests,
		result.ProvisionSeconds,
		result.ReadySeconds,
		result.Error,
	)
}

func ensureFileDoesNotExist(path string) error {
	_, err := os.Lstat(path)
	if err == nil {
		return fmt.Errorf("output already exists: %s", path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect output %s: %w", path, err)
	}
	return nil
}

func inventoryRecordFromResult(cfg benchConfig, result tenantResult) inventoryRecord {
	return inventoryRecord{
		SchemaVersion:    inventorySchema,
		Sequence:         result.Index,
		Server:           cfg.Server,
		TenantID:         result.TenantID,
		APIKey:           result.APIKey,
		CreatedAt:        result.CreatedAt,
		SpendingLimit:    cfg.SpendingLimit,
		InitialStatus:    result.InitialStatus,
		FinalStatus:      result.FinalStatus,
		ProvisionSeconds: result.ProvisionSeconds,
		ReadySeconds:     result.ReadySeconds,
		StatusRequests:   result.StatusRequests,
		Active:           result.Success && result.FinalStatus == "active",
		Error:            result.Error,
	}
}

func runTenant(
	parent context.Context,
	cfg benchConfig,
	client httpDoer,
	index int,
	logger *log.Logger,
) tenantResult {
	result := tenantResult{Index: index, ProvisionRequests: 1}
	ctx, cancel := context.WithTimeout(parent, cfg.Timeout)
	defer cancel()

	started := time.Now()
	accepted, err := provisionTenant(ctx, cfg, client)
	provisionLatency := time.Since(started)
	result.ProvisionSeconds = provisionLatency.Seconds()
	if err != nil {
		logRequestError(
			logger,
			"provision",
			index,
			"",
			1,
			provisionLatency,
			err,
			cfg.PublicKey,
			cfg.PrivateKey,
		)
		result.Error = redactSecrets(err.Error(), cfg.PublicKey, cfg.PrivateKey)
		return result
	}
	result.ProvisionAccepted = true
	result.TenantID = accepted.TenantID
	result.APIKey = accepted.APIKey
	result.CreatedAt = time.Now().UTC()
	result.InitialStatus = accepted.Status
	result.FinalStatus = accepted.Status
	if !cfg.WaitReady {
		result.Success = true
		return result
	}
	if accepted.Status == "active" {
		result.Success = true
		result.ReadySeconds = time.Since(started).Seconds()
		result.readyMeasured = true
		return result
	}

	status, requests, err := waitTenantReady(
		ctx,
		cfg,
		client,
		accepted,
		index,
		logger,
	)
	result.StatusRequests = requests
	result.FinalStatus = status
	if err != nil {
		result.Error = redactSecrets(err.Error(), cfg.PublicKey, cfg.PrivateKey, accepted.APIKey)
		return result
	}
	result.Success = true
	result.ReadySeconds = time.Since(started).Seconds()
	result.readyMeasured = true
	return result
}

func provisionTenant(ctx context.Context, cfg benchConfig, client httpDoer) (provisionResponse, error) {
	var body io.Reader
	if cfg.PublicKey != "" {
		payload := map[string]any{
			"public_key":  cfg.PublicKey,
			"private_key": cfg.PrivateKey,
		}
		if cfg.SpendingLimit != nil {
			payload["tidbcloud_spending_limit"] = *cfg.SpendingLimit
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return provisionResponse{}, err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Server+"/v1/provision", body)
	if err != nil {
		return provisionResponse{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return provisionResponse{}, fmt.Errorf("provision request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return provisionResponse{}, fmt.Errorf("read provision response: %w", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		return provisionResponse{}, responseError("provision", resp.StatusCode, raw)
	}
	var accepted provisionResponse
	if err := json.Unmarshal(raw, &accepted); err != nil {
		return provisionResponse{}, fmt.Errorf("decode provision response: %w", err)
	}
	if accepted.TenantID == "" || accepted.APIKey == "" || accepted.Status == "" {
		return provisionResponse{}, fmt.Errorf("provision response missing tenant_id, api_key, or status")
	}
	return accepted, nil
}

func waitTenantReady(
	ctx context.Context,
	cfg benchConfig,
	client httpDoer,
	accepted provisionResponse,
	index int,
	logger *log.Logger,
) (string, int, error) {
	status := accepted.Status
	requests := 0
	for {
		if terminalStatus(status) {
			return status, requests, fmt.Errorf("tenant entered terminal status %q", status)
		}
		requestStarted := time.Now()
		next, err := getTenantStatus(ctx, cfg.Server, accepted.APIKey, client)
		requestLatency := time.Since(requestStarted)
		requests++
		if err != nil {
			logRequestError(
				logger,
				"status",
				index,
				accepted.TenantID,
				requests,
				requestLatency,
				err,
				cfg.PublicKey,
				cfg.PrivateKey,
				accepted.APIKey,
			)
		}
		if err == nil {
			status = next
			if status == "active" {
				return status, requests, nil
			}
			if terminalStatus(status) {
				return status, requests, fmt.Errorf("tenant entered terminal status %q", status)
			}
		} else if isClientHTTPError(err) {
			return status, requests, err
		}

		timer := time.NewTimer(cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status, requests, fmt.Errorf("wait for tenant active: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func getTenantStatus(ctx context.Context, server, apiKey string, client httpDoer) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server+"/v1/status", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("status request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return "", fmt.Errorf("read status response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", responseError("status", resp.StatusCode, raw)
	}
	var status tenantStatusResponse
	if err := json.Unmarshal(raw, &status); err != nil {
		return "", fmt.Errorf("decode status response: %w", err)
	}
	if status.Status == "" {
		return "", fmt.Errorf("status response missing status")
	}
	return status.Status, nil
}

type httpStatusError struct {
	operation string
	code      int
	message   string
}

func logRequestError(
	logger *log.Logger,
	operation string,
	index int,
	tenantID string,
	attempt int,
	latency time.Duration,
	err error,
	secrets ...string,
) {
	statusCode := 0
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		statusCode = statusErr.code
	}
	logger.Printf(
		"request error: operation=%s index=%d tenant=%q attempt=%d "+
			"http_status=%d latency_seconds=%.6f error=%q",
		operation,
		index,
		tenantID,
		attempt,
		statusCode,
		latency.Seconds(),
		redactSecrets(err.Error(), secrets...),
	)
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("%s returned HTTP %d: %s", e.operation, e.code, e.message)
}

func responseError(operation string, code int, body []byte) error {
	message := strings.TrimSpace(string(body))
	if len(message) > 1024 {
		message = message[:1024] + "..."
	}
	if message == "" {
		message = http.StatusText(code)
	}
	return &httpStatusError{operation: operation, code: code, message: message}
}

func isClientHTTPError(err error) bool {
	var statusErr *httpStatusError
	return errors.As(err, &statusErr) && statusErr.code >= 400 && statusErr.code < 500
}

func terminalStatus(status string) bool {
	switch status {
	case "failed", "suspended", "deleting", "deleted":
		return true
	default:
		return false
	}
}

type pacedLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newPacedLimiter(rps float64) *pacedLimiter {
	return &pacedLimiter{interval: time.Duration(float64(time.Second) / rps)}
}

func (l *pacedLimiter) Wait(ctx context.Context) error {
	l.mu.Lock()
	now := time.Now()
	if l.next.IsZero() || l.next.Before(now) {
		l.next = now
	}
	slot := l.next
	l.next = l.next.Add(l.interval)
	l.mu.Unlock()

	delay := time.Until(slot)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func safeReportConfig(cfg benchConfig) reportConfig {
	mode := "anonymous"
	if cfg.PublicKey != "" {
		mode = "tidbcloud"
	}
	return reportConfig{
		Server:         cfg.Server,
		Mode:           mode,
		Inventory:      cfg.Inventory,
		SampleOut:      cfg.SampleOut,
		SampleSeed:     cfg.SampleSeed,
		SpendingLimit:  cfg.SpendingLimit,
		Total:          cfg.Total,
		SampleSize:     cfg.SampleSize,
		Concurrency:    cfg.Concurrency,
		RPS:            cfg.RPS,
		WaitReady:      cfg.WaitReady,
		PollInterval:   cfg.PollInterval,
		Timeout:        cfg.Timeout,
		ReportInterval: cfg.ReportInterval,
	}
}

func writeReport(path string, report benchmarkReport) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(report)
	closeErr := file.Close()
	if encodeErr != nil {
		return encodeErr
	}
	return closeErr
}

func printSummary(w io.Writer, report benchmarkReport) {
	summary := report.Summary
	_, _ = fmt.Fprintf(w,
		"drive9 create bench done: success=%d failed=%d completed=%d requested=%d provision=%d status=%d tenants_per_minute=%.2f duration=%.3fs\n",
		summary.Success, summary.Failed, summary.Completed, summary.Requested,
		summary.ProvisionRequests, summary.StatusRequests, summary.TenantsPerMinute,
		summary.ElapsedSeconds)
	printLatency := func(name string, latency latencySummary) {
		if latency.SampleCount == 0 {
			return
		}
		_, _ = fmt.Fprintf(w, "%s: avg=%.3fs median=%.3fs p95=%.3fs max=%.3fs samples=%d\n",
			name, latency.Average, latency.Median, latency.P95, latency.Max, latency.SampleCount)
	}
	printLatency("provision latency", summary.ProvisionLatency)
	printLatency("ready latency", summary.ReadyLatency)
	printLatencyHistogram(w, "Provision Latency Histogram", summary.ProvisionLatency)
	printLatencyHistogram(w, "Ready Latency Histogram", summary.ReadyLatency)
}

func printLatencyHistogram(w io.Writer, title string, summary latencySummary) {
	if summary.SampleCount == 0 {
		return
	}
	maxCount := 0
	for _, bucket := range summary.Buckets {
		maxCount = max(maxCount, bucket.Count)
	}

	_, _ = fmt.Fprintf(w, "\n%s:\n", title)
	lowerBound := "0s"
	for _, bucket := range summary.Buckets {
		if bucket.Count == 0 {
			lowerBound = bucket.UpperBound
			continue
		}
		barLength := max(
			1,
			int(float64(bucket.Count)/float64(maxCount)*histogramBarWidth),
		)
		_, _ = fmt.Fprintf(
			w,
			"  (%9s, %9s] %8d |%s\n",
			lowerBound,
			bucket.UpperBound,
			bucket.Count,
			strings.Repeat("█", barLength),
		)
		lowerBound = bucket.UpperBound
	}
}

func redactSecrets(message string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return message
}
