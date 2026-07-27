package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	envServer     = "DRIVE9_SERVER"
	envPublicKey  = "DRIVE9_PUBLIC_KEY"
	envPrivateKey = "DRIVE9_PRIVATE_KEY"

	defaultSpaceCount          = 500
	defaultSpendingLimit int64 = 10000
	maxWorkloadFileSize        = 1 << 20
	maxSpendingLimit     int64 = 1_000_000
)

type benchConfig struct {
	Server               string
	SpacesFile           string
	Out                  string
	PublicKey            string
	PrivateKey           string
	SpaceCount           int
	ProvisionConcurrency int
	ProvisionRPS         float64
	PollInterval         time.Duration
	ProvisionTimeout     time.Duration
	SpendingLimit        int64
	WorkersPerSpace      int
	FilesPerWorker       int
	DeleteEvery          int
	FileSize             int
	Duration             time.Duration
	RequestTimeout       time.Duration
	ReportInterval       time.Duration
	IORPS                float64
	SpaceStartRPS        float64
}

type benchFileConfig struct {
	Server        *string `json:"server"`
	PublicKey     *string `json:"tidbcloud_public_key"`
	PrivateKey    *string `json:"tidbcloud_private_key"`
	SpaceCount    *int    `json:"spaces"`
	SpendingLimit *int64  `json:"tidbcloud_spending_limit"`
}

func parseConfig(
	args []string,
	getenv func(string) string,
	homeDir string,
	output io.Writer,
) (benchConfig, error) {
	benchDir := ""
	if strings.TrimSpace(homeDir) != "" {
		benchDir = filepath.Join(homeDir, ".drive9", "bench")
	}
	configFile := filepath.Join(benchDir, "config.json")
	cfg := benchConfig{
		SpacesFile:           filepath.Join(benchDir, "spaces.json"),
		Out:                  filepath.Join(benchDir, "latest-report.json"),
		SpaceCount:           defaultSpaceCount,
		ProvisionConcurrency: 10,
		ProvisionRPS:         1,
		PollInterval:         5 * time.Second,
		ProvisionTimeout:     10 * time.Minute,
		SpendingLimit:        defaultSpendingLimit,
		WorkersPerSpace:      1,
		FilesPerWorker:       16,
		FileSize:             4096,
		RequestTimeout:       30 * time.Second,
		ReportInterval:       10 * time.Second,
	}
	var publicKeyFlag, privateKeyFlag string
	fs := flag.NewFlagSet("drive9-space-bench", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&configFile, "config", configFile, "optional benchmark input config file (mode 0600)")
	fs.StringVar(&cfg.Server, "server", cfg.Server, "Drive9 server URL (or DRIVE9_SERVER)")
	fs.StringVar(&cfg.SpacesFile, "spaces-file", cfg.SpacesFile, "space credential file")
	fs.StringVar(&cfg.Out, "out", cfg.Out, "JSON report output path")
	fs.IntVar(&cfg.SpaceCount, "spaces", cfg.SpaceCount, "number of spaces to reuse or provision")
	fs.IntVar(&cfg.ProvisionConcurrency, "provision-concurrency", cfg.ProvisionConcurrency, "concurrent provision and readiness workers")
	fs.Float64Var(&cfg.ProvisionRPS, "provision-rps", cfg.ProvisionRPS, "maximum provision requests per second")
	fs.DurationVar(&cfg.PollInterval, "poll-interval", cfg.PollInterval, "space readiness polling interval")
	fs.DurationVar(&cfg.ProvisionTimeout, "provision-timeout", cfg.ProvisionTimeout, "per-space readiness timeout")
	fs.StringVar(&publicKeyFlag, "tidbcloud-public-key", "", "TiDB Cloud public key (or DRIVE9_PUBLIC_KEY)")
	fs.StringVar(&privateKeyFlag, "tidbcloud-private-key", "", "TiDB Cloud private key (or DRIVE9_PRIVATE_KEY)")
	fs.Int64Var(&cfg.SpendingLimit, "tidbcloud-spending-limit", cfg.SpendingLimit, "monthly TiDB Cloud spending limit in RMB for new spaces")
	fs.IntVar(&cfg.WorkersPerSpace, "workers-per-space", cfg.WorkersPerSpace, "concurrent write/read workers per space")
	fs.IntVar(&cfg.FilesPerWorker, "files-per-worker", cfg.FilesPerWorker, "rotating remote files owned by each worker")
	fs.IntVar(&cfg.DeleteEvery, "delete-every", cfg.DeleteEvery, "delete the verified file after every N successful write/read cycles; zero disables deletes")
	fs.IntVar(&cfg.FileSize, "file-size", cfg.FileSize, "payload size in bytes")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "workload duration; zero runs until interrupted")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "timeout for each filesystem request")
	fs.DurationVar(&cfg.ReportInterval, "report-interval", cfg.ReportInterval, "periodic progress interval")
	fs.Float64Var(&cfg.IORPS, "io-rps", cfg.IORPS, "global filesystem request limit; zero is unlimited")
	fs.Float64Var(&cfg.SpaceStartRPS, "space-start-rps", cfg.SpaceStartRPS, "maximum spaces activated per second; zero starts all immediately")
	fs.Usage = func() {
		_, _ = fmt.Fprintln(output, "Usage: drive9-space-bench [flags]")
		_, _ = fmt.Fprintln(output)
		fs.PrintDefaults()
		_, _ = fmt.Fprintf(output,
			"\nConfiguration precedence: flags > non-empty environment variables > "+
				"config file > built-in defaults.\n"+
				"TiDB Cloud credentials can be read from the config file, %s, or %s. "+
				"They are required only when new spaces must be provisioned.\n",
			envPublicKey, envPrivateKey)
	}
	if err := fs.Parse(args); err != nil {
		return benchConfig{}, err
	}
	if fs.NArg() != 0 {
		return benchConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	configFile = strings.TrimSpace(configFile)
	fileCfg, err := loadBenchFileConfig(configFile, flagProvided(fs, "config"))
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
	if !flagProvided(fs, "spaces") && fileCfg.SpaceCount != nil {
		cfg.SpaceCount = *fileCfg.SpaceCount
	}
	if !flagProvided(fs, "tidbcloud-spending-limit") && fileCfg.SpendingLimit != nil {
		cfg.SpendingLimit = *fileCfg.SpendingLimit
	}

	publicFlagSet := flagProvided(fs, "tidbcloud-public-key")
	privateFlagSet := flagProvided(fs, "tidbcloud-private-key")
	if publicFlagSet {
		cfg.PublicKey = strings.TrimSpace(publicKeyFlag)
	} else if value := strings.TrimSpace(getenv(envPublicKey)); value != "" {
		cfg.PublicKey = value
	} else if fileCfg.PublicKey != nil {
		cfg.PublicKey = strings.TrimSpace(*fileCfg.PublicKey)
	}
	if privateFlagSet {
		cfg.PrivateKey = strings.TrimSpace(privateKeyFlag)
	} else if value := strings.TrimSpace(getenv(envPrivateKey)); value != "" {
		cfg.PrivateKey = value
	} else if fileCfg.PrivateKey != nil {
		cfg.PrivateKey = strings.TrimSpace(*fileCfg.PrivateKey)
	}
	if publicFlagSet && cfg.PublicKey == "" {
		return benchConfig{}, fmt.Errorf("--tidbcloud-public-key was given an empty value")
	}
	if privateFlagSet && cfg.PrivateKey == "" {
		return benchConfig{}, fmt.Errorf("--tidbcloud-private-key was given an empty value")
	}

	cfg.Server = strings.TrimRight(strings.TrimSpace(cfg.Server), "/")
	cfg.SpacesFile = strings.TrimSpace(cfg.SpacesFile)
	cfg.Out = strings.TrimSpace(cfg.Out)
	if err := validateConfig(cfg); err != nil {
		return benchConfig{}, err
	}
	return cfg, nil
}

func loadBenchFileConfig(path string, required bool) (benchFileConfig, error) {
	if path == "" {
		return benchFileConfig{}, fmt.Errorf("config path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			if !required {
				return benchFileConfig{}, nil
			}
			return benchFileConfig{}, fmt.Errorf("config file does not exist: %s", path)
		}
		return benchFileConfig{}, fmt.Errorf("open config file %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	info, err := file.Stat()
	if err != nil {
		return benchFileConfig{}, fmt.Errorf("inspect config file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return benchFileConfig{}, fmt.Errorf("config file must be a regular file: %s", path)
	}
	if info.Mode().Perm() != 0o600 {
		return benchFileConfig{}, fmt.Errorf(
			"config file %s must have mode 0600; got %04o",
			path,
			info.Mode().Perm(),
		)
	}

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var cfg *benchFileConfig
	if err := decoder.Decode(&cfg); err != nil {
		return benchFileConfig{}, fmt.Errorf("decode config file %s: %w", path, err)
	}
	if cfg == nil {
		return benchFileConfig{}, fmt.Errorf("decode config file %s: expected a JSON object", path)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return benchFileConfig{}, fmt.Errorf(
				"decode config file %s: expected exactly one JSON object",
				path,
			)
		}
		return benchFileConfig{}, fmt.Errorf("decode config file %s: %w", path, err)
	}
	return *cfg, nil
}

func validateConfig(cfg benchConfig) error {
	if cfg.Server == "" {
		return fmt.Errorf("server is required; pass --server or set %s", envServer)
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
	if cfg.SpacesFile == "" {
		return fmt.Errorf("spaces-file is required")
	}
	if cfg.Out == "" {
		return fmt.Errorf("out is required")
	}
	if samePath(cfg.SpacesFile, cfg.Out) {
		return fmt.Errorf("out and spaces-file must be different paths")
	}
	if cfg.SpaceCount <= 0 {
		return fmt.Errorf("spaces must be positive")
	}
	if cfg.ProvisionConcurrency <= 0 {
		return fmt.Errorf("provision-concurrency must be positive")
	}
	if err := validateRPS("provision-rps", cfg.ProvisionRPS, false); err != nil {
		return err
	}
	if cfg.PollInterval <= 0 {
		return fmt.Errorf("poll-interval must be positive")
	}
	if cfg.ProvisionTimeout <= 0 {
		return fmt.Errorf("provision-timeout must be positive")
	}
	if (cfg.PublicKey == "") != (cfg.PrivateKey == "") {
		return fmt.Errorf("both TiDB Cloud public and private keys must be provided together")
	}
	if cfg.SpendingLimit < 0 {
		return fmt.Errorf("tidbcloud-spending-limit must be non-negative")
	}
	if cfg.SpendingLimit > 0 && cfg.SpendingLimit < 10 {
		return fmt.Errorf("tidbcloud-spending-limit must be 0 or at least 10 RMB")
	}
	if cfg.SpendingLimit > maxSpendingLimit {
		return fmt.Errorf("tidbcloud-spending-limit must not exceed %d", maxSpendingLimit)
	}
	if cfg.WorkersPerSpace <= 0 {
		return fmt.Errorf("workers-per-space must be positive")
	}
	if cfg.FilesPerWorker <= 0 {
		return fmt.Errorf("files-per-worker must be positive")
	}
	if cfg.DeleteEvery < 0 {
		return fmt.Errorf("delete-every must not be negative")
	}
	if cfg.FileSize <= 0 {
		return fmt.Errorf("file-size must be positive")
	}
	if cfg.FileSize > maxWorkloadFileSize {
		return fmt.Errorf("file-size must not exceed %d", maxWorkloadFileSize)
	}
	if cfg.Duration < 0 {
		return fmt.Errorf("duration must not be negative")
	}
	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("request-timeout must be positive")
	}
	if cfg.ReportInterval <= 0 {
		return fmt.Errorf("report-interval must be positive")
	}
	if err := validateRPS("io-rps", cfg.IORPS, true); err != nil {
		return err
	}
	if err := validateRPS("space-start-rps", cfg.SpaceStartRPS, true); err != nil {
		return err
	}
	return nil
}

func validateRPS(name string, value float64, zeroAllowed bool) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("%s must be finite", name)
	}
	if value < 0 || (!zeroAllowed && value == 0) {
		if zeroAllowed {
			return fmt.Errorf("%s must not be negative", name)
		}
		return fmt.Errorf("%s must be positive", name)
	}
	if value == 0 {
		return nil
	}
	interval := float64(time.Second) / value
	if interval < 1 || interval > float64(math.MaxInt64) {
		return fmt.Errorf("%s must produce an interval representable by time.Duration", name)
	}
	return nil
}

func flagProvided(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func samePath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr == nil && rightErr == nil {
		return filepath.Clean(leftAbs) == filepath.Clean(rightAbs)
	}
	return filepath.Clean(left) == filepath.Clean(right)
}
