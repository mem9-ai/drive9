package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const maxInventoryLineBytes = 1 << 20

type sampleModeConfig struct {
	Inventories []string
	Out         string
	SampleSeed  string
	SampleSize  int
}

type sampleModeSummary struct {
	Records    int
	Active     int
	Duplicates int
	Selected   int
}

type stringListFlag []string

func (v *stringListFlag) String() string {
	return strings.Join(*v, ",")
}

func (v *stringListFlag) Set(raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fmt.Errorf("must not be empty")
	}
	*v = append(*v, value)
	return nil
}

func runSampleMain(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseSampleModeConfig(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "drive9-create-bench sample: %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	summary, err := sampleInventories(ctx, cfg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "drive9-create-bench sample: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(
		stdout,
		"drive9-create-bench sample done: inventories=%d records=%d "+
			"active=%d duplicates=%d selected=%d out=%s\n",
		len(cfg.Inventories),
		summary.Records,
		summary.Active,
		summary.Duplicates,
		summary.Selected,
		cfg.Out,
	)
	return 0
}

func parseSampleModeConfig(args []string, output io.Writer) (sampleModeConfig, error) {
	cfg := sampleModeConfig{SampleSeed: "drive9-create-bench"}
	var inventories stringListFlag
	fs := flag.NewFlagSet("drive9-create-bench sample", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.Var(&inventories, "inventory", "input credential inventory JSONL path (repeatable)")
	fs.IntVar(&cfg.SampleSize, "sample-size", 0, "number of active spaces to select")
	fs.StringVar(&cfg.SampleSeed, "sample-seed", cfg.SampleSeed, "deterministic selection seed")
	fs.StringVar(&cfg.Out, "out", "", "drive9-space-bench JSON snapshot output path")
	fs.Usage = func() {
		_, _ = fmt.Fprintln(
			output,
			"Usage: drive9-create-bench sample --inventory PATH "+
				"[--inventory PATH ...] --sample-size N --out PATH",
		)
		_, _ = fmt.Fprintln(output)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return sampleModeConfig{}, err
	}
	if fs.NArg() != 0 {
		return sampleModeConfig{}, fmt.Errorf(
			"unexpected arguments: %s",
			strings.Join(fs.Args(), " "),
		)
	}

	cfg.Inventories = append([]string(nil), inventories...)
	cfg.Out = strings.TrimSpace(cfg.Out)
	cfg.SampleSeed = strings.TrimSpace(cfg.SampleSeed)
	if len(cfg.Inventories) == 0 {
		return sampleModeConfig{}, fmt.Errorf("inventory is required")
	}
	if cfg.SampleSize <= 0 {
		return sampleModeConfig{}, fmt.Errorf("sample-size must be positive")
	}
	if cfg.Out == "" {
		return sampleModeConfig{}, fmt.Errorf("out is required")
	}
	if cfg.SampleSeed == "" {
		return sampleModeConfig{}, fmt.Errorf("sample-seed must not be empty")
	}
	for index, path := range cfg.Inventories {
		for previous := 0; previous < index; previous++ {
			if samePath(path, cfg.Inventories[previous]) {
				return sampleModeConfig{}, fmt.Errorf(
					"inventory path provided more than once: %s",
					path,
				)
			}
		}
		if samePath(cfg.Out, path) {
			return sampleModeConfig{}, fmt.Errorf(
				"out and inventory must use different paths",
			)
		}
	}
	return cfg, nil
}

func sampleInventories(
	ctx context.Context,
	cfg sampleModeConfig,
) (sampleModeSummary, error) {
	var summary sampleModeSummary
	if err := ensureFileDoesNotExist(cfg.Out); err != nil {
		return summary, err
	}
	sampler := newSpaceSampler(cfg.SampleSize, cfg.SampleSeed)
	seenCredentials := make(map[string]selectedSpaceCredential)
	server := ""
	for _, path := range cfg.Inventories {
		err := scanInventory(ctx, path, func(line int, record inventoryRecord) error {
			summary.Records++
			recordServer := strings.TrimRight(strings.TrimSpace(record.Server), "/")
			if server == "" {
				server = recordServer
			} else if recordServer != server {
				return fmt.Errorf(
					"inventory %s line %d: server mismatch: got %q, require %q",
					path,
					line,
					recordServer,
					server,
				)
			}
			if !record.Active || record.FinalStatus != "active" {
				return nil
			}
			summary.Active++
			tenantID := strings.TrimSpace(record.TenantID)
			apiKey := strings.TrimSpace(record.APIKey)
			spendingLimit := int64(0)
			if record.SpendingLimit != nil {
				spendingLimit = *record.SpendingLimit
			}
			credential := selectedSpaceCredential{
				TenantID:      tenantID,
				APIKey:        apiKey,
				CreatedAt:     record.CreatedAt,
				SpendingLimit: spendingLimit,
			}
			if previous, ok := seenCredentials[tenantID]; ok {
				if previous.APIKey != credential.APIKey ||
					!previous.CreatedAt.Equal(credential.CreatedAt) ||
					previous.SpendingLimit != credential.SpendingLimit {
					return fmt.Errorf(
						"inventory %s line %d: conflicting credentials for tenant %q",
						path,
						line,
						tenantID,
					)
				}
				summary.Duplicates++
				return nil
			}
			seenCredentials[tenantID] = credential
			sampler.Offer(record)
			return nil
		})
		if err != nil {
			return summary, err
		}
	}
	if err := ctx.Err(); err != nil {
		return summary, err
	}
	if server == "" {
		return summary, fmt.Errorf("inventories contain no records")
	}
	spaces := sampler.Spaces()
	if len(spaces) != cfg.SampleSize {
		return summary, fmt.Errorf(
			"only %d unique active spaces available; sample-size requires %d",
			len(spaces),
			cfg.SampleSize,
		)
	}
	if err := writeSelectedSnapshot(cfg.Out, server, spaces); err != nil {
		return summary, err
	}
	summary.Selected = len(spaces)
	return summary, nil
}

func scanInventory(
	ctx context.Context,
	path string,
	visit func(line int, record inventoryRecord) error,
) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open inventory %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect inventory %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("inventory must be a regular file: %s", path)
	}
	if info.Mode().Perm() != 0o600 {
		return fmt.Errorf(
			"inventory %s must have mode 0600; got %04o",
			path,
			info.Mode().Perm(),
		)
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), maxInventoryLineBytes)
	line := 0
	for scanner.Scan() {
		line++
		if err := ctx.Err(); err != nil {
			return err
		}
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			return fmt.Errorf("inventory %s line %d: record is empty", path, line)
		}
		var record inventoryRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return fmt.Errorf(
				"inventory %s line %d: decode record: %w",
				path,
				line,
				err,
			)
		}
		if err := validateInventoryRecord(record); err != nil {
			return fmt.Errorf("inventory %s line %d: %w", path, line, err)
		}
		if err := visit(line, record); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read inventory %s: %w", path, err)
	}
	return nil
}
