package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// createBenchFileConfig accepts the shared drive9-space-bench config schema.
// Space count and spending limit are intentionally ignored by create-bench.
type createBenchFileConfig struct {
	Server        *string `json:"server"`
	PublicKey     *string `json:"tidbcloud_public_key"`
	PrivateKey    *string `json:"tidbcloud_private_key"`
	SpaceCount    *int    `json:"spaces"`
	SpendingLimit *int64  `json:"tidbcloud_spending_limit"`
}

func loadCreateBenchFileConfig(
	path string,
	required bool,
) (createBenchFileConfig, error) {
	if path == "" {
		if !required {
			return createBenchFileConfig{}, nil
		}
		return createBenchFileConfig{}, fmt.Errorf("config path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			if !required {
				return createBenchFileConfig{}, nil
			}
			return createBenchFileConfig{}, fmt.Errorf(
				"config file does not exist: %s",
				path,
			)
		}
		return createBenchFileConfig{}, fmt.Errorf("open config file %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	info, err := file.Stat()
	if err != nil {
		return createBenchFileConfig{}, fmt.Errorf("inspect config file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return createBenchFileConfig{}, fmt.Errorf(
			"config file must be a regular file: %s",
			path,
		)
	}
	if info.Mode().Perm() != 0o600 {
		return createBenchFileConfig{}, fmt.Errorf(
			"config file %s must have mode 0600; got %04o",
			path,
			info.Mode().Perm(),
		)
	}

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var cfg *createBenchFileConfig
	if err := decoder.Decode(&cfg); err != nil {
		return createBenchFileConfig{}, fmt.Errorf("decode config file %s: %w", path, err)
	}
	if cfg == nil {
		return createBenchFileConfig{}, fmt.Errorf(
			"decode config file %s: expected a JSON object",
			path,
		)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return createBenchFileConfig{}, fmt.Errorf(
				"decode config file %s: expected exactly one JSON object",
				path,
			)
		}
		return createBenchFileConfig{}, fmt.Errorf("decode config file %s: %w", path, err)
	}
	return *cfg, nil
}
