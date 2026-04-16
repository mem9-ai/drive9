// Package buildinfo provides shared build metadata for drive9 binaries.
package buildinfo

import (
	"fmt"
	"runtime"

	"go.uber.org/zap"
)

var (
	Version   = "dev"
	GitHash   = "unknown"
	GitBranch = "unknown"
	BuildTime = "unknown"
)

type Info struct {
	Component string
	Version   string
	GitHash   string
	GitBranch string
	BuildTime string
	GoVersion string
}

func Get(component string) Info {
	return Info{
		Component: component,
		Version:   Version,
		GitHash:   GitHash,
		GitBranch: GitBranch,
		BuildTime: BuildTime,
		GoVersion: runtime.Version(),
	}
}

func Fields(component string) []zap.Field {
	return Get(component).Fields()
}

func String(component string) string {
	return Get(component).String()
}

func (i Info) Fields() []zap.Field {
	return []zap.Field{
		zap.String("component", i.Component),
		zap.String("version", i.Version),
		zap.String("git_hash", i.GitHash),
		zap.String("git_branch", i.GitBranch),
		zap.String("build_time", i.BuildTime),
		zap.String("go_version", i.GoVersion),
	}
}

func (i Info) String() string {
	return fmt.Sprintf(
		"component: %s\nversion: %s\ngit_hash: %s\ngit_branch: %s\nbuild_time: %s\ngo_version: %s\n",
		i.Component,
		i.Version,
		i.GitHash,
		i.GitBranch,
		i.BuildTime,
		i.GoVersion,
	)
}
