package fuse

import (
	"log"
	"time"
)

const (
	fuseDebugSlowReadThreshold = 100 * time.Millisecond
	fuseDebugSlowOpThreshold   = 250 * time.Millisecond
)

func (fs *Dat9FS) debugEnabled() bool {
	return fs != nil && fs.opts != nil && fs.opts.Debug
}

func (fs *Dat9FS) debugf(format string, args ...any) {
	if !fs.debugEnabled() {
		return
	}
	log.Printf("dat9 debug: "+format, args...)
}

func (fs *Dat9FS) debugDurationf(start time.Time, threshold time.Duration, format string, args ...any) {
	if !fs.debugEnabled() {
		return
	}
	d := time.Since(start)
	if d < threshold {
		return
	}
	args = append(args, d)
	log.Printf("dat9 debug: "+format+" dur=%s", args...)
}
