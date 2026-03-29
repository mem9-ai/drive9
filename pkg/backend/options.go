package backend

import (
	"context"
	"time"
)

const (
	defaultImageExtractQueueSize = 128
	defaultImageExtractWorkers   = 1
	defaultImageExtractMaxSize   = int64(8 << 20) // 8 MiB
	defaultImageExtractTimeout   = 20 * time.Second
	defaultMaxExtractedTextBytes = 8 << 10 // 8 KiB
)

// Options configures Dat9Backend behavior.
type Options struct {
	AsyncImageExtract AsyncImageExtractOptions
}

// AsyncImageExtractOptions controls async image->text extraction.
type AsyncImageExtractOptions struct {
	Enabled             bool
	QueueSize           int
	Workers             int
	MaxImageBytes       int64
	TaskTimeout         time.Duration
	MaxExtractTextBytes int
	Extractor           ImageTextExtractor
}

func (b *Dat9Backend) configureOptions(opts Options) {
	cfg := opts.AsyncImageExtract
	if !cfg.Enabled {
		return
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultImageExtractQueueSize
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultImageExtractWorkers
	}
	if cfg.MaxImageBytes <= 0 {
		cfg.MaxImageBytes = defaultImageExtractMaxSize
	}
	if cfg.TaskTimeout <= 0 {
		cfg.TaskTimeout = defaultImageExtractTimeout
	}
	if cfg.MaxExtractTextBytes <= 0 {
		cfg.MaxExtractTextBytes = defaultMaxExtractedTextBytes
	}
	if cfg.Extractor == nil {
		cfg.Extractor = NewBasicImageTextExtractor()
	}

	b.imageExtractEnabled = true
	b.imageExtractor = cfg.Extractor
	b.imageExtractTimeout = cfg.TaskTimeout
	b.imageExtractMaxSize = cfg.MaxImageBytes
	b.maxExtractTextBytes = cfg.MaxExtractTextBytes
	b.imageExtractQueue = make(chan imageExtractTask, cfg.QueueSize)

	ctx, cancel := context.WithCancel(backgroundWithTrace())
	b.imageExtractCancel = cancel
	for i := 0; i < cfg.Workers; i++ {
		b.imageExtractWG.Add(1)
		go b.runImageExtractWorker(ctx)
	}
}

// Close stops background workers owned by this backend instance.
func (b *Dat9Backend) Close() {
	if b.imageExtractCancel == nil {
		return
	}
	b.imageExtractCancel()
	b.imageExtractWG.Wait()
	b.imageExtractCancel = nil
}
