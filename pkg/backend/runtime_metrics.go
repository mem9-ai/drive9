package backend

import (
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

type backendRuntimeMetrics struct {
	mu     sync.Mutex
	nextID uint64
	image  map[uint64]imageRuntimeState
	audio  map[uint64]audioRuntimeState
}

type imageRuntimeState struct {
	queueCapacity int
	workers       int
	queueDepth    int
}

type audioRuntimeState struct {
	maxAudioBytes       int64
	maxExtractTextBytes int
	taskTimeoutSeconds  float64
}

type imageRuntimeSummary struct {
	active        bool
	queueCapacity float64
	workers       float64
	queueDepth    float64
}

type audioRuntimeSummary struct {
	active              bool
	maxAudioBytes       float64
	maxExtractTextBytes float64
	taskTimeoutSeconds  float64
}

var globalBackendRuntimeMetrics = newBackendRuntimeMetrics()

func newBackendRuntimeMetrics() *backendRuntimeMetrics {
	return &backendRuntimeMetrics{
		image: make(map[uint64]imageRuntimeState),
		audio: make(map[uint64]audioRuntimeState),
	}
}

func (m *backendRuntimeMetrics) allocateID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	return m.nextID
}

func (m *backendRuntimeMetrics) activateImage(id uint64, queueCapacity, workers int) {
	m.mu.Lock()
	m.image[id] = imageRuntimeState{
		queueCapacity: queueCapacity,
		workers:       workers,
	}
	summary := m.imageSummaryLocked()
	m.mu.Unlock()
	publishImageRuntimeSummary(summary)
}

func (m *backendRuntimeMetrics) deactivateImage(id uint64) {
	m.mu.Lock()
	delete(m.image, id)
	summary := m.imageSummaryLocked()
	m.mu.Unlock()
	publishImageRuntimeSummary(summary)
}

func (m *backendRuntimeMetrics) activateAudio(id uint64, maxAudioBytes int64, maxExtractTextBytes int, taskTimeout time.Duration) {
	m.mu.Lock()
	m.audio[id] = audioRuntimeState{
		maxAudioBytes:       maxAudioBytes,
		maxExtractTextBytes: maxExtractTextBytes,
		taskTimeoutSeconds:  taskTimeout.Seconds(),
	}
	summary := m.audioSummaryLocked()
	m.mu.Unlock()
	publishAudioRuntimeSummary(summary)
}

func (m *backendRuntimeMetrics) deactivateAudio(id uint64) {
	m.mu.Lock()
	delete(m.audio, id)
	summary := m.audioSummaryLocked()
	m.mu.Unlock()
	publishAudioRuntimeSummary(summary)
}

func (m *backendRuntimeMetrics) imageSummaryLocked() imageRuntimeSummary {
	var summary imageRuntimeSummary
	if len(m.image) == 0 {
		return summary
	}
	summary.active = true
	for _, state := range m.image {
		summary.queueCapacity += float64(state.queueCapacity)
		summary.workers += float64(state.workers)
		summary.queueDepth += float64(state.queueDepth)
	}
	return summary
}

func (m *backendRuntimeMetrics) audioSummaryLocked() audioRuntimeSummary {
	var summary audioRuntimeSummary
	if len(m.audio) == 0 {
		return summary
	}
	summary.active = true
	for _, state := range m.audio {
		if value := float64(state.maxAudioBytes); value > summary.maxAudioBytes {
			summary.maxAudioBytes = value
		}
		if value := float64(state.maxExtractTextBytes); value > summary.maxExtractTextBytes {
			summary.maxExtractTextBytes = value
		}
		if state.taskTimeoutSeconds > summary.taskTimeoutSeconds {
			summary.taskTimeoutSeconds = state.taskTimeoutSeconds
		}
	}
	return summary
}

func publishImageRuntimeSummary(summary imageRuntimeSummary) {
	metrics.SetModuleAvailability("image_extract", summary.active)
	metrics.RecordGauge("image_extract", "queue_capacity", summary.queueCapacity)
	metrics.RecordGauge("image_extract", "workers", summary.workers)
	metrics.RecordGauge("image_extract", "queue_depth", summary.queueDepth)
}

func publishAudioRuntimeSummary(summary audioRuntimeSummary) {
	metrics.SetModuleAvailability("audio_extract", summary.active)
	metrics.RecordGauge("audio_extract", "max_audio_bytes", summary.maxAudioBytes)
	metrics.RecordGauge("audio_extract", "max_extract_text_bytes", summary.maxExtractTextBytes)
	metrics.RecordGauge("audio_extract", "task_timeout_seconds", summary.taskTimeoutSeconds)
}
