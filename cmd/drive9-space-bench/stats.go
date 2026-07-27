package main

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const histogramBarWidth = 50

var defaultLatencyBounds = []time.Duration{
	100 * time.Microsecond,
	250 * time.Microsecond,
	500 * time.Microsecond,
	time.Millisecond,
	2500 * time.Microsecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

type latencyHistogram struct {
	mu           sync.Mutex
	bounds       []time.Duration
	buckets      []uint64
	count        uint64
	totalSeconds float64
	min          time.Duration
	max          time.Duration
}

type histogramBucketSnapshot struct {
	UpperBound        string   `json:"upper_bound"`
	UpperBoundSeconds *float64 `json:"upper_bound_seconds,omitempty"`
	Count             uint64   `json:"count"`
}

type histogramSnapshot struct {
	Count          uint64                    `json:"count"`
	AverageSeconds float64                   `json:"average_seconds"`
	MinSeconds     float64                   `json:"min_seconds"`
	P50Seconds     float64                   `json:"p50_seconds"`
	P95Seconds     float64                   `json:"p95_seconds"`
	P99Seconds     float64                   `json:"p99_seconds"`
	MaxSeconds     float64                   `json:"max_seconds"`
	Buckets        []histogramBucketSnapshot `json:"buckets"`
}

func newLatencyHistogram(bounds []time.Duration) *latencyHistogram {
	ordered := append([]time.Duration(nil), bounds...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return &latencyHistogram{
		bounds:  ordered,
		buckets: make([]uint64, len(ordered)+1),
	}
}

func (h *latencyHistogram) Record(duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	index := sort.Search(len(h.bounds), func(index int) bool {
		return duration <= h.bounds[index]
	})
	h.buckets[index]++
	if h.count == 0 || duration < h.min {
		h.min = duration
	}
	if duration > h.max {
		h.max = duration
	}
	h.count++
	h.totalSeconds += duration.Seconds()
}

func (h *latencyHistogram) Snapshot() histogramSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	snapshot := histogramSnapshot{
		Count:      h.count,
		MinSeconds: h.min.Seconds(),
		MaxSeconds: h.max.Seconds(),
		Buckets:    make([]histogramBucketSnapshot, len(h.buckets)),
	}
	if h.count > 0 {
		snapshot.AverageSeconds = h.totalSeconds / float64(h.count)
		snapshot.P50Seconds = h.percentileLocked(50)
		snapshot.P95Seconds = h.percentileLocked(95)
		snapshot.P99Seconds = h.percentileLocked(99)
	}
	for index, count := range h.buckets {
		if index == len(h.bounds) {
			snapshot.Buckets[index] = histogramBucketSnapshot{
				UpperBound: "+Inf",
				Count:      count,
			}
			continue
		}
		seconds := h.bounds[index].Seconds()
		snapshot.Buckets[index] = histogramBucketSnapshot{
			UpperBound:        h.bounds[index].String(),
			UpperBoundSeconds: &seconds,
			Count:             count,
		}
	}
	return snapshot
}

func (h *latencyHistogram) percentileLocked(percentile float64) float64 {
	if h.count == 0 {
		return 0
	}
	target := uint64(math.Ceil(percentile / 100 * float64(h.count)))
	if target == 0 {
		target = 1
	}
	var cumulative uint64
	for index, count := range h.buckets {
		cumulative += count
		if cumulative < target {
			continue
		}
		if index == len(h.bounds) {
			return h.max.Seconds()
		}
		return h.bounds[index].Seconds()
	}
	return h.max.Seconds()
}

func printLatencyHistogram(w io.Writer, title string, snapshot histogramSnapshot) {
	if snapshot.Count == 0 {
		return
	}
	var maxCount uint64
	for _, bucket := range snapshot.Buckets {
		maxCount = max(maxCount, bucket.Count)
	}
	_, _ = fmt.Fprintf(w, "%s:\n", title)
	lowerBound := "0s"
	for _, bucket := range snapshot.Buckets {
		upperBound := bucket.UpperBound
		if bucket.Count > 0 {
			barLength := max(1, int(float64(bucket.Count)/float64(maxCount)*histogramBarWidth))
			_, _ = fmt.Fprintf(
				w,
				"  (%9s, %9s] %8d |%s\n",
				lowerBound,
				upperBound,
				bucket.Count,
				strings.Repeat("#", barLength),
			)
		}
		lowerBound = upperBound
	}
}

type workloadStats struct {
	writeRequests      atomic.Uint64
	writeSuccess       atomic.Uint64
	writeErrors        atomic.Uint64
	readRequests       atomic.Uint64
	readSuccess        atomic.Uint64
	readErrors         atomic.Uint64
	deleteRequests     atomic.Uint64
	deleteSuccess      atomic.Uint64
	deleteErrors       atomic.Uint64
	verificationErrors atomic.Uint64
	bytesWritten       atomic.Uint64
	bytesRead          atomic.Uint64
	writeLatency       *latencyHistogram
	readLatency        *latencyHistogram
	deleteLatency      *latencyHistogram
	lastWriteError     atomic.Pointer[workloadErrorSample]
	lastReadError      atomic.Pointer[workloadErrorSample]
	lastDeleteError    atomic.Pointer[workloadErrorSample]
}

type workloadErrorSample struct {
	TenantID    string
	WorkerIndex int
	RemotePath  string
	Message     string
}

type workloadStatsSnapshot struct {
	WriteRequests      uint64            `json:"write_requests"`
	WriteSuccess       uint64            `json:"write_success"`
	WriteErrors        uint64            `json:"write_errors"`
	ReadRequests       uint64            `json:"read_requests"`
	ReadSuccess        uint64            `json:"read_success"`
	ReadErrors         uint64            `json:"read_errors"`
	DeleteRequests     uint64            `json:"delete_requests"`
	DeleteSuccess      uint64            `json:"delete_success"`
	DeleteErrors       uint64            `json:"delete_errors"`
	VerificationErrors uint64            `json:"verification_errors"`
	BytesWritten       uint64            `json:"bytes_written"`
	BytesRead          uint64            `json:"bytes_read"`
	WriteLatency       histogramSnapshot `json:"write_latency"`
	ReadLatency        histogramSnapshot `json:"read_latency"`
	DeleteLatency      histogramSnapshot `json:"delete_latency"`
	lastWriteError     *workloadErrorSample
	lastReadError      *workloadErrorSample
	lastDeleteError    *workloadErrorSample
}

func newWorkloadStats() *workloadStats {
	return &workloadStats{
		writeLatency:  newLatencyHistogram(defaultLatencyBounds),
		readLatency:   newLatencyHistogram(defaultLatencyBounds),
		deleteLatency: newLatencyHistogram(defaultLatencyBounds),
	}
}

func (s *workloadStats) recordWrite(duration time.Duration, bytes int, err error) {
	s.writeRequests.Add(1)
	s.writeLatency.Record(duration)
	if err != nil {
		s.writeErrors.Add(1)
		return
	}
	s.writeSuccess.Add(1)
	s.bytesWritten.Add(uint64(bytes))
}

func (s *workloadStats) recordRead(duration time.Duration, bytes int, err error) {
	s.readRequests.Add(1)
	s.readLatency.Record(duration)
	if err != nil {
		s.readErrors.Add(1)
		return
	}
	s.readSuccess.Add(1)
	s.bytesRead.Add(uint64(bytes))
}

func (s *workloadStats) recordDelete(duration time.Duration, err error) {
	s.deleteRequests.Add(1)
	s.deleteLatency.Record(duration)
	if err != nil {
		s.deleteErrors.Add(1)
		return
	}
	s.deleteSuccess.Add(1)
}

func (s *workloadStats) recordVerificationError() {
	s.verificationErrors.Add(1)
}

func (s *workloadStats) setLastWriteError(sample workloadErrorSample) {
	s.lastWriteError.Store(&sample)
}

func (s *workloadStats) setLastReadError(sample workloadErrorSample) {
	s.lastReadError.Store(&sample)
}

func (s *workloadStats) setLastDeleteError(sample workloadErrorSample) {
	s.lastDeleteError.Store(&sample)
}

func (s *workloadStats) Snapshot() workloadStatsSnapshot {
	return workloadStatsSnapshot{
		WriteRequests:      s.writeRequests.Load(),
		WriteSuccess:       s.writeSuccess.Load(),
		WriteErrors:        s.writeErrors.Load(),
		ReadRequests:       s.readRequests.Load(),
		ReadSuccess:        s.readSuccess.Load(),
		ReadErrors:         s.readErrors.Load(),
		DeleteRequests:     s.deleteRequests.Load(),
		DeleteSuccess:      s.deleteSuccess.Load(),
		DeleteErrors:       s.deleteErrors.Load(),
		VerificationErrors: s.verificationErrors.Load(),
		BytesWritten:       s.bytesWritten.Load(),
		BytesRead:          s.bytesRead.Load(),
		WriteLatency:       s.writeLatency.Snapshot(),
		ReadLatency:        s.readLatency.Snapshot(),
		DeleteLatency:      s.deleteLatency.Snapshot(),
		lastWriteError:     s.lastWriteError.Load(),
		lastReadError:      s.lastReadError.Load(),
		lastDeleteError:    s.lastDeleteError.Load(),
	}
}
