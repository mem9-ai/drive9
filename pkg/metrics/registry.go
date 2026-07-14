package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type Attribute struct {
	Key   string
	Value string
}

func Attr(key, value string) Attribute {
	return Attribute{Key: key, Value: value}
}

type instrumentKind string

const (
	instrumentCounter   instrumentKind = "counter"
	instrumentGauge     instrumentKind = "gauge"
	instrumentHistogram instrumentKind = "histogram"
)

type descriptor struct {
	name   string
	help   string
	scope  string
	kind   instrumentKind
	bounds []float64
}

type counterInstrument struct {
	desc   descriptor
	values map[string]int64
}

type gaugeInstrument struct {
	desc   descriptor
	values map[string]float64
}

type histogramSample struct {
	count   int64
	sum     float64
	buckets []int64
}

type histogramInstrument struct {
	desc    descriptor
	values  map[string]*histogramSample
	bounds  []float64
	boundsS []string
}

type counterSnapshot struct {
	desc   descriptor
	labels []string
	values map[string]int64
}

type gaugeSnapshot struct {
	desc   descriptor
	labels []string
	values map[string]float64
}

type histogramSnapshot struct {
	desc    descriptor
	labels  []string
	samples map[string]histogramSample
	boundsS []string
}

type moduleState struct {
	registeredAt time.Time
	up           float64
}

type Registry struct {
	mu sync.RWMutex

	counters   map[string]*counterInstrument
	gauges     map[string]*gaugeInstrument
	histograms map[string]*histogramInstrument
	modules    map[string]moduleState
}

type MeterProvider struct {
	registry *Registry
}

type Meter struct {
	scope    string
	registry *Registry
}

type Int64Counter struct {
	registry *Registry
	name     string
}

type Float64Gauge struct {
	registry *Registry
	name     string
}

type Float64Histogram struct {
	registry *Registry
	name     string
}

func NewRegistry() *Registry {
	return &Registry{
		counters:   map[string]*counterInstrument{},
		gauges:     map[string]*gaugeInstrument{},
		histograms: map[string]*histogramInstrument{},
		modules:    map[string]moduleState{},
	}
}

func NewMeterProvider(registry *Registry) *MeterProvider {
	if registry == nil {
		registry = NewRegistry()
	}
	return &MeterProvider{registry: registry}
}

func (p *MeterProvider) Meter(scope string) *Meter {
	return &Meter{scope: strings.TrimSpace(scope), registry: p.registry}
}

func (m *Meter) Int64Counter(name, help string) *Int64Counter {
	m.registry.ensureCounter(name, help, m.scope)
	return &Int64Counter{registry: m.registry, name: name}
}

func (m *Meter) Float64Gauge(name, help string) *Float64Gauge {
	m.registry.ensureGauge(name, help, m.scope)
	return &Float64Gauge{registry: m.registry, name: name}
}

func (m *Meter) Float64Histogram(name, help string, bounds []float64) *Float64Histogram {
	m.registry.ensureHistogram(name, help, m.scope, bounds)
	return &Float64Histogram{registry: m.registry, name: name}
}

func (c *Int64Counter) Add(value int64, attrs ...Attribute) {
	if c == nil || c.registry == nil || value == 0 {
		return
	}
	c.registry.addCounter(c.name, labelsKey(attrs), value)
}

func (g *Float64Gauge) Set(value float64, attrs ...Attribute) {
	if g == nil || g.registry == nil {
		return
	}
	g.registry.setGauge(g.name, labelsKey(attrs), value)
}

func (g *Float64Gauge) Delete(attrs ...Attribute) {
	if g == nil || g.registry == nil {
		return
	}
	g.registry.deleteGauge(g.name, labelsKey(attrs))
}

func (h *Float64Histogram) Observe(value float64, attrs ...Attribute) {
	if h == nil || h.registry == nil {
		return
	}
	h.registry.observeHistogram(h.name, labelsKey(attrs), value)
}

func (r *Registry) RegisterModule(module string) {
	module = strings.TrimSpace(module)
	if module == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.modules[module]; ok {
		return
	}
	r.modules[module] = moduleState{registeredAt: time.Now(), up: 1}
}

func (r *Registry) SetModuleAvailability(module string, up bool) {
	module = strings.TrimSpace(module)
	if module == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.modules[module]
	if state.registeredAt.IsZero() {
		state.registeredAt = time.Now()
	}
	if up {
		state.up = 1
	} else {
		state.up = 0
	}
	r.modules[module] = state
}

func (r *Registry) ensureCounter(name, help, scope string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if inst, ok := r.counters[name]; ok {
		if inst.desc.kind != instrumentCounter {
			panic(fmt.Sprintf("metrics: %s already registered with kind %s", name, inst.desc.kind))
		}
		return
	}
	r.counters[name] = &counterInstrument{
		desc:   descriptor{name: name, help: help, scope: scope, kind: instrumentCounter},
		values: map[string]int64{},
	}
}

func (r *Registry) ensureGauge(name, help, scope string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if inst, ok := r.gauges[name]; ok {
		if inst.desc.kind != instrumentGauge {
			panic(fmt.Sprintf("metrics: %s already registered with kind %s", name, inst.desc.kind))
		}
		return
	}
	r.gauges[name] = &gaugeInstrument{
		desc:   descriptor{name: name, help: help, scope: scope, kind: instrumentGauge},
		values: map[string]float64{},
	}
}

func (r *Registry) ensureHistogram(name, help, scope string, bounds []float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if inst, ok := r.histograms[name]; ok {
		if inst.desc.kind != instrumentHistogram {
			panic(fmt.Sprintf("metrics: %s already registered with kind %s", name, inst.desc.kind))
		}
		return
	}
	cleanBounds := make([]float64, len(bounds))
	copy(cleanBounds, bounds)
	sort.Float64s(cleanBounds)
	boundStrings := make([]string, len(cleanBounds))
	for i, bound := range cleanBounds {
		boundStrings[i] = FormatPromBound(bound)
	}
	r.histograms[name] = &histogramInstrument{
		desc:    descriptor{name: name, help: help, scope: scope, kind: instrumentHistogram, bounds: cleanBounds},
		values:  map[string]*histogramSample{},
		bounds:  cleanBounds,
		boundsS: boundStrings,
	}
}

func (r *Registry) addCounter(name, labels string, value int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.counters[name]
	if inst == nil {
		panic(fmt.Sprintf("metrics: counter %s not registered", name))
	}
	inst.values[labels] += value
}

func (r *Registry) setGauge(name, labels string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.gauges[name]
	if inst == nil {
		panic(fmt.Sprintf("metrics: gauge %s not registered", name))
	}
	inst.values[labels] = value
}

func (r *Registry) deleteGauge(name, labels string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.gauges[name]
	if inst == nil {
		panic(fmt.Sprintf("metrics: gauge %s not registered", name))
	}
	delete(inst.values, labels)
}

func (r *Registry) observeHistogram(name, labels string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst := r.histograms[name]
	if inst == nil {
		panic(fmt.Sprintf("metrics: histogram %s not registered", name))
	}
	sample := inst.values[labels]
	if sample == nil {
		sample = &histogramSample{buckets: make([]int64, len(inst.bounds))}
		inst.values[labels] = sample
	}
	sample.count++
	sample.sum += value
	for i, bound := range inst.bounds {
		if value <= bound {
			sample.buckets[i]++
		}
	}
}

func (r *Registry) WritePrometheus(w io.Writer) {
	if r == nil || w == nil {
		return
	}
	r.writeModules(w)
	r.writeCounters(w)
	r.writeHistograms(w)
	r.writeGauges(w)
}

func (r *Registry) writeModules(w io.Writer) {
	r.mu.RLock()
	modules := make(map[string]moduleState, len(r.modules))
	for module, state := range r.modules {
		modules[module] = state
	}
	r.mu.RUnlock()

	now := time.Now()
	names := make([]string, 0, len(modules))
	for module := range modules {
		names = append(names, module)
	}
	sort.Strings(names)

	_, _ = fmt.Fprintln(w, "# HELP drive9_module_up Module availability state")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_module_up gauge")
	for _, module := range names {
		writeMetricLine(w, "drive9_module_up", labelsKey([]Attribute{Attr("module", module)}), formatFloat(modules[module].up))
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_module_uptime_seconds Module uptime in seconds since first registration")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_module_uptime_seconds gauge")
	for _, module := range names {
		state := modules[module]
		uptime := 0.0
		if !state.registeredAt.IsZero() {
			uptime = now.Sub(state.registeredAt).Seconds()
		}
		writeMetricLine(w, "drive9_module_uptime_seconds", labelsKey([]Attribute{Attr("module", module)}), formatFloat(uptime))
	}
}

func (r *Registry) writeCounters(w io.Writer) {
	r.mu.RLock()
	names := make([]string, 0, len(r.counters))
	for name := range r.counters {
		names = append(names, name)
	}
	sort.Strings(names)
	snapshots := make([]counterSnapshot, 0, len(names))
	for _, name := range names {
		inst := r.counters[name]
		snapshots = append(snapshots, counterSnapshot{
			desc:   inst.desc,
			labels: SortedKeys(inst.values),
			values: CloneIntMap(inst.values),
		})
	}
	r.mu.RUnlock()
	for _, snapshot := range snapshots {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n", snapshot.desc.name, snapshot.desc.help)
		_, _ = fmt.Fprintf(w, "# TYPE %s counter\n", snapshot.desc.name)
		for _, labelSet := range snapshot.labels {
			writeMetricLine(w, snapshot.desc.name, labelSet, fmt.Sprintf("%d", snapshot.values[labelSet]))
		}
	}
}

func (r *Registry) writeHistograms(w io.Writer) {
	r.mu.RLock()
	names := make([]string, 0, len(r.histograms))
	for name := range r.histograms {
		names = append(names, name)
	}
	sort.Strings(names)
	snapshots := make([]histogramSnapshot, 0, len(names))
	for _, name := range names {
		inst := r.histograms[name]
		labels := make([]string, 0, len(inst.values))
		samples := make(map[string]histogramSample, len(inst.values))
		for labelSet, sample := range inst.values {
			labels = append(labels, labelSet)
			copied := histogramSample{count: sample.count, sum: sample.sum, buckets: make([]int64, len(sample.buckets))}
			copy(copied.buckets, sample.buckets)
			samples[labelSet] = copied
		}
		sort.Strings(labels)
		snapshots = append(snapshots, histogramSnapshot{
			desc:    inst.desc,
			labels:  labels,
			samples: samples,
			boundsS: append([]string(nil), inst.boundsS...),
		})
	}
	r.mu.RUnlock()
	for _, snapshot := range snapshots {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n", snapshot.desc.name, snapshot.desc.help)
		_, _ = fmt.Fprintf(w, "# TYPE %s histogram\n", snapshot.desc.name)
		for _, labelSet := range snapshot.labels {
			sample := snapshot.samples[labelSet]
			for i, bound := range snapshot.boundsS {
				writeMetricLine(w, snapshot.desc.name+"_bucket", appendBound(labelSet, bound), fmt.Sprintf("%d", sample.buckets[i]))
			}
			writeMetricLine(w, snapshot.desc.name+"_bucket", appendBound(labelSet, "+Inf"), fmt.Sprintf("%d", sample.count))
			writeMetricLine(w, snapshot.desc.name+"_count", labelSet, fmt.Sprintf("%d", sample.count))
			writeMetricLine(w, snapshot.desc.name+"_sum", labelSet, formatFloat(sample.sum))
		}
	}
}

func (r *Registry) writeGauges(w io.Writer) {
	r.mu.RLock()
	names := make([]string, 0, len(r.gauges))
	for name := range r.gauges {
		names = append(names, name)
	}
	sort.Strings(names)
	snapshots := make([]gaugeSnapshot, 0, len(names))
	for _, name := range names {
		inst := r.gauges[name]
		snapshots = append(snapshots, gaugeSnapshot{
			desc:   inst.desc,
			labels: SortedKeys(inst.values),
			values: CloneFloatMap(inst.values),
		})
	}
	r.mu.RUnlock()
	for _, snapshot := range snapshots {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n", snapshot.desc.name, snapshot.desc.help)
		_, _ = fmt.Fprintf(w, "# TYPE %s gauge\n", snapshot.desc.name)
		for _, labelSet := range snapshot.labels {
			writeMetricLine(w, snapshot.desc.name, labelSet, formatFloat(snapshot.values[labelSet]))
		}
	}
}

func labelsKey(attrs []Attribute) string {
	if len(attrs) == 0 {
		return ""
	}
	labels := make(map[string]string, len(attrs))
	keys := make([]string, 0, len(attrs))
	seen := make(map[string]struct{}, len(attrs))
	for _, attr := range attrs {
		key := normalizeLabelKey(attr.Key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; !ok {
			keys = append(keys, key)
			seen[key] = struct{}{}
		}
		labels[key] = attr.Value
	}
	if len(labels) == 0 {
		return ""
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(key)
		b.WriteString(`="`)
		b.WriteString(EscapePromLabel(labels[key]))
		b.WriteString(`"`)
	}
	return b.String()
}

func normalizeLabelKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func appendBound(labels, bound string) string {
	if labels == "" {
		return `le="` + bound + `"`
	}
	return labels + `,le="` + bound + `"`
}

func writeMetricLine(w io.Writer, name, labels, value string) {
	if labels == "" {
		_, _ = fmt.Fprintf(w, "%s %s\n", name, value)
		return
	}
	_, _ = fmt.Fprintf(w, "%s{%s} %s\n", name, labels, value)
}

func formatFloat(value float64) string {
	return fmt.Sprintf("%.6f", value)
}
