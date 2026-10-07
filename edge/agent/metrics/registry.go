package metrics

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// MetricType denotes the Prometheus metric type.
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
)

// Metric is the common interface implemented by all observable metrics.
type Metric interface {
	Name() string
	Help() string
	Type() MetricType
	WritePrometheus(w io.Writer) error
}

// ============================================================================
// Counter Implementation
// ============================================================================

// Counter represents a monotonically increasing numerical counter.
type Counter interface {
	Inc()
	Add(val float64)
	Value() float64
}

type atomicCounter struct {
	bits uint64
}

func newAtomicCounter() *atomicCounter {
	return &atomicCounter{}
}

func (c *atomicCounter) Inc() {
	c.Add(1.0)
}

func (c *atomicCounter) Add(val float64) {
	if val <= 0 {
		return
	}
	for {
		oldBits := atomic.LoadUint64(&c.bits)
		newVal := mathFloat64frombits(oldBits) + val
		newBits := mathFloat64bits(newVal)
		if atomic.CompareAndSwapUint64(&c.bits, oldBits, newBits) {
			return
		}
	}
}

func (c *atomicCounter) Value() float64 {
	return mathFloat64frombits(atomic.LoadUint64(&c.bits))
}

// SingleCounter is a standalone unlabelled counter.
type SingleCounter struct {
	name string
	help string
	*atomicCounter
}

func NewSingleCounter(name, help string) *SingleCounter {
	return &SingleCounter{
		name:          name,
		help:          help,
		atomicCounter: newAtomicCounter(),
	}
}

func (c *SingleCounter) Name() string     { return c.name }
func (c *SingleCounter) Help() string     { return c.help }
func (c *SingleCounter) Type() MetricType { return MetricTypeCounter }

func (c *SingleCounter) WritePrometheus(w io.Writer) error {
	fmt.Fprintf(w, "# HELP %s %s\n", c.name, c.help)
	fmt.Fprintf(w, "# TYPE %s %s\n", c.name, MetricTypeCounter)
	fmt.Fprintf(w, "%s %s\n", c.name, formatFloat(c.Value()))
	return nil
}

// CounterVec is a counter partitioned across bounded label dimensions.
type CounterVec struct {
	mu         sync.RWMutex
	name       string
	help       string
	labelNames []string
	counters   map[string]*atomicCounter
	labelsMap  map[string][]string
}

func NewCounterVec(name, help string, labelNames []string) *CounterVec {
	return &CounterVec{
		name:       name,
		help:       help,
		labelNames: labelNames,
		counters:   make(map[string]*atomicCounter),
		labelsMap:  make(map[string][]string),
	}
}

func (cv *CounterVec) Name() string     { return cv.name }
func (cv *CounterVec) Help() string     { return cv.help }
func (cv *CounterVec) Type() MetricType { return MetricTypeCounter }

func (cv *CounterVec) WithLabelValues(lvs ...string) Counter {
	key := strings.Join(lvs, "\x00")
	cv.mu.RLock()
	c, ok := cv.counters[key]
	cv.mu.RUnlock()
	if ok {
		return c
	}

	cv.mu.Lock()
	defer cv.mu.Unlock()
	if c, ok = cv.counters[key]; ok {
		return c
	}
	c = newAtomicCounter()
	cv.counters[key] = c
	cv.labelsMap[key] = append([]string(nil), lvs...)
	return c
}

func (cv *CounterVec) WritePrometheus(w io.Writer) error {
	cv.mu.RLock()
	defer cv.mu.RUnlock()

	fmt.Fprintf(w, "# HELP %s %s\n", cv.name, cv.help)
	fmt.Fprintf(w, "# TYPE %s %s\n", cv.name, MetricTypeCounter)

	keys := make([]string, 0, len(cv.counters))
	for k := range cv.counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		c := cv.counters[k]
		lvs := cv.labelsMap[k]
		labelStr := formatLabels(cv.labelNames, lvs)
		fmt.Fprintf(w, "%s%s %s\n", cv.name, labelStr, formatFloat(c.Value()))
	}
	return nil
}

// ============================================================================
// Gauge Implementation
// ============================================================================

// Gauge represents a numerical value that can arbitrarily go up and down.
type Gauge interface {
	Set(val float64)
	Inc()
	Dec()
	Add(val float64)
	Sub(val float64)
	Value() float64
}

type atomicGauge struct {
	bits uint64
}

func newAtomicGauge() *atomicGauge {
	return &atomicGauge{}
}

func (g *atomicGauge) Set(val float64) {
	atomic.StoreUint64(&g.bits, mathFloat64bits(val))
}

func (g *atomicGauge) Inc() {
	g.Add(1.0)
}

func (g *atomicGauge) Dec() {
	g.Sub(1.0)
}

func (g *atomicGauge) Add(val float64) {
	for {
		oldBits := atomic.LoadUint64(&g.bits)
		newVal := mathFloat64frombits(oldBits) + val
		newBits := mathFloat64bits(newVal)
		if atomic.CompareAndSwapUint64(&g.bits, oldBits, newBits) {
			return
		}
	}
}

func (g *atomicGauge) Sub(val float64) {
	g.Add(-val)
}

func (g *atomicGauge) Value() float64 {
	return mathFloat64frombits(atomic.LoadUint64(&g.bits))
}

// SingleGauge is a standalone unlabelled gauge.
type SingleGauge struct {
	name string
	help string
	*atomicGauge
}

func NewSingleGauge(name, help string) *SingleGauge {
	return &SingleGauge{
		name:        name,
		help:        help,
		atomicGauge: newAtomicGauge(),
	}
}

func (g *SingleGauge) Name() string     { return g.name }
func (g *SingleGauge) Help() string     { return g.help }
func (g *SingleGauge) Type() MetricType { return MetricTypeGauge }

func (g *SingleGauge) WritePrometheus(w io.Writer) error {
	fmt.Fprintf(w, "# HELP %s %s\n", g.name, g.help)
	fmt.Fprintf(w, "# TYPE %s %s\n", g.name, MetricTypeGauge)
	fmt.Fprintf(w, "%s %s\n", g.name, formatFloat(g.Value()))
	return nil
}

// GaugeVec is a gauge partitioned across bounded label dimensions.
type GaugeVec struct {
	mu         sync.RWMutex
	name       string
	help       string
	labelNames []string
	gauges     map[string]*atomicGauge
	labelsMap  map[string][]string
}

func NewGaugeVec(name, help string, labelNames []string) *GaugeVec {
	return &GaugeVec{
		name:       name,
		help:       help,
		labelNames: labelNames,
		gauges:     make(map[string]*atomicGauge),
		labelsMap:  make(map[string][]string),
	}
}

func (gv *GaugeVec) Name() string     { return gv.name }
func (gv *GaugeVec) Help() string     { return gv.help }
func (gv *GaugeVec) Type() MetricType { return MetricTypeGauge }

func (gv *GaugeVec) WithLabelValues(lvs ...string) Gauge {
	key := strings.Join(lvs, "\x00")
	gv.mu.RLock()
	g, ok := gv.gauges[key]
	gv.mu.RUnlock()
	if ok {
		return g
	}

	gv.mu.Lock()
	defer gv.mu.Unlock()
	if g, ok = gv.gauges[key]; ok {
		return g
	}
	g = newAtomicGauge()
	gv.gauges[key] = g
	gv.labelsMap[key] = append([]string(nil), lvs...)
	return g
}

func (gv *GaugeVec) WritePrometheus(w io.Writer) error {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	fmt.Fprintf(w, "# HELP %s %s\n", gv.name, gv.help)
	fmt.Fprintf(w, "# TYPE %s %s\n", gv.name, MetricTypeGauge)

	keys := make([]string, 0, len(gv.gauges))
	for k := range gv.gauges {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		g := gv.gauges[k]
		lvs := gv.labelsMap[k]
		labelStr := formatLabels(gv.labelNames, lvs)
		fmt.Fprintf(w, "%s%s %s\n", gv.name, labelStr, formatFloat(g.Value()))
	}
	return nil
}

// ============================================================================
// Histogram Implementation
// ============================================================================

// DefaultDurationBuckets represents standard sub-second to multi-second latency bounds.
var DefaultDurationBuckets = []float64{
	0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
}

// Histogram tracks the statistical distribution of observations across fixed buckets.
type Histogram interface {
	Observe(val float64)
	Count() uint64
	Sum() float64
	Buckets() []float64
	BucketCounts() []uint64
}

type histogramState struct {
	mu      sync.RWMutex
	buckets []float64 // strictly increasing
	counts  []uint64  // length len(buckets) + 1 (last is +Inf)
	sum     float64
	total   uint64
}

func newHistogramState(buckets []float64) *histogramState {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	return &histogramState{
		buckets: b,
		counts:  make([]uint64, len(b)+1),
	}
}

func (h *histogramState) Observe(val float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.total++
	h.sum += val

	for i, upper := range h.buckets {
		if val <= upper {
			h.counts[i]++
		}
	}
	// +Inf bucket receives all observations
	h.counts[len(h.buckets)]++
}

func (h *histogramState) Count() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.total
}

func (h *histogramState) Sum() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.sum
}

func (h *histogramState) Buckets() []float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return append([]float64(nil), h.buckets...)
}

func (h *histogramState) BucketCounts() []uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return append([]uint64(nil), h.counts...)
}

// SingleHistogram is a standalone unlabelled histogram.
type SingleHistogram struct {
	name string
	help string
	*histogramState
}

func NewSingleHistogram(name, help string, buckets []float64) *SingleHistogram {
	if len(buckets) == 0 {
		buckets = DefaultDurationBuckets
	}
	return &SingleHistogram{
		name:           name,
		help:           help,
		histogramState: newHistogramState(buckets),
	}
}

func (h *SingleHistogram) Name() string     { return h.name }
func (h *SingleHistogram) Help() string     { return h.help }
func (h *SingleHistogram) Type() MetricType { return MetricTypeHistogram }

func (h *SingleHistogram) WritePrometheus(w io.Writer) error {
	fmt.Fprintf(w, "# HELP %s %s\n", h.name, h.help)
	fmt.Fprintf(w, "# TYPE %s %s\n", h.name, MetricTypeHistogram)

	buckets := h.Buckets()
	counts := h.BucketCounts()

	for i, b := range buckets {
		fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", h.name, formatFloat(b), counts[i])
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", h.name, counts[len(buckets)])
	fmt.Fprintf(w, "%s_sum %s\n", h.name, formatFloat(h.Sum()))
	fmt.Fprintf(w, "%s_count %d\n", h.name, h.Count())
	return nil
}

// HistogramVec is a histogram partitioned across bounded label dimensions.
type HistogramVec struct {
	mu         sync.RWMutex
	name       string
	help       string
	labelNames []string
	buckets    []float64
	histograms map[string]*histogramState
	labelsMap  map[string][]string
}

func NewHistogramVec(name, help string, labelNames []string, buckets []float64) *HistogramVec {
	if len(buckets) == 0 {
		buckets = DefaultDurationBuckets
	}
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	return &HistogramVec{
		name:       name,
		help:       help,
		labelNames: labelNames,
		buckets:    b,
		histograms: make(map[string]*histogramState),
		labelsMap:  make(map[string][]string),
	}
}

func (hv *HistogramVec) Name() string     { return hv.name }
func (hv *HistogramVec) Help() string     { return hv.help }
func (hv *HistogramVec) Type() MetricType { return MetricTypeHistogram }

func (hv *HistogramVec) WithLabelValues(lvs ...string) Histogram {
	key := strings.Join(lvs, "\x00")
	hv.mu.RLock()
	h, ok := hv.histograms[key]
	hv.mu.RUnlock()
	if ok {
		return h
	}

	hv.mu.Lock()
	defer hv.mu.Unlock()
	if h, ok = hv.histograms[key]; ok {
		return h
	}
	h = newHistogramState(hv.buckets)
	hv.histograms[key] = h
	hv.labelsMap[key] = append([]string(nil), lvs...)
	return h
}

func (hv *HistogramVec) WritePrometheus(w io.Writer) error {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	fmt.Fprintf(w, "# HELP %s %s\n", hv.name, hv.help)
	fmt.Fprintf(w, "# TYPE %s %s\n", hv.name, MetricTypeHistogram)

	keys := make([]string, 0, len(hv.histograms))
	for k := range hv.histograms {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		h := hv.histograms[k]
		lvs := hv.labelsMap[k]

		buckets := h.Buckets()
		counts := h.BucketCounts()

		for i, b := range buckets {
			lbl := formatLabelsWithExtra(hv.labelNames, lvs, "le", formatFloat(b))
			fmt.Fprintf(w, "%s_bucket%s %d\n", hv.name, lbl, counts[i])
		}
		infLbl := formatLabelsWithExtra(hv.labelNames, lvs, "le", "+Inf")
		fmt.Fprintf(w, "%s_bucket%s %d\n", hv.name, infLbl, counts[len(buckets)])

		baseLbl := formatLabels(hv.labelNames, lvs)
		fmt.Fprintf(w, "%s_sum%s %s\n", hv.name, baseLbl, formatFloat(h.Sum()))
		fmt.Fprintf(w, "%s_count%s %d\n", hv.name, baseLbl, h.Count())
	}
	return nil
}

// ============================================================================
// Registry Implementation
// ============================================================================

// Registry manages thread-safe registration and Prometheus formatting of metrics.
type Registry struct {
	mu          sync.RWMutex
	metrics     map[string]Metric
	metricOrder []string
}

// NewRegistry constructs a clean, isolated metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		metrics:     make(map[string]Metric),
		metricOrder: make([]string, 0),
	}
}

// Register registers a metric into the registry. Returns error on duplicate name.
func (r *Registry) Register(m Metric) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := m.Name()
	if _, exists := r.metrics[name]; exists {
		return fmt.Errorf("metric already registered: %s", name)
	}
	r.metrics[name] = m
	r.metricOrder = append(r.metricOrder, name)
	return nil
}

// Get retrieves a metric by name.
func (r *Registry) Get(name string) (Metric, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.metrics[name]
	return m, ok
}

// FormatPrometheus serializes all registered metrics into Prometheus text format.
func (r *Registry) FormatPrometheus() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var buf bytes.Buffer
	for _, name := range r.metricOrder {
		m := r.metrics[name]
		_ = m.WritePrometheus(&buf)
	}
	return buf.String()
}

// Handler returns a standard read-only http.Handler exposing /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// HTTP SAFETY: strictly read-only; only GET and HEAD permitted
		if req.Method != http.MethodGet && req.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if req.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		body := r.FormatPrometheus()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})
}

// ============================================================================
// Formatting Helpers (Zero external dependencies)
// ============================================================================

func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func escapeString(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func formatLabels(names, values []string) string {
	if len(names) == 0 || len(values) == 0 {
		return ""
	}
	pairs := make([]string, len(names))
	for i := range names {
		pairs[i] = fmt.Sprintf("%s=\"%s\"", names[i], escapeString(values[i]))
	}
	return "{" + strings.Join(pairs, ",") + "}"
}

func formatLabelsWithExtra(names, values []string, extraKey, extraVal string) string {
	pairs := make([]string, 0, len(names)+1)
	for i := range names {
		pairs = append(pairs, fmt.Sprintf("%s=\"%s\"", names[i], escapeString(values[i])))
	}
	pairs = append(pairs, fmt.Sprintf("%s=\"%s\"", extraKey, escapeString(extraVal)))
	return "{" + strings.Join(pairs, ",") + "}"
}

func mathFloat64bits(f float64) uint64 {
	return math.Float64bits(f)
}

func mathFloat64frombits(b uint64) float64 {
	return math.Float64frombits(b)
}
