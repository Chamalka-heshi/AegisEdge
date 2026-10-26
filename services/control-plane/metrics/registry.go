package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	metricNameRegex = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelNameRegex  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// ValidateMetricName asserts that a metric name conforms to the Prometheus naming specification.
func ValidateMetricName(name string) error {
	if name == "" {
		return errors.New("metric name cannot be empty")
	}
	if !metricNameRegex.MatchString(name) {
		return fmt.Errorf("invalid metric name %q: must match [a-zA-Z_:][a-zA-Z0-9_:]*", name)
	}
	return nil
}

// ValidateLabelName asserts that a label name conforms to the Prometheus naming specification.
func ValidateLabelName(name string) error {
	if name == "" {
		return errors.New("label name cannot be empty")
	}
	if !labelNameRegex.MatchString(name) {
		return fmt.Errorf("invalid label name %q: must match [a-zA-Z_][a-zA-Z0-9_]*", name)
	}
	return nil
}

// LabeledMetric represents a metric associated with specific label dimensions.
type LabeledMetric interface {
	Metric
	LabelNames() []string
}

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
		newVal := math.Float64frombits(oldBits) + val
		newBits := math.Float64bits(newVal)
		if atomic.CompareAndSwapUint64(&c.bits, oldBits, newBits) {
			return
		}
	}
}

func (c *atomicCounter) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&c.bits))
}

// SingleCounter is a standalone unlabelled counter.
type SingleCounter struct {
	name string
	help string
	*atomicCounter
}

// NewSingleCounter constructs a new standalone counter.
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
	fmt.Fprintf(w, "# HELP %s %s\n", c.name, escapeHelp(c.help))
	fmt.Fprintf(w, "# TYPE %s %s\n", c.name, MetricTypeCounter)
	fmt.Fprintf(w, "%s %s\n", c.name, formatFloat(c.Value()))
	return nil
}

// FuncCounter is a counter whose value is evaluated dynamically from a provider function.
type FuncCounter struct {
	name string
	help string
	fn   func() float64
}

// NewFuncCounter constructs a dynamic counter evaluated at scrape time.
func NewFuncCounter(name, help string, fn func() float64) *FuncCounter {
	return &FuncCounter{
		name: name,
		help: help,
		fn:   fn,
	}
}

func (fc *FuncCounter) Name() string     { return fc.name }
func (fc *FuncCounter) Help() string     { return fc.help }
func (fc *FuncCounter) Type() MetricType { return MetricTypeCounter }

func (fc *FuncCounter) Value() float64 {
	if fc.fn == nil {
		return 0
	}
	return fc.fn()
}

func (fc *FuncCounter) WritePrometheus(w io.Writer) error {
	fmt.Fprintf(w, "# HELP %s %s\n", fc.name, escapeHelp(fc.help))
	fmt.Fprintf(w, "# TYPE %s %s\n", fc.name, MetricTypeCounter)
	fmt.Fprintf(w, "%s %s\n", fc.name, formatFloat(fc.Value()))
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

// NewCounterVec constructs a counter vector with specified label dimensions.
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

// LabelNames returns the configured label dimension names.
func (cv *CounterVec) LabelNames() []string {
	cv.mu.RLock()
	defer cv.mu.RUnlock()
	return append([]string(nil), cv.labelNames...)
}

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

	fmt.Fprintf(w, "# HELP %s %s\n", cv.name, escapeHelp(cv.help))
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
	atomic.StoreUint64(&g.bits, math.Float64bits(val))
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
		newVal := math.Float64frombits(oldBits) + val
		newBits := math.Float64bits(newVal)
		if atomic.CompareAndSwapUint64(&g.bits, oldBits, newBits) {
			return
		}
	}
}

func (g *atomicGauge) Sub(val float64) {
	g.Add(-val)
}

func (g *atomicGauge) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&g.bits))
}

// SingleGauge is a standalone unlabelled gauge.
type SingleGauge struct {
	name string
	help string
	*atomicGauge
}

// NewSingleGauge constructs a new standalone gauge.
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
	fmt.Fprintf(w, "# HELP %s %s\n", g.name, escapeHelp(g.help))
	fmt.Fprintf(w, "# TYPE %s %s\n", g.name, MetricTypeGauge)
	fmt.Fprintf(w, "%s %s\n", g.name, formatFloat(g.Value()))
	return nil
}

// FuncGauge is a gauge whose value is evaluated dynamically from a provider function at scrape time.
type FuncGauge struct {
	name string
	help string
	fn   func() float64
}

// NewFuncGauge constructs a gauge whose value is pulled dynamically.
func NewFuncGauge(name, help string, fn func() float64) *FuncGauge {
	return &FuncGauge{
		name: name,
		help: help,
		fn:   fn,
	}
}

func (fg *FuncGauge) Name() string     { return fg.name }
func (fg *FuncGauge) Help() string     { return fg.help }
func (fg *FuncGauge) Type() MetricType { return MetricTypeGauge }

func (fg *FuncGauge) Value() float64 {
	if fg.fn == nil {
		return 0
	}
	return fg.fn()
}

func (fg *FuncGauge) WritePrometheus(w io.Writer) error {
	fmt.Fprintf(w, "# HELP %s %s\n", fg.name, escapeHelp(fg.help))
	fmt.Fprintf(w, "# TYPE %s %s\n", fg.name, MetricTypeGauge)
	fmt.Fprintf(w, "%s %s\n", fg.name, formatFloat(fg.Value()))
	return nil
}

// ============================================================================
// Histogram Implementation
// ============================================================================

// DefaultStorageProbeBuckets represents sub-millisecond to multi-second latency bounds for database probes.
var DefaultStorageProbeBuckets = []float64{
	0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0,
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

type histogramSnapshot struct {
	buckets []float64
	counts  []uint64
	sum     float64
	total   uint64
}

func (h *histogramState) snapshot() histogramSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	b := append([]float64(nil), h.buckets...)
	c := append([]uint64(nil), h.counts...)
	return histogramSnapshot{
		buckets: b,
		counts:  c,
		sum:     h.sum,
		total:   h.total,
	}
}

// SingleHistogram is a standalone unlabelled histogram.
type SingleHistogram struct {
	name string
	help string
	*histogramState
}

// NewSingleHistogram constructs an unlabelled histogram.
func NewSingleHistogram(name, help string, buckets []float64) *SingleHistogram {
	if len(buckets) == 0 {
		buckets = DefaultStorageProbeBuckets
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
	fmt.Fprintf(w, "# HELP %s %s\n", h.name, escapeHelp(h.help))
	fmt.Fprintf(w, "# TYPE %s %s\n", h.name, MetricTypeHistogram)

	snap := h.snapshot()

	for i, b := range snap.buckets {
		fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", h.name, formatFloat(b), snap.counts[i])
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", h.name, snap.counts[len(snap.buckets)])
	fmt.Fprintf(w, "%s_sum %s\n", h.name, formatFloat(snap.sum))
	fmt.Fprintf(w, "%s_count %d\n", h.name, snap.total)
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

// Register registers a metric into the registry. Returns error on invalid name, invalid labels, or duplicate name.
func (r *Registry) Register(m Metric) error {
	if m == nil {
		return errors.New("cannot register nil metric")
	}
	name := m.Name()
	if err := ValidateMetricName(name); err != nil {
		return err
	}
	if lm, ok := m.(LabeledMetric); ok {
		for _, lbl := range lm.LabelNames() {
			if err := ValidateLabelName(lbl); err != nil {
				return fmt.Errorf("metric %q has invalid label: %w", name, err)
			}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

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
// Formatting Helpers
// ============================================================================

func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

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
