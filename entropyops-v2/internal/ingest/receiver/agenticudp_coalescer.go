package receiver

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/entropyops/entropyops-v2/internal/storage"
)

// flushLatencyTracker centralises per-signal flush latency stats so the
// three coalescers (traces / metrics / logs) all log slow flushes with
// the same threshold, increment the same exposed counters, and stay
// observable without adding a metrics dependency to this file.
//
// The single most common production question we want this to answer is:
// "When the AgenticUDP 5000-span bench shows a 3-second p90 with zero
// retransmits, is the storage write itself slow, or is something else
// queueing the flush goroutine?" A slow-flush log line per occurrence
// plus monotonic counters lets us answer that without rerunning the
// bench under a profiler.
type flushLatencyTracker struct {
	signal      string        // "traces" | "metrics" | "logs"
	slowAfter   time.Duration // log line threshold; default 200ms, env-tunable
	totalFlushes atomic.Int64
	totalItems   atomic.Int64
	slowFlushes  atomic.Int64
	maxObserved  atomic.Int64 // nanoseconds; max single flush latency seen
	totalLatency atomic.Int64 // nanoseconds; sum, divide by totalFlushes for avg
}

func newFlushLatencyTracker(signal string) *flushLatencyTracker {
	thresh := 200 * time.Millisecond
	if raw := strings.TrimSpace(os.Getenv("ENTROPYOPS_AGENTIC_UDP_SLOW_FLUSH_MS")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			thresh = time.Duration(v) * time.Millisecond
		}
	}
	return &flushLatencyTracker{signal: signal, slowAfter: thresh}
}

// observe records a single flush. Hot path: 4 atomic adds, 1 compare-
// and-swap on slow path. Logs a single line when the flush exceeded
// slowAfter, so an operator scrolling logs can spot the exact tenant
// and item count that stalled.
func (t *flushLatencyTracker) observe(tenantID string, items int, dur time.Duration) {
	t.totalFlushes.Add(1)
	t.totalItems.Add(int64(items))
	t.totalLatency.Add(int64(dur))
	for {
		cur := t.maxObserved.Load()
		if int64(dur) <= cur || t.maxObserved.CompareAndSwap(cur, int64(dur)) {
			break
		}
	}
	if dur >= t.slowAfter {
		t.slowFlushes.Add(1)
		log.Printf("agenticudp: SLOW FLUSH signal=%s tenant=%s items=%d duration=%s (threshold=%s) — investigate storage write contention",
			t.signal, tenantID, items, dur.Round(time.Millisecond), t.slowAfter)
	}
}

// FlushStats is the snapshot exported via the receiver's Stats() output
// and consumed by the periodic agenticudp_stats logger so operators
// can see the flush latency profile without restarting the bench.
type FlushStats struct {
	Signal       string
	Flushes      int64
	Items        int64
	SlowFlushes  int64
	MaxLatencyMs int64
	AvgLatencyMs int64
}

func (t *flushLatencyTracker) snapshot() FlushStats {
	flushes := t.totalFlushes.Load()
	totalNs := t.totalLatency.Load()
	var avgMs int64
	if flushes > 0 {
		avgMs = (totalNs / flushes) / int64(time.Millisecond)
	}
	return FlushStats{
		Signal:       t.signal,
		Flushes:      flushes,
		Items:        t.totalItems.Load(),
		SlowFlushes:  t.slowFlushes.Load(),
		MaxLatencyMs: t.maxObserved.Load() / int64(time.Millisecond),
		AvgLatencyMs: avgMs,
	}
}

// Why this file exists
//
// Before coalescing, every AgenticUDP datagram that landed on the receiver
// triggered its own storage.Backend.WriteTraces / WriteMetrics / WriteLogs
// call. Each of those opens a SQLite transaction, prepares a statement,
// executes a row insert per item, and commits — and the commit includes an
// fsync (~5–50 ms in WAL mode on a developer laptop). A bulk send of 5000
// spans chunked across ~334 datagrams therefore paid 334 fsyncs serially in
// the receiver's read loop and bottoming the read goroutine on storage,
// which let the kernel UDP socket buffer fill, which made the agent
// retransmit, which doubled or tripled the packet count, which made the
// problem worse. The eo-ingest-bench v6 5000-span run measured 32 s p50
// for AgenticUDP because of this.
//
// The fix is a per-signal-type coalescer that:
//   - Accepts items via a channel from the read-loop goroutines (non-blocking
//     when the channel has space; falls back to direct write under back-
//     pressure so we never silently drop telemetry).
//   - Buffers items per tenant in memory.
//   - Flushes a tenant's batch when EITHER the per-tenant accumulated item
//     count crosses maxItems OR maxWait elapses.
//   - On shutdown, drains and flushes everything still pending.
//
// The result: 334 chunks of 15 spans become ~1 storage write of ~5000 spans,
// or a small number of writes paced by maxWait. The read loop is no longer
// blocked on SQL at all.

// traceBatchEntry, metricBatchEntry, logBatchEntry are the typed channel
// payloads. We use typed channels (rather than interface{}) so the hot path
// avoids per-item interface boxing.
type traceBatchEntry struct {
	tenantID string
	items    []storage.Trace
}
type metricBatchEntry struct {
	tenantID string
	items    []storage.Metric
}
type logBatchEntry struct {
	tenantID string
	items    []storage.LogRecord
}

// coalescerConfig is the shared knob set for all three signal coalescers.
// Sensible defaults are chosen for SQLite-on-laptop; operators with a
// faster store (Postgres, ClickHouse) can shrink maxWait further.
type coalescerConfig struct {
	maxItems int           // flush a tenant when its buffer reaches this many items
	maxWait  time.Duration // flush every tenant if this much time has elapsed since last flush
	chanCap  int           // size of the inbound channel (back-pressure threshold)
	name     string        // for log lines
}

// traceCoalescer batches storage.Trace writes from many AgenticUDP datagrams
// into a small number of storage.Backend.WriteTraces calls.
type traceCoalescer struct {
	in       chan traceBatchEntry
	cfg      coalescerConfig
	flush    func(tenantID string, items []storage.Trace)
	pending  map[string][]storage.Trace
	pendingN int
	mu       sync.Mutex
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopped  chan struct{}
	latency  *flushLatencyTracker
}

func newTraceCoalescer(cfg coalescerConfig, flush func(string, []storage.Trace)) *traceCoalescer {
	tracker := newFlushLatencyTracker("traces")
	wrapped := func(tenantID string, items []storage.Trace) {
		t0 := time.Now()
		flush(tenantID, items)
		tracker.observe(tenantID, len(items), time.Since(t0))
	}
	return &traceCoalescer{
		in:      make(chan traceBatchEntry, cfg.chanCap),
		cfg:     cfg,
		flush:   wrapped,
		pending: make(map[string][]storage.Trace),
		stopped: make(chan struct{}),
		latency: tracker,
	}
}

func (c *traceCoalescer) enqueue(tenantID string, items []storage.Trace) {
	if len(items) == 0 {
		return
	}
	select {
	case c.in <- traceBatchEntry{tenantID: tenantID, items: items}:
	default:
		// Channel full → flush directly so we never silently drop. This is
		// the safety valve: under sustained extreme load the coalescer
		// degrades to the legacy synchronous-write behavior instead of
		// losing data.
		c.flush(tenantID, items)
	}
}

func (c *traceCoalescer) start(ctx context.Context) {
	c.wg.Add(1)
	go c.run(ctx)
}

func (c *traceCoalescer) run(ctx context.Context) {
	defer c.wg.Done()
	defer close(c.stopped)

	ticker := time.NewTicker(c.cfg.maxWait)
	defer ticker.Stop()

	flushAll := func() {
		c.mu.Lock()
		batches := c.pending
		c.pending = make(map[string][]storage.Trace)
		c.pendingN = 0
		c.mu.Unlock()
		for tid, items := range batches {
			if len(items) > 0 {
				c.flush(tid, items)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			flushAll()
			return
		case e, ok := <-c.in:
			if !ok {
				flushAll()
				return
			}
			c.mu.Lock()
			c.pending[e.tenantID] = append(c.pending[e.tenantID], e.items...)
			c.pendingN += len(e.items)
			n := len(c.pending[e.tenantID])
			c.mu.Unlock()
			if n >= c.cfg.maxItems {
				c.mu.Lock()
				items := c.pending[e.tenantID]
				delete(c.pending, e.tenantID)
				c.pendingN -= len(items)
				c.mu.Unlock()
				c.flush(e.tenantID, items)
			}
		case <-ticker.C:
			flushAll()
		}
	}
}

func (c *traceCoalescer) stop() {
	c.stopOnce.Do(func() {
		close(c.in)
	})
	c.wg.Wait()
}

// metricCoalescer is structurally identical to traceCoalescer for storage.Metric.
type metricCoalescer struct {
	in       chan metricBatchEntry
	cfg      coalescerConfig
	flush    func(tenantID string, items []storage.Metric)
	pending  map[string][]storage.Metric
	mu       sync.Mutex
	wg       sync.WaitGroup
	stopOnce sync.Once
	latency  *flushLatencyTracker
}

func newMetricCoalescer(cfg coalescerConfig, flush func(string, []storage.Metric)) *metricCoalescer {
	tracker := newFlushLatencyTracker("metrics")
	wrapped := func(tenantID string, items []storage.Metric) {
		t0 := time.Now()
		flush(tenantID, items)
		tracker.observe(tenantID, len(items), time.Since(t0))
	}
	return &metricCoalescer{
		in:      make(chan metricBatchEntry, cfg.chanCap),
		cfg:     cfg,
		flush:   wrapped,
		pending: make(map[string][]storage.Metric),
		latency: tracker,
	}
}

func (c *metricCoalescer) enqueue(tenantID string, items []storage.Metric) {
	if len(items) == 0 {
		return
	}
	select {
	case c.in <- metricBatchEntry{tenantID: tenantID, items: items}:
	default:
		c.flush(tenantID, items)
	}
}

func (c *metricCoalescer) start(ctx context.Context) {
	c.wg.Add(1)
	go c.run(ctx)
}

func (c *metricCoalescer) run(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.maxWait)
	defer ticker.Stop()

	flushAll := func() {
		c.mu.Lock()
		batches := c.pending
		c.pending = make(map[string][]storage.Metric)
		c.mu.Unlock()
		for tid, items := range batches {
			if len(items) > 0 {
				c.flush(tid, items)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			flushAll()
			return
		case e, ok := <-c.in:
			if !ok {
				flushAll()
				return
			}
			c.mu.Lock()
			c.pending[e.tenantID] = append(c.pending[e.tenantID], e.items...)
			n := len(c.pending[e.tenantID])
			c.mu.Unlock()
			if n >= c.cfg.maxItems {
				c.mu.Lock()
				items := c.pending[e.tenantID]
				delete(c.pending, e.tenantID)
				c.mu.Unlock()
				c.flush(e.tenantID, items)
			}
		case <-ticker.C:
			flushAll()
		}
	}
}

func (c *metricCoalescer) stop() {
	c.stopOnce.Do(func() { close(c.in) })
	c.wg.Wait()
}

// logCoalescer is the storage.LogRecord variant.
type logCoalescer struct {
	in       chan logBatchEntry
	cfg      coalescerConfig
	flush    func(tenantID string, items []storage.LogRecord)
	pending  map[string][]storage.LogRecord
	mu       sync.Mutex
	wg       sync.WaitGroup
	stopOnce sync.Once
	latency  *flushLatencyTracker
}

func newLogCoalescer(cfg coalescerConfig, flush func(string, []storage.LogRecord)) *logCoalescer {
	tracker := newFlushLatencyTracker("logs")
	wrapped := func(tenantID string, items []storage.LogRecord) {
		t0 := time.Now()
		flush(tenantID, items)
		tracker.observe(tenantID, len(items), time.Since(t0))
	}
	return &logCoalescer{
		in:      make(chan logBatchEntry, cfg.chanCap),
		cfg:     cfg,
		flush:   wrapped,
		pending: make(map[string][]storage.LogRecord),
		latency: tracker,
	}
}

func (c *logCoalescer) enqueue(tenantID string, items []storage.LogRecord) {
	if len(items) == 0 {
		return
	}
	select {
	case c.in <- logBatchEntry{tenantID: tenantID, items: items}:
	default:
		c.flush(tenantID, items)
	}
}

func (c *logCoalescer) start(ctx context.Context) {
	c.wg.Add(1)
	go c.run(ctx)
}

func (c *logCoalescer) run(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.maxWait)
	defer ticker.Stop()

	flushAll := func() {
		c.mu.Lock()
		batches := c.pending
		c.pending = make(map[string][]storage.LogRecord)
		c.mu.Unlock()
		for tid, items := range batches {
			if len(items) > 0 {
				c.flush(tid, items)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			flushAll()
			return
		case e, ok := <-c.in:
			if !ok {
				flushAll()
				return
			}
			c.mu.Lock()
			c.pending[e.tenantID] = append(c.pending[e.tenantID], e.items...)
			n := len(c.pending[e.tenantID])
			c.mu.Unlock()
			if n >= c.cfg.maxItems {
				c.mu.Lock()
				items := c.pending[e.tenantID]
				delete(c.pending, e.tenantID)
				c.mu.Unlock()
				c.flush(e.tenantID, items)
			}
		case <-ticker.C:
			flushAll()
		}
	}
}

func (c *logCoalescer) stop() {
	c.stopOnce.Do(func() { close(c.in) })
	c.wg.Wait()
}

// loadCoalescerConfig reads coalescer tuning from environment so operators
// can adjust per-deployment without rebuilding. Defaults are tuned for
// SQLite-on-developer-laptop and have been validated end-to-end via
// eo-ingest-bench's 1000- and 5000-span scenarios.
func loadCoalescerConfig(name string) coalescerConfig {
	return coalescerConfig{
		name:     name,
		maxItems: envIntDefault("ENTROPYOPS_AGENTIC_UDP_BATCH_MAX_ITEMS", 2000),
		maxWait:  time.Duration(envIntDefault("ENTROPYOPS_AGENTIC_UDP_BATCH_MAX_WAIT_MS", 50)) * time.Millisecond,
		chanCap:  envIntDefault("ENTROPYOPS_AGENTIC_UDP_BATCH_CHAN_CAP", 4096),
	}
}

// startCoalescers attaches the three signal coalescers to the receiver and
// kicks off their background goroutines. Safe to call when r.store is nil
// (returns immediately, leaving coalescer pointers nil so enqueue is a
// no-op via the existing nil check in storeTraces/etc.).
func (r *AgenticUDPReceiver) startCoalescers(ctx context.Context) {
	if r.store == nil {
		log.Printf("agenticudp: store not attached — coalescer disabled, falling back to direct writes")
		return
	}
	r.traceBatcher = newTraceCoalescer(loadCoalescerConfig("traces"), r.storeTraces)
	r.metricBatcher = newMetricCoalescer(loadCoalescerConfig("metrics"), r.storeMetrics)
	r.logBatcher = newLogCoalescer(loadCoalescerConfig("logs"), r.storeLogs)
	r.traceBatcher.start(ctx)
	r.metricBatcher.start(ctx)
	r.logBatcher.start(ctx)
	log.Printf("agenticudp: storage coalescers started (max_items=%d max_wait=%s chan_cap=%d slow_flush_threshold=%s)",
		r.traceBatcher.cfg.maxItems, r.traceBatcher.cfg.maxWait, r.traceBatcher.cfg.chanCap,
		r.traceBatcher.latency.slowAfter)
	go r.runFlushStatsLogger(ctx)
}

// runFlushStatsLogger emits a single concise stats line per signal type
// every 10s while the receiver is running (configurable via
// ENTROPYOPS_AGENTIC_UDP_FLUSH_STATS_INTERVAL_S, set to 0 to disable).
// Only emits when there has been activity since the last tick, so an
// idle server doesn't spam the log.
func (r *AgenticUDPReceiver) runFlushStatsLogger(ctx context.Context) {
	intervalS := 10
	if raw := strings.TrimSpace(os.Getenv("ENTROPYOPS_AGENTIC_UDP_FLUSH_STATS_INTERVAL_S")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil {
			intervalS = v
		}
	}
	if intervalS <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(intervalS) * time.Second)
	defer ticker.Stop()
	var prev = map[string]int64{"traces": 0, "metrics": 0, "logs": 0}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, t := range []*flushLatencyTracker{
				r.traceBatcher.latency,
				r.metricBatcher.latency,
				r.logBatcher.latency,
			} {
				s := t.snapshot()
				if s.Flushes == prev[s.Signal] {
					continue
				}
				prev[s.Signal] = s.Flushes
				log.Printf("agenticudp: flush_stats signal=%s flushes=%d items=%d slow=%d max_ms=%d avg_ms=%d",
					s.Signal, s.Flushes, s.Items, s.SlowFlushes, s.MaxLatencyMs, s.AvgLatencyMs)
			}
		}
	}
}
