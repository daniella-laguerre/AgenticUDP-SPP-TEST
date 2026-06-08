package receiver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/entropyops/entropyops-v2/internal/storage"
)

// TestTraceCoalescer_CollapsesManySmallChunksIntoOneFlush is the regression
// test for the AgenticUDP "5000-span 32-second p50" issue. Without the
// coalescer, each datagram triggered its own SQLite WriteTraces transaction
// with its own fsync. With the coalescer, many small enqueues collapse into
// a small number of flush calls — that is the actual mechanism by which the
// receiver stops bottlenecking on storage commits.
func TestTraceCoalescer_CollapsesManySmallChunksIntoOneFlush(t *testing.T) {
	var flushCount int32
	var totalItems int32
	var mu sync.Mutex
	flushedPerTenant := map[string]int{}

	c := newTraceCoalescer(coalescerConfig{
		name:     "test",
		maxItems: 1000,
		maxWait:  100 * time.Millisecond,
		chanCap:  4096,
	}, func(tenantID string, items []storage.Trace) {
		atomic.AddInt32(&flushCount, 1)
		atomic.AddInt32(&totalItems, int32(len(items)))
		mu.Lock()
		flushedPerTenant[tenantID] += len(items)
		mu.Unlock()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.start(ctx)

	// Simulate the AgenticUDP read loop: 100 datagrams of 15 spans each
	// land within a few milliseconds. Without coalescing this would be
	// 100 storage writes; with coalescing the per-tenant accumulator
	// should hit maxItems=1000 after ~67 enqueues and flush once, then
	// the remaining ~33 datagrams flush via the maxWait ticker.
	const datagrams = 100
	const spansPerDatagram = 15
	for i := 0; i < datagrams; i++ {
		batch := make([]storage.Trace, spansPerDatagram)
		c.enqueue("tenant-A", batch)
	}

	// Allow the maxWait ticker to fire at least twice.
	time.Sleep(300 * time.Millisecond)

	flushes := atomic.LoadInt32(&flushCount)
	got := atomic.LoadInt32(&totalItems)
	wantItems := int32(datagrams * spansPerDatagram)

	if got != wantItems {
		t.Errorf("expected %d total items flushed, got %d", wantItems, got)
	}
	// 100 enqueues should collapse to far fewer than 100 flushes — the
	// regression we're guarding against. With maxItems=1000 and 1500 items
	// arriving rapidly, the realistic upper bound is 3 flushes (one
	// triggered by hitting maxItems, possibly one more on the next tick,
	// and a final ticker flush after the data settled).
	if flushes >= int32(datagrams) {
		t.Fatalf("coalescer did not collapse: got %d flushes for %d datagrams", flushes, datagrams)
	}
	if flushes > 5 {
		t.Errorf("expected <=5 flushes for 100 small datagrams, got %d", flushes)
	}
	mu.Lock()
	tenantTotal := flushedPerTenant["tenant-A"]
	mu.Unlock()
	if tenantTotal != int(wantItems) {
		t.Errorf("per-tenant flushed total: got %d, want %d", tenantTotal, wantItems)
	}
}

// TestTraceCoalescer_FlushesOnContextCancel verifies that any items still
// pending when the receiver shuts down are flushed before the goroutine
// exits — we never silently drop already-acked telemetry.
func TestTraceCoalescer_FlushesOnContextCancel(t *testing.T) {
	var flushed int32
	c := newTraceCoalescer(coalescerConfig{
		maxItems: 10000,
		maxWait:  10 * time.Second, // never naturally fires
		chanCap:  64,
	}, func(_ string, items []storage.Trace) {
		atomic.AddInt32(&flushed, int32(len(items)))
	})

	ctx, cancel := context.WithCancel(context.Background())
	c.start(ctx)

	c.enqueue("tenant-X", make([]storage.Trace, 5))
	c.enqueue("tenant-X", make([]storage.Trace, 7))

	// Allow run() to drain the channel into pending.
	time.Sleep(20 * time.Millisecond)

	cancel()
	// Wait for the goroutine to exit (which only happens after flushAll).
	<-c.stopped

	if got := atomic.LoadInt32(&flushed); got != 12 {
		t.Errorf("expected 12 items flushed on shutdown, got %d", got)
	}
}

// TestTraceCoalescer_DirectFlushOnFullChannel verifies the back-pressure
// safety valve: when the inbound channel is saturated we fall back to a
// direct flush rather than dropping data.
func TestTraceCoalescer_DirectFlushOnFullChannel(t *testing.T) {
	var directFlushes int32
	// Tiny channel, never start the consumer goroutine — every enqueue
	// should hit the default branch and flush directly.
	c := newTraceCoalescer(coalescerConfig{
		maxItems: 1000,
		maxWait:  time.Second,
		chanCap:  1,
	}, func(_ string, items []storage.Trace) {
		atomic.AddInt32(&directFlushes, 1)
	})
	// First enqueue fills the channel buffer.
	c.enqueue("t", []storage.Trace{{}})
	// Subsequent enqueues see the channel full and flush directly.
	for i := 0; i < 5; i++ {
		c.enqueue("t", []storage.Trace{{}})
	}
	if got := atomic.LoadInt32(&directFlushes); got != 5 {
		t.Errorf("expected 5 direct flushes (channel full), got %d", got)
	}
}

// TestMetricCoalescer_BasicCollapse mirrors the trace test for metrics so
// the metrics path is also covered.
func TestMetricCoalescer_BasicCollapse(t *testing.T) {
	var flushes int32
	c := newMetricCoalescer(coalescerConfig{
		maxItems: 500,
		maxWait:  80 * time.Millisecond,
		chanCap:  256,
	}, func(_ string, items []storage.Metric) {
		atomic.AddInt32(&flushes, 1)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.start(ctx)
	for i := 0; i < 30; i++ {
		c.enqueue("t", make([]storage.Metric, 10))
	}
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&flushes); got >= 30 {
		t.Fatalf("metric coalescer did not collapse: %d flushes for 30 enqueues", got)
	}
}

// TestLogCoalescer_BasicCollapse mirrors the trace test for logs.
func TestLogCoalescer_BasicCollapse(t *testing.T) {
	var flushes int32
	c := newLogCoalescer(coalescerConfig{
		maxItems: 500,
		maxWait:  80 * time.Millisecond,
		chanCap:  256,
	}, func(_ string, items []storage.LogRecord) {
		atomic.AddInt32(&flushes, 1)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.start(ctx)
	for i := 0; i < 30; i++ {
		c.enqueue("t", make([]storage.LogRecord, 10))
	}
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&flushes); got >= 30 {
		t.Fatalf("log coalescer did not collapse: %d flushes for 30 enqueues", got)
	}
}

// TestFlushLatencyTracker_RecordsAndFlagsSlow proves the per-coalescer
// instrumentation that backs "agenticudp: SLOW FLUSH ..." log lines
// actually counts what we claim it counts. Without this test, the
// "investigate the bimodal 5000-span tail" follow-up would have no
// ground truth for what the production stats line means.
func TestFlushLatencyTracker_RecordsAndFlagsSlow(t *testing.T) {
	t.Setenv("ENTROPYOPS_AGENTIC_UDP_SLOW_FLUSH_MS", "10")
	tr := newFlushLatencyTracker("traces")
	if tr.slowAfter != 10*time.Millisecond {
		t.Fatalf("slowAfter env override: got %v, want 10ms", tr.slowAfter)
	}

	tr.observe("tenant-A", 100, 1*time.Millisecond)   // fast — not slow
	tr.observe("tenant-A", 200, 5*time.Millisecond)   // fast
	tr.observe("tenant-B", 1000, 50*time.Millisecond) // SLOW
	tr.observe("tenant-B", 1000, 75*time.Millisecond) // SLOW (and new max)

	s := tr.snapshot()
	if s.Flushes != 4 {
		t.Fatalf("flushes: want 4, got %d", s.Flushes)
	}
	if s.Items != 2300 {
		t.Fatalf("items: want 2300, got %d", s.Items)
	}
	if s.SlowFlushes != 2 {
		t.Fatalf("slow_flushes: want 2 (10ms threshold), got %d", s.SlowFlushes)
	}
	if s.MaxLatencyMs != 75 {
		t.Fatalf("max_latency_ms: want 75, got %d", s.MaxLatencyMs)
	}
	// Avg = (1+5+50+75)/4 = 32.75 → floor div by 1ms = 32
	if s.AvgLatencyMs != 32 {
		t.Fatalf("avg_latency_ms: want 32, got %d", s.AvgLatencyMs)
	}
}

// TestFlushLatencyTracker_DefaultThresholdHonoured guards against an
// env-typo silently leaving the threshold at some surprising value.
// 200ms is the documented default; if anyone changes it, this test
// breaks loudly.
func TestFlushLatencyTracker_DefaultThresholdHonoured(t *testing.T) {
	t.Setenv("ENTROPYOPS_AGENTIC_UDP_SLOW_FLUSH_MS", "")
	tr := newFlushLatencyTracker("metrics")
	if tr.slowAfter != 200*time.Millisecond {
		t.Fatalf("default threshold: want 200ms, got %v", tr.slowAfter)
	}
	tr.observe("t", 1, 100*time.Millisecond) // under default threshold
	if got := tr.snapshot().SlowFlushes; got != 0 {
		t.Fatalf("100ms should NOT count as slow under 200ms default; got %d", got)
	}
	tr.observe("t", 1, 250*time.Millisecond) // over default
	if got := tr.snapshot().SlowFlushes; got != 1 {
		t.Fatalf("250ms should count as slow under 200ms default; got %d", got)
	}
}

// TestTraceCoalescer_PopulatesLatencyTracker is the integration check —
// the coalescer's wrapped flush must actually feed the tracker, not
// just allocate it. A regression here would make the whole "log slow
// flushes" mechanism a no-op without anything in the build failing.
func TestTraceCoalescer_PopulatesLatencyTracker(t *testing.T) {
	t.Setenv("ENTROPYOPS_AGENTIC_UDP_SLOW_FLUSH_MS", "5")
	c := newTraceCoalescer(coalescerConfig{
		maxItems: 1, maxWait: 50 * time.Millisecond, chanCap: 16,
	}, func(_ string, items []storage.Trace) {
		// Simulate a slow storage write so the tracker flags it.
		time.Sleep(20 * time.Millisecond)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.start(ctx)
	c.enqueue("t", []storage.Trace{{}})
	c.enqueue("t", []storage.Trace{{}})
	time.Sleep(100 * time.Millisecond)
	s := c.latency.snapshot()
	if s.Flushes < 2 {
		t.Fatalf("expected at least 2 flushes recorded, got %d", s.Flushes)
	}
	if s.SlowFlushes < 2 {
		t.Fatalf("each 20ms flush should be flagged slow at 5ms threshold; got %d", s.SlowFlushes)
	}
	if s.MaxLatencyMs < 15 { // allow scheduling jitter
		t.Fatalf("max_latency_ms unexpectedly low: %d", s.MaxLatencyMs)
	}
}
