package transport

import (
	"math"
	"sync"
	"time"
)

// SignalContext holds all contextual features about a signal that the AI
// classifier uses to decide reliability tier assignment.
type SignalContext struct {
	SignalType      string  `json:"signal_type"`      // metrics, traces, logs, fingerprint
	EntityRegime    string  `json:"entity_regime"`    // stable, transitioning, degraded, critical
	AnomalyScore    float64 `json:"anomaly_score"`    // 0.0-1.0
	TopologyDepth   string  `json:"topology_depth"`   // leaf, intermediate, root
	EntropyDelta    float64 `json:"entropy_delta"`    // change over last 5 min
	SignalRarity    string  `json:"signal_rarity"`    // routine, first-seen, schema-change
	RTTMs           float64 `json:"rtt_ms"`
	LossRate        float64 `json:"loss_rate"`
	CongestionLevel string  `json:"congestion_level"` // low, medium, high
	PayloadSizeKB   float64 `json:"payload_size_kb"`
	TimeSinceLastS  float64 `json:"time_since_last_s"`
}

// FeatureExtractor builds signal context from observed transport state.
type FeatureExtractor struct {
	mu            sync.Mutex
	signalCounts  map[string]int
	lastSeen      map[string]time.Time
	entropyWindow []float64
	maxWindow     int
}

func NewFeatureExtractor() *FeatureExtractor {
	return &FeatureExtractor{
		signalCounts:  make(map[string]int),
		lastSeen:      make(map[string]time.Time),
		entropyWindow: make([]float64, 0, 60),
		maxWindow:     60,
	}
}

// Extract builds a SignalContext from the current transport state.
func (fe *FeatureExtractor) Extract(signalType string, payloadSize int, rttMs float64, lossRate float64) SignalContext {
	fe.mu.Lock()
	defer fe.mu.Unlock()

	key := signalType
	fe.signalCounts[key]++
	count := fe.signalCounts[key]

	var timeSinceLast float64
	if last, ok := fe.lastSeen[key]; ok {
		timeSinceLast = time.Since(last).Seconds()
	}
	fe.lastSeen[key] = time.Now()

	rarity := "routine"
	if count == 1 {
		rarity = "first-seen"
	} else if timeSinceLast > 300 {
		rarity = "schema-change"
	}

	congestion := "low"
	if lossRate > 0.05 {
		congestion = "high"
	} else if lossRate > 0.01 {
		congestion = "medium"
	}

	return SignalContext{
		SignalType:      signalType,
		EntityRegime:    fe.inferRegime(),
		AnomalyScore:    fe.computeAnomalyScore(rttMs),
		TopologyDepth:   "leaf",
		EntropyDelta:    fe.computeEntropyDelta(),
		SignalRarity:    rarity,
		RTTMs:           rttMs,
		LossRate:        lossRate,
		CongestionLevel: congestion,
		PayloadSizeKB:   float64(payloadSize) / 1024.0,
		TimeSinceLastS:  timeSinceLast,
	}
}

// UpdateEntropy records a new entropy observation for delta computation.
func (fe *FeatureExtractor) UpdateEntropy(entropy float64) {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	fe.entropyWindow = append(fe.entropyWindow, entropy)
	if len(fe.entropyWindow) > fe.maxWindow {
		fe.entropyWindow = fe.entropyWindow[1:]
	}
}

func (fe *FeatureExtractor) inferRegime() string {
	if len(fe.entropyWindow) < 5 {
		return "stable"
	}
	recent := fe.entropyWindow[len(fe.entropyWindow)-5:]
	var sum float64
	for _, v := range recent {
		sum += v
	}
	avg := sum / float64(len(recent))
	switch {
	case avg < 0.3:
		return "stable"
	case avg < 0.5:
		return "transitioning"
	case avg < 0.7:
		return "degraded"
	default:
		return "critical"
	}
}

func (fe *FeatureExtractor) computeEntropyDelta() float64 {
	n := len(fe.entropyWindow)
	if n < 2 {
		return 0
	}
	halfN := n / 2
	var firstHalf, secondHalf float64
	for i := 0; i < halfN; i++ {
		firstHalf += fe.entropyWindow[i]
	}
	for i := halfN; i < n; i++ {
		secondHalf += fe.entropyWindow[i]
	}
	firstHalf /= float64(halfN)
	secondHalf /= float64(n - halfN)
	return secondHalf - firstHalf
}

func (fe *FeatureExtractor) computeAnomalyScore(rttMs float64) float64 {
	if len(fe.entropyWindow) < 10 {
		return 0
	}
	var sum, sumSq float64
	for _, v := range fe.entropyWindow {
		sum += v
		sumSq += v * v
	}
	n := float64(len(fe.entropyWindow))
	mean := sum / n
	variance := sumSq/n - mean*mean
	if variance < 1e-10 {
		return 0
	}
	stddev := math.Sqrt(variance)
	latest := fe.entropyWindow[len(fe.entropyWindow)-1]
	zscore := math.Abs(latest-mean) / stddev
	return 1.0 / (1.0 + math.Exp(-0.5*(zscore-2.0)))
}
