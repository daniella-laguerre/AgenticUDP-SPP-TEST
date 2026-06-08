package transport

import (
	"math"
	"sync"
	"time"
)

// CongestionPredictor uses time-series analysis over RTT/loss history
// to predict congestion 30-60 seconds ahead and recommend traffic shaping.
type CongestionPredictor struct {
	mu          sync.Mutex
	rttHistory  []rttSample
	lossHistory []lossSample
	windowSize  int
	predictions []CongestionPrediction
}

type rttSample struct {
	time  time.Time
	value float64
}

type lossSample struct {
	time  time.Time
	value float64
}

// CongestionPrediction is the output of the predictor.
type CongestionPrediction struct {
	Timestamp         time.Time `json:"timestamp"`
	Probability       float64   `json:"probability"`    // 0.0-1.0 chance of congestion in next 30s
	Severity          string    `json:"severity"`        // none, mild, moderate, severe
	RecommendedAction string    `json:"action"`          // normal, pace, reduce, defer
	PredictedRTTMs    float64   `json:"predicted_rtt_ms"`
	PredictedLoss     float64   `json:"predicted_loss"`
}

// ShapingParams are the traffic shaping parameters recommended by the predictor.
type ShapingParams struct {
	MaxBatchSizeKB   int           `json:"max_batch_size_kb"`
	PacingIntervalMs int           `json:"pacing_interval_ms"`
	FragmentSizeKB   int           `json:"fragment_size_kb"`
	DeferBestEffort  bool          `json:"defer_besteff"`
	BackoffDuration  time.Duration `json:"backoff_duration"`
}

func NewCongestionPredictor() *CongestionPredictor {
	return &CongestionPredictor{
		rttHistory:  make([]rttSample, 0, 300),
		lossHistory: make([]lossSample, 0, 300),
		windowSize:  300,
		predictions: make([]CongestionPrediction, 0, 60),
	}
}

// RecordRTT adds an RTT observation.
func (cp *CongestionPredictor) RecordRTT(rttMs float64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.rttHistory = append(cp.rttHistory, rttSample{time: time.Now(), value: rttMs})
	if len(cp.rttHistory) > cp.windowSize {
		cp.rttHistory = cp.rttHistory[1:]
	}
}

// RecordLoss adds a loss rate observation.
func (cp *CongestionPredictor) RecordLoss(lossRate float64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.lossHistory = append(cp.lossHistory, lossSample{time: time.Now(), value: lossRate})
	if len(cp.lossHistory) > cp.windowSize {
		cp.lossHistory = cp.lossHistory[1:]
	}
}

// Predict returns the current congestion prediction.
func (cp *CongestionPredictor) Predict() CongestionPrediction {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if len(cp.rttHistory) < 10 {
		return CongestionPrediction{
			Timestamp: time.Now(), Severity: "none", RecommendedAction: "normal",
		}
	}

	rttTrend := cp.computeTrend(cp.rttHistory)
	rttMean, rttStddev := cp.computeStats(cp.rttHistory)

	var lossTrend, lossMean float64
	if len(cp.lossHistory) > 5 {
		lossTrend = cp.computeLossTrend()
		lossMean = cp.computeLossMean()
	}

	predictedRTT := rttMean + rttTrend*30.0
	predictedLoss := lossMean + lossTrend*30.0
	if predictedLoss < 0 {
		predictedLoss = 0
	}

	rttScore := 0.0
	if rttStddev > 0 {
		rttScore = (predictedRTT - rttMean) / rttStddev
	}
	rttProb := 1.0 / (1.0 + math.Exp(-0.5*(rttScore-1.5)))
	lossProb := math.Min(predictedLoss*10.0, 1.0)
	probability := 0.6*rttProb + 0.4*lossProb

	severity := "none"
	action := "normal"
	switch {
	case probability > 0.8:
		severity = "severe"
		action = "defer"
	case probability > 0.6:
		severity = "moderate"
		action = "reduce"
	case probability > 0.3:
		severity = "mild"
		action = "pace"
	}

	pred := CongestionPrediction{
		Timestamp:         time.Now(),
		Probability:       probability,
		Severity:          severity,
		RecommendedAction: action,
		PredictedRTTMs:    predictedRTT,
		PredictedLoss:     predictedLoss,
	}

	cp.predictions = append(cp.predictions, pred)
	if len(cp.predictions) > 60 {
		cp.predictions = cp.predictions[1:]
	}

	return pred
}

// RecommendShaping returns traffic shaping parameters based on the current prediction.
func (cp *CongestionPredictor) RecommendShaping() ShapingParams {
	pred := cp.Predict()

	params := ShapingParams{
		MaxBatchSizeKB:   32,
		PacingIntervalMs: 0,
		FragmentSizeKB:   1400,
	}

	switch pred.RecommendedAction {
	case "pace":
		params.PacingIntervalMs = 50
		params.MaxBatchSizeKB = 24
	case "reduce":
		params.PacingIntervalMs = 100
		params.MaxBatchSizeKB = 16
		params.FragmentSizeKB = 1000
		params.DeferBestEffort = true
	case "defer":
		params.PacingIntervalMs = 200
		params.MaxBatchSizeKB = 8
		params.FragmentSizeKB = 576
		params.DeferBestEffort = true
		params.BackoffDuration = 5 * time.Second
	}

	return params
}

func (cp *CongestionPredictor) computeTrend(samples []rttSample) float64 {
	n := len(samples)
	if n < 5 {
		return 0
	}
	recent := samples[n-5:]
	alpha := 0.3
	ewma := recent[0].value
	for i := 1; i < len(recent); i++ {
		ewma = alpha*recent[i].value + (1-alpha)*ewma
	}
	return (ewma - recent[0].value) / float64(len(recent))
}

func (cp *CongestionPredictor) computeStats(samples []rttSample) (mean, stddev float64) {
	n := float64(len(samples))
	var sum, sumSq float64
	for _, s := range samples {
		sum += s.value
		sumSq += s.value * s.value
	}
	mean = sum / n
	variance := sumSq/n - mean*mean
	if variance < 0 {
		variance = 0
	}
	stddev = math.Sqrt(variance)
	return
}

func (cp *CongestionPredictor) computeLossTrend() float64 {
	n := len(cp.lossHistory)
	if n < 5 {
		return 0
	}
	recent := cp.lossHistory[n-5:]
	return (recent[len(recent)-1].value - recent[0].value) / float64(len(recent))
}

func (cp *CongestionPredictor) computeLossMean() float64 {
	var sum float64
	for _, s := range cp.lossHistory {
		sum += s.value
	}
	return sum / float64(len(cp.lossHistory))
}
