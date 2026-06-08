package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// classificationCacheEntry holds a cached tier decision and its expiry.
// A single atomic bool guards concurrent background refreshes so exactly
// one goroutine re-queries the LLM per stale entry — all other callers
// receive the stale-but-valid result (stale-while-revalidate pattern).
type classificationCacheEntry struct {
	result     AIClassification
	expiresAt  time.Time
	refreshing atomic.Bool
}

// AIClassifier assigns reliability tiers using LLM reasoning with
// Thompson Sampling fallback when the LLM is unavailable.
//
// Classification cache: the LLM path can take 50–2000 ms per call.
// Blocking every datagram transmission on an LLM call makes the hot
// send path unusable at any real throughput. The cache stores one
// AIClassification per (signalType, regime, congestionLevel) key for
// cacheTTL (default 5 s). On a hit the result is returned in <1 μs.
// On a miss the LLM/Thompson path runs once; on staleness the stale
// result is returned immediately while a single goroutine refreshes in
// the background. This bounds the hot-path overhead to a map lookup
// regardless of LLM provider latency.
type AIClassifier struct {
	enabled    bool
	provider   string // ollama, openai, anthropic, rule
	endpoint   string
	modelName  string
	fallback   string // thompson, random, guaranteed
	httpClient *http.Client

	mu              sync.Mutex
	betaParams      map[string][3][2]float64 // key → [tier][alpha, beta]
	classifications atomic.Int64
	fallbacks       atomic.Int64
	latencySum      atomic.Int64
	latencyCount    atomic.Int64

	// Classification cache. sync.Map is chosen over a plain map+mutex
	// because writes (cache stores) are rare vs reads (hot-path hits).
	cache    sync.Map // string → *classificationCacheEntry
	cacheTTL time.Duration
}

// AIClassification is the result of a tier assignment decision.
type AIClassification struct {
	Tier       Tier    `json:"tier"`
	Reasoning  string  `json:"reasoning"`
	Confidence float64 `json:"confidence"`
	Model      string  `json:"model"`
	LatencyMs  int64   `json:"latency_ms"`
	UsedAI     bool    `json:"used_ai"`
}

func NewAIClassifier() *AIClassifier {
	ttl := 5 * time.Second
	if raw := os.Getenv("ENTROPYOPS_AGENT_AI_CACHE_TTL_S"); raw != "" {
		if s, err := time.ParseDuration(raw + "s"); err == nil && s > 0 {
			ttl = s
		}
	}
	c := &AIClassifier{
		enabled:    os.Getenv("ENTROPYOPS_AGENT_AI_ENABLED") == "true",
		provider:   envOr("ENTROPYOPS_AGENT_AI_MODEL", "rule"),
		endpoint:   envOr("ENTROPYOPS_AGENT_AI_ENDPOINT", "http://localhost:11434"),
		modelName:  envOr("ENTROPYOPS_AGENT_AI_MODEL_NAME", "llama3.2:3b"),
		fallback:   envOr("ENTROPYOPS_AGENT_AI_FALLBACK", "thompson"),
		httpClient: &http.Client{Timeout: 2 * time.Second},
		betaParams: make(map[string][3][2]float64),
		cacheTTL:   ttl,
	}
	return c
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Classify assigns a reliability tier to a signal based on its context.
//
// Hot path: checks the in-process cache first. A cache hit costs <1 μs
// regardless of LLM provider latency. A cold miss runs classifyFresh
// synchronously; a stale entry returns the last result immediately and
// triggers a single background goroutine to refresh the cache — so the
// caller is never blocked by LLM latency after the first call.
func (c *AIClassifier) Classify(ctx SignalContext) AIClassification {
	// AnomalyScore is bucketed into 10 equal intervals so signals with
	// materially different anomaly scores (e.g. 0.1 vs 0.9) get distinct
	// cache entries. Bucketing avoids creating one entry per float64 value
	// while still capturing the coarse signal that drives tier upgrades.
	anomalyBucket := int(ctx.AnomalyScore * 10)
	cacheKey := fmt.Sprintf("%s:%s:%s:%s:%d",
		ctx.SignalType, ctx.EntityRegime, ctx.CongestionLevel, ctx.SignalRarity, anomalyBucket)

	if v, ok := c.cache.Load(cacheKey); ok {
		entry := v.(*classificationCacheEntry)
		if time.Now().Before(entry.expiresAt) {
			return entry.result // fresh hit — zero LLM overhead
		}
		// Stale: return immediately, refresh in background (at most once).
		if entry.refreshing.CompareAndSwap(false, true) {
			go func() {
				fresh := c.classifyFresh(ctx)
				c.cache.Store(cacheKey, &classificationCacheEntry{
					result:    fresh,
					expiresAt: time.Now().Add(c.cacheTTL),
				})
				// refreshing flag is on the old entry; new entry is independent.
			}()
		}
		return entry.result
	}

	// Cold miss: classify synchronously and prime the cache.
	result := c.classifyFresh(ctx)
	c.cache.Store(cacheKey, &classificationCacheEntry{
		result:    result,
		expiresAt: time.Now().Add(c.cacheTTL),
	})
	return result
}

// classifyFresh runs the actual LLM or Thompson Sampling classification
// with no cache involvement. Called by Classify on a cold miss and from
// the background refresh goroutine on a stale hit.
func (c *AIClassifier) classifyFresh(ctx SignalContext) AIClassification {
	start := time.Now()

	if c.enabled && c.provider != "rule" {
		result, err := c.classifyWithLLM(ctx)
		if err == nil {
			result.LatencyMs = time.Since(start).Milliseconds()
			c.classifications.Add(1)
			c.latencySum.Add(result.LatencyMs)
			c.latencyCount.Add(1)
			return result
		}
		log.Printf("ai classifier: LLM failed (%v), falling back to %s", err, c.fallback)
		c.fallbacks.Add(1)
	}

	result := c.classifyDeterministic(ctx)
	result.LatencyMs = time.Since(start).Milliseconds()
	return result
}

// classifyWithLLM calls the configured LLM provider for tier assignment.
func (c *AIClassifier) classifyWithLLM(ctx SignalContext) (AIClassification, error) {
	prompt := fmt.Sprintf(`You are the transport tier classifier for a system physics observability platform.

Given the following signal context:
- Signal type: %s
- Entity regime: %s
- Anomaly score: %.3f
- Entity topology depth: %s
- Entropy delta (last 5 min): %+.3f
- Signal rarity: %s
- Network conditions: RTT=%0.fms, loss=%.1f%%, congestion=%s
- Payload size: %.1f KB

Assign a reliability tier and explain your reasoning:
- GUARANTEED (0): Must arrive, retransmit indefinitely. Use for signals that would change an operator decision.
- RELIABLE (1): Should arrive, retry 3x. Use for signals that add value but aren't critical.
- BESTEFF (2): Fire and forget. Use for routine, redundant, or low-information signals.

Output JSON only: {"tier": 0, "reasoning": "...", "confidence": 0.9}`,
		ctx.SignalType, ctx.EntityRegime, ctx.AnomalyScore, ctx.TopologyDepth,
		ctx.EntropyDelta, ctx.SignalRarity, ctx.RTTMs, ctx.LossRate*100, ctx.CongestionLevel,
		ctx.PayloadSizeKB)

	var result AIClassification
	var err error

	switch c.provider {
	case "ollama":
		result, err = c.callOllama(prompt)
	case "openai":
		result, err = c.callOpenAI(prompt)
	case "anthropic":
		result, err = c.callAnthropic(prompt)
	default:
		return AIClassification{}, fmt.Errorf("unknown provider: %s", c.provider)
	}

	if err != nil {
		return AIClassification{}, err
	}
	result.UsedAI = true
	result.Model = c.provider + "/" + c.modelName
	return result, nil
}

func (c *AIClassifier) callOllama(prompt string) (AIClassification, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"model":  c.modelName,
		"prompt": prompt,
		"stream": false,
		"format": "json",
		"options": map[string]interface{}{
			"temperature": 0.1,
			"num_predict": 256,
		},
	})
	resp, err := c.httpClient.Post(c.endpoint+"/api/generate", "application/json", bytes.NewReader(body))
	if err != nil {
		return AIClassification{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return AIClassification{}, fmt.Errorf("ollama returned %d", resp.StatusCode)
	}
	var ollamaResp struct {
		Response string `json:"response"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ollamaResp); err != nil {
		return AIClassification{}, err
	}
	return parseClassificationJSON(ollamaResp.Response)
}

func (c *AIClassifier) callOpenAI(prompt string) (AIClassification, error) {
	apiKey := os.Getenv("ENTROPYOPS_AI_API_KEY")
	if apiKey == "" {
		return AIClassification{}, fmt.Errorf("ENTROPYOPS_AI_API_KEY not set")
	}
	body, _ := json.Marshal(map[string]interface{}{
		"model": c.modelName,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a transport tier classifier. Output only JSON."},
			{"role": "user", "content": prompt},
		},
		"temperature":     0.1,
		"max_tokens":      256,
		"response_format": map[string]string{"type": "json_object"},
	})
	req, _ := http.NewRequest("POST", "https://api.openai.com/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return AIClassification{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return AIClassification{}, fmt.Errorf("openai returned %d", resp.StatusCode)
	}
	var oaiResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&oaiResp); err != nil {
		return AIClassification{}, err
	}
	if len(oaiResp.Choices) == 0 {
		return AIClassification{}, fmt.Errorf("no choices in response")
	}
	return parseClassificationJSON(oaiResp.Choices[0].Message.Content)
}

func (c *AIClassifier) callAnthropic(prompt string) (AIClassification, error) {
	apiKey := os.Getenv("ENTROPYOPS_AI_API_KEY")
	if apiKey == "" {
		return AIClassification{}, fmt.Errorf("ENTROPYOPS_AI_API_KEY not set")
	}
	body, _ := json.Marshal(map[string]interface{}{
		"model":      c.modelName,
		"max_tokens": 256,
		"messages":   []map[string]string{{"role": "user", "content": prompt}},
	})
	req, _ := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(body))
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return AIClassification{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return AIClassification{}, fmt.Errorf("anthropic returned %d", resp.StatusCode)
	}
	var cResp struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cResp); err != nil {
		return AIClassification{}, err
	}
	if len(cResp.Content) == 0 {
		return AIClassification{}, fmt.Errorf("no content in response")
	}
	return parseClassificationJSON(cResp.Content[0].Text)
}

func parseClassificationJSON(raw string) (AIClassification, error) {
	var parsed struct {
		Tier       int     `json:"tier"`
		Reasoning  string  `json:"reasoning"`
		Confidence float64 `json:"confidence"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return AIClassification{}, fmt.Errorf("parse LLM response: %w", err)
	}
	tier := Tier(parsed.Tier)
	if tier > TierBesteff {
		tier = TierBesteff
	}
	return AIClassification{
		Tier:       tier,
		Reasoning:  parsed.Reasoning,
		Confidence: parsed.Confidence,
	}, nil
}

// classifyDeterministic uses Thompson Sampling or simple rules as fallback.
func (c *AIClassifier) classifyDeterministic(ctx SignalContext) AIClassification {
	switch c.fallback {
	case "thompson":
		return c.classifyThompson(ctx)
	case "guaranteed":
		return AIClassification{Tier: TierGuaranteed, Reasoning: "fallback: all guaranteed", Confidence: 1.0, Model: "rule/guaranteed"}
	default:
		return c.classifyRuleBased(ctx)
	}
}

// classifyThompson implements Thompson Sampling over Beta distributions per context key.
func (c *AIClassifier) classifyThompson(ctx SignalContext) AIClassification {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", ctx.SignalType, ctx.EntityRegime, ctx.CongestionLevel)
	params, ok := c.betaParams[key]
	if !ok {
		params = [3][2]float64{{1, 1}, {1, 1}, {1, 1}}
	}

	bestTier := 0
	bestSample := -1.0
	for tier := 0; tier < 3; tier++ {
		alpha := params[tier][0]
		beta := params[tier][1]
		sample := betaSample(alpha, beta)
		if sample > bestSample {
			bestSample = sample
			bestTier = tier
		}
	}

	c.betaParams[key] = params
	return AIClassification{
		Tier:       Tier(bestTier),
		Reasoning:  fmt.Sprintf("thompson sampling: key=%s, sampled tier %d", key, bestTier),
		Confidence: bestSample,
		Model:      "thompson",
	}
}

// UpdateThompson records an outcome for Thompson Sampling policy update.
func (c *AIClassifier) UpdateThompson(signalType, regime, congestion string, tier Tier, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s:%s:%s", signalType, regime, congestion)
	params, ok := c.betaParams[key]
	if !ok {
		params = [3][2]float64{{1, 1}, {1, 1}, {1, 1}}
	}
	t := int(tier)
	if t >= 0 && t < 3 {
		if success {
			params[t][0]++ // alpha (success)
		} else {
			params[t][1]++ // beta (failure)
		}
	}
	c.betaParams[key] = params
}

func betaSample(alpha, beta float64) float64 {
	x := gammaSample(alpha)
	y := gammaSample(beta)
	if x+y == 0 {
		return 0.5
	}
	return x / (x + y)
}

func gammaSample(shape float64) float64 {
	if shape < 1 {
		return gammaSample(shape+1) * math.Pow(rand.Float64(), 1.0/shape)
	}
	d := shape - 1.0/3.0
	c := 1.0 / math.Sqrt(9.0*d)
	for {
		var x, v float64
		for {
			x = rand.NormFloat64()
			v = 1.0 + c*x
			if v > 0 {
				break
			}
		}
		v = v * v * v
		u := rand.Float64()
		if u < 1.0-0.0331*(x*x)*(x*x) || math.Log(u) < 0.5*x*x+d*(1.0-v+math.Log(v)) {
			return d * v
		}
	}
}

// classifyRuleBased applies deterministic rules based on signal context.
func (c *AIClassifier) classifyRuleBased(ctx SignalContext) AIClassification {
	tier := TierReliable
	reasoning := "rule-based default"

	if ctx.EntityRegime == "critical" || ctx.EntityRegime == "degraded" {
		tier = TierGuaranteed
		reasoning = "regime is " + ctx.EntityRegime + " — guaranteed delivery"
	} else if ctx.AnomalyScore > 0.7 {
		tier = TierGuaranteed
		reasoning = fmt.Sprintf("high anomaly score (%.2f) — guaranteed delivery", ctx.AnomalyScore)
	} else if ctx.SignalRarity == "first-seen" || ctx.SignalRarity == "schema-change" {
		tier = TierGuaranteed
		reasoning = "signal rarity=" + ctx.SignalRarity + " — guaranteed delivery"
	} else if ctx.EntityRegime == "stable" && ctx.AnomalyScore < 0.2 && ctx.SignalRarity == "routine" {
		tier = TierBesteff
		reasoning = "stable regime, low anomaly, routine signal — best effort"
	} else if ctx.CongestionLevel == "high" && ctx.SignalType == "metrics" {
		tier = TierBesteff
		reasoning = "high congestion + routine metrics — best effort to reduce load"
	}

	return AIClassification{
		Tier:       tier,
		Reasoning:  reasoning,
		Confidence: 0.8,
		Model:      "rule",
	}
}

// Stats returns classification statistics.
func (c *AIClassifier) Stats() map[string]interface{} {
	count := c.latencyCount.Load()
	var avgLatency float64
	if count > 0 {
		avgLatency = float64(c.latencySum.Load()) / float64(count)
	}
	var cacheEntries int
	c.cache.Range(func(_, _ interface{}) bool { cacheEntries++; return true })
	return map[string]interface{}{
		"total_classifications": c.classifications.Load(),
		"total_fallbacks":       c.fallbacks.Load(),
		"avg_latency_ms":        avgLatency,
		"provider":              c.provider,
		"enabled":               c.enabled,
		"cache_entries":         cacheEntries,
		"cache_ttl_s":           c.cacheTTL.Seconds(),
	}
}
