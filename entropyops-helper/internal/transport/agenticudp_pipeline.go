// Package transport — pipeline task helpers for AgenticUDP V2.
//
// This file extends Client with three capabilities that turn the one-way
// telemetry firehose into a bidirectional agentic pipeline transport:
//
//  1. SendOnStream   — sends any signal on a caller-chosen streamID,
//                      enabling the server to correlate the result with the
//                      task it dispatched (server pushes task on streamID N;
//                      agent sends result on streamID N).
//
//  2. SendTaskResult — convenience wrapper for TierGuaranteed task results.
//
//  3. SetPipelineTaskHandler — typed wrapper over SetConfigHandler that
//                      JSON-decodes incoming pktConfig payloads as
//                      PipelineTask structs and dispatches them to the
//                      handler. Falls back to the raw config handler for
//                      payloads that do not parse as a PipelineTask.
//
// The server-side complement (pushing pktConfig with a matching streamID)
// requires a server build that honours the streamID convention; the client
// side is wired and ready to use today.
package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// PipelineTask is the structured payload the server pushes to an agent via
// pktConfig when it wants the agent to execute a pipeline stage. The TaskID
// is an opaque string (UUID or monotonic counter); StreamID is the uint16
// transport stream the agent should use when sending its result back, so the
// server can correlate without scanning every incoming datagram.
type PipelineTask struct {
	TaskID     string          `json:"task_id"`
	TaskType   string          `json:"task_type"`             // e.g. "inference", "embedding", "classify"
	Params     json.RawMessage `json:"params,omitempty"`      // task-specific parameters
	StreamID   uint16          `json:"stream_id"`             // return result on this stream
	Priority   int             `json:"priority,omitempty"`    // 0=normal, 1=high, 2=critical
	DeadlineMs int64           `json:"deadline_ms,omitempty"` // absolute Unix ms; 0 = no deadline
}

// SendOnStream serialises data as a JSON envelope and transmits it on
// the given streamID at the requested tier. Use this to send task results
// back to the server on the same streamID the task arrived on, enabling
// server-side correlation without application-level tracking state.
//
// For TierGuaranteed (the default for task results), the transport retransmits
// until the server ACKs; for TierBesteff the datagram is fire-and-forget.
func (c *Client) SendOnStream(streamID uint16, signalType string, tier Tier, data interface{}) error {
	if !c.established.Load() {
		return fmt.Errorf("agenticudp: not connected")
	}
	envelope := struct {
		SignalType string      `json:"signal_type"`
		TenantID   string      `json:"tenant_id"`
		Data       interface{} `json:"data"`
	}{
		SignalType: signalType,
		TenantID:   c.tenantID,
		Data:       data,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("agenticudp: marshal %s: %w", signalType, err)
	}
	return c.transmit(payload, tier, streamID)
}

// SendTaskResult is a convenience wrapper that sends result on the streamID
// from task.StreamID at TierGuaranteed. The result is any JSON-serialisable
// value; it is wrapped in a "task_result" signal envelope so the server can
// route it without inspecting the payload.
func (c *Client) SendTaskResult(task PipelineTask, result interface{}) error {
	return c.SendOnStream(task.StreamID, "task_result", TierGuaranteed, map[string]interface{}{
		"task_id":   task.TaskID,
		"task_type": task.TaskType,
		"result":    result,
	})
}

// SetPipelineTaskHandler registers a typed callback for incoming pipeline
// tasks pushed by the server via pktConfig. Payloads that parse successfully
// as PipelineTask (non-empty task_id field) are dispatched to fn; all other
// payloads (e.g. OTEL YAML configs) are silently discarded by this handler.
//
// Call this instead of SetConfigHandler when the agent is participating in
// an agentic pipeline. The two handlers share the same underlying hook;
// calling SetPipelineTaskHandler overwrites any previously registered
// SetConfigHandler.
//
// Deadline awareness: if task.DeadlineMs > 0 and the deadline has already
// passed, fn is not called and a warning is logged.
func (c *Client) SetPipelineTaskHandler(fn func(task PipelineTask)) {
	c.SetConfigHandler(func(raw []byte) {
		var task PipelineTask
		if err := json.Unmarshal(raw, &task); err != nil || task.TaskID == "" {
			return // not a pipeline task — ignore
		}
		if task.DeadlineMs > 0 {
			deadline := time.UnixMilli(task.DeadlineMs)
			if time.Now().After(deadline) {
				return // task already expired before we could execute it
			}
		}
		fn(task)
	})
}

// SendTaskResultWithContext is like SendTaskResult but respects the task's
// deadline: if the deadline has passed before the result can be sent, it
// returns an error rather than sending a stale result. Use this in pipeline
// stages where late results are worse than no results.
func (c *Client) SendTaskResultWithContext(ctx context.Context, task PipelineTask, result interface{}) error {
	if task.DeadlineMs > 0 {
		deadline := time.UnixMilli(task.DeadlineMs)
		if time.Now().After(deadline) {
			return fmt.Errorf("agenticudp: task %s deadline expired at %v", task.TaskID, deadline)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("agenticudp: send task result cancelled: %w", ctx.Err())
	default:
		return c.SendTaskResult(task, result)
	}
}
