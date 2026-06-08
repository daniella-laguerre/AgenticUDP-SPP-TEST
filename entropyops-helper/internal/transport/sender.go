package transport

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/entropyops/entropyops-helper/internal/agentlight"
)

// MetricRecord matches storage.Metric on the core server side.
type MetricRecord struct {
	Timestamp   time.Time         `json:"timestamp"`
	ServiceName string            `json:"service_name"`
	MetricName  string            `json:"metric_name"`
	MetricType  string            `json:"metric_type"`
	Value       float64           `json:"value"`
	Labels      map[string]string `json:"labels"`
	TenantID    string            `json:"tenant_id"`
}

// TraceRecord matches storage.Trace on the core server side.
type TraceRecord struct {
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id"`
	ServiceName   string            `json:"service_name"`
	OperationName string            `json:"operation_name"`
	StartTime     time.Time         `json:"start_time"`
	EndTime       time.Time         `json:"end_time"`
	DurationUS    int64             `json:"duration_us"`
	StatusCode    string            `json:"status_code"`
	SpanKind      string            `json:"span_kind,omitempty"`
	Attributes    map[string]string `json:"attributes"`
	TenantID      string            `json:"tenant_id"`
}

// LogRecord matches storage.LogRecord on the core server side.
type LogRecord struct {
	Timestamp      time.Time         `json:"timestamp"`
	TenantID       string            `json:"tenant_id"`
	TraceID        string            `json:"trace_id,omitempty"`
	SpanID         string            `json:"span_id,omitempty"`
	ServiceName    string            `json:"service_name"`
	SeverityText   string            `json:"severity_text"`
	SeverityNumber int               `json:"severity_number"`
	Body           string            `json:"body"`
	Attributes     map[string]string `json:"attributes,omitempty"`
	ResourceAttrs  map[string]string `json:"resource_attributes,omitempty"`
}

// SnapshotToMetrics converts a HostSnapshot into storage-compatible MetricRecords
// for transmission over AgenticUDP (Path B).
func SnapshotToMetrics(snap *agentlight.HostSnapshot, tenantID string) []MetricRecord {
	now := snap.Timestamp
	e := snap.EntityID
	labels := map[string]string{"host.name": e, "source": "agenticudp"}

	var m []MetricRecord
	g := func(name string, val float64) {
		m = append(m, MetricRecord{
			Timestamp: now, ServiceName: e, MetricName: name,
			MetricType: "gauge", Value: val, Labels: labels, TenantID: tenantID,
		})
	}
	s := func(name string, val float64) {
		m = append(m, MetricRecord{
			Timestamp: now, ServiceName: e, MetricName: name,
			MetricType: "sum", Value: val, Labels: labels, TenantID: tenantID,
		})
	}
	sd := func(name string, val float64, dir string) {
		l := map[string]string{"host.name": e, "source": "agenticudp", "direction": dir}
		m = append(m, MetricRecord{
			Timestamp: now, ServiceName: e, MetricName: name,
			MetricType: "sum", Value: val, Labels: l, TenantID: tenantID,
		})
	}

	g("system.cpu.utilization", snap.CPU)
	g("system.memory.utilization", snap.Memory)
	s("system.memory.usage", float64(snap.MemoryBytes))
	g("system.memory.limit", float64(snap.MemoryTotal))
	sd("system.disk.io", float64(snap.DiskReadBytes), "read")
	sd("system.disk.io", float64(snap.DiskWriteBytes), "write")
	s("system.filesystem.usage", float64(snap.DiskUsedBytes))
	g("system.filesystem.limit", float64(snap.DiskTotalBytes))
	sd("system.network.io", float64(snap.NetBytesSent), "transmit")
	sd("system.network.io", float64(snap.NetBytesRecv), "receive")
	sd("system.network.packets", float64(snap.NetPacketsSent), "transmit")
	sd("system.network.packets", float64(snap.NetPacketsRecv), "receive")
	sd("system.network.errors", float64(snap.NetErrIn), "receive")
	sd("system.network.errors", float64(snap.NetErrOut), "transmit")
	g("system.cpu.load_average.1m", snap.LoadAvg1)
	g("system.cpu.load_average.5m", snap.LoadAvg5)
	g("system.cpu.load_average.15m", snap.LoadAvg15)
	s("system.uptime", float64(snap.Uptime))
	g("system.process.count", float64(snap.ProcessCount))

	return m
}

// SpansToTraces converts agent spans into storage-compatible TraceRecords
// for transmission over AgenticUDP.
func SpansToTraces(spans []agentlight.AgentSpan, tenantID string, entityID string) []TraceRecord {
	records := make([]TraceRecord, 0, len(spans))
	for _, s := range spans {
		svcName := entityID
		if cs, ok := s.Attributes["client.service"]; ok && s.Kind == "client" {
			svcName = entityID + "/" + cs
		} else if ss, ok := s.Attributes["server.service"]; ok && s.Kind == "server" {
			svcName = entityID + "/" + ss
		}

		rec := TraceRecord{
			TraceID:       hex.EncodeToString(s.TraceID[:]),
			SpanID:        hex.EncodeToString(s.SpanID[:]),
			ServiceName:   svcName,
			OperationName: s.Name,
			StartTime:     s.StartTime,
			EndTime:       s.EndTime,
			DurationUS:    s.EndTime.Sub(s.StartTime).Microseconds(),
			StatusCode:    s.Status,
			SpanKind:      spanKindToOTel(s.Kind),
			Attributes:    s.Attributes,
			TenantID:      tenantID,
		}
		if s.ParentID != [8]byte{} {
			rec.ParentSpanID = hex.EncodeToString(s.ParentID[:])
		}
		records = append(records, rec)
	}
	return records
}

// AgentLogsToLogRecords converts agent log records into storage-compatible
// LogRecords for AgenticUDP transmission.
func AgentLogsToLogRecords(records []agentlight.AgentLogRecord, tenantID string, entityID string) []LogRecord {
	out := make([]LogRecord, 0, len(records))
	for _, r := range records {
		sevText, sevNum := mapSeverity(r.Severity)
		attrs := r.Attributes
		if attrs == nil {
			attrs = make(map[string]string)
		}
		if r.EventName != "" {
			attrs["event.name"] = r.EventName
		}

		svcName := entityID
		if pn, ok := attrs["process.name"]; ok && pn != "" {
			src, _ := attrs["log.source"]
			if src == "process" || src == "application" {
				svcName = entityID + "/" + pn
			}
		}

		out = append(out, LogRecord{
			Timestamp:      r.Timestamp,
			TenantID:       tenantID,
			ServiceName:    svcName,
			SeverityText:   sevText,
			SeverityNumber: sevNum,
			Body:           r.Body,
			Attributes:     attrs,
			ResourceAttrs:  map[string]string{"host.name": entityID},
		})
	}
	return out
}

// ProcessMetricsToRecords converts process scope metrics into MetricRecords.
func ProcessMetricsToRecords(entityID, tenantID string, procs []agentlight.ProcessMetric, services []agentlight.ServiceStatus) []MetricRecord {
	var records []MetricRecord
	now := time.Now()
	for _, p := range procs {
		resolvedName := agentlight.ResolveServiceName(p.Name, p.Cmdline)
		procEntity := entityID + "/" + resolvedName
		labels := map[string]string{
			"host.name":    entityID,
			"process.name": resolvedName,
			"process.exe":  p.Name,
			"process.pid":  fmt.Sprintf("%d", p.PID),
			"source":       "agenticudp",
		}
		if p.Cmdline != "" {
			labels["process.command_line"] = truncate(p.Cmdline, 200)
		}
		records = append(records,
			MetricRecord{Timestamp: now, ServiceName: procEntity, MetricName: "process.cpu.utilization",
				MetricType: "gauge", Value: p.CPUPercent, Labels: labels, TenantID: tenantID},
			MetricRecord{Timestamp: now, ServiceName: procEntity, MetricName: "process.memory.rss",
				MetricType: "gauge", Value: float64(p.MemBytes), Labels: labels, TenantID: tenantID},
			MetricRecord{Timestamp: now, ServiceName: procEntity, MetricName: "process.memory.physical_usage",
				MetricType: "gauge", Value: float64(p.MemBytes), Labels: labels, TenantID: tenantID},
		)
	}
	for _, s := range services {
		active := 0.0
		if s.Status == "listening" || s.Status == "established" {
			active = 1.0
		}
		svcEntity := entityID + "/" + s.Name
		if s.Name == "" {
			svcEntity = entityID + "/" + fmt.Sprintf("port-%d", s.Port)
		}
		labels := map[string]string{
			"host.name":      entityID,
			"service.name":   s.Name,
			"service.port":   fmt.Sprintf("%d", s.Port),
			"service.proto":  s.Protocol,
			"service.status": s.Status,
			"source":         "agenticudp",
		}
		records = append(records,
			MetricRecord{Timestamp: now, ServiceName: svcEntity, MetricName: "service.active",
				MetricType: "gauge", Value: active, Labels: labels, TenantID: tenantID},
		)
		if s.CPUPercent > 0 {
			records = append(records,
				MetricRecord{Timestamp: now, ServiceName: svcEntity, MetricName: "process.cpu.utilization",
					MetricType: "gauge", Value: s.CPUPercent, Labels: labels, TenantID: tenantID},
			)
		}
		if s.MemBytes > 0 {
			records = append(records,
				MetricRecord{Timestamp: now, ServiceName: svcEntity, MetricName: "process.memory.physical_usage",
					MetricType: "gauge", Value: float64(s.MemBytes), Labels: labels, TenantID: tenantID},
			)
		}
	}
	return records
}

func spanKindToOTel(kind string) string {
	switch kind {
	case "client":
		return "SPAN_KIND_CLIENT"
	case "server":
		return "SPAN_KIND_SERVER"
	default:
		return "SPAN_KIND_INTERNAL"
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

func mapSeverity(s string) (string, int) {
	switch s {
	case "trace":
		return "TRACE", 1
	case "debug":
		return "DEBUG", 5
	case "warn":
		return "WARN", 13
	case "error":
		return "ERROR", 17
	default:
		return "INFO", 9
	}
}
