package transport

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/entropyops/entropyops-helper/internal/agentlight"
	"github.com/entropyops/entropyops-helper/internal/fingerprint"
	sppv1 "github.com/entropyops/entropyops-v2/pkg/sppv1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SnapshotToProtoMetrics converts a HostSnapshot into sppv1.MetricBatch.
func SnapshotToProtoMetrics(snap *agentlight.HostSnapshot, tenantID string) *sppv1.MetricBatch {
	ts := timestamppb.New(snap.Timestamp)
	e := snap.EntityID
	labels := map[string]string{"host.name": e, "source": "agenticudp"}

	var metrics []*sppv1.Metric
	g := func(name string, val float64) {
		metrics = append(metrics, &sppv1.Metric{
			Timestamp: ts, ServiceName: e, MetricName: name,
			MetricType: "gauge", Value: val, Labels: labels, TenantId: tenantID,
		})
	}
	s := func(name string, val float64) {
		metrics = append(metrics, &sppv1.Metric{
			Timestamp: ts, ServiceName: e, MetricName: name,
			MetricType: "sum", Value: val, Labels: labels, TenantId: tenantID,
		})
	}
	sd := func(name string, val float64, dir string) {
		l := map[string]string{"host.name": e, "source": "agenticudp", "direction": dir}
		metrics = append(metrics, &sppv1.Metric{
			Timestamp: ts, ServiceName: e, MetricName: name,
			MetricType: "sum", Value: val, Labels: l, TenantId: tenantID,
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

	return &sppv1.MetricBatch{Metrics: metrics}
}

// SpansToProtoTraces converts agent spans into sppv1.TraceBatch.
func SpansToProtoTraces(spans []agentlight.AgentSpan, tenantID, entityID string) *sppv1.TraceBatch {
	protoSpans := make([]*sppv1.Span, 0, len(spans))
	for _, sp := range spans {
		svcName := entityID
		if cs, ok := sp.Attributes["client.service"]; ok && sp.Kind == "client" {
			svcName = entityID + "/" + cs
		} else if ss, ok := sp.Attributes["server.service"]; ok && sp.Kind == "server" {
			svcName = entityID + "/" + ss
		}

		ps := &sppv1.Span{
			TraceId:       hex.EncodeToString(sp.TraceID[:]),
			SpanId:        hex.EncodeToString(sp.SpanID[:]),
			ServiceName:   svcName,
			OperationName: sp.Name,
			StartTime:     timestamppb.New(sp.StartTime),
			EndTime:       timestamppb.New(sp.EndTime),
			DurationUs:    sp.EndTime.Sub(sp.StartTime).Microseconds(),
			StatusCode:    sp.Status,
			SpanKind:      spanKindToOTel(sp.Kind),
			Attributes:    sp.Attributes,
			TenantId:      tenantID,
		}
		if sp.ParentID != [8]byte{} {
			ps.ParentSpanId = hex.EncodeToString(sp.ParentID[:])
		}
		protoSpans = append(protoSpans, ps)
	}
	return &sppv1.TraceBatch{Spans: protoSpans}
}

// AgentLogsToProtoLogs converts agent log records into sppv1.LogBatch.
func AgentLogsToProtoLogs(records []agentlight.AgentLogRecord, tenantID, entityID string) *sppv1.LogBatch {
	out := make([]*sppv1.LogRecord, 0, len(records))
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

		out = append(out, &sppv1.LogRecord{
			Timestamp:      timestamppb.New(r.Timestamp),
			TenantId:       tenantID,
			ServiceName:    svcName,
			SeverityText:   sevText,
			SeverityNumber: int32(sevNum),
			Body:           r.Body,
			Attributes:     attrs,
			ResourceAttrs:  map[string]string{"host.name": entityID},
		})
	}
	return &sppv1.LogBatch{Logs: out}
}

// HostFingerprintToProto converts an agent HostFingerprint into sppv1.Fingerprint.
func HostFingerprintToProto(hfp *fingerprint.HostFingerprint) *sppv1.Fingerprint {
	fp := &sppv1.Fingerprint{
		Os:               hfp.OS,
		Arch:             hfp.Arch,
		Kernel:           hfp.Kernel,
		CloudProvider:    hfp.CloudProvider,
		Hostname:         hfp.Hostname,
		ContainerRuntime: hfp.ContainerRuntime,
		Platform:         hfp.Platform,
		FingerprintId:    hfp.FingerprintID,
		CollectedAt:      timestamppb.New(hfp.CollectedAt),
		DurationNs:       int64(hfp.Duration),
		Confidence:       hfp.Confidence,
	}

	if hfp.LPARInfo != nil {
		fp.LparInfo = &sppv1.LPARInfo{
			Name:      hfp.LPARInfo.Name,
			Type:      hfp.LPARInfo.Type,
			SysplexId: hfp.LPARInfo.SysplexID,
			CpcSerial: hfp.LPARInfo.CPCSerial,
		}
	}

	for _, s := range hfp.SMFTypes {
		fp.SmfTypes = append(fp.SmfTypes, int32(s))
	}
	fp.ActiveSubsystems = hfp.ActiveSubsystems
	fp.MqManagers = hfp.MQManagers
	fp.IndustrialProtos = hfp.IndustrialProtos

	for _, ld := range hfp.LegacyDetected {
		fp.LegacyDetected = append(fp.LegacyDetected, &sppv1.LegacyDetection{
			Source:     ld.Source,
			Confidence: ld.Confidence,
			Details:    ld.Details,
		})
	}

	for _, ds := range hfp.DetectedSoftware {
		ports := make([]int32, len(ds.MatchedPorts))
		for i, p := range ds.MatchedPorts {
			ports[i] = int32(p)
		}
		fp.DetectedSoftware = append(fp.DetectedSoftware, &sppv1.DetectedSoftware{
			Name:             ds.Name,
			DisplayName:      ds.DisplayName,
			Category:         ds.Category,
			MatchedProcesses: ds.MatchedProcess,
			MatchedPorts:     ports,
			MatchedConfigs:   ds.MatchedConfigs,
			Confidence:       ds.Confidence,
			Layer:            ds.Layer,
			CollectStrategy:  ds.CollectStrategy,
		})
	}

	for _, da := range hfp.DetectedAgents {
		fp.DetectedAgents = append(fp.DetectedAgents, &sppv1.DetectedAgent{
			Name:    da.Name,
			Pid:     da.PID,
			Cmdline: da.Cmdline,
		})
	}

	for _, p := range hfp.OpenPorts {
		fp.OpenPorts = append(fp.OpenPorts, int32(p))
	}

	for _, pi := range hfp.Processes {
		fp.Processes = append(fp.Processes, &sppv1.ProcessInfo{
			Pid:     pi.PID,
			Name:    pi.Name,
			Cmdline: pi.Cmdline,
		})
	}

	return fp
}

// ProcessMetricsToProtoBatch converts process-level metrics into sppv1.MetricBatch.
func ProcessMetricsToProtoBatch(entityID, tenantID string, procs []agentlight.ProcessMetric, services []agentlight.ServiceStatus) *sppv1.MetricBatch {
	var metrics []*sppv1.Metric
	ts := timestamppb.New(time.Now())
	for _, p := range procs {
		labels := map[string]string{
			"host.name":    entityID,
			"process.name": p.Name,
			"process.pid":  fmt.Sprintf("%d", p.PID),
			"source":       "agenticudp",
		}
		metrics = append(metrics,
			&sppv1.Metric{Timestamp: ts, ServiceName: entityID, MetricName: "process.cpu.utilization",
				MetricType: "gauge", Value: p.CPUPercent, Labels: labels, TenantId: tenantID},
			&sppv1.Metric{Timestamp: ts, ServiceName: entityID, MetricName: "process.memory.rss",
				MetricType: "gauge", Value: float64(p.MemBytes), Labels: labels, TenantId: tenantID},
		)
	}
	for _, s := range services {
		active := 0.0
		if s.Status == "listening" || s.Status == "established" {
			active = 1.0
		}
		labels := map[string]string{
			"host.name":    entityID,
			"service.name": s.Name,
			"source":       "agenticudp",
		}
		metrics = append(metrics,
			&sppv1.Metric{Timestamp: ts, ServiceName: entityID, MetricName: "service.active",
				MetricType: "gauge", Value: active, Labels: labels, TenantId: tenantID},
		)
	}
	return &sppv1.MetricBatch{Metrics: metrics}
}
