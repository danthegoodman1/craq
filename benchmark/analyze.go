package benchmark

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

type AnalysisSummary struct {
	RunID           string                        `json:"run_id"`
	RunName         string                        `json:"run_name"`
	Topology        string                        `json:"topology"`
	ClientPlacement string                        `json:"client_placement"`
	GitSHA          string                        `json:"git_sha"`
	Scenarios       []ScenarioReport              `json:"scenarios"`
	WriteBudget     map[string]WriteBudgetSummary `json:"write_budget,omitempty"`
	Coordinator     map[string]float64            `json:"coordinator_metrics"`
	Storage         map[string]map[string]float64 `json:"storage_metrics"`
	System          map[string]SystemSeries       `json:"system"`
}

type WriteLatencyBudgetReport struct {
	RunID       string                `json:"run_id"`
	GeneratedAt time.Time             `json:"generated_at"`
	Scenarios   []ScenarioWriteBudget `json:"scenarios"`
}

type ScenarioWriteBudget struct {
	Scenario      string                 `json:"scenario"`
	ClientPUT     OperationSummary       `json:"client_put"`
	Stages        []WriteStageBudget     `json:"stages"`
	StageDetails  []WriteStageBudgetPart `json:"stage_details,omitempty"`
	GRPCMethods   []GRPCMethodBudget     `json:"grpc_methods,omitempty"`
	DominantStage DominantStageSummary   `json:"dominant_stage"`
}

type WriteStageBudget struct {
	Stage        string  `json:"stage"`
	Count        uint64  `json:"count"`
	SumSeconds   float64 `json:"sum_seconds"`
	MeanMillis   float64 `json:"mean_ms"`
	SharePercent float64 `json:"share_percent"`
}

type WriteStageBudgetPart struct {
	Stage      string  `json:"stage"`
	Role       string  `json:"role"`
	Result     string  `json:"result"`
	Count      uint64  `json:"count"`
	SumSeconds float64 `json:"sum_seconds"`
	MeanMillis float64 `json:"mean_ms"`
}

type GRPCMethodBudget struct {
	Method     string  `json:"method"`
	Count      uint64  `json:"count"`
	SumSeconds float64 `json:"sum_seconds"`
	MeanMillis float64 `json:"mean_ms"`
}

type DominantStageSummary struct {
	Stage           string  `json:"stage"`
	Count           uint64  `json:"count"`
	SumSeconds      float64 `json:"sum_seconds"`
	MeanMillis      float64 `json:"mean_ms"`
	SharePercent    float64 `json:"share_percent"`
	ClearlyDominant bool    `json:"clearly_dominant"`
}

type WriteBudgetSummary struct {
	ClientPUTP50Millis float64 `json:"client_put_p50_ms"`
	DominantStage      string  `json:"dominant_stage"`
	DominantMeanMillis float64 `json:"dominant_mean_ms"`
	DominantShare      float64 `json:"dominant_share_percent"`
}

type SystemSeries struct {
	CPUUtilization []Point `json:"cpu_utilization"`
	NetworkRXMBps  []Point `json:"network_rx_mbps"`
	NetworkTXMBps  []Point `json:"network_tx_mbps"`
	DiskReadMBps   []Point `json:"disk_read_mbps"`
	DiskWriteMBps  []Point `json:"disk_write_mbps"`
}

type Point struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

func AnalyzeRun(runDir string) (AnalysisSummary, error) {
	state, err := ReadRunState(filepath.Join(runDir, RunStateFileName))
	if err != nil {
		return AnalysisSummary{}, err
	}
	var report LoadGenReport
	if err := LoadJSON(filepath.Join(runDir, "artifacts", "client", "loadgen-report.json"), &report); err != nil {
		return AnalysisSummary{}, err
	}
	summary := AnalysisSummary{
		RunID:           state.RunID,
		RunName:         state.RunName,
		Topology:        state.Topology,
		ClientPlacement: state.ClientPlacement,
		GitSHA:          state.GitSHA,
		Scenarios:       report.Scenarios,
		WriteBudget:     map[string]WriteBudgetSummary{},
		Coordinator:     map[string]float64{},
		Storage:         map[string]map[string]float64{},
		System:          map[string]SystemSeries{},
	}

	coordMetrics, err := parsePromMetrics(filepath.Join(runDir, "artifacts", "coordinator", "metrics.prom"))
	if err == nil {
		summary.Coordinator = coordMetrics
	}
	for _, nodeID := range []string{"storage-a", "storage-b", "storage-c", "client", "coordinator"} {
		probePath := filepath.Join(runDir, "artifacts", nodeID, "probe.jsonl")
		if series, err := buildSystemSeries(probePath); err == nil {
			summary.System[nodeID] = series
		}
		if strings.HasPrefix(nodeID, "storage-") {
			metrics, err := parsePromMetrics(filepath.Join(runDir, "artifacts", nodeID, "metrics.prom"))
			if err == nil {
				summary.Storage[nodeID] = metrics
			}
		}
	}

	analysisDir := filepath.Join(runDir, "analysis")
	if err := os.MkdirAll(analysisDir, 0o755); err != nil {
		return AnalysisSummary{}, fmt.Errorf("mkdir analysis dir: %w", err)
	}
	writeBudget, err := buildWriteLatencyBudget(runDir, report)
	if err == nil {
		for _, scenario := range writeBudget.Scenarios {
			summary.WriteBudget[scenario.Scenario] = WriteBudgetSummary{
				ClientPUTP50Millis: scenario.ClientPUT.P50Millis,
				DominantStage:      scenario.DominantStage.Stage,
				DominantMeanMillis: scenario.DominantStage.MeanMillis,
				DominantShare:      scenario.DominantStage.SharePercent,
			}
		}
		if err := SaveJSON(filepath.Join(analysisDir, "write_latency_budget.json"), writeBudget); err != nil {
			return AnalysisSummary{}, err
		}
	}
	if err := SaveJSON(filepath.Join(analysisDir, "summary.json"), summary); err != nil {
		return AnalysisSummary{}, err
	}
	if err := writeHTMLReport(filepath.Join(analysisDir, "index.html"), summary); err != nil {
		return AnalysisSummary{}, err
	}
	return summary, nil
}

func parsePromMetrics(path string) (map[string]float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parse prometheus metrics %q: %w", path, err)
	}
	out := map[string]float64{}
	for name, family := range families {
		if len(family.Metric) == 0 {
			continue
		}
		if len(family.Metric) == 1 {
			out[name] = metricValue(family.Metric[0])
			continue
		}
		for _, metric := range family.Metric {
			labelParts := make([]string, 0, len(metric.Label))
			for _, label := range metric.Label {
				labelParts = append(labelParts, label.GetName()+"="+label.GetValue())
			}
			sort.Strings(labelParts)
			out[name+"{"+strings.Join(labelParts, ",")+"}"] = metricValue(metric)
		}
	}
	return out, nil
}

func metricValue(metric *dto.Metric) float64 {
	switch {
	case metric.Gauge != nil:
		return metric.Gauge.GetValue()
	case metric.Counter != nil:
		return metric.Counter.GetValue()
	case metric.Untyped != nil:
		return metric.Untyped.GetValue()
	case metric.Histogram != nil:
		return metric.Histogram.GetSampleSum()
	default:
		return 0
	}
}

type histogramCountSum struct {
	Count uint64
	Sum   float64
}

type labeledHistogram struct {
	Labels map[string]string
	Sample histogramCountSum
}

type writeStageMetricKey struct {
	Stage  string
	Role   string
	Result string
}

func buildWriteLatencyBudget(runDir string, report LoadGenReport) (WriteLatencyBudgetReport, error) {
	metricRoot := filepath.Join(runDir, "artifacts", "client", "metric-snapshots")
	if _, err := os.Stat(metricRoot); err != nil {
		return WriteLatencyBudgetReport{}, err
	}
	out := WriteLatencyBudgetReport{
		RunID:       report.RunID,
		GeneratedAt: time.Now().UTC(),
	}
	for _, scenario := range report.Scenarios {
		clientPUT, ok := scenarioOperationSummary(scenario, "put")
		if !ok || clientPUT.TotalOps == 0 {
			continue
		}
		startName := sanitizeScenarioName(scenario.Name) + "-start"
		endName := sanitizeScenarioName(scenario.Name) + "-end"
		startStages, err := loadWriteStageSnapshot(metricRoot, startName)
		if err != nil {
			return WriteLatencyBudgetReport{}, err
		}
		endStages, err := loadWriteStageSnapshot(metricRoot, endName)
		if err != nil {
			return WriteLatencyBudgetReport{}, err
		}
		startGRPC, err := loadStorageGRPCSnapshot(metricRoot, startName)
		if err != nil {
			return WriteLatencyBudgetReport{}, err
		}
		endGRPC, err := loadStorageGRPCSnapshot(metricRoot, endName)
		if err != nil {
			return WriteLatencyBudgetReport{}, err
		}

		stageDetails := diffWriteStageBudgets(startStages, endStages)
		stages, dominant := summarizeWriteStages(stageDetails)
		grpcMethods := diffGRPCMethodBudgets(startGRPC, endGRPC)
		out.Scenarios = append(out.Scenarios, ScenarioWriteBudget{
			Scenario:      scenario.Name,
			ClientPUT:     clientPUT,
			Stages:        stages,
			StageDetails:  stageDetails,
			GRPCMethods:   grpcMethods,
			DominantStage: dominant,
		})
	}
	return out, nil
}

func scenarioOperationSummary(scenario ScenarioReport, operation string) (OperationSummary, bool) {
	for _, summary := range scenario.Operations {
		if summary.Operation == operation {
			return summary, true
		}
	}
	if scenario.Kind != operation {
		return OperationSummary{}, false
	}
	return OperationSummary{
		Operation:  operation,
		TotalOps:   scenario.TotalOps,
		SuccessOps: scenario.SuccessOps,
		ErrorOps:   scenario.ErrorOps,
		P50Millis:  scenario.P50Millis,
		P95Millis:  scenario.P95Millis,
		P99Millis:  scenario.P99Millis,
		MaxMillis:  scenario.MaxMillis,
		Throughput: scenario.Throughput,
	}, true
}

func loadWriteStageSnapshot(metricRoot string, snapshotName string) (map[writeStageMetricKey]histogramCountSum, error) {
	paths, err := snapshotTargetPaths(metricRoot, snapshotName, "storage-")
	if err != nil {
		return nil, err
	}
	aggregated := map[writeStageMetricKey]histogramCountSum{}
	for _, path := range paths {
		series, err := parseHistogramFamily(path, "craq_storage_write_stage_seconds")
		if err != nil {
			return nil, err
		}
		for _, series := range series {
			key := writeStageMetricKey{
				Stage:  series.Labels["stage"],
				Role:   series.Labels["role"],
				Result: series.Labels["result"],
			}
			current := aggregated[key]
			current.Count += series.Sample.Count
			current.Sum += series.Sample.Sum
			aggregated[key] = current
		}
	}
	return aggregated, nil
}

func loadStorageGRPCSnapshot(metricRoot string, snapshotName string) (map[string]histogramCountSum, error) {
	paths, err := snapshotTargetPaths(metricRoot, snapshotName, "storage-")
	if err != nil {
		return nil, err
	}
	aggregated := map[string]histogramCountSum{}
	for _, path := range paths {
		series, err := parseHistogramFamily(path, "craq_grpc_request_duration_seconds")
		if err != nil {
			return nil, err
		}
		for _, series := range series {
			if series.Labels["component"] != "storage" {
				continue
			}
			method := series.Labels["method"]
			switch method {
			case "/craq.v1.StorageService/Put", "/craq.v1.StorageService/ForwardWrite", "/craq.v1.StorageService/CommitWrite":
			default:
				continue
			}
			current := aggregated[method]
			current.Count += series.Sample.Count
			current.Sum += series.Sample.Sum
			aggregated[method] = current
		}
	}
	return aggregated, nil
}

func snapshotTargetPaths(metricRoot string, snapshotName string, prefix string) ([]string, error) {
	dir := filepath.Join(metricRoot, snapshotName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read metric snapshot %q: %w", dir, err)
	}
	var paths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".prom") {
			continue
		}
		paths = append(paths, filepath.Join(dir, name))
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no metric snapshots with prefix %q in %q", prefix, dir)
	}
	return paths, nil
}

func parseHistogramFamily(path string, familyName string) ([]labeledHistogram, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parse prometheus metrics %q: %w", path, err)
	}
	family, ok := families[familyName]
	if !ok {
		return nil, nil
	}
	out := make([]labeledHistogram, 0, len(family.GetMetric()))
	for _, metric := range family.GetMetric() {
		hist := metric.GetHistogram()
		if hist == nil {
			continue
		}
		labels := map[string]string{}
		for _, label := range metric.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		out = append(out, labeledHistogram{
			Labels: labels,
			Sample: histogramCountSum{
				Count: hist.GetSampleCount(),
				Sum:   hist.GetSampleSum(),
			},
		})
	}
	return out, nil
}

func diffWriteStageBudgets(start map[writeStageMetricKey]histogramCountSum, end map[writeStageMetricKey]histogramCountSum) []WriteStageBudgetPart {
	keys := make([]writeStageMetricKey, 0, len(end))
	for key := range end {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Stage != keys[j].Stage {
			return keys[i].Stage < keys[j].Stage
		}
		if keys[i].Role != keys[j].Role {
			return keys[i].Role < keys[j].Role
		}
		return keys[i].Result < keys[j].Result
	})
	parts := make([]WriteStageBudgetPart, 0, len(keys))
	for _, key := range keys {
		delta := diffHistogramCountSum(start[key], end[key])
		if delta.Count == 0 && delta.Sum == 0 {
			continue
		}
		meanMillis := 0.0
		if delta.Count > 0 {
			meanMillis = (delta.Sum / float64(delta.Count)) * 1000
		}
		parts = append(parts, WriteStageBudgetPart{
			Stage:      key.Stage,
			Role:       key.Role,
			Result:     key.Result,
			Count:      delta.Count,
			SumSeconds: delta.Sum,
			MeanMillis: meanMillis,
		})
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].SumSeconds == parts[j].SumSeconds {
			if parts[i].Stage != parts[j].Stage {
				return parts[i].Stage < parts[j].Stage
			}
			if parts[i].Role != parts[j].Role {
				return parts[i].Role < parts[j].Role
			}
			return parts[i].Result < parts[j].Result
		}
		return parts[i].SumSeconds > parts[j].SumSeconds
	})
	return parts
}

func summarizeWriteStages(parts []WriteStageBudgetPart) ([]WriteStageBudget, DominantStageSummary) {
	aggregated := map[string]histogramCountSum{}
	for _, part := range parts {
		if part.Result != "success" {
			continue
		}
		current := aggregated[part.Stage]
		current.Count += part.Count
		current.Sum += part.SumSeconds
		aggregated[part.Stage] = current
	}
	stageNames := make([]string, 0, len(aggregated))
	totalSum := 0.0
	for stage, sample := range aggregated {
		stageNames = append(stageNames, stage)
		totalSum += sample.Sum
	}
	sort.Slice(stageNames, func(i, j int) bool {
		left := aggregated[stageNames[i]]
		right := aggregated[stageNames[j]]
		if left.Sum == right.Sum {
			return stageNames[i] < stageNames[j]
		}
		return left.Sum > right.Sum
	})
	stages := make([]WriteStageBudget, 0, len(stageNames))
	for _, stage := range stageNames {
		sample := aggregated[stage]
		meanMillis := 0.0
		if sample.Count > 0 {
			meanMillis = (sample.Sum / float64(sample.Count)) * 1000
		}
		share := 0.0
		if totalSum > 0 {
			share = (sample.Sum / totalSum) * 100
		}
		stages = append(stages, WriteStageBudget{
			Stage:        stage,
			Count:        sample.Count,
			SumSeconds:   sample.Sum,
			MeanMillis:   meanMillis,
			SharePercent: share,
		})
	}
	if len(stages) == 0 {
		return nil, DominantStageSummary{}
	}
	dominant := DominantStageSummary{
		Stage:        stages[0].Stage,
		Count:        stages[0].Count,
		SumSeconds:   stages[0].SumSeconds,
		MeanMillis:   stages[0].MeanMillis,
		SharePercent: stages[0].SharePercent,
	}
	nextSum := 0.0
	if len(stages) > 1 {
		nextSum = stages[1].SumSeconds
	}
	dominant.ClearlyDominant = dominant.SharePercent >= 35 && (nextSum == 0 || dominant.SumSeconds >= nextSum*1.5)
	return stages, dominant
}

func diffGRPCMethodBudgets(start map[string]histogramCountSum, end map[string]histogramCountSum) []GRPCMethodBudget {
	methods := make([]string, 0, len(end))
	for method := range end {
		methods = append(methods, method)
	}
	sort.Slice(methods, func(i, j int) bool {
		left := diffHistogramCountSum(start[methods[i]], end[methods[i]])
		right := diffHistogramCountSum(start[methods[j]], end[methods[j]])
		if left.Sum == right.Sum {
			return methods[i] < methods[j]
		}
		return left.Sum > right.Sum
	})
	out := make([]GRPCMethodBudget, 0, len(methods))
	for _, method := range methods {
		delta := diffHistogramCountSum(start[method], end[method])
		if delta.Count == 0 && delta.Sum == 0 {
			continue
		}
		meanMillis := 0.0
		if delta.Count > 0 {
			meanMillis = (delta.Sum / float64(delta.Count)) * 1000
		}
		out = append(out, GRPCMethodBudget{
			Method:     method,
			Count:      delta.Count,
			SumSeconds: delta.Sum,
			MeanMillis: meanMillis,
		})
	}
	return out
}

func diffHistogramCountSum(start histogramCountSum, end histogramCountSum) histogramCountSum {
	count := end.Count
	if end.Count >= start.Count {
		count = end.Count - start.Count
	} else {
		count = 0
	}
	sum := end.Sum - start.Sum
	if sum < 0 {
		sum = 0
	}
	return histogramCountSum{
		Count: count,
		Sum:   sum,
	}
}

func buildSystemSeries(path string) (SystemSeries, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SystemSeries{}, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		return SystemSeries{}, fmt.Errorf("not enough probe data")
	}
	var samples []ProbeSample
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var sample ProbeSample
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			return SystemSeries{}, fmt.Errorf("decode probe line: %w", err)
		}
		samples = append(samples, sample)
	}
	if len(samples) < 2 {
		return SystemSeries{}, fmt.Errorf("not enough probe samples")
	}
	out := SystemSeries{}
	for i := 1; i < len(samples); i++ {
		prev := samples[i-1]
		cur := samples[i]
		seconds := cur.Timestamp.Sub(prev.Timestamp).Seconds()
		if seconds <= 0 {
			continue
		}
		totalPrev := prev.CPU.User + prev.CPU.Nice + prev.CPU.System + prev.CPU.Idle + prev.CPU.IOWait + prev.CPU.IRQ + prev.CPU.SoftIRQ + prev.CPU.Steal
		totalCur := cur.CPU.User + cur.CPU.Nice + cur.CPU.System + cur.CPU.Idle + cur.CPU.IOWait + cur.CPU.IRQ + cur.CPU.SoftIRQ + cur.CPU.Steal
		totalDelta := float64(totalCur - totalPrev)
		idleDelta := float64(cur.CPU.Idle - prev.CPU.Idle)
		cpuUtil := 0.0
		if totalDelta > 0 {
			cpuUtil = 100 * (1 - idleDelta/totalDelta)
		}
		out.CPUUtilization = append(out.CPUUtilization, Point{Timestamp: cur.Timestamp, Value: cpuUtil})
		out.NetworkRXMBps = append(out.NetworkRXMBps, Point{Timestamp: cur.Timestamp, Value: sumNetDelta(prev.Network, cur.Network, true) / seconds / 1024 / 1024})
		out.NetworkTXMBps = append(out.NetworkTXMBps, Point{Timestamp: cur.Timestamp, Value: sumNetDelta(prev.Network, cur.Network, false) / seconds / 1024 / 1024})
		readBytes, writeBytes := sumDiskDelta(prev.Disks, cur.Disks)
		out.DiskReadMBps = append(out.DiskReadMBps, Point{Timestamp: cur.Timestamp, Value: readBytes / seconds / 1024 / 1024})
		out.DiskWriteMBps = append(out.DiskWriteMBps, Point{Timestamp: cur.Timestamp, Value: writeBytes / seconds / 1024 / 1024})
	}
	return out, nil
}

func sumNetDelta(prev map[string]NetSample, cur map[string]NetSample, rx bool) float64 {
	total := 0.0
	for name, now := range cur {
		before := prev[name]
		if rx {
			total += float64(now.RXBytes - before.RXBytes)
		} else {
			total += float64(now.TXBytes - before.TXBytes)
		}
	}
	return total
}

func sumDiskDelta(prev map[string]DiskSample, cur map[string]DiskSample) (float64, float64) {
	var readBytes float64
	var writeBytes float64
	for name, now := range cur {
		before := prev[name]
		readBytes += float64(now.ReadSectors-before.ReadSectors) * 512
		writeBytes += float64(now.WriteSectors-before.WriteSectors) * 512
	}
	return readBytes, writeBytes
}

func writeHTMLReport(path string, summary AnalysisSummary) error {
	tpl := template.Must(template.New("report").Funcs(template.FuncMap{
		"chart":      renderPolyline,
		"metricRows": formatMetricRows,
	}).Parse(reportTemplate))
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create html report: %w", err)
	}
	defer file.Close()
	return tpl.Execute(file, summary)
}

func formatMetricRows(metrics map[string]float64) []string {
	keys := make([]string, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]string, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, fmt.Sprintf("%s = %.3f", key, metrics[key]))
	}
	return rows
}

func renderPolyline(points []Point, width float64, height float64) template.HTML {
	if len(points) == 0 {
		return template.HTML("")
	}
	maxValue := 0.0
	for _, point := range points {
		maxValue = math.Max(maxValue, point.Value)
	}
	if maxValue == 0 {
		maxValue = 1
	}
	var parts []string
	for idx, point := range points {
		x := 0.0
		if len(points) > 1 {
			x = width * float64(idx) / float64(len(points)-1)
		}
		y := height - (height * point.Value / maxValue)
		parts = append(parts, fmt.Sprintf("%.2f,%.2f", x, y))
	}
	return template.HTML(strings.Join(parts, " "))
}

const reportTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>chainrep benchmark {{.RunID}}</title>
  <style>
    body { font-family: Georgia, serif; margin: 2rem auto; max-width: 1100px; color: #1d1d1d; background: linear-gradient(180deg, #f8f4ec, #ffffff); }
    h1, h2 { margin-bottom: 0.25rem; }
    table { width: 100%; border-collapse: collapse; margin-bottom: 1.5rem; }
    th, td { border-bottom: 1px solid #d7d1c4; padding: 0.5rem; text-align: left; vertical-align: top; }
    .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1rem; }
    .card { background: rgba(255,255,255,0.8); border: 1px solid #e2dbcf; border-radius: 12px; padding: 1rem; }
    code { font-size: 0.9rem; white-space: pre-wrap; }
    svg { width: 100%; height: 120px; background: #fffdf8; border-radius: 8px; border: 1px solid #eee6d8; }
    polyline { fill: none; stroke: #2b6b7f; stroke-width: 2; }
  </style>
</head>
<body>
  <h1>chainrep benchmark {{.RunID}}</h1>
  <p>run={{.RunName}} topology={{.Topology}} client_placement={{.ClientPlacement}} git={{.GitSHA}}</p>

  <h2>Scenarios</h2>
  <table>
    <thead><tr><th>Name</th><th>Kind</th><th>Concurrency</th><th>Throughput</th><th>P50</th><th>P95</th><th>P99</th><th>Errors</th></tr></thead>
    <tbody>
      {{range .Scenarios}}
      <tr>
        <td>{{.Name}}</td>
        <td>{{.Kind}}</td>
        <td>{{.Concurrency}}</td>
        <td>{{printf "%.2f ops/s" .Throughput}}</td>
        <td>{{printf "%.3f ms" .P50Millis}}</td>
        <td>{{printf "%.3f ms" .P95Millis}}</td>
        <td>{{printf "%.3f ms" .P99Millis}}</td>
        <td>{{.ErrorOps}}</td>
      </tr>
      {{end}}
    </tbody>
  </table>

  <div class="grid">
    <div class="card">
      <h2>Coordinator Metrics</h2>
      <code>{{range metricRows .Coordinator}}{{.}}
{{end}}</code>
    </div>
    <div class="card">
      <h2>Storage Metrics</h2>
      {{range $node, $metrics := .Storage}}
      <h3>{{$node}}</h3>
      <code>{{range metricRows $metrics}}{{.}}
{{end}}</code>
      {{end}}
    </div>
  </div>

  <h2>System Charts</h2>
  <div class="grid">
    {{range $node, $series := .System}}
    <div class="card">
      <h3>{{$node}} CPU</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.CPUUtilization 300 120}}"></polyline></svg>
      <h3>{{$node}} Network RX</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.NetworkRXMBps 300 120}}"></polyline></svg>
      <h3>{{$node}} Network TX</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.NetworkTXMBps 300 120}}"></polyline></svg>
      <h3>{{$node}} Disk Read</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.DiskReadMBps 300 120}}"></polyline></svg>
      <h3>{{$node}} Disk Write</h3>
      <svg viewBox="0 0 300 120"><polyline points="{{chart $series.DiskWriteMBps 300 120}}"></polyline></svg>
    </div>
    {{end}}
  </div>
</body>
</html>`
