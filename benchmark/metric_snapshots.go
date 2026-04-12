package benchmark

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/danthegoodman1/craq/quickstart"
)

type metricSnapshotTarget struct {
	Name string
	URL  string
}

func loadgenMetricSnapshotTargets(manifest quickstart.Config) []metricSnapshotTarget {
	targets := []metricSnapshotTarget{{
		Name: "coordinator",
		URL:  "http://" + manifest.Coordinator.AdminAddress + "/metrics",
	}}
	nodes := append([]quickstart.Node(nil), manifest.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	for _, node := range nodes {
		targets = append(targets, metricSnapshotTarget{
			Name: "storage-" + node.ID,
			URL:  "http://" + node.AdminAddress + "/metrics",
		})
	}
	return targets
}

func collectMetricSnapshot(ctx context.Context, outputDir string, snapshotName string, targets []metricSnapshotTarget) error {
	for _, target := range targets {
		if err := collectHTTPContext(
			ctx,
			target.URL,
			filepath.Join(outputDir, "metric-snapshots", snapshotName, target.Name+".prom"),
		); err != nil {
			return fmt.Errorf("collect metric snapshot %q target %q: %w", snapshotName, target.Name, err)
		}
	}
	return nil
}
