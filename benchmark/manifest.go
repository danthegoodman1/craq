package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/danthegoodman1/craq/quickstart"
)

type RenderedManifestParams struct {
	SlotCount         int
	ReplicationFactor int
	PrivateIPs        map[string]string
	NodeZones         map[string]string
}

func RenderManifest(params RenderedManifestParams) (quickstart.Config, error) {
	required := []string{"coordinator", "client", "storage-a", "storage-b", "storage-c"}
	for _, name := range required {
		if params.PrivateIPs[name] == "" {
			return quickstart.Config{}, fmt.Errorf("missing private ip for %q", name)
		}
	}
	cfg := quickstart.Config{
		Coordinator: quickstart.Coordinator{
			RPCAddress:        params.PrivateIPs["coordinator"] + ":7400",
			AdminAddress:      params.PrivateIPs["coordinator"] + ":7401",
			SlotCount:         params.SlotCount,
			ReplicationFactor: params.ReplicationFactor,
		},
		Nodes: []quickstart.Node{
			renderNode("a", "storage-a", params.PrivateIPs["storage-a"], params.NodeZones["storage-a"], shouldExposeZoneFailureDomain(params)),
			renderNode("b", "storage-b", params.PrivateIPs["storage-b"], params.NodeZones["storage-b"], shouldExposeZoneFailureDomain(params)),
			renderNode("c", "storage-c", params.PrivateIPs["storage-c"], params.NodeZones["storage-c"], shouldExposeZoneFailureDomain(params)),
		},
	}
	return cfg, cfg.Validate()
}

func shouldExposeZoneFailureDomain(params RenderedManifestParams) bool {
	if params.ReplicationFactor <= 1 {
		return true
	}
	uniqueZones := map[string]struct{}{}
	for _, key := range []string{"storage-a", "storage-b", "storage-c"} {
		zone := params.NodeZones[key]
		if zone == "" {
			continue
		}
		uniqueZones[zone] = struct{}{}
	}
	return len(uniqueZones) >= params.ReplicationFactor
}

func renderNode(id string, key string, ip string, zone string, includeZone bool) quickstart.Node {
	offset := map[string]int{"a": 11, "b": 12, "c": 13}[id]
	failureDomains := map[string]string{
		"host": key,
		"rack": key,
	}
	if includeZone {
		if zone == "" {
			zone = "unknown-zone"
		}
		failureDomains["az"] = zone
	}
	return quickstart.Node{
		ID:           id,
		RPCAddress:   fmt.Sprintf("%s:74%02d", ip, offset),
		AdminAddress: fmt.Sprintf("%s:75%02d", ip, offset),
		FailureDomains: failureDomains,
	}
}

func SaveManifest(path string, cfg quickstart.Config) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir manifest dir: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}
