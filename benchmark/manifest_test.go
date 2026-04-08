package benchmark

import "testing"

func TestRenderManifestOmitsZoneFailureDomainWhenSingleZoneWouldMakePlacementImpossible(t *testing.T) {
	cfg, err := RenderManifest(RenderedManifestParams{
		SlotCount:         1024,
		ReplicationFactor: 3,
		PrivateIPs: map[string]string{
			"client":      "10.42.0.5",
			"coordinator": "10.42.0.4",
			"storage-a":   "10.42.0.2",
			"storage-b":   "10.42.1.2",
			"storage-c":   "10.42.2.2",
		},
		NodeZones: map[string]string{
			"storage-a": "us-central1-a",
			"storage-b": "us-central1-a",
			"storage-c": "us-central1-a",
		},
	})
	if err != nil {
		t.Fatalf("RenderManifest returned error: %v", err)
	}
	for _, node := range cfg.Nodes {
		if _, ok := node.FailureDomains["az"]; ok {
			t.Fatalf("node %q unexpectedly included az failure domain in single-zone layout: %#v", node.ID, node.FailureDomains)
		}
	}
}

func TestRenderManifestIncludesZoneFailureDomainWhenZonesAreDistinct(t *testing.T) {
	cfg, err := RenderManifest(RenderedManifestParams{
		SlotCount:         1024,
		ReplicationFactor: 3,
		PrivateIPs: map[string]string{
			"client":      "10.42.0.5",
			"coordinator": "10.42.0.4",
			"storage-a":   "10.42.0.2",
			"storage-b":   "10.42.1.2",
			"storage-c":   "10.42.2.2",
		},
		NodeZones: map[string]string{
			"storage-a": "us-central1-a",
			"storage-b": "us-central1-b",
			"storage-c": "us-central1-c",
		},
	})
	if err != nil {
		t.Fatalf("RenderManifest returned error: %v", err)
	}
	for _, node := range cfg.Nodes {
		if got := node.FailureDomains["az"]; got == "" {
			t.Fatalf("node %q missing az failure domain in multi-zone layout: %#v", node.ID, node.FailureDomains)
		}
	}
}
