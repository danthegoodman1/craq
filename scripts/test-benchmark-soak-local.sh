#!/usr/bin/env bash
set -euo pipefail

export CRAQ_RUN_BENCHMARK_SOAK_LOCAL=1

go test -count=1 -timeout 20m ./benchmark -run 'TestRuntimeCloudShapeStartupConvergence|TestRuntimeCloudShapeStartupConvergenceWithInjectedControlPlaneLatency|TestRuntimeCloudShapeRoutingProgressDoesNotCollapse'
go test -count=1 -timeout 10m ./coordserver -run 'TestCloudShapeStartupScaleProgressAndHeartbeatStayBoundedUnderLargeOutbox'
