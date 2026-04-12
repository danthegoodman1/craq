#!/usr/bin/env bash
set -euo pipefail

go test ./benchmark ./coordserver ./storage
go test -race -count=1 ./benchmark ./coordserver ./storage
go test -count=1 ./benchmark -run 'TestRuntimeProcessesBecomeWritableWithBudgetedBenchmarkStartup|TestRuntimeLargeStartupConvergence|TestRuntimeStartupConvergenceWithInjectedControlPlaneLatency|TestRuntimeRoutingProgressDoesNotFlatlineOrCollapse|TestRuntimeAllNodesAppearInWritableRouting'
