package benchmark

import (
	"time"

	"github.com/danthegoodman1/craq/coordserver"
)

func benchmarkCoordinatorLivenessPolicy(cluster ClusterProfile) coordserver.LivenessPolicy {
	return coordserver.LivenessPolicy{
		SuspectAfter:  cluster.SuspectAfter,
		DeadAfter:     cluster.DeadAfter,
		FlapWindow:    maxDuration(cluster.DeadAfter*4, 10*time.Second),
		FlapThreshold: 8,
	}
}

func maxDuration(a time.Duration, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
