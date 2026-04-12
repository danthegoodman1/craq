package benchmark

import (
	"strings"
	"testing"
)

func TestCraqLoadgenUnitRunsAsLongLivedService(t *testing.T) {
	unit := craqLoadgenUnit()
	if !strings.Contains(unit, "Type=simple") {
		t.Fatalf("loadgen unit must be Type=simple, got:\n%s", unit)
	}
	if strings.Contains(unit, "Type=oneshot") {
		t.Fatalf("loadgen unit must not be Type=oneshot, got:\n%s", unit)
	}
}
