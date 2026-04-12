package coordserver

import "testing"

func TestOutboxAckCommandIDUsesExactSortedEntrySet(t *testing.T) {
	first := outboxAckCommandID(7, []string{"entry-b", "entry-a"})
	second := outboxAckCommandID(7, []string{"entry-a", "entry-b"})
	if first != second {
		t.Fatalf("ack command IDs differ for reordered entry sets: %q vs %q", first, second)
	}

	different := outboxAckCommandID(7, []string{"entry-a", "entry-c"})
	if first == different {
		t.Fatalf("ack command IDs unexpectedly matched for different entry sets: %q", first)
	}
}
