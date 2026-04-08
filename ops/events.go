package ops

import (
	"sync"
	"time"
)

type Event struct {
	Time         time.Time `json:"time"`
	Level        string    `json:"level"`
	Component    string    `json:"component"`
	Kind         string    `json:"kind"`
	Message      string    `json:"message"`
	NodeID       string    `json:"node_id,omitempty"`
	Slot         *int      `json:"slot,omitempty"`
	ChainVersion *uint64   `json:"chain_version,omitempty"`
	Sequence     *uint64   `json:"sequence,omitempty"`
	PeerNodeID   string    `json:"peer_node_id,omitempty"`
	CommandID    string    `json:"command_id,omitempty"`
	Error        string    `json:"error,omitempty"`
}

type EventRing struct {
	mu     sync.RWMutex
	events []Event
	next   int
	full   bool
}

func NewEventRing(capacity int) *EventRing {
	if capacity <= 0 {
		capacity = 64
	}
	return &EventRing{events: make([]Event, capacity)}
}

func (r *EventRing) Add(event Event) {
	if r == nil || len(r.events) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events[r.next] = event
	r.next = (r.next + 1) % len(r.events)
	if r.next == 0 {
		r.full = true
	}
}

func (r *EventRing) Snapshot() []Event {
	if r == nil || len(r.events) == 0 {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.full {
		out := make([]Event, 0, r.next)
		out = append(out, r.events[:r.next]...)
		return out
	}
	out := make([]Event, 0, len(r.events))
	out = append(out, r.events[r.next:]...)
	out = append(out, r.events[:r.next]...)
	return out
}

func IntPtr(v int) *int { return &v }

func Uint64Ptr(v uint64) *uint64 { return &v }
