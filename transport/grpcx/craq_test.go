package grpcx

import (
	"errors"
	"reflect"
	"testing"

	"github.com/danthegoodman1/craq/coordserver"
	grpcproto "github.com/danthegoodman1/craq/proto/craq/v1"
	"github.com/danthegoodman1/craq/storage"
)

func TestReadDependencyErrorRoundTrip(t *testing.T) {
	encoded := encodeError(&storage.ReadDependencyError{
		Slot:                 7,
		ExpectedChainVersion: 3,
		TailNodeID:           "tail-a",
		Cause:                errors.New("fetch committed sequence failed"),
	})
	decoded := decodeError(encoded)

	var dependency *storage.ReadDependencyError
	if !errors.As(decoded, &dependency) {
		t.Fatalf("decoded error = %v, want ReadDependencyError", decoded)
	}
	if got, want := dependency.Slot, 7; got != want {
		t.Fatalf("slot = %d, want %d", got, want)
	}
	if got, want := dependency.ExpectedChainVersion, uint64(3); got != want {
		t.Fatalf("expected chain version = %d, want %d", got, want)
	}
	if got, want := dependency.TailNodeID, "tail-a"; got != want {
		t.Fatalf("tail node = %q, want %q", got, want)
	}
	if !errors.Is(decoded, storage.ErrReadDependencyUnavailable) {
		t.Fatalf("decoded error = %v, want ErrReadDependencyUnavailable", decoded)
	}
}

func TestRoutingSnapshotRoundTripIncludesReadReplicas(t *testing.T) {
	snapshot := coordserver.RoutingSnapshot{
		Version:   4,
		SlotCount: 2,
		Slots: []coordserver.SlotRoute{{
			Slot:         0,
			ChainVersion: 9,
			HeadNodeID:   "head",
			HeadEndpoint: "head:1234",
			TailNodeID:   "tail",
			TailEndpoint: "tail:1234",
			ReadReplicas: []coordserver.ReadReplicaRoute{
				{NodeID: "head", Endpoint: "head:1234", Role: storage.ReplicaRoleHead},
				{NodeID: "mid", Endpoint: "mid:1234", Role: storage.ReplicaRoleMiddle},
				{NodeID: "tail", Endpoint: "tail:1234", Role: storage.ReplicaRoleTail},
			},
			Writable: true,
			Readable: true,
		}},
	}

	got := fromProtoRoutingSnapshot(protoRoutingSnapshot(snapshot))
	if !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("routing snapshot round-trip mismatch\ngot=%#v\nwant=%#v", got, snapshot)
	}
}

func TestReadConsistencyProtoRoundTrip(t *testing.T) {
	if got, want := protoReadConsistency(storage.ReadConsistencyLinearizable), grpcproto.ReadConsistency_READ_CONSISTENCY_LINEARIZABLE; got != want {
		t.Fatalf("proto linearizable = %v, want %v", got, want)
	}
	if got, want := protoReadConsistency(storage.ReadConsistencyLocalCommitted), grpcproto.ReadConsistency_READ_CONSISTENCY_LOCAL_COMMITTED; got != want {
		t.Fatalf("proto local committed = %v, want %v", got, want)
	}
	if got, want := fromProtoReadConsistency(grpcproto.ReadConsistency_READ_CONSISTENCY_UNSPECIFIED), storage.ReadConsistencyLinearizable; got != want {
		t.Fatalf("unspecified read consistency = %q, want %q", got, want)
	}
	if got, want := fromProtoReadConsistency(grpcproto.ReadConsistency_READ_CONSISTENCY_LOCAL_COMMITTED), storage.ReadConsistencyLocalCommitted; got != want {
		t.Fatalf("local committed read consistency = %q, want %q", got, want)
	}
}
