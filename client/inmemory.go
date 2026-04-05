package client

import (
	"context"
	"fmt"

	"github.com/danthegoodman1/craq/storage"
)

type clientHandler interface {
	HandleClientGet(ctx context.Context, req storage.ClientGetRequest) (storage.ReadResult, error)
	HandleClientPut(ctx context.Context, req storage.ClientPutRequest) (storage.CommitResult, error)
	HandleClientDelete(ctx context.Context, req storage.ClientDeleteRequest) (storage.CommitResult, error)
}

type InMemoryTransport struct {
	nodes map[string]clientHandler
}

func NewInMemoryTransport() *InMemoryTransport {
	return &InMemoryTransport{
		nodes: make(map[string]clientHandler),
	}
}

func (t *InMemoryTransport) RegisterNode(nodeID string, node clientHandler) {
	t.nodes[nodeID] = node
}

func (t *InMemoryTransport) Get(ctx context.Context, nodeID string, req storage.ClientGetRequest) (storage.ReadResult, error) {
	node, ok := t.nodes[nodeID]
	if !ok {
		return storage.ReadResult{}, fmt.Errorf("%w: node %q", ErrNoRoute, nodeID)
	}
	result, err := node.HandleClientGet(ctx, req)
	if err != nil {
		return storage.ReadResult{}, fmt.Errorf("err in node.HandleClientGet: %w", err)
	}
	return result, nil
}

func (t *InMemoryTransport) Put(ctx context.Context, nodeID string, req storage.ClientPutRequest) (storage.CommitResult, error) {
	node, ok := t.nodes[nodeID]
	if !ok {
		return storage.CommitResult{}, fmt.Errorf("%w: node %q", ErrNoRoute, nodeID)
	}
	result, err := node.HandleClientPut(ctx, req)
	if err != nil {
		return storage.CommitResult{}, fmt.Errorf("err in node.HandleClientPut: %w", err)
	}
	return result, nil
}

func (t *InMemoryTransport) Delete(ctx context.Context, nodeID string, req storage.ClientDeleteRequest) (storage.CommitResult, error) {
	node, ok := t.nodes[nodeID]
	if !ok {
		return storage.CommitResult{}, fmt.Errorf("%w: node %q", ErrNoRoute, nodeID)
	}
	result, err := node.HandleClientDelete(ctx, req)
	if err != nil {
		return storage.CommitResult{}, fmt.Errorf("err in node.HandleClientDelete: %w", err)
	}
	return result, nil
}
