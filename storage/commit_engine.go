package storage

import (
	"context"
	"errors"
	"time"
)

const (
	defaultCommitBatchDelay    = 250 * time.Microsecond
	defaultCommitBatchMaxOps   = 128
	defaultCommitBatchMaxBytes = 1 << 20
)

type durableCommitResult struct {
	committed bool
	err       error
}

type commitIntent struct {
	ctx      context.Context
	commit   DurableCommit
	resp     chan durableCommitResult
	queuedAt time.Time
}

type durableCommitEngine struct {
	node     *Node
	submitCh chan *commitIntent
	closeCh  chan struct{}
	closedCh chan struct{}
}

func newDurableCommitEngine(node *Node) *durableCommitEngine {
	engine := &durableCommitEngine{
		node:     node,
		submitCh: make(chan *commitIntent, 4096),
		closeCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
	}
	go engine.run()
	return engine
}

func (e *durableCommitEngine) close() {
	select {
	case <-e.closeCh:
	default:
		close(e.closeCh)
	}
	<-e.closedCh
}

func (e *durableCommitEngine) submit(ctx context.Context, commit DurableCommit) (bool, error) {
	intent := &commitIntent{
		ctx:      ctx,
		commit:   commit,
		resp:     make(chan durableCommitResult, 1),
		queuedAt: time.Now(),
	}
	select {
	case <-e.closeCh:
		return false, context.Canceled
	case e.submitCh <- intent:
	}
	select {
	case <-e.closeCh:
		return false, context.Canceled
	case result := <-intent.resp:
		return result.committed, result.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (e *durableCommitEngine) run() {
	defer close(e.closedCh)
	var (
		batch     []*commitIntent
		batchSize int
		timer     *time.Timer
		timerCh   <-chan time.Time
	)

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerCh = nil
	}

	resetTimer := func() {
		if timer == nil {
			timer = time.NewTimer(defaultCommitBatchDelay)
		} else {
			timer.Reset(defaultCommitBatchDelay)
		}
		timerCh = timer.C
	}

	flush := func() {
		if len(batch) == 0 {
			stopTimer()
			return
		}
		intents := batch
		batch = nil
		batchSize = 0
		stopTimer()

		commits := make([]DurableCommit, 0, len(intents))
		filtered := make([]*commitIntent, 0, len(intents))
		filteredBytes := 0
		for _, intent := range intents {
			if intent.ctx != nil {
				if err := intent.ctx.Err(); err != nil {
					intent.resp <- durableCommitResult{err: err}
					continue
				}
			}
			e.node.observeWriteStage(writeStageCommitBatchWait, intent.commit.Persisted.Assignment.Role, writeStageResultSuccess, time.Since(intent.queuedAt))
			commits = append(commits, intent.commit)
			filtered = append(filtered, intent)
			filteredBytes += estimatedCommitBytes(intent.commit)
		}
		if len(filtered) == 0 {
			return
		}
		e.node.observeCommitBatchSize(len(filtered), filteredBytes)
		for _, intent := range filtered {
			if stage := writeTraceFlushStartStage(intent.commit.Persisted.Assignment.Role); stage != "" {
				e.node.traceWriteEvent(intent.commit.Persisted.Assignment, intent.commit.Operation.Sequence, stage)
			}
		}
		flushStarted := time.Now()

		applyBatchErr := errors.New("batch backend unavailable")
		if backend, ok := e.node.backend.(batchCommitBackend); ok {
			applyBatchErr = backend.ApplyCommittedBatch(context.Background(), e.node.nodeID, commits)
		}
		flushDuration := time.Since(flushStarted)
		if applyBatchErr == nil {
			for _, intent := range filtered {
				e.node.observeCommitFlush(intent.commit.Persisted.Assignment.Role, writeStageResultSuccess, flushDuration)
				if stage := writeTraceFlushEndStage(intent.commit.Persisted.Assignment.Role); stage != "" {
					e.node.traceWriteEvent(intent.commit.Persisted.Assignment, intent.commit.Operation.Sequence, stage)
				}
				intent.resp <- durableCommitResult{committed: true}
			}
			return
		}

		for i, intent := range filtered {
			result := durableCommitResult{}
			commit := commits[i]
			applyErr := e.node.backend.ApplyCommitted(context.Background(), e.node.nodeID, commit.Operation, nil)
			if applyErr != nil {
				highestCommitted, err := e.node.backend.HighestCommittedSequence(commit.Operation.Slot)
				if err == nil && highestCommitted == commit.Operation.Sequence {
					result.committed = true
				}
				result.err = applyErr
			} else {
				result.committed = true
			}
			if upstreamBackend, ok := e.node.backend.(upstreamConfirmationBackend); ok && result.committed {
				if err := upstreamBackend.SetHighestUpstreamConfirmedSequence(commit.Operation.Slot, commit.UpstreamConfirmedSequence); err != nil && result.err == nil {
					result.err = err
				}
			}
			e.node.observeCommitFlush(commit.Persisted.Assignment.Role, writeStageResult(result.err), flushDuration)
			if stage := writeTraceFlushEndStage(commit.Persisted.Assignment.Role); stage != "" {
				e.node.traceWriteEvent(commit.Persisted.Assignment, commit.Operation.Sequence, stage)
			}
			intent.resp <- result
		}
	}

	for {
		select {
		case <-e.closeCh:
			for _, intent := range batch {
				intent.resp <- durableCommitResult{err: context.Canceled}
			}
			return
		case intent := <-e.submitCh:
			batch = append(batch, intent)
			batchSize += estimatedCommitBytes(intent.commit)
			if len(batch) == 1 {
				resetTimer()
			}
			if len(batch) >= defaultCommitBatchMaxOps || batchSize >= defaultCommitBatchMaxBytes {
				flush()
			}
		case <-timerCh:
			flush()
		}
	}
}

func estimatedCommitBytes(commit DurableCommit) int {
	return len(commit.Operation.Key) + len(commit.Operation.Value) + 128
}
