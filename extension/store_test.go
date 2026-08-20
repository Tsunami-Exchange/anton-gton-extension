package extension

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

func TestBlockStoreIsDurableAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	store, err := newBlockStore(dir)
	require.NoError(t, err)
	event := testEnvelope(-1, -1<<63, 100)

	require.NoError(t, store.put(event))
	require.NoError(t, store.put(event))
	files, err := filepath.Glob(filepath.Join(dir, "*.pb"))
	require.NoError(t, err)
	require.Len(t, files, 1)

	reloaded, err := newBlockStore(dir)
	require.NoError(t, err)
	require.Equal(t, event.EventId, reloaded.master(100).EventId)
	block, err := reloaded.waitBlock(context.Background(), event.Block)
	require.NoError(t, err)
	require.Equal(t, event.EventId, block.EventId)
}

func TestBlockStoreWaitsForAppliedBlock(t *testing.T) {
	store, err := newBlockStore(t.TempDir())
	require.NoError(t, err)
	event := testEnvelope(-1, -1<<63, 101)

	done := make(chan error, 1)
	go func() {
		block, waitErr := store.waitMaster(context.Background(), 101)
		if waitErr == nil && block.EventId != event.EventId {
			t.Errorf("event id = %q, want %q", block.EventId, event.EventId)
		}
		done <- waitErr
	}()
	require.NoError(t, store.put(event))

	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("waitMaster did not unblock")
	}
}

func TestBlockStoreSubscriptionReplaysAndTailsFullEnvelopes(t *testing.T) {
	store, err := newBlockStore(t.TempDir())
	require.NoError(t, err)
	oldMaster := testEnvelope(-1, -1<<63, 99)
	master := testEnvelope(-1, -1<<63, 100)
	shard := testEnvelope(0, -1<<63, 42)
	shard.InclusionMaster = master.Block
	require.NoError(t, store.put(oldMaster))
	require.NoError(t, store.put(master))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	events := make(chan *antonv1.AppliedBlock, 2)
	done := make(chan error, 1)
	go func() {
		done <- store.subscribe(ctx, 100, func(event *antonv1.AppliedBlock) error {
			events <- event
			return nil
		})
	}()

	require.Equal(t, master.EventId, receiveEvent(t, events).EventId)
	require.NoError(t, store.put(shard))
	require.Equal(t, shard.EventId, receiveEvent(t, events).EventId)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestProcessedMasterRetainsLatestCommittedBundle(t *testing.T) {
	dir := t.TempDir()
	store, err := newBlockStore(dir)
	require.NoError(t, err)
	previous := testEnvelope(-1, -1<<63, 99)
	current := testEnvelope(-1, -1<<63, 100)
	shard := testEnvelope(0, -1<<63, 42)
	shard.InclusionMaster = current.Block

	require.NoError(t, store.put(previous))
	require.NoError(t, store.put(current))
	require.NoError(t, store.put(shard))
	require.NoError(t, store.processedMaster(100))
	require.Nil(t, store.master(99))
	require.NotNil(t, store.master(100))
	_, err = store.waitBlock(context.Background(), shard.Block)
	require.NoError(t, err)

	files, err := filepath.Glob(filepath.Join(dir, "*.pb"))
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.NoError(t, store.processedMaster(101))
	files, err = filepath.Glob(filepath.Join(dir, "*.pb"))
	require.NoError(t, err)
	require.Empty(t, files)
}

func testEnvelope(workchain int32, shard int64, seqno uint32) *antonv1.AppliedBlock {
	root := bytes.Repeat([]byte{byte(seqno)}, 32)
	return &antonv1.AppliedBlock{
		EventId: string(root),
		Block: &antonv1.BlockId{
			Workchain: workchain,
			Shard:     shard,
			Seqno:     seqno,
			RootHash:  root,
			FileHash:  bytes.Repeat([]byte{byte(seqno + 1)}, 32),
		},
		BlockBoc: []byte{1},
	}
}

func receiveEvent(t *testing.T, events <-chan *antonv1.AppliedBlock) *antonv1.AppliedBlock {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("subscription did not send an applied block")
		return nil
	}
}
