package extension

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

// fakeBlockReader serves masterchain blocks by seqno the way the node
// database would. Block roots are empty cells, so enrichment fails and the
// envelope is partial, exactly like the live path on a parse failure.
type fakeBlockReader struct {
	masters map[uint32]ton.BlockIDExt
	metas   map[string]*storage.BlockMeta
	roots   map[string]*cell.Cell
}

func newFakeBlockReader() *fakeBlockReader {
	return &fakeBlockReader{masters: map[uint32]ton.BlockIDExt{}, metas: map[string]*storage.BlockMeta{}, roots: map[string]*cell.Cell{}}
}

func (f *fakeBlockReader) addMaster(seqno uint32) ton.BlockIDExt {
	id := testBlockID(-1, -1<<63, seqno)
	f.masters[seqno] = id
	f.metas[storage.FormatBlockRef(id)] = &storage.BlockMeta{ID: id, GenUTime: uint32(time.Now().Unix())}
	return id
}

func (f *fakeBlockReader) addShard(seqno uint32, masterSeqno uint32) ton.BlockIDExt {
	id := testBlockID(0, -1<<63, seqno)
	f.metas[storage.FormatBlockRef(id)] = &storage.BlockMeta{ID: id, MasterchainRefSeqno: masterSeqno}
	return id
}

func (f *fakeBlockReader) LookupBlockBySeqNo(_ context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	if ref.Workchain == -1 {
		if id, ok := f.masters[ref.SeqNo]; ok {
			return id, nil
		}
	}
	return ton.BlockIDExt{}, errBlockNotFound
}

func (f *fakeBlockReader) BlockMeta(_ context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	if meta, ok := f.metas[storage.FormatBlockRef(block)]; ok {
		return meta, nil
	}
	return nil, errBlockNotFound
}

func (f *fakeBlockReader) BlockRoot(_ context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	if root, ok := f.roots[storage.FormatBlockRef(block)]; ok {
		return root, nil
	}
	return cell.BeginCell().EndCell(), nil
}

func (f *fakeBlockReader) BlockData(_ context.Context, block ton.BlockIDExt) ([]byte, error) {
	return []byte{byte(block.SeqNo)}, nil
}

func (f *fakeBlockReader) BlockProof(context.Context, storage.ServedProofKind, ton.BlockIDExt) ([]byte, error) {
	return []byte{0xaa}, nil
}

func (f *fakeBlockReader) BlockState(_ context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	return &storage.BlockState{Block: block, Cell: cell.BeginCell().EndCell()}, nil
}

var errBlockNotFound = errors.New("block not found")

func testBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 1)}, 32),
	}
}

func TestSubscribeReplaysMastersFromNodeBeforeStoreTail(t *testing.T) {
	store, err := newBlockStore(t.TempDir())
	require.NoError(t, err)
	reader := newFakeBlockReader()
	for seqno := uint32(100); seqno <= 103; seqno++ {
		reader.addMaster(seqno)
	}
	live := testEnvelope(-1, -1<<63, 103)
	require.NoError(t, store.put(live))
	server := &sourceServer{store: store, node: newNodeSource(reader, zerolog.Nop())}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	events := make(chan *antonv1.AppliedBlock, 8)
	done := make(chan error, 1)
	go func() {
		done <- server.subscribe(ctx, 100, func(event *antonv1.AppliedBlock) error {
			events <- event
			return nil
		})
	}()

	for seqno := uint32(100); seqno <= 102; seqno++ {
		event := receiveEvent(t, events)
		require.Equal(t, seqno, event.Block.Seqno, "replayed master order")
		require.True(t, isMaster(event.Block))
		require.Equal(t, []byte{byte(seqno)}, event.BlockBoc)
		require.Equal(t, []byte{0xaa}, event.ProofBoc)
	}
	require.Equal(t, live.EventId, receiveEvent(t, events).EventId, "hands off to the store tail")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestSubscribeReplaysTopShardsBeforeTheirMaster(t *testing.T) {
	store, err := newBlockStore(t.TempDir())
	require.NoError(t, err)
	reader := newFakeBlockReader()
	master := reader.addMaster(100)
	shard := reader.addShard(42, 100)
	reader.roots[storage.FormatBlockRef(master)] = testMasterBlockRoot(t, master, shard)
	live := testEnvelope(-1, -1<<63, 101)
	require.NoError(t, store.put(live))
	server := &sourceServer{store: store, node: newNodeSource(reader, zerolog.Nop())}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	events := make(chan *antonv1.AppliedBlock, 8)
	go func() {
		_ = server.subscribe(ctx, 100, func(event *antonv1.AppliedBlock) error {
			events <- event
			return nil
		})
	}()

	first := receiveEvent(t, events)
	require.Equal(t, shard.SeqNo, first.Block.Seqno, "shard comes before its master")
	require.Equal(t, int32(0), first.Block.Workchain)
	require.NotNil(t, first.InclusionMaster)
	require.Equal(t, master.SeqNo, first.InclusionMaster.Seqno)
	second := receiveEvent(t, events)
	require.Equal(t, master.SeqNo, second.Block.Seqno)
	require.Len(t, second.Shards, 1)
	require.Equal(t, live.EventId, receiveEvent(t, events).EventId)
}

func TestGetBlockFallsBackToNodeStorageWithInclusionMaster(t *testing.T) {
	store, err := newBlockStore(t.TempDir())
	require.NoError(t, err)
	reader := newFakeBlockReader()
	master := reader.addMaster(100)
	shard := reader.addShard(42, 100)
	server := &sourceServer{store: store, node: newNodeSource(reader, zerolog.Nop())}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	got, err := server.GetBlock(ctx, &antonv1.BlockRequest{Block: protoBlockID(shard)})
	require.NoError(t, err)
	require.Equal(t, shard.SeqNo, got.Block.Seqno)
	require.Equal(t, []byte{byte(shard.SeqNo)}, got.BlockBoc)
	require.NotNil(t, got.InclusionMaster)
	require.Equal(t, master.SeqNo, got.InclusionMaster.Seqno)
	require.Equal(t, master.RootHash, got.InclusionMaster.RootHash)
}

func TestSubscribeStopsReplayAtUnavailableMasterAndTailsStore(t *testing.T) {
	store, err := newBlockStore(t.TempDir())
	require.NoError(t, err)
	reader := newFakeBlockReader()
	reader.addMaster(100)
	// 101 is missing from node storage (pruned or not applied yet); the store
	// later receives 103 live.
	server := &sourceServer{store: store, node: newNodeSource(reader, zerolog.Nop())}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	events := make(chan *antonv1.AppliedBlock, 8)
	go func() {
		_ = server.subscribe(ctx, 100, func(event *antonv1.AppliedBlock) error {
			events <- event
			return nil
		})
	}()

	require.Equal(t, uint32(100), receiveEvent(t, events).Block.Seqno)
	live := testEnvelope(-1, -1<<63, 103)
	require.NoError(t, store.put(live))
	require.Equal(t, live.EventId, receiveEvent(t, events).EventId)
}
