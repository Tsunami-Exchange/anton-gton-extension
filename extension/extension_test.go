package extension

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"google.golang.org/protobuf/proto"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

func testExtension(t *testing.T) (*Extension, jetstream.Stream) {
	t.Helper()
	stream := testStream(t, t.TempDir())
	return &Extension{stream: stream, libraries: &librariesCache{}, log: zerolog.Nop()}, readStream(t, stream)
}

func storedBlock(t *testing.T, s jetstream.Stream, subject string) *antonv1.AppliedBlock {
	t.Helper()
	raw, err := s.GetLastMsgForSubject(context.Background(), subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil
	}
	require.NoError(t, err)
	event := new(antonv1.AppliedBlock)
	require.NoError(t, proto.Unmarshal(raw.Data, event))
	return event
}

func TestOnBlockAppliedPublishesCatchUpBlock(t *testing.T) {
	extension, s := testExtension(t)
	id := ton.BlockIDExt{
		Workchain: -1,
		Shard:     -1 << 63,
		SeqNo:     1,
		RootHash:  bytes.Repeat([]byte{1}, 32),
		FileHash:  bytes.Repeat([]byte{2}, 32),
	}
	event := hooks.BlockAppliedEvent{
		BlockBOC:  []byte{1, 2, 3},
		BlockRoot: cell.BeginCell().EndCell(),
		Meta: &storage.BlockMeta{
			ID:       id,
			GenUTime: uint32(time.Now().Add(-time.Hour).Unix()),
		},
	}

	// An hour-old block applied during node catch-up is still news to the
	// stream: nothing has been published yet.
	require.NoError(t, extension.OnBlockApplied(context.Background(), event))
	require.NotNil(t, storedBlock(t, s, "gton.blocks.1.master"))
}

func TestOnBlockAppliedSkipsBlocksOfOlderMasters(t *testing.T) {
	extension, s := testExtension(t)
	now := uint32(time.Now().Unix())
	newer := ton.BlockIDExt{
		Workchain: -1, Shard: -1 << 63, SeqNo: 100,
		RootHash: bytes.Repeat([]byte{5}, 32), FileHash: bytes.Repeat([]byte{6}, 32),
	}
	require.NoError(t, extension.OnBlockApplied(context.Background(), hooks.BlockAppliedEvent{
		BlockBOC: []byte{1}, BlockRoot: cell.BeginCell().EndCell(),
		Meta: &storage.BlockMeta{ID: newer, GenUTime: now},
	}))

	master := ton.BlockIDExt{
		Workchain: -1, Shard: -1 << 63, SeqNo: 99,
		RootHash: bytes.Repeat([]byte{1}, 32), FileHash: bytes.Repeat([]byte{2}, 32),
	}
	shard := ton.BlockIDExt{
		Workchain: 0, Shard: -1 << 63, SeqNo: 42,
		RootHash: bytes.Repeat([]byte{3}, 32), FileHash: bytes.Repeat([]byte{4}, 32),
	}
	require.NoError(t, extension.OnBlockApplied(context.Background(), hooks.BlockAppliedEvent{
		BlockBOC: []byte{1}, BlockRoot: cell.BeginCell().EndCell(),
		Meta: &storage.BlockMeta{ID: master, GenUTime: now},
	}))
	require.NoError(t, extension.OnBlockApplied(context.Background(), hooks.BlockAppliedEvent{
		BlockBOC: []byte{1}, BlockRoot: cell.BeginCell().EndCell(),
		Meta:               &storage.BlockMeta{ID: shard, GenUTime: now},
		InclusionMasterRef: &master,
	}))

	require.Nil(t, storedBlock(t, s, "gton.blocks.99.master"), "master below the published seqno is not published")
	require.Nil(t, storedBlock(t, s, "gton.blocks.99.shard"), "shard included by it is not published either")
}

func TestOnBlockAppliedPublishesRawEnvelopeWhenEnrichmentFails(t *testing.T) {
	extension, s := testExtension(t)
	id := ton.BlockIDExt{
		Workchain: -1,
		Shard:     -1 << 63,
		SeqNo:     80260332,
		RootHash:  bytes.Repeat([]byte{1}, 32),
		FileHash:  bytes.Repeat([]byte{2}, 32),
	}
	event := hooks.BlockAppliedEvent{
		BlockBOC:  []byte{1, 2, 3},
		ProofBOC:  []byte{4, 5, 6},
		BlockRoot: cell.BeginCell().EndCell(),
		Meta: &storage.BlockMeta{
			ID:       id,
			GenUTime: uint32(time.Now().Unix()),
		},
	}

	require.NoError(t, extension.OnBlockApplied(context.Background(), event))
	stored := storedBlock(t, s, "gton.blocks.80260332.master")
	require.NotNil(t, stored)
	require.Equal(t, event.BlockBOC, stored.BlockBoc)
	require.Equal(t, event.ProofBOC, stored.ProofBoc)
	require.Empty(t, stored.Shards)
}

func TestAppliedAccountStateReadsCurrentStateDictionary(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x11}, 32)
	accountCell, err := (tlb.AccountState{
		IsValid: true,
		Address: address.NewAddress(0, 0, accountID),
		StorageInfo: tlb.StorageInfo{
			StorageUsed: tlb.StorageUsed{
				CellsUsed: big.NewInt(0),
				BitsUsed:  big.NewInt(0),
			},
			StorageExtra: tlb.StorageExtraNone{},
		},
		AccountStorage: tlb.AccountStorage{
			Status:            tlb.AccountStatusUninit,
			LastTransactionLT: 12,
			Balance:           tlb.FromNanoTON(big.NewInt(1)),
		},
	}).ToCell()
	require.NoError(t, err)
	lastHash := bytes.Repeat([]byte{0x22}, 32)
	shardCell, err := tlb.ToCell(tlb.ShardAccount{
		Account:       accountCell,
		LastTransHash: lastHash,
		LastTransLT:   12,
	})
	require.NoError(t, err)
	accounts, err := tlb.NewShardAccountsAugDict()
	require.NoError(t, err)
	require.NoError(t, accounts.Set(accountKey(accountID), shardCell))
	state := new(tlb.ShardStateUnsplit)
	state.Accounts.ShardAccounts = accounts

	got, err := appliedAccountState(state, 0, accountID)
	require.NoError(t, err)
	require.True(t, got.Exists)
	require.Equal(t, uint64(12), got.LastTransactionLt)
	require.Equal(t, lastHash, got.LastTransactionHash)
	require.Equal(t, accountCell.ToBOC(), got.StateBoc)
}

func TestAppliedAccountStateReturnsNonexistentAccount(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x33}, 32)
	accounts, err := tlb.NewShardAccountsAugDict()
	require.NoError(t, err)
	state := new(tlb.ShardStateUnsplit)
	state.Accounts.ShardAccounts = accounts

	got, err := appliedAccountState(state, 0, accountID)
	require.NoError(t, err)
	require.False(t, got.Exists)
	require.Empty(t, got.StateBoc)
}

func TestMasterBlockShardsReadsBlockExtra(t *testing.T) {
	shard := ton.BlockIDExt{
		Workchain: 0,
		Shard:     -1 << 63,
		SeqNo:     85515777,
		RootHash:  bytes.Repeat([]byte{0x44}, 32),
		FileHash:  bytes.Repeat([]byte{0x55}, 32),
	}
	desc, err := tlb.ToCell(&tlb.ShardDesc{
		SeqNo:              shard.SeqNo,
		RootHash:           shard.RootHash,
		FileHash:           shard.FileHash,
		NextValidatorShard: shard.Shard,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	})
	require.NoError(t, err)
	leaf := cell.BeginCell().MustStoreUInt(0, 1).MustStoreBuilder(desc.ToBuilder()).EndCell()
	shardHashes := cell.NewDict(32)
	require.NoError(t, shardHashes.Set(
		cell.BeginCell().MustStoreInt(int64(shard.Workchain), 32).EndCell(),
		cell.BeginCell().MustStoreRef(leaf).EndCell(),
	))
	block := &tlb.Block{Extra: &tlb.BlockExtra{Custom: &tlb.McBlockExtra{ShardHashes: shardHashes}}}

	got, err := masterBlockShards(block)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.True(t, got[0].Equals(&shard))
}

func TestLibrariesCacheDropsRepeatedDictionaryBody(t *testing.T) {
	cache := &librariesCache{}
	hash := bytes.Repeat([]byte{7}, 32)
	first := &antonv1.AppliedBlock{GlobalLibrariesHash: hash, GlobalLibrariesBoc: []byte{1, 2, 3}}
	second := &antonv1.AppliedBlock{GlobalLibrariesHash: hash, GlobalLibrariesBoc: []byte{1, 2, 3}}

	cache.prune(first)
	cache.prune(second)

	require.Equal(t, []byte{1, 2, 3}, first.GlobalLibrariesBoc, "first master carries the dictionary")
	require.Empty(t, second.GlobalLibrariesBoc, "unchanged dictionary is not repeated")
	require.Equal(t, hash, second.GlobalLibrariesHash, "hash is kept so the consumer can detect a gap")
}

func TestLibrariesCacheKeepsChangedDictionaryBody(t *testing.T) {
	cache := &librariesCache{}
	first := &antonv1.AppliedBlock{GlobalLibrariesHash: bytes.Repeat([]byte{7}, 32), GlobalLibrariesBoc: []byte{1}}
	second := &antonv1.AppliedBlock{GlobalLibrariesHash: bytes.Repeat([]byte{8}, 32), GlobalLibrariesBoc: []byte{2}}

	cache.prune(first)
	cache.prune(second)

	require.Equal(t, []byte{2}, second.GlobalLibrariesBoc)
}

func TestLibrariesCacheIgnoresBlocksWithoutDictionary(t *testing.T) {
	cache := &librariesCache{}
	cache.prune(&antonv1.AppliedBlock{GlobalLibrariesHash: bytes.Repeat([]byte{7}, 32), GlobalLibrariesBoc: []byte{1}})
	cache.prune(&antonv1.AppliedBlock{}) // shard block

	current := cache.current()
	require.NotNil(t, current)
	require.Equal(t, []byte{1}, current.Boc)
}
