package extension

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestOnBlockAppliedStoresCatchUpBlockUntilAcknowledged(t *testing.T) {
	store, err := newBlockStore(t.TempDir())
	require.NoError(t, err)
	extension := &Extension{store: store, log: zerolog.Nop()}
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
	// consumer: nothing has been acknowledged yet.
	require.NoError(t, extension.OnBlockApplied(context.Background(), event))
	require.NotNil(t, store.master(id.SeqNo))
}

func TestOnBlockAppliedSkipsBlocksAlreadyAcknowledged(t *testing.T) {
	store, err := newBlockStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, store.processedMaster(100))
	extension := &Extension{store: store, log: zerolog.Nop()}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     -1 << 63,
		SeqNo:     99,
		RootHash:  bytes.Repeat([]byte{1}, 32),
		FileHash:  bytes.Repeat([]byte{2}, 32),
	}
	shard := ton.BlockIDExt{
		Workchain: 0,
		Shard:     -1 << 63,
		SeqNo:     42,
		RootHash:  bytes.Repeat([]byte{3}, 32),
		FileHash:  bytes.Repeat([]byte{4}, 32),
	}
	now := uint32(time.Now().Unix())

	require.NoError(t, extension.OnBlockApplied(context.Background(), hooks.BlockAppliedEvent{
		BlockBOC: []byte{1}, BlockRoot: cell.BeginCell().EndCell(),
		Meta: &storage.BlockMeta{ID: master, GenUTime: now},
	}))
	require.NoError(t, extension.OnBlockApplied(context.Background(), hooks.BlockAppliedEvent{
		BlockBOC: []byte{1}, BlockRoot: cell.BeginCell().EndCell(),
		Meta:               &storage.BlockMeta{ID: shard, GenUTime: now},
		InclusionMasterRef: &master,
	}))

	require.Nil(t, store.master(master.SeqNo), "master below the acknowledged seqno is not stored")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = store.waitBlock(ctx, protoBlockID(shard))
	require.ErrorIs(t, err, context.DeadlineExceeded, "shard included by it is not stored either")
}

func TestOnBlockAppliedStoresRawEnvelopeWhenEnrichmentFails(t *testing.T) {
	store, err := newBlockStore(t.TempDir())
	require.NoError(t, err)
	extension := &Extension{store: store, log: zerolog.Nop()}
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
	stored := store.master(id.SeqNo)
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
