package extension

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// testMasterBlockRoot builds the smallest masterchain block cell that
// ParseVerifiedBlockCell accepts for id and that lists shards in ShardHashes.
func testMasterBlockRoot(t *testing.T, id ton.BlockIDExt, shards ...ton.BlockIDExt) *cell.Cell {
	t.Helper()
	shardHashes := cell.NewDict(32)
	for _, shard := range shards {
		desc, err := tlb.ToCell(&tlb.ShardDesc{
			SeqNo:              shard.SeqNo,
			RootHash:           shard.RootHash,
			FileHash:           shard.FileHash,
			NextValidatorShard: shard.Shard,
			SplitMergeAt:       tlb.FutureSplitMergeNone{},
		})
		require.NoError(t, err)
		leaf := cell.BeginCell().MustStoreUInt(0, 1).MustStoreBuilder(desc.ToBuilder()).EndCell()
		require.NoError(t, shardHashes.Set(
			cell.BeginCell().MustStoreInt(int64(shard.Workchain), 32).EndCell(),
			cell.BeginCell().MustStoreRef(leaf).EndCell(),
		))
	}
	hash := bytes.Repeat([]byte{0x99}, 32)
	var header tlb.BlockHeader
	header.SeqNo = id.SeqNo
	header.Shard = tlb.ShardIdent{WorkchainID: id.Workchain}
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{SeqNo: id.SeqNo - 1, RootHash: hash, FileHash: hash}}
	info, err := tlb.ToCell(header)
	require.NoError(t, err)

	// McBlockExtra: #cca5 key_block:Bool shard_hashes:ShardHashes
	// shard_fees:ShardFees (empty HashmapAugE = $0 + two empty CurrencyCollections)
	// ^[prev_blk_signatures recover_create_msg mint_msg]; no config for a non-key block.
	emptyCurrency := func(b *cell.Builder) {
		b.MustStoreUInt(0, 4)
		b.MustStoreBoolBit(false)
	}
	custom := cell.BeginCell().MustStoreUInt(0xcca5, 16).MustStoreBoolBit(false).MustStoreDict(shardHashes).MustStoreBoolBit(false)
	emptyCurrency(custom)
	emptyCurrency(custom)
	custom.MustStoreRef(cell.BeginCell().MustStoreBoolBit(false).MustStoreBoolBit(false).MustStoreBoolBit(false).EndCell())

	extra := cell.BeginCell().MustStoreUInt(0x4a33f6fd, 32).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreRef(cell.BeginCell().MustStoreBoolBit(false).EndCell()).
		MustStoreSlice(hash, 256).
		MustStoreSlice(hash, 256).
		MustStoreBoolBit(true).MustStoreRef(custom.EndCell()).
		EndCell()

	return cell.BeginCell().MustStoreUInt(0x11ef55aa, 32).MustStoreInt(0, 32).
		MustStoreRef(info).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreRef(extra).
		EndCell()
}

func TestMasterBlockRootFixtureParses(t *testing.T) {
	id := testBlockID(-1, -1<<63, 7)
	root := testMasterBlockRoot(t, id, testBlockID(0, -1<<63, 42))
	block, err := storage.ParseVerifiedBlockCell(id, root)
	require.NoError(t, err)
	shards, err := masterBlockShards(block)
	require.NoError(t, err)
	require.Len(t, shards, 1)
}
