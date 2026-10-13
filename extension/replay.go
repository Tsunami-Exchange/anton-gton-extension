package extension

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

// blockReader is the slice of hooks.Store needed to rebuild applied-block
// envelopes from the node database.
type blockReader interface {
	LookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
}

type nodeSource struct {
	store blockReader
	log   zerolog.Logger
}

func newNodeSource(store blockReader, log zerolog.Logger) *nodeSource {
	return &nodeSource{store: store, log: log}
}

// replay sends masters from seqNo up to the earliest master the store holds
// and returns the seqno the store subscription should continue from. A block
// the node no longer has (beyond retention or not applied yet) ends the
// replay early; the consumer falls back to liteservers for that gap as before.
func (n *nodeSource) replay(ctx context.Context, seqNo uint32, earliest func() (uint32, bool), send func(*antonv1.AppliedBlock) error) (uint32, error) {
	for {
		if first, ok := earliest(); ok && seqNo >= first {
			return seqNo, nil
		}
		id, err := n.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: -1, Shard: -1 << 63, SeqNo: seqNo})
		if err != nil {
			n.log.Warn().Err(err).Uint32("seqno", seqNo).Msg("replay stopped: master block is not available in node storage")
			return seqNo, nil
		}
		master, err := n.partialOK(ctx, id, nil)
		if err != nil {
			return seqNo, err
		}
		// Same order as the live path: the consumer treats a master bundle as
		// ready once every top shard block it lists has arrived.
		for _, shardID := range master.Shards {
			shard, err := n.partialOK(ctx, blockIDFromProto(shardID), &id)
			if err != nil {
				return seqNo, err
			}
			if err = send(shard); err != nil {
				return seqNo, err
			}
		}
		if err = send(master); err != nil {
			return seqNo, err
		}
		seqNo++
	}
}

// appliedBlock rebuilds one block the consumer asked for by id. Shard blocks
// carry the masterchain block that included them, resolved from block meta,
// because the consumer rejects shard events without it.
func (n *nodeSource) appliedBlock(ctx context.Context, id ton.BlockIDExt) (*antonv1.AppliedBlock, error) {
	if id.Workchain == -1 && id.Shard == -1<<63 {
		return n.partialOK(ctx, id, nil)
	}
	meta, err := n.store.BlockMeta(ctx, id)
	if err != nil {
		return nil, err
	}
	master, err := n.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: -1, Shard: -1 << 63, SeqNo: meta.MasterchainRefSeqno})
	if err != nil {
		return nil, err
	}
	return n.partialOK(ctx, id, &master)
}

// partialOK builds an envelope and, like the live path, keeps a partially
// enriched one: the consumer can still index from the raw block and fall back
// for whatever is missing. Only a block the node cannot serve at all is an error.
func (n *nodeSource) partialOK(ctx context.Context, id ton.BlockIDExt, inclusionMaster *ton.BlockIDExt) (*antonv1.AppliedBlock, error) {
	envelope, err := n.envelope(ctx, id, inclusionMaster)
	if envelope == nil {
		return nil, err
	}
	if err != nil {
		n.log.Error().Err(err).Str("block", storage.FormatBlockRef(id)).Msg("failed to enrich block from node storage; sending partial event")
	}
	return envelope, nil
}

func (n *nodeSource) envelope(ctx context.Context, id ton.BlockIDExt, inclusionMaster *ton.BlockIDExt) (*antonv1.AppliedBlock, error) {
	blockBOC, err := n.store.BlockData(ctx, id)
	if err != nil {
		return nil, err
	}
	proofBOC, err := n.store.BlockProof(ctx, storage.ServedProofBlock, id)
	if err != nil {
		return nil, err
	}
	root, err := n.store.BlockRoot(ctx, id)
	if err != nil {
		return nil, err
	}
	state, err := n.store.BlockState(ctx, id)
	if err != nil {
		return nil, err
	}
	return blockEnvelope(id, blockBOC, proofBOC, root, state.Cell, inclusionMaster)
}

func blockIDFromProto(id *antonv1.BlockId) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: id.Workchain,
		Shard:     id.Shard,
		SeqNo:     id.Seqno,
		RootHash:  id.RootHash,
		FileHash:  id.FileHash,
	}
}
