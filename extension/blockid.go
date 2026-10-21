package extension

import (
	"fmt"

	"github.com/xssnick/tonutils-go/ton"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

func inclusionSeqNo(event *antonv1.AppliedBlock) uint32 {
	if event.InclusionMaster != nil {
		return event.InclusionMaster.Seqno
	}
	if isMaster(event.Block) {
		return event.Block.Seqno
	}
	return ^uint32(0)
}

func isMaster(id *antonv1.BlockId) bool {
	return id != nil && id.Workchain == -1 && id.Shard == -1<<63
}

func blockKey(id *antonv1.BlockId) string {
	if id == nil {
		return ""
	}
	return fmt.Sprintf("%d:%d:%d:%x", id.Workchain, id.Shard, id.Seqno, id.RootHash)
}

func isMasterBlock(id ton.BlockIDExt) bool {
	return id.Workchain == -1 && id.Shard == -1<<63
}
