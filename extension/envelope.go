package extension

import (
	"bytes"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

func appliedBlockEnvelope(event hooks.BlockAppliedEvent) (*antonv1.AppliedBlock, error) {
	if event.Meta == nil {
		return nil, fmt.Errorf("applied block metadata is missing")
	}
	id := event.Meta.ID
	block, err := storage.ParseVerifiedBlockCell(id, event.BlockRoot)
	if err != nil {
		return nil, fmt.Errorf("parse applied block: %w", err)
	}
	view, err := liveview.NewBlockView(id, event.BlockRoot, event.CurrentState)
	if err != nil {
		return nil, fmt.Errorf("build applied block view: %w", err)
	}

	accountIDs, err := transactionAccountIDs(block)
	if err != nil {
		return nil, err
	}
	accounts := make([]*antonv1.AccountState, 0, len(accountIDs))
	for _, accountID := range accountIDs {
		shardAccount, state, err := view.ExternalMessageAccount(address.NewAddress(0, byte(id.Workchain), accountID))
		if err != nil {
			return nil, fmt.Errorf("load account %x: %w", accountID, err)
		}
		account := &antonv1.AccountState{
			Workchain:           id.Workchain,
			Address:             bytes.Clone(accountID),
			Exists:              state.IsValid,
			LastTransactionLt:   shardAccount.LastTransLT,
			LastTransactionHash: bytes.Clone(shardAccount.LastTransHash),
		}
		if state.IsValid {
			account.StateBoc = shardAccount.Account.ToBOC()
		}
		accounts = append(accounts, account)
	}

	envelope := &antonv1.AppliedBlock{
		EventId:             fmt.Sprintf("%d:%d:%d:%x", id.Workchain, id.Shard, id.SeqNo, id.RootHash),
		Block:               protoBlockID(id),
		BlockBoc:            bytes.Clone(event.BlockBOC),
		ProofBoc:            bytes.Clone(event.ProofBOC),
		Accounts:            accounts,
		ObservedAtUnixMilli: time.Now().UnixMilli(),
	}
	if event.InclusionMasterRef != nil {
		envelope.InclusionMaster = protoBlockID(*event.InclusionMasterRef)
	}
	if id.Workchain == -1 && id.Shard == -1<<63 {
		if err = addMasterData(view, envelope); err != nil {
			return nil, err
		}
	}
	return envelope, nil
}

func addMasterData(view *liveview.BlockView, envelope *antonv1.AppliedBlock) error {
	extra, err := view.McStateExtra()
	if err != nil {
		return fmt.Errorf("load masterchain state extra: %w", err)
	}
	shards, err := ton.LoadShardsFromHashes(extra.ShardHashes, false)
	if err != nil {
		return fmt.Errorf("load masterchain shards: %w", err)
	}
	envelope.Shards = make([]*antonv1.BlockId, 0, len(shards))
	for _, shard := range shards {
		envelope.Shards = append(envelope.Shards, protoBlockID(*shard))
	}

	config, err := tlb.ToCell(extra.ConfigParams.Config.Params)
	if err != nil {
		return fmt.Errorf("serialize blockchain config: %w", err)
	}
	envelope.BlockchainConfigBoc = config.ToBOC()

	libraries, err := view.GlobalLibraries()
	if err != nil {
		return fmt.Errorf("load global libraries: %w", err)
	}
	if libraries != nil && libraries.AsCell() != nil {
		envelope.GlobalLibrariesBoc = libraries.AsCell().ToBOC()
	}
	return nil
}

func transactionAccountIDs(block *tlb.Block) ([][]byte, error) {
	if block.Extra == nil || block.Extra.ShardAccountBlocks == nil {
		return nil, fmt.Errorf("block has no shard account blocks")
	}
	var shardAccounts tlb.ShardAccountBlocks
	if err := tlb.LoadFromCell(&shardAccounts, block.Extra.ShardAccountBlocks.MustBeginParse()); err != nil {
		return nil, fmt.Errorf("load shard account blocks: %w", err)
	}
	if shardAccounts.Accounts == nil {
		return nil, nil
	}

	items, err := shardAccounts.Accounts.Range(false, false)
	if err != nil {
		return nil, fmt.Errorf("iterate shard account blocks: %w", err)
	}
	accounts := make([][]byte, 0, len(items))
	for _, item := range items {
		if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), item.Value); err != nil {
			return nil, fmt.Errorf("load account block fees: %w", err)
		}
		var accountBlock tlb.AccountBlock
		if err = tlb.LoadFromCell(&accountBlock, item.Value); err != nil {
			return nil, fmt.Errorf("load account block: %w", err)
		}
		if len(accountBlock.Addr) != 32 {
			return nil, fmt.Errorf("account address is %d bytes", len(accountBlock.Addr))
		}
		accounts = append(accounts, bytes.Clone(accountBlock.Addr))
	}
	return accounts, nil
}

func protoBlockID(id ton.BlockIDExt) *antonv1.BlockId {
	return &antonv1.BlockId{
		Workchain: id.Workchain,
		Shard:     id.Shard,
		Seqno:     id.SeqNo,
		RootHash:  bytes.Clone(id.RootHash),
		FileHash:  bytes.Clone(id.FileHash),
	}
}
