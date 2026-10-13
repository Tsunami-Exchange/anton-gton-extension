package extension

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

func appliedBlockEnvelope(event hooks.BlockAppliedEvent) (*antonv1.AppliedBlock, error) {
	if event.Meta == nil {
		return nil, fmt.Errorf("applied block metadata is missing")
	}
	return blockEnvelope(event.Meta.ID, event.BlockBOC, event.ProofBOC, event.BlockRoot, event.CurrentState, event.InclusionMasterRef)
}

// blockEnvelope builds the Anton event for one applied block. The inputs are
// exactly what the apply hook hands over, so the same builder serves blocks
// read back from the node database for replay.
func blockEnvelope(id ton.BlockIDExt, blockBOC, proofBOC []byte, blockRoot, stateRoot *cell.Cell, inclusionMaster *ton.BlockIDExt) (*antonv1.AppliedBlock, error) {
	envelope := &antonv1.AppliedBlock{
		EventId:             fmt.Sprintf("%d:%d:%d:%x", id.Workchain, id.Shard, id.SeqNo, id.RootHash),
		Block:               protoBlockID(id),
		BlockBoc:            bytes.Clone(blockBOC),
		ProofBoc:            bytes.Clone(proofBOC),
		ObservedAtUnixMilli: time.Now().UnixMilli(),
	}
	if inclusionMaster != nil {
		envelope.InclusionMaster = protoBlockID(*inclusionMaster)
	}

	block, err := storage.ParseVerifiedBlockCell(id, blockRoot)
	if err != nil {
		return envelope, fmt.Errorf("parse applied block: %w", err)
	}
	master := id.Workchain == -1 && id.Shard == -1<<63
	if master {
		// Shards come from the block itself, so a partial envelope still tells
		// the consumer which shard blocks to expect even if the state fails.
		shards, err := masterBlockShards(block)
		if err != nil {
			return envelope, fmt.Errorf("load masterchain shards: %w", err)
		}
		envelope.Shards = make([]*antonv1.BlockId, 0, len(shards))
		for _, shard := range shards {
			envelope.Shards = append(envelope.Shards, protoBlockID(*shard))
		}
	}
	state, err := appliedShardState(stateRoot)
	if err != nil {
		return envelope, fmt.Errorf("parse applied state: %w", err)
	}

	accountIDs, err := transactionAccountIDs(block)
	if err != nil {
		return envelope, err
	}

	accounts := make([]*antonv1.AccountState, 0, len(accountIDs))
	for _, accountID := range accountIDs {
		account, err := appliedAccountState(state, id.Workchain, accountID)
		if err != nil {
			return envelope, fmt.Errorf("load account %x: %w", accountID, err)
		}
		accounts = append(accounts, account)
	}

	envelope.Accounts = accounts
	if master {
		if err = addMasterData(state, envelope); err != nil {
			return envelope, err
		}
	}

	return envelope, nil
}

func addMasterData(state *tlb.ShardStateUnsplit, envelope *antonv1.AppliedBlock) error {
	extra, err := masterStateExtra(state)
	if err != nil {
		return fmt.Errorf("load masterchain state extra: %w", err)
	}
	config, err := tlb.ToCell(extra.ConfigParams.Config.Params)
	if err != nil {
		return fmt.Errorf("serialize blockchain config: %w", err)
	}
	envelope.BlockchainConfigBoc = config.ToBOC()

	libraries, err := globalLibraries(state)
	if err != nil {
		return fmt.Errorf("load global libraries: %w", err)
	}
	if libraries != nil && libraries.AsCell() != nil {
		envelope.GlobalLibrariesBoc = libraries.AsCell().ToBOC()
	}
	return nil
}

func masterBlockShards(block *tlb.Block) ([]*ton.BlockIDExt, error) {
	if block == nil || block.Extra == nil || block.Extra.Custom == nil || block.Extra.Custom.ShardHashes == nil {
		return nil, fmt.Errorf("master block has no shard hashes")
	}
	return ton.LoadShardsFromHashes(block.Extra.Custom.ShardHashes, false)
}

func appliedShardState(root *cell.Cell) (*tlb.ShardStateUnsplit, error) {
	if root == nil {
		return nil, fmt.Errorf("current state root is missing")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	var state tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&state, loader); err != nil {
		return nil, err
	}
	return &state, nil
}

func appliedAccountState(state *tlb.ShardStateUnsplit, workchain int32, accountID []byte) (*antonv1.AccountState, error) {
	account := &antonv1.AccountState{
		Workchain: workchain,
		Address:   bytes.Clone(accountID),
	}
	if state.Accounts.ShardAccounts == nil || state.Accounts.ShardAccounts.IsEmpty() {
		return account, nil
	}
	value, err := state.Accounts.ShardAccounts.LoadValue(accountKey(accountID))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return account, nil
	}
	if err != nil {
		return nil, err
	}

	var shardAccount tlb.ShardAccount
	if err = tlb.LoadFromCell(&shardAccount, value); err != nil {
		return nil, err
	}
	loader, err := shardAccount.Account.BeginParse()
	if err != nil {
		return nil, err
	}
	var stateAccount tlb.AccountState
	if err = tlb.LoadFromCell(&stateAccount, loader); err != nil {
		return nil, err
	}
	account.Exists = stateAccount.IsValid
	account.LastTransactionLt = shardAccount.LastTransLT
	account.LastTransactionHash = bytes.Clone(shardAccount.LastTransHash)
	if stateAccount.IsValid {
		account.StateBoc = shardAccount.Account.ToBOC()
	}
	return account, nil
}

func masterStateExtra(state *tlb.ShardStateUnsplit) (*tlb.McStateExtra, error) {
	if state.McStateExtra == nil {
		return nil, fmt.Errorf("state is missing mc_state_extra")
	}
	loader, err := state.McStateExtra.BeginParse()
	if err != nil {
		return nil, err
	}
	var extra tlb.McStateExtra
	if err = tlb.LoadFromCell(&extra, loader); err != nil {
		return nil, err
	}
	return &extra, nil
}

func globalLibraries(state *tlb.ShardStateUnsplit) (*cell.Dictionary, error) {
	if state.Stats == nil {
		return nil, fmt.Errorf("state is missing shard state extras")
	}
	loader, err := state.Stats.BeginParse()
	if err != nil {
		return nil, err
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if err = skipCurrencyCollectionBoundary(loader); err != nil {
		return nil, err
	}
	if err = skipCurrencyCollectionBoundary(loader); err != nil {
		return nil, err
	}
	return loader.LoadDict(256)
}

func transactionAccountIDs(block *tlb.Block) ([][]byte, error) {
	if block.Extra == nil || block.Extra.ShardAccountBlocks == nil {
		return nil, fmt.Errorf("block has no shard account blocks")
	}
	accounts, err := block.Extra.ShardAccountBlocks.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("load shard account blocks: %w", err)
	}
	hasAccounts, err := accounts.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load shard account blocks root flag: %w", err)
	}
	if !hasAccounts {
		return nil, nil
	}
	root, err := accounts.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load shard account blocks root: %w", err)
	}
	items, err := root.AsDict(256).LoadAll()
	if err != nil {
		return nil, fmt.Errorf("iterate shard account blocks: %w", err)
	}
	accountIDs := make([][]byte, 0, len(items))
	for _, item := range items {
		if err = skipCurrencyCollectionBoundary(item.Value); err != nil {
			return nil, fmt.Errorf("load account block fees: %w", err)
		}
		var accountBlock tlb.AccountBlock
		if err = tlb.LoadFromCell(&accountBlock, item.Value); err != nil {
			return nil, fmt.Errorf("load account block: %w", err)
		}
		if len(accountBlock.Addr) != 32 {
			return nil, fmt.Errorf("account address is %d bytes", len(accountBlock.Addr))
		}
		accountIDs = append(accountIDs, bytes.Clone(accountBlock.Addr))
	}
	return accountIDs, nil
}

func skipCurrencyCollectionBoundary(loader *cell.Slice) error {
	if _, err := loader.LoadBigCoins(); err != nil {
		return err
	}
	_, err := loader.LoadMaybeRef()
	return err
}

func accountKey(accountID []byte) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(accountID, 256).EndCell()
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
