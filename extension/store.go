package extension

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

type storedBlock struct {
	event *antonv1.AppliedBlock
	path  string
}

type blockStore struct {
	dir string

	storeMu sync.Mutex
	mu      sync.RWMutex
	blocks  map[string]*storedBlock
	masters map[uint32]*storedBlock
	wake    chan struct{}
}

func newBlockStore(dir string) (*blockStore, error) {
	if dir == "" {
		return nil, errors.New("block store directory is required")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create block store directory: %w", err)
	}
	store := &blockStore{
		dir:     dir,
		blocks:  make(map[string]*storedBlock),
		masters: make(map[uint32]*storedBlock),
		wake:    make(chan struct{}),
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *blockStore) put(event *antonv1.AppliedBlock) error {
	if err := validateEnvelope(event); err != nil {
		return err
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(event)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(event.EventId))
	path := filepath.Join(s.dir, fmt.Sprintf("%x.pb", sum[:]))

	s.storeMu.Lock()
	defer s.storeMu.Unlock()

	if existing, readErr := os.ReadFile(path); readErr == nil {
		stored := new(antonv1.AppliedBlock)
		if err = proto.Unmarshal(existing, stored); err != nil {
			return fmt.Errorf("decode existing block event: %w", err)
		}
		if blockKey(stored.Block) != blockKey(event.Block) {
			return errors.New("event id collision")
		}
		s.add(&storedBlock{event: stored, path: path})
		return nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}

	temp, err := os.CreateTemp(s.dir, ".incoming-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)

	if err = temp.Chmod(0o640); err == nil {
		_, err = temp.Write(data)
	}
	if err == nil {
		err = temp.Sync()
	}
	closeErr := temp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(tempPath, path); err != nil {
		return err
	}
	if err = syncDir(s.dir); err != nil {
		return err
	}

	s.add(&storedBlock{event: proto.Clone(event).(*antonv1.AppliedBlock), path: path})
	return nil
}

func (s *blockStore) waitBlock(ctx context.Context, id *antonv1.BlockId) (*antonv1.AppliedBlock, error) {
	key := blockKey(id)
	for {
		s.mu.RLock()
		item := s.blocks[key]
		wake := s.wake
		s.mu.RUnlock()
		if item != nil {
			return proto.Clone(item.event).(*antonv1.AppliedBlock), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wake:
		}
	}
}

func (s *blockStore) master(seqNo uint32) *antonv1.AppliedBlock {
	s.mu.RLock()
	item := s.masters[seqNo]
	s.mu.RUnlock()
	if item == nil {
		return nil
	}
	return proto.Clone(item.event).(*antonv1.AppliedBlock)
}

func (s *blockStore) waitMaster(ctx context.Context, seqNo uint32) (*antonv1.AppliedBlock, error) {
	for {
		s.mu.RLock()
		item := s.masters[seqNo]
		wake := s.wake
		s.mu.RUnlock()
		if item != nil {
			return proto.Clone(item.event).(*antonv1.AppliedBlock), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wake:
		}
	}
}

func (s *blockStore) subscribe(ctx context.Context, seqNo uint32, send func(*antonv1.AppliedBlock) error) error {
	sent := make(map[string]struct{})
	for {
		events, wake := s.eventsFrom(seqNo)
		retained := make(map[string]struct{}, len(events))
		for _, event := range events {
			key := blockKey(event.Block)
			if _, ok := sent[key]; !ok {
				if err := send(event); err != nil {
					return err
				}
			}
			retained[key] = struct{}{}
		}
		sent = retained

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

func (s *blockStore) eventsFrom(seqNo uint32) ([]*antonv1.AppliedBlock, <-chan struct{}) {
	s.mu.RLock()
	events := make([]*antonv1.AppliedBlock, 0, len(s.blocks))
	for _, item := range s.blocks {
		if inclusionSeqNo(item.event) >= seqNo {
			events = append(events, proto.Clone(item.event).(*antonv1.AppliedBlock))
		}
	}
	wake := s.wake
	s.mu.RUnlock()

	sort.Slice(events, func(i, j int) bool {
		iSeq, jSeq := inclusionSeqNo(events[i]), inclusionSeqNo(events[j])
		if iSeq != jSeq {
			return iSeq < jSeq
		}
		iMaster, jMaster := isMaster(events[i].Block), isMaster(events[j].Block)
		if iMaster != jMaster {
			return !iMaster
		}
		return blockKey(events[i].Block) < blockKey(events[j].Block)
	})
	return events, wake
}

func (s *blockStore) latestMaster() *antonv1.AppliedBlock {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var latest *storedBlock
	for _, item := range s.masters {
		if latest == nil || item.event.Block.Seqno > latest.event.Block.Seqno {
			latest = item
		}
	}
	if latest == nil {
		return nil
	}
	return proto.Clone(latest.event).(*antonv1.AppliedBlock)
}

func (s *blockStore) processedMaster(seqNo uint32) error {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()

	s.mu.RLock()
	items := make([]*storedBlock, 0)
	for _, item := range s.blocks {
		if inclusionSeqNo(item.event) < seqNo {
			items = append(items, item)
		}
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].path < items[j].path })

	for _, item := range items {
		if err := os.Remove(item.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove processed block event: %w", err)
		}
		s.remove(item)
	}
	return syncDir(s.dir)
}

func (s *blockStore) load() error {
	paths, err := filepath.Glob(filepath.Join(s.dir, "*.pb"))
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read block event %s: %w", path, err)
		}
		event := new(antonv1.AppliedBlock)
		if err = proto.Unmarshal(data, event); err != nil {
			return fmt.Errorf("decode block event %s: %w", path, err)
		}
		if err = validateEnvelope(event); err != nil {
			return fmt.Errorf("validate block event %s: %w", path, err)
		}
		s.add(&storedBlock{event: event, path: path})
	}
	return nil
}

func (s *blockStore) add(item *storedBlock) {
	s.mu.Lock()
	s.blocks[blockKey(item.event.Block)] = item
	if isMaster(item.event.Block) {
		s.masters[item.event.Block.Seqno] = item
	}
	close(s.wake)
	s.wake = make(chan struct{})
	s.mu.Unlock()
}

func (s *blockStore) remove(item *storedBlock) {
	s.mu.Lock()
	key := blockKey(item.event.Block)
	if current := s.blocks[key]; current != nil && current.path == item.path {
		delete(s.blocks, key)
	}
	if isMaster(item.event.Block) {
		if current := s.masters[item.event.Block.Seqno]; current != nil && current.path == item.path {
			delete(s.masters, item.event.Block.Seqno)
		}
	}
	s.mu.Unlock()
}

func validateEnvelope(event *antonv1.AppliedBlock) error {
	if event == nil || event.Block == nil {
		return errors.New("block event is required")
	}
	if event.EventId == "" {
		return errors.New("event id is required")
	}
	if err := validateBlockID(event.Block); err != nil {
		return err
	}
	if len(event.BlockBoc) == 0 {
		return errors.New("block BOC is required")
	}
	return nil
}

func validateBlockID(id *antonv1.BlockId) error {
	if id == nil {
		return errors.New("block id is required")
	}
	if len(id.RootHash) != 32 || len(id.FileHash) != 32 {
		return errors.New("block hashes must be 32 bytes")
	}
	return nil
}

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

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
