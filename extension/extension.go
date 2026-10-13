package extension

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

const maxMessageSize = 64 << 20

type Extension struct {
	listen    string
	store     *blockStore
	libraries *librariesCache
	log       zerolog.Logger

	listener net.Listener
	server   *grpc.Server
}

var _ hooks.Extension = (*Extension)(nil)

func New(node hooks.Node) (hooks.Extension, error) {
	listen := os.Getenv("GTON_GRPC_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:50051"
	}
	dir := os.Getenv("GTON_BLOCK_DIR")
	if dir == "" {
		dir = "./data/anton-blocks"
	}
	store, err := newBlockStore(dir)
	if err != nil {
		return nil, err
	}
	return &Extension{listen: listen, store: store, libraries: &librariesCache{}, log: node.Logger}, nil
}

func (e *Extension) Start(context.Context) error {
	listener, err := net.Listen("tcp", e.listen)
	if err != nil {
		return fmt.Errorf("listen for Anton on %s: %w", e.listen, err)
	}
	server := grpc.NewServer(grpc.MaxSendMsgSize(maxMessageSize))
	antonv1.RegisterBlockSourceServer(server, &sourceServer{store: e.store, libraries: e.libraries})
	e.listener = listener
	e.server = server
	go func() { _ = server.Serve(listener) }()
	return nil
}

func (e *Extension) Close(ctx context.Context) error {
	if e.server == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		e.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		e.server.Stop()
		return ctx.Err()
	}
}

func (e *Extension) OnBlockApplied(_ context.Context, event hooks.BlockAppliedEvent) error {
	// The hook is at-least-once and fires for catch-up blocks too. Store every
	// block the consumer has not acknowledged, whatever its age: the consumer
	// syncs from this store alone and must not have to fill gaps elsewhere.
	if isAcknowledged(event, e.store.acknowledged()) {
		return nil
	}
	envelope, err := appliedBlockEnvelope(event)
	if err != nil {
		block := "<unknown>"
		if event.Meta != nil {
			block = storage.FormatBlockRef(event.Meta.ID)
		}
		logEvent := e.log.Error().
			Err(err).
			Str("block", block)
		if envelope == nil {
			logEvent.Msg("failed to build applied block envelope; skipping extension event")
			return nil
		}
		logEvent.Msg("failed to enrich applied block; storing partial event")
	}
	e.libraries.prune(envelope)
	return e.store.put(envelope)
}

// librariesCache remembers the global library dictionary carried by the latest
// master block, so unchanged dictionaries are not repeated in every envelope
// and a consumer joining mid-stream can fetch the current one.
type librariesCache struct {
	mu   sync.Mutex
	hash []byte
	boc  []byte
}

// prune records the dictionary a master envelope carries and strips its body
// when the previous master already carried the same one.
func (c *librariesCache) prune(event *antonv1.AppliedBlock) {
	if len(event.GlobalLibrariesHash) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if bytes.Equal(event.GlobalLibrariesHash, c.hash) {
		event.GlobalLibrariesBoc = nil
		return
	}
	c.hash = bytes.Clone(event.GlobalLibrariesHash)
	c.boc = bytes.Clone(event.GlobalLibrariesBoc)
}

func (c *librariesCache) current() *antonv1.GlobalLibraries {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.hash) == 0 {
		return nil
	}
	return &antonv1.GlobalLibraries{Hash: bytes.Clone(c.hash), Boc: bytes.Clone(c.boc)}
}

// isAcknowledged reports whether the block belongs to a master the consumer
// has already committed.
func isAcknowledged(event hooks.BlockAppliedEvent, acked uint32) bool {
	if event.Meta == nil {
		return false
	}
	inclusion := event.Meta.ID.SeqNo
	if event.InclusionMasterRef != nil {
		inclusion = event.InclusionMasterRef.SeqNo
	}
	return inclusion < acked
}

func (*Extension) OnExternalMessage(context.Context, hooks.ExternalMessageEvent) error {
	return nil
}

func (*Extension) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}

type sourceServer struct {
	antonv1.UnimplementedBlockSourceServer
	store     *blockStore
	libraries *librariesCache
}

var _ antonv1.BlockSourceServer = (*sourceServer)(nil)

func (s *sourceServer) LookupMaster(_ context.Context, request *antonv1.MasterRequest) (*antonv1.BlockId, error) {
	event := s.store.master(request.Seqno)
	if event == nil {
		return nil, status.Error(codes.NotFound, "master block is not available")
	}
	return event.Block, nil
}

func (s *sourceServer) WaitMaster(ctx context.Context, request *antonv1.MasterRequest) (*antonv1.BlockId, error) {
	event, err := s.store.waitMaster(ctx, request.Seqno)
	if err != nil {
		return nil, err
	}
	return event.Block, nil
}

func (s *sourceServer) SubscribeAppliedBlocks(request *antonv1.MasterRequest, stream grpc.ServerStreamingServer[antonv1.AppliedBlock]) error {
	return s.store.subscribe(stream.Context(), request.Seqno, stream.Send)
}

func (s *sourceServer) GetBlock(ctx context.Context, request *antonv1.BlockRequest) (*antonv1.AppliedBlock, error) {
	if request == nil || validateBlockID(request.Block) != nil {
		return nil, status.Error(codes.InvalidArgument, "valid block id is required")
	}
	return s.store.waitBlock(ctx, request.Block)
}

func (s *sourceServer) CurrentMaster(context.Context, *antonv1.Empty) (*antonv1.BlockId, error) {
	event := s.store.latestMaster()
	if event == nil {
		return nil, status.Error(codes.NotFound, "master block is not available")
	}
	return event.Block, nil
}

func (s *sourceServer) ProcessedMaster(_ context.Context, request *antonv1.ProcessedMasterRequest) (*antonv1.Empty, error) {
	if err := s.store.processedMaster(request.Seqno); err != nil {
		return nil, status.Errorf(codes.Internal, "release processed blocks: %v", err)
	}
	return &antonv1.Empty{}, nil
}

func (s *sourceServer) GetGlobalLibraries(context.Context, *antonv1.Empty) (*antonv1.GlobalLibraries, error) {
	libraries := s.libraries.current()
	if libraries == nil {
		return nil, status.Error(codes.NotFound, "global libraries are not available")
	}
	return libraries, nil
}
