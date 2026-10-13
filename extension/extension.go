package extension

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

const (
	defaultNATSListen     = "127.0.0.1:4222"
	defaultNATSDir        = "./data/anton-nats"
	defaultBlockRetention = 7 * 24 * time.Hour
)

type Extension struct {
	cfg       streamConfig
	stream    *blockStream
	libraries *librariesCache
	log       zerolog.Logger
}

var _ hooks.Extension = (*Extension)(nil)

func New(node hooks.Node) (hooks.Extension, error) {
	cfg, err := streamConfigFromEnv()
	if err != nil {
		return nil, err
	}
	cfg.log = node.Logger
	return &Extension{cfg: cfg, libraries: &librariesCache{}, log: node.Logger}, nil
}

func streamConfigFromEnv() (streamConfig, error) {
	listen := os.Getenv("GTON_NATS_LISTEN")
	if listen == "" {
		listen = defaultNATSListen
	}
	host, portText, err := net.SplitHostPort(listen)
	if err != nil {
		return streamConfig{}, fmt.Errorf("GTON_NATS_LISTEN: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return streamConfig{}, fmt.Errorf("GTON_NATS_LISTEN port: %w", err)
	}
	dir := os.Getenv("GTON_NATS_DIR")
	if dir == "" {
		dir = defaultNATSDir
	}
	retention := defaultBlockRetention
	if raw := os.Getenv("GTON_BLOCK_RETENTION"); raw != "" {
		if retention, err = time.ParseDuration(raw); err != nil {
			return streamConfig{}, fmt.Errorf("GTON_BLOCK_RETENTION: %w", err)
		}
		if retention <= 0 {
			return streamConfig{}, fmt.Errorf("GTON_BLOCK_RETENTION must be positive, got %s", raw)
		}
	}
	return streamConfig{host: host, port: port, dir: dir, retention: retention}, nil
}

func (e *Extension) Start(ctx context.Context) error {
	stream, err := newBlockStream(ctx, e.cfg)
	if err != nil {
		return err
	}
	e.stream = stream
	return nil
}

func (e *Extension) Close(context.Context) error {
	if e.stream == nil {
		return nil
	}
	return e.stream.close()
}

func (e *Extension) OnBlockApplied(ctx context.Context, event hooks.BlockAppliedEvent) error {
	// The hook is at-least-once and fires for catch-up blocks too. Publish
	// every block of a master the stream does not hold yet, whatever its age:
	// consumers sync from the stream alone and must not fill gaps elsewhere.
	if isPublished(event, e.stream.publishedMaster()) {
		return nil
	}
	if event.Meta != nil && !isMasterBlock(event.Meta.ID) && event.InclusionMasterRef == nil {
		// Consumers index shards by the master that includes them; without
		// one the block cannot be placed in the stream.
		e.log.Warn().Str("block", storage.FormatBlockRef(event.Meta.ID)).
			Msg("applied shard block has no inclusion master; skipping extension event")
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
		logEvent.Msg("failed to enrich applied block; publishing partial event")
	}
	return e.publish(ctx, envelope)
}

// publish stores a changed library dictionary and the envelope. The
// dictionary is recorded only once both are stored, so a failed attempt is
// retried with the dictionary body.
func (e *Extension) publish(ctx context.Context, envelope *antonv1.AppliedBlock) error {
	librariesChanged := e.libraries.prune(envelope)
	if librariesChanged {
		if err := e.stream.putLibraries(ctx, &antonv1.GlobalLibraries{
			Hash: envelope.GlobalLibrariesHash,
			Boc:  envelope.GlobalLibrariesBoc,
		}); err != nil {
			return err
		}
	}
	if err := e.stream.publish(ctx, envelope); err != nil {
		return err
	}
	if librariesChanged {
		e.libraries.record(envelope)
	}
	return nil
}

// isPublished reports whether the block belongs to a master older than the
// latest one already in the stream. Repeats of the latest master itself are
// dropped by the stream's duplicate window.
func isPublished(event hooks.BlockAppliedEvent, published uint32) bool {
	if event.Meta == nil {
		return false
	}
	inclusion := event.Meta.ID.SeqNo
	if event.InclusionMasterRef != nil {
		inclusion = event.InclusionMasterRef.SeqNo
	}
	return inclusion < published
}

func (*Extension) OnExternalMessage(context.Context, hooks.ExternalMessageEvent) error {
	return nil
}

func (*Extension) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}

// librariesCache remembers the hash of the global library dictionary last
// stored in the libraries bucket, so unchanged dictionaries are not repeated in
// every envelope.
type librariesCache struct {
	mu   sync.Mutex
	hash []byte
}

// prune strips the dictionary body from a master envelope when that
// dictionary is already stored, and reports whether the envelope carries one
// that still has to be stored.
func (c *librariesCache) prune(event *antonv1.AppliedBlock) bool {
	if len(event.GlobalLibrariesHash) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if bytes.Equal(event.GlobalLibrariesHash, c.hash) {
		event.GlobalLibrariesBoc = nil
		return false
	}
	return len(event.GlobalLibrariesBoc) != 0
}

// record marks the envelope's dictionary as stored.
func (c *librariesCache) record(event *antonv1.AppliedBlock) {
	c.mu.Lock()
	c.hash = bytes.Clone(event.GlobalLibrariesHash)
	c.mu.Unlock()
}
