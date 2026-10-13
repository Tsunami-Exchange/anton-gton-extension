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
	e.libraries.prune(envelope)
	if len(envelope.GlobalLibrariesBoc) != 0 {
		if err = e.stream.putLibraries(ctx, &antonv1.GlobalLibraries{
			Hash: envelope.GlobalLibrariesHash,
			Boc:  envelope.GlobalLibrariesBoc,
		}); err != nil {
			return err
		}
	}
	return e.stream.publish(ctx, envelope)
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
