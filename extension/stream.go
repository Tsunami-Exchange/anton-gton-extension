package extension

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

const (
	streamName    = "GTON_BLOCKS"
	subjectPrefix = "gton.blocks"
	// headerMasterSeqno carries the master seqno a message belongs to, so a
	// consumer can position itself without decoding payloads.
	headerMasterSeqno = "Gton-Master-Seqno"
	// librariesBucket holds every global library dictionary a master carried,
	// keyed by the hex hash the master envelopes refer to.
	librariesBucket = "GTON_LIBRARIES"

	subjectKindMaster = "master"
	subjectKindShard  = "shard"

	// duplicateWindow is how long JetStream remembers event ids to drop the
	// repeats of the at-least-once applied hook; JetStream caps it at the
	// retention.
	duplicateWindow = 10 * time.Minute
	startupTimeout  = 30 * time.Second
	// maxPayload fits a master envelope with the full library dictionary and
	// shard envelopes with many account states; the NATS default is 1 MB.
	maxPayload = 64 << 20
)

type streamConfig struct {
	host      string
	port      int
	dir       string
	retention time.Duration
	log       zerolog.Logger
}

// blockStream is an embedded JetStream server holding every applied block
// for the retention period, independent of who consumes it.
type blockStream struct {
	server *server.Server
	conn   *nats.Conn
	js     jetstream.JetStream
	kv     jetstream.KeyValue

	mu sync.Mutex
	// published is the highest master seqno in the stream.
	published uint32
}

func newBlockStream(ctx context.Context, cfg streamConfig) (*blockStream, error) {
	srv, err := server.NewServer(&server.Options{
		Host:       cfg.host,
		Port:       cfg.port,
		JetStream:  true,
		StoreDir:   cfg.dir,
		MaxPayload: maxPayload,
		// The hook returns once a block is published and gton then
		// checkpoints past it, so an acknowledged block must survive a crash.
		SyncAlways: true,
		NoSigs:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("configure embedded nats: %w", err)
	}
	srv.SetLoggerV2(natsLogger{log: cfg.log.With().Str("component", "anton-nats").Logger()}, false, false, false)
	go srv.Start()
	if !srv.ReadyForConnections(startupTimeout) {
		srv.Shutdown()
		return nil, errors.New("embedded nats did not start")
	}
	stream := &blockStream{server: srv}
	if err = stream.open(ctx, cfg.retention); err != nil {
		stream.close()
		return nil, err
	}
	return stream, nil
}

func (s *blockStream) open(ctx context.Context, retention time.Duration) error {
	conn, err := nats.Connect("", nats.InProcessServer(s.server))
	if err != nil {
		return fmt.Errorf("connect to embedded nats: %w", err)
	}
	s.conn = conn
	js, err := jetstream.New(conn)
	if err != nil {
		return err
	}
	s.js = js
	blocks, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       streamName,
		Subjects:   []string{subjectPrefix + ".>"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Discard:    jetstream.DiscardOld,
		MaxAge:     retention,
		Duplicates: min(duplicateWindow, retention),
	})
	if err != nil {
		return fmt.Errorf("create block stream: %w", err)
	}
	s.kv, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  librariesBucket,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("create libraries bucket: %w", err)
	}
	s.published, err = lastMasterSeqno(ctx, blocks)
	return err
}

// lastMasterSeqno returns the seqno of the newest master still in the stream,
// zero when there is none (empty, or every block expired).
func lastMasterSeqno(ctx context.Context, stream jetstream.Stream) (uint32, error) {
	last, err := stream.GetLastMsgForSubject(ctx, subjectPrefix+".*."+subjectKindMaster)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read last stored master: %w", err)
	}
	seqNo, err := strconv.ParseUint(last.Header.Get(headerMasterSeqno), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("last stored master has no seqno: %w", err)
	}
	return uint32(seqNo), nil
}

func (s *blockStream) publish(ctx context.Context, event *antonv1.AppliedBlock) error {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(event)
	if err != nil {
		return err
	}
	inclusion := inclusionSeqNo(event)
	msg := &nats.Msg{Subject: blockSubject(event), Data: data, Header: nats.Header{}}
	msg.Header.Set(jetstream.MsgIDHeader, event.EventId)
	msg.Header.Set(headerMasterSeqno, strconv.FormatUint(uint64(inclusion), 10))
	if _, err = s.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("publish block: %w", err)
	}
	if isMaster(event.Block) {
		s.mu.Lock()
		if inclusion > s.published {
			s.published = inclusion
		}
		s.mu.Unlock()
	}
	return nil
}

func (s *blockStream) publishedMaster() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.published
}

func (s *blockStream) putLibraries(ctx context.Context, libraries *antonv1.GlobalLibraries) error {
	data, err := proto.Marshal(libraries)
	if err != nil {
		return err
	}
	_, err = s.kv.Put(ctx, hex.EncodeToString(libraries.Hash), data)
	return err
}

func (s *blockStream) clientURL() string {
	return s.server.ClientURL()
}

func (s *blockStream) close() error {
	if s.conn != nil {
		s.conn.Close()
	}
	s.server.Shutdown()
	s.server.WaitForShutdown()
	return nil
}

func blockSubject(event *antonv1.AppliedBlock) string {
	kind := subjectKindShard
	if isMaster(event.Block) {
		kind = subjectKindMaster
	}
	return fmt.Sprintf("%s.%d.%s", subjectPrefix, inclusionSeqNo(event), kind)
}

// natsLogger routes the embedded server's log to the node log. Fatal messages
// are logged as errors: the server must not exit the node.
type natsLogger struct {
	log zerolog.Logger
}

func (l natsLogger) Noticef(format string, v ...any) { l.log.Info().Msgf(format, v...) }
func (l natsLogger) Warnf(format string, v ...any)   { l.log.Warn().Msgf(format, v...) }
func (l natsLogger) Fatalf(format string, v ...any)  { l.log.Error().Msgf(format, v...) }
func (l natsLogger) Errorf(format string, v ...any)  { l.log.Error().Msgf(format, v...) }
func (l natsLogger) Debugf(format string, v ...any)  { l.log.Debug().Msgf(format, v...) }
func (l natsLogger) Tracef(format string, v ...any)  { l.log.Trace().Msgf(format, v...) }
