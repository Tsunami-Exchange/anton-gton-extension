package extension

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	antonv1 "github.com/Tsunami-Exchange/anton-gton-extension/api/anton/v1"
)

func testStream(t *testing.T, dir string) *blockStream {
	t.Helper()
	stream, err := newBlockStream(context.Background(), streamConfig{
		host:      "127.0.0.1",
		port:      -1, // random free port
		dir:       dir,
		retention: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.close()) })
	return stream
}

func testStreamEvent(workchain int32, seqno uint32, inclusion *antonv1.BlockId) *antonv1.AppliedBlock {
	id := &antonv1.BlockId{
		Workchain: workchain,
		Shard:     -1 << 63,
		Seqno:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 1)}, 32),
	}
	return &antonv1.AppliedBlock{
		EventId:         blockKey(id),
		Block:           id,
		InclusionMaster: inclusion,
		BlockBoc:        []byte{1},
	}
}

func readStream(t *testing.T, stream *blockStream) jetstream.Stream {
	t.Helper()
	conn, err := nats.Connect(stream.clientURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	require.NoError(t, err)
	s, err := js.Stream(context.Background(), streamName)
	require.NoError(t, err)
	return s
}

func TestBlockStreamPublishesBlocksUnderTheirMasterSubject(t *testing.T) {
	stream := testStream(t, t.TempDir())
	master := testStreamEvent(-1, 100, nil)
	shard := testStreamEvent(0, 42, master.Block)

	// gton applies a master and then the shard blocks it includes.
	require.NoError(t, stream.publish(context.Background(), master))
	require.NoError(t, stream.publish(context.Background(), shard))

	s := readStream(t, stream)
	raw, err := s.GetLastMsgForSubject(context.Background(), "gton.blocks.100.master")
	require.NoError(t, err)
	got := new(antonv1.AppliedBlock)
	require.NoError(t, proto.Unmarshal(raw.Data, got))
	require.Equal(t, master.Block.RootHash, got.Block.RootHash)
	require.Equal(t, "100", raw.Header.Get(headerMasterSeqno))

	raw, err = s.GetLastMsgForSubject(context.Background(), "gton.blocks.100.shard")
	require.NoError(t, err)
	require.NoError(t, proto.Unmarshal(raw.Data, got))
	require.Equal(t, shard.Block.RootHash, got.Block.RootHash)
	require.Equal(t, "100", raw.Header.Get(headerMasterSeqno))
}

func TestBlockStreamPublishesEnvelopeLargerThanDefaultPayload(t *testing.T) {
	stream := testStream(t, t.TempDir())
	master := testStreamEvent(-1, 100, nil)
	master.BlockBoc = bytes.Repeat([]byte{1}, 2<<20)

	require.NoError(t, stream.publish(context.Background(), master))
}

func TestBlockStreamDropsRepeatedEvent(t *testing.T) {
	stream := testStream(t, t.TempDir())
	master := testStreamEvent(-1, 100, nil)

	require.NoError(t, stream.publish(context.Background(), master))
	require.NoError(t, stream.publish(context.Background(), master))

	info, err := readStream(t, stream).Info(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), info.State.Msgs)
}

func TestBlockStreamRestoresPublishedMasterAfterRestart(t *testing.T) {
	dir := t.TempDir()
	stream := testStream(t, dir)
	require.Zero(t, stream.publishedMaster(), "empty stream")
	require.NoError(t, stream.publish(context.Background(), testStreamEvent(-1, 100, nil)))
	require.Equal(t, uint32(100), stream.publishedMaster())
	require.NoError(t, stream.close())

	reopened, err := newBlockStream(context.Background(), streamConfig{host: "127.0.0.1", port: -1, dir: dir, retention: time.Hour})
	require.NoError(t, err)
	defer reopened.close()
	require.Equal(t, uint32(100), reopened.publishedMaster(), "watermark comes from the stored stream")
}

func TestBlockStreamStoresLibrariesByHash(t *testing.T) {
	stream := testStream(t, t.TempDir())
	older := &antonv1.GlobalLibraries{Hash: bytes.Repeat([]byte{7}, 32), Boc: []byte{1}}
	newer := &antonv1.GlobalLibraries{Hash: bytes.Repeat([]byte{8}, 32), Boc: []byte{2}}
	require.NoError(t, stream.putLibraries(context.Background(), older))
	require.NoError(t, stream.putLibraries(context.Background(), newer))

	conn, err := nats.Connect(stream.clientURL())
	require.NoError(t, err)
	defer conn.Close()
	js, err := jetstream.New(conn)
	require.NoError(t, err)
	kv, err := js.KeyValue(context.Background(), librariesBucket)
	require.NoError(t, err)
	// A consumer replaying an old master looks up that master's dictionary,
	// not the newest one.
	entry, err := kv.Get(context.Background(), hex.EncodeToString(older.Hash))
	require.NoError(t, err)
	got := new(antonv1.GlobalLibraries)
	require.NoError(t, proto.Unmarshal(entry.Value(), got))
	require.Equal(t, older.Boc, got.Boc)
}

func TestBlockStreamOpensAfterEveryBlockExpired(t *testing.T) {
	dir := t.TempDir()
	stream := testStream(t, dir)
	require.NoError(t, stream.publish(context.Background(), testStreamEvent(-1, 100, nil)))
	// Expiry leaves the stream's last sequence in place with no messages.
	require.NoError(t, readStream(t, stream).Purge(context.Background()))
	require.NoError(t, stream.close())

	reopened, err := newBlockStream(context.Background(), streamConfig{host: "127.0.0.1", port: -1, dir: dir, retention: time.Hour})
	require.NoError(t, err)
	defer reopened.close()
	require.Zero(t, reopened.publishedMaster())
}

func TestBlockStreamOpensWithRetentionShorterThanDuplicateWindow(t *testing.T) {
	stream, err := newBlockStream(context.Background(), streamConfig{
		host: "127.0.0.1", port: -1, dir: t.TempDir(), retention: time.Minute,
	})
	require.NoError(t, err)
	require.NoError(t, stream.close())
}
