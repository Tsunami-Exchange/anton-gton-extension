# Anton gton extension

This repository builds a gton node binary with an Anton block-source
extension. The extension writes every applied block to a local durable store
before returning from the gton hook and serves complete blocks to Anton over
gRPC.

The data flow is stream-first:

1. `OnBlockApplied` copies the borrowed hook data and fsyncs the block store.
2. The gRPC stream replays stored events and tails new full block envelopes.
3. Anton caches a full master-and-shard bundle before indexing it, so account
   states touched by the block do not require separate network calls.
4. A subscription that starts behind the live window is replayed from the
   node database first: masters from the requested seqno up to the oldest
   stored one, each preceded by its top shard blocks, then the stream tails
   the store. Blocks the node no longer holds end the replay early.
5. Missing or historical blocks fall back to the unary gRPC API, which also
   reads from the node database, and then to Anton's existing liteservers.
6. After committing a master block, Anton acknowledges the seqno. The store
   keeps that latest committed master bundle and removes older bundles.

The event contains the raw block and proof BOCs, block identity, inclusion
master, master shard list, all transaction-account states, blockchain config,
and global libraries. Anton keeps its liteclient as a fallback for history and
for related contracts not touched by the block.

## Versions

The repositories intentionally have independent dependency graphs:

- Anton: Go 1.24, tonutils-go 1.13.
- This extension: Go 1.26, gton 0.7, gton's tonutils-go version.

## Build

    make build

## Configuration

- `GTON_GRPC_LISTEN` — gRPC listen address; defaults to
  `127.0.0.1:50051`.
- `GTON_BLOCK_DIR` — durable block directory; defaults to
  `./data/anton-blocks`.

Start Anton with `GTON_SOURCE_ADDR=127.0.0.1:50051`. If Anton runs in another
container, explicitly set `GTON_GRPC_LISTEN=:50051` and keep both services on a
trusted private network; the transport does not provide TLS or authentication.

The protobuf contract is mirrored from Anton's api/anton/v1/ingest.proto.
