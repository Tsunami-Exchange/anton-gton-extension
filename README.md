# Anton gton extension

This repository builds a gton node binary with an Anton block-source
extension. The extension runs an embedded NATS JetStream server and publishes
every applied block to it before returning from the gton hook. Consumers read
the stream over the NATS protocol; the extension knows nothing about them.

The data flow:

1. `OnBlockApplied` copies the borrowed hook data into an `AppliedBlock`
   envelope and publishes it to the `GTON_BLOCKS` stream. Every block of a
   master the stream does not hold yet is published, catch-up blocks included:
   the hook is at-least-once and consumers sync from the stream alone, so a
   node restart must not leave gaps. Repeats of the latest master are dropped
   by the stream's duplicate window keyed on the event id.
2. Messages are published as `gton.blocks.<master seqno>.master` and
   `gton.blocks.<master seqno>.shard`, where the seqno is the master that
   includes the block. gton applies a master and then the shard blocks it
   includes, so master N comes first, followed by its shards, and master N is
   complete once `gton.blocks.N+1.master` arrives. A consumer that has
   committed master N resumes from the first message of `gton.blocks.N+1.>`.
   Every message carries the `Gton-Master-Seqno` header.
3. The stream keeps blocks for `GTON_BLOCK_RETENTION` regardless of who
   consumed them. Any number of consumers can read it with their own cursors.

The envelope contains the raw block and proof BOCs, block identity, inclusion
master, master shard list and all transaction-account states. A master event
also carries the hash of the global library dictionary, and its BOC when the
dictionary changed since the last one the extension stored (always for the
first master after a node restart). Every dictionary is stored in the
`GTON_LIBRARIES` key-value bucket under its hex hash, so a consumer replaying
an old master looks up that master's dictionary.

Writes are fsynced before the hook returns, and messages up to 64 MB are
accepted. A publish error is returned to gton, which retries the block flow,
so a full disk stalls the node until space is freed.

## Versions

The repositories intentionally have independent dependency graphs:

- Anton: Go 1.24, tonutils-go 1.13.
- This extension: Go 1.26, gton 1.0, gton's tonutils-go version.

## Build

    make build

## Configuration

- `GTON_NATS_LISTEN` — NATS listen address; defaults to `127.0.0.1:4222`.
- `GTON_NATS_DIR` — JetStream store directory; defaults to
  `./data/anton-nats`.
- `GTON_BLOCK_RETENTION` — how long blocks stay in the stream; a Go duration,
  defaults to `168h`.

If the consumer runs on another host, set `GTON_NATS_LISTEN=:4222` and keep
both on a trusted private network; the embedded server runs without TLS or
authentication.

The protobuf envelope is mirrored from Anton's api/anton/v1/ingest.proto.
