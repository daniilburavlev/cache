# Rust Cache

`cache` is almost one-to-one [tokio mini-redis](https://github.com/tokio-rs/mini-redis) rewritten for learning purpose.

It is an in-memory key/value store with pub/sub, spoken over TCP using the RESP wire
protocol — so any Redis client (e.g. `redis-cli`) can talk to it.

## Running

The repository provides a server:

```bash
RUST_LOG=debug cargo run --release
```

By default it listens on `127.0.0.1:6789`. Override the port with `--port`:

```bash
cargo run --release -- --port 6379
```

Logging verbosity is controlled by the `RUST_LOG` environment variable (`error`, `info`,
`debug`, ...).

## Commands

| Command | Form | Response |
| --- | --- | --- |
| `GET` | `GET key` | value, or nil (`$-1`) if absent |
| `SET` | `SET key value [EX secs \| PX millis]` | `+OK` |
| `DEL` | `DEL key` | `+OK` |
| `PING` | `PING [message]` | `+PONG`, or the message echoed back |
| `PUBLISH` | `PUBLISH channel message` | integer count of subscribers reached |
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` | a confirmation per channel, then `message` frames as they arrive |
| `UNSUBSCRIBE` | `UNSUBSCRIBE [channel ...]` | a confirmation per channel (no args = all channels) |

`SET` supports an optional expiry: `EX` in seconds or `PX` in milliseconds. Expired keys
are evicted by a background task. `SUBSCRIBE` puts the connection into subscriber mode,
where it can keep subscribing/unsubscribing until it disconnects.

## Architecture

Request flow: a TCP listener accepts a connection, hands it to a per-connection handler
that reads RESP frames, parses them into a `Command`, and applies the command against the
shared store.

```
main → server::run → Handler (one task per connection)
                        │
                        ├── Connection      RESP framing over the socket
                        ├── parse::Command  dispatch by command name
                        ├── cmd/*           per-command logic
                        └── storage::Db     shared in-memory state
```

- **`server.rs`** — the accept loop. Concurrency is capped by a semaphore
  (`MAX_CONNECTIONS = 256`) and each connection runs in its own Tokio task. Graceful
  shutdown fans a signal out to every connection over a `broadcast` channel, then waits
  for all handlers to finish via an `mpsc` channel whose senders drop as tasks end.

- **`connection.rs`** — reads RESP frames from a buffered socket and writes `Entity`
  values back out. It buffers bytes until a full frame is available.

- **`storage/entity.rs`** — `Entity` is the core type: it is simultaneously the RESP
  frame representation, the stored value, and the map key. `check` verifies a frame is
  fully buffered; `parse` then decodes it.

- **`parse.rs`** — `Command` dispatches by (lowercased) command name; `Parse` is a cursor
  that each command uses to pull its arguments off the frame.

- **`cmd/*.rs`** — one module per command. Each defines a struct built by `parse_frames`
  and an async `apply` that touches the store and writes a response.

- **`storage.rs`** — `Db` is a cheap-to-clone handle over shared state behind a mutex:
  the key/value map, pub/sub channels (`broadcast` senders), and a time-ordered set of
  expirations. A background task evicts keys as they expire.

## Testing

```bash
cargo test                       # all tests
cargo test key_value_get_set_del # a single test by name
```

Integration tests spin up a real in-process server and drive it with raw RESP bytes
(see `server.rs`); unit tests for frame parsing live in `storage/entity.rs`.