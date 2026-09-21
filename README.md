# matchbox

A C++17 limit order book and matching engine behind a low-latency TCP gateway.
Price-time priority, add/cancel/replace, a packed binary wire protocol, a resumable
market-data stream, and a benchmark harness. No dependencies beyond a C++17 compiler
and CMake.

```
 clients (binary frames over TCP)            md subscribers
        |                                          ^
        v                                          |
+-----------------------------------------------------------+
| gateway thread: kqueue/epoll Poller, Sessions,            |
|   Publisher = ReplayRing + one cursor per subscriber      |
+-----------------------------------------------------------+
        | SpscQueue<Command>              ^ SpscQueue<Event>
        v                                |
+-----------------------------------------------------------+
| engine thread: Engine -> Book[symbol] over one OrderPool  |
|   flat Level[price] arrays + intrusive FIFO per level     |
|   bitmap over prices to find the next best level          |
+-----------------------------------------------------------+
```

## Run

```
make build            # cmake configure + build (Release)
make test             # unit, differential and TCP end-to-end tests (doctest)
make asan             # same tests under AddressSanitizer + UBSan
make bench            # in-process engine benchmark

./build/matchbox_server --port 9001 --md-port 9002
./build/matchbox_client load --orders 100000 --inflight 1     # ack round-trip latency
./build/matchbox_client load --orders 500000 --inflight 32    # throughput
./build/matchbox_client md                                    # print trades / top of book, resume on drop
./build/matchbox_client md --stats --seconds 5                # fan-out latency + gap counts instead
./build/matchbox_client md --from 1234                        # resume from a stream sequence
python3 scripts/replay.py scripts/sample_orders.csv           # replay a CSV of orders
python3 scripts/mdstream.py --from 1234 --expect-no-loss      # same resumable client, in Python
```

Numbers from an Apple M5 laptop, loopback, single client:

```
engine (no sockets):  10.1M ops/s   per-op p50 42ns  p99 167ns  p999 250ns
tcp, inflight=1:      33k orders/s  ack rtt p50 29us  p99 47us  p999 64us
tcp, inflight=32:     960k orders/s ack rtt p50 32us  p99 48us  p999 66us
market data, 1 sub:   357k frames/s fan-out (engine ts -> client) p50 25us  p99 225us  p999 1.7ms, 0 lost
  + a slow subscriber sleeping 50ms per read alongside: fast one still 0 lost; slow one stays
    connected and gets 8 Gap frames instead of an unbounded buffer
```

## Design

- **No maps on the hot path.** Each book is two `vector<Level>` indexed by price tick;
  a level is an intrusive doubly-linked FIFO of pool slots. Order ids are
  `(generation << 32) | slot`, so cancel/replace is one array index plus a liveness
  check. When the best level empties, a 64-bit-per-word bitmap over prices finds the
  next one with `ctz`/`clz`.
- **One engine thread, one gateway thread, two SPSC rings.** The engine never touches a
  socket and the gateway never touches a book. Both rings are the classic wait-free
  ring with cached head/tail so the fast path stays on one cache line.
- **Poller abstraction is 100 lines.** `Poller` wraps kqueue on macOS and epoll on Linux
  with the same level-triggered `add / set_writable / remove / wait` surface; the server
  only toggles write interest when a socket has backlog.
- **Fixed-size packed frames, per-connection sequence numbers.** Every message is a
  `#pragma pack(1)` struct with a static-asserted size; a sequence gap gets a
  `Reject(bad_seq)` and a disconnect. Structs are memcpy'd in and out so nothing is
  read through a misaligned pointer.
- **Market data is a resumable stream, not a firehose.** Every Trade/Top frame is
  appended once to a `ReplayRing` (64k frames by default) and stamped with a global
  stream sequence; each subscriber is a cursor into that ring plus a bounded socket
  backlog. A client sends `Subscribe{from_seq}` first: 0 means live, any retained seq
  replays from there, and a seq the ring no longer holds gets a `Gap{from, resumed_at}`
  followed by the oldest retained frame. A slow reader is never buffered past
  `--md-backlog` bytes: its cursor waits for the socket to drain, and if the ring laps
  it, it gets one `Gap` and continues from the oldest frame. Idle streams carry a
  `Heartbeat{ts, head_seq}` every `--md-heartbeat-ms`, so a client can tell quiet from
  dead, and knows where to resume even if it never saw a data frame. Only a peer that
  accepts no bytes for `--md-stall-ms` is dropped. Both clients (`matchbox_client md`
  and `scripts/mdstream.py`) reconnect with backoff from their last seq on EOF or a
  missed heartbeat.
- **Replace keeps the id.** Same price and no size increase shrinks in place and keeps
  queue position; anything else is unlink + re-match + re-insert at the back, which is
  what exchanges do.

Tests: a randomised differential test runs 30k mixed add/cancel/replace/IOC operations
against a `std::map` reference book and compares every fill and top-of-book, checking
list/bitmap invariants along the way; scenario tests pin price-time priority, IOC/market
semantics, replace priority rules and every reject reason; stream tests drive the
`Publisher` over a `socketpair` with 4 KB kernel buffers to pin live/resume/gap/heartbeat
semantics and check that a subscriber that reads nothing for 20k frames keeps a bounded
backlog, sees exactly one `Gap`, then catches up to the head, and is dropped only after
the stall timeout; an end-to-end test drives two TCP clients and a market-data
subscriber (including a drop and a resume from its last seq) through the real gateway on
an ephemeral port. CI runs the suite on gcc/Linux and clang/macOS plus an ASan+UBSan job,
and runs the Python stream client against a live server under load with `--expect-no-loss`.

## Next

- Cancel-on-disconnect and a per-session order index, so a dropped client does not leave
  resting orders behind.
- Self-trade prevention and FOK time-in-force.
- A second-level summary bitmap so a sparse book never scans more than two words.
- Pin the engine thread and swap `steady_clock` for `rdtsc`/`cntvct` timestamps.
