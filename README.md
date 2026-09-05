# matchbox

A C++17 limit order book and matching engine behind a low-latency TCP gateway.
Price-time priority, add/cancel/replace, a packed binary wire protocol, market-data
fan-out, and a benchmark harness. No dependencies beyond a C++17 compiler and CMake.

```
 clients (binary frames over TCP)            md subscribers
        |                                          ^
        v                                          |
+-----------------------------------------------------------+
| gateway thread: kqueue/epoll Poller, Sessions, Publisher  |
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
./build/matchbox_client md                                    # print trades / top of book
python3 scripts/replay.py scripts/sample_orders.csv           # replay a CSV of orders
```

Numbers from an Apple M5 laptop, loopback, single client:

```
engine (no sockets):  10.1M ops/s   per-op p50 42ns  p99 167ns  p999 250ns
tcp, inflight=1:      33k orders/s  ack rtt p50 29us  p99 47us  p999 64us
tcp, inflight=32:     960k orders/s ack rtt p50 32us  p99 48us  p999 66us
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
- **Replace keeps the id.** Same price and no size increase shrinks in place and keeps
  queue position; anything else is unlink + re-match + re-insert at the back, which is
  what exchanges do.

Tests: a randomised differential test runs 30k mixed add/cancel/replace/IOC operations
against a `std::map` reference book and compares every fill and top-of-book, checking
list/bitmap invariants along the way; scenario tests pin price-time priority, IOC/market
semantics, replace priority rules and every reject reason; an end-to-end test drives two
TCP clients and a market-data subscriber through the real gateway on an ephemeral port.
CI runs the suite on gcc/Linux and clang/macOS plus an ASan+UBSan job.

## Next

- Cancel-on-disconnect and a per-session order index, so a dropped client does not leave
  resting orders behind.
- Self-trade prevention and FOK time-in-force.
- A second-level summary bitmap so a sparse book never scans more than two words.
- Pin the engine thread and swap `steady_clock` for `rdtsc`/`cntvct` timestamps.
