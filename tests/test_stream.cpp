// Market-data stream: replay ring, resume from a sequence, gap on lapped
// cursor, bounded backlog for slow consumers, heartbeats, stall drop. Drives
// the Publisher directly over a socketpair so every step is deterministic.
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include "doctest.h"
#include "matchbox/net.hpp"
#include "matchbox/publisher.hpp"
#include "matchbox/replay_ring.hpp"

using namespace matchbox;

namespace {

constexpr uint64_t kMs = 1000000;

struct Peer {
  int server = -1, client = -1;
  Peer() {
    int sv[2];
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
    server = sv[0];
    client = sv[1];
    // Small kernel buffers so backpressure shows up after a few hundred frames.
    int sz = 4096;
    ::setsockopt(server, SOL_SOCKET, SO_SNDBUF, &sz, sizeof sz);
    ::setsockopt(client, SOL_SOCKET, SO_RCVBUF, &sz, sizeof sz);
    set_nonblocking(server);
    set_nonblocking(client);
  }
  ~Peer() { ::close(client); }  // server end is owned by the Publisher
};

struct Frame {
  wire::Header hdr;
  uint8_t bytes[wire::kMaxFrame];
};

// Reads everything currently available from fd into frames.
std::vector<Frame> drain(int fd, std::vector<uint8_t>& rx) {
  uint8_t buf[1 << 16];
  for (;;) {
    const ssize_t n = ::read(fd, buf, sizeof buf);
    if (n <= 0) break;
    rx.insert(rx.end(), buf, buf + n);
  }
  std::vector<Frame> out;
  size_t off = 0;
  while (rx.size() - off >= sizeof(wire::Header)) {
    Frame f;
    f.hdr = wire::read_header(rx.data() + off);
    if (rx.size() - off < f.hdr.len) break;
    std::memcpy(f.bytes, rx.data() + off, f.hdr.len);
    out.push_back(f);
    off += f.hdr.len;
  }
  rx.erase(rx.begin(), rx.begin() + static_cast<std::ptrdiff_t>(off));
  return out;
}

void subscribe(Publisher& pub, const Peer& p, uint64_t from) {
  auto s = wire::make<wire::Subscribe>();
  s.from_seq = from;
  REQUIRE(write_full(p.client, &s, sizeof s));
  pub.subscribe(p.server);
  pub.on_readable(p.server);
}

wire::Top top(uint32_t bid_qty) {
  auto m = wire::make<wire::Top>();
  m.bid = 100;
  m.bid_qty = bid_qty;
  return m;
}

// Pump until the client has read everything the publisher will give it,
// playing the Poller's part: after the client drains the socket, report the
// server end writable so the cursor refills from the ring.
std::vector<Frame> stream_all(Publisher& pub, const Peer& p, std::vector<uint8_t>& rx, uint64_t now) {
  std::vector<Frame> all;
  for (int i = 0; i < 10000; ++i) {
    pub.flush_all(now);
    auto got = drain(p.client, rx);
    if (got.empty()) break;
    all.insert(all.end(), got.begin(), got.end());
    pub.on_writable(p.server);
  }
  return all;
}

bool contiguous(const std::vector<Frame>& fs, size_t from, size_t to) {
  for (size_t i = from; i + 1 < to; ++i)
    if (fs[i + 1].hdr.seq != fs[i].hdr.seq + 1) return false;
  return true;
}

}  // namespace

TEST_CASE("replay ring keeps the newest capacity frames and stamps stream seq") {
  ReplayRing ring(3);  // 8 frames
  CHECK(ring.head() == 0);
  CHECK_FALSE(ring.holds(1));
  for (uint32_t i = 1; i <= 20; ++i) CHECK(ring.append(top(i)) == i);
  CHECK(ring.head() == 20);
  CHECK(ring.oldest() == 13);
  CHECK_FALSE(ring.holds(12));
  CHECK(ring.holds(13));
  CHECK(ring.holds(20));
  size_t len;
  const auto t = wire::read<wire::Top>(ring.frame(17, &len));
  CHECK(len == sizeof(wire::Top));
  CHECK(t.hdr.seq == 17);
  CHECK(t.bid_qty == 17);
}

TEST_CASE("stream: live subscribe, resume from seq, gap when too old, heartbeat when idle") {
  Poller poller;
  Publisher::Config cfg;
  cfg.ring_log2 = 4;  // 16 frames
  cfg.max_backlog = 1 << 16;
  cfg.heartbeat_ms = 10;
  Publisher pub(poller, cfg);
  uint64_t now = 1000 * kMs;

  // Frames published before anyone subscribes are retained for late joiners.
  for (uint32_t i = 1; i <= 5; ++i) pub.publish(top(i));

  Peer live;
  std::vector<uint8_t> rx_live;
  subscribe(pub, live, 0);
  for (uint32_t i = 6; i <= 8; ++i) pub.publish(top(i));
  auto got = stream_all(pub, live, rx_live, now);
  REQUIRE(got.size() == 3);
  CHECK(got[0].hdr.seq == 6);  // live: nothing from before the subscribe
  CHECK(got[2].hdr.seq == 8);
  CHECK(contiguous(got, 0, got.size()));

  // Late joiner resumes from 4 and gets 4..8 exactly.
  Peer late;
  std::vector<uint8_t> rx_late;
  subscribe(pub, late, 4);
  got = stream_all(pub, late, rx_late, now);
  REQUIRE(got.size() == 5);
  CHECK(got[0].hdr.seq == 4);
  CHECK(got[4].hdr.seq == 8);
  CHECK(pub.count() == 2);

  // Push the ring past what a resume from 2 could satisfy: expect Gap then oldest..head.
  for (uint32_t i = 9; i <= 30; ++i) pub.publish(top(i));
  Peer old;
  std::vector<uint8_t> rx_old;
  subscribe(pub, old, 2);
  got = stream_all(pub, old, rx_old, now);
  REQUIRE(got.size() >= 2);
  CHECK(got[0].hdr.type == static_cast<uint8_t>(wire::MsgType::Gap));
  const auto gap = wire::read<wire::Gap>(got[0].bytes);
  CHECK(gap.from == 2);
  CHECK(gap.resumed_at == pub.oldest());
  CHECK(got[1].hdr.seq == pub.oldest());
  CHECK(got.back().hdr.seq == 30);
  CHECK(contiguous(got, 1, got.size()));

  // A client ahead of the head (server restarted) is told so and goes live.
  Peer ahead;
  std::vector<uint8_t> rx_ahead;
  subscribe(pub, ahead, 1000);
  pub.publish(top(31));
  got = stream_all(pub, ahead, rx_ahead, now);
  REQUIRE(got.size() == 2);
  CHECK(wire::read<wire::Gap>(got[0].bytes).resumed_at == 31);
  CHECK(got[1].hdr.seq == 31);

  // Idle: the live subscriber gets a heartbeat carrying the head seq, and only one per interval.
  drain(live.client, rx_live);
  now += 11 * kMs;
  pub.flush_all(now);
  got = drain(live.client, rx_live);
  REQUIRE(got.size() == 1);
  CHECK(got[0].hdr.type == static_cast<uint8_t>(wire::MsgType::Heartbeat));
  CHECK(wire::read<wire::Heartbeat>(got[0].bytes).head_seq == 31);
  now += 2 * kMs;
  pub.flush_all(now);
  CHECK(drain(live.client, rx_live).empty());

  // Client EOF drops the subscriber.
  ::close(late.client);
  late.client = -1;
  pub.on_readable(late.server);
  CHECK(pub.count() == 3);
}

TEST_CASE("stream: slow consumer keeps a bounded backlog, sees one Gap, then catches up; stalled peer is dropped") {
  Poller poller;
  Publisher::Config cfg;
  cfg.ring_log2 = 8;  // 256 frames
  cfg.max_backlog = 2048;
  cfg.heartbeat_ms = 1000;
  cfg.stall_timeout_ms = 50;
  Publisher pub(poller, cfg);
  uint64_t now = 1000 * kMs;

  Peer slow;
  std::vector<uint8_t> rx;
  subscribe(pub, slow, 0);

  // 20k frames (800 KB) while the client reads nothing: the socket fills,
  // then the user-space backlog caps at max_backlog and the ring laps the cursor.
  for (uint32_t i = 1; i <= 20000; ++i) {
    pub.publish(top(i));
    if (i % 100 == 0) pub.flush_all(now);
  }
  pub.flush_all(now);
  CHECK(pub.count() == 1);

  // Now read everything: a contiguous prefix, exactly one Gap, then contiguous through head.
  auto got = stream_all(pub, slow, rx, now);
  REQUIRE(got.size() > 300);
  size_t gap_at = got.size();
  for (size_t i = 0; i < got.size(); ++i)
    if (got[i].hdr.type == static_cast<uint8_t>(wire::MsgType::Gap)) {
      CHECK(gap_at == got.size());  // only one
      gap_at = i;
    }
  REQUIRE(gap_at < got.size());
  CHECK(gap_at > 100);  // socket + backlog held a few hundred frames before the lap
  CHECK(contiguous(got, 0, gap_at));
  const auto gap = wire::read<wire::Gap>(got[gap_at].bytes);
  CHECK(gap.from == got[gap_at - 1].hdr.seq + 1);
  CHECK(gap.resumed_at == got[gap_at + 1].hdr.seq);
  CHECK(got.back().hdr.seq == 20000);
  CHECK(contiguous(got, gap_at + 1, got.size()));
  // Delivered = prefix + the ring's contents at the moment it read; loss is bounded.
  CHECK(got.size() - 1 + (gap.resumed_at - gap.from) == 20000);

  // Fill it up again and never read: dropped once nothing has been accepted for stall_timeout.
  for (uint32_t i = 20001; i <= 40000; ++i) pub.publish(top(i));
  for (int tick = 0; tick < 3; ++tick) {  // socket fills on the first ticks, then no progress
    pub.flush_all(now);
    now += 30 * kMs;
  }
  CHECK(pub.count() == 1);
  now += 30 * kMs;
  pub.flush_all(now);
  CHECK(pub.count() == 0);
}
