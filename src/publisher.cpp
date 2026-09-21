#include "matchbox/publisher.hpp"

#include <cerrno>
#include <unistd.h>
#include <vector>

namespace matchbox {

void Publisher::subscribe(int fd) {
  Subscriber s;
  s.fd = fd;
  subs_.emplace(fd, std::move(s));
  poller_.add(fd);
}

void Publisher::drop(int fd) {
  poller_.remove(fd);
  ::close(fd);
  subs_.erase(fd);
}

bool Publisher::pump(Subscriber& s, uint64_t now_ns) {
  if (s.last_tx_ns == 0) s.last_tx_ns = now_ns;  // heartbeat clock starts at the first pump
  if (s.subscribed) {
    // Only when the cursor can move: a stalled subscriber gets one Gap when it drains, not one per pump.
    if (s.backlog() < cfg_.max_backlog && s.next_seq < ring_.oldest()) {  // lapped, or resumed from too far back
      auto g = wire::make<wire::Gap>();
      g.from = s.next_seq;
      g.resumed_at = ring_.oldest();
      control(s, g, now_ns);
      s.next_seq = ring_.oldest();
    }
    while (s.next_seq <= ring_.head() && s.backlog() < cfg_.max_backlog) {
      size_t len;
      const uint8_t* p = ring_.frame(s.next_seq, &len);
      s.tx.insert(s.tx.end(), p, p + len);
      ++s.next_seq;
      s.last_tx_ns = now_ns;
    }
    if (now_ns - s.last_tx_ns >= cfg_.heartbeat_ms * 1000000 && !s.pending()) {
      auto hb = wire::make<wire::Heartbeat>();
      hb.ts_ns = now_ns;
      hb.head_seq = ring_.head();
      control(s, hb, now_ns);
    }
  }
  const size_t before = s.backlog();
  if (!flush_and_arm(s, poller_)) return false;
  // A peer with bytes pending that accepts none of them for stall_timeout is gone.
  if (s.pending() && s.backlog() == before) {
    if (s.stalled_since_ns == 0) s.stalled_since_ns = now_ns;
    if (now_ns - s.stalled_since_ns >= cfg_.stall_timeout_ms * 1000000) return false;
  } else {
    s.stalled_since_ns = 0;
  }
  return true;
}

void Publisher::flush_all(uint64_t now_ns) {
  // Heartbeats and stall checks only need millisecond granularity; skip the
  // walk over subscribers on quiet iterations.
  if (!dirty_ && now_ns < next_tick_ns_) return;
  dirty_ = false;
  next_tick_ns_ = now_ns + 1000000;
  std::vector<int> dead;
  for (auto& kv : subs_)
    if (!pump(kv.second, now_ns)) dead.push_back(kv.first);
  for (int fd : dead) drop(fd);
}

void Publisher::on_writable(int fd) {
  auto it = subs_.find(fd);
  if (it == subs_.end()) return;
  // Socket drained: refill from the ring right away rather than waiting for
  // the next publish, so a catching-up subscriber streams at socket speed.
  dirty_ = true;
  if (!flush_and_arm(it->second, poller_)) drop(fd);
}

void Publisher::on_readable(int fd) {
  auto it = subs_.find(fd);
  if (it == subs_.end()) return;
  Subscriber& s = it->second;
  uint8_t buf[256];
  for (;;) {
    const ssize_t n = ::read(fd, buf, sizeof buf);
    if (n > 0) {
      s.rx.insert(s.rx.end(), buf, buf + n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    drop(fd);  // EOF or error
    return;
  }
  if (s.rx.size() < sizeof(wire::Subscribe)) return;
  const wire::Header h = wire::read_header(s.rx.data());
  // Exactly one Subscribe per connection; anything else is a protocol error.
  if (s.subscribed || h.version != wire::kVersion || h.len != sizeof(wire::Subscribe) ||
      static_cast<wire::MsgType>(h.type) != wire::MsgType::Subscribe || s.rx.size() > sizeof(wire::Subscribe)) {
    drop(fd);
    return;
  }
  const auto m = wire::read<wire::Subscribe>(s.rx.data());
  s.rx.clear();
  s.subscribed = true;
  const uint32_t head = ring_.head();
  const uint64_t from = m.from_seq;
  if (from == 0 || from > head + 1) {
    // Live from now. A client ahead of our head has a stale stream (server
    // restarted), so tell it its old frames are gone.
    if (from > head + 1) {
      auto g = wire::make<wire::Gap>();
      g.from = from;
      g.resumed_at = head + 1;
      control(s, g, 0);
    }
    s.next_seq = head + 1;
  } else {
    s.next_seq = static_cast<uint32_t>(from);  // pump() emits a Gap if this is too old
  }
  dirty_ = true;
}

}  // namespace matchbox
