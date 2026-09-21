#pragma once
#include <cstddef>
#include <cstdint>
#include <unordered_map>

#include "matchbox/net.hpp"
#include "matchbox/poller.hpp"
#include "matchbox/replay_ring.hpp"

namespace matchbox {

struct Subscriber : Conn {
  bool subscribed = false;  // Subscribe frame seen
  uint32_t next_seq = 0;    // next stream seq to copy into tx
  uint64_t last_tx_ns = 0;  // for heartbeats
  uint64_t stalled_since_ns = 0;
};

// Fans market-data frames out to every subscriber through a shared replay
// ring. Each subscriber owns a cursor; a slow one is never buffered past
// max_backlog bytes, its cursor just waits for the socket to drain, and if
// the ring laps it the client gets a Gap and continues from the oldest
// retained frame. A reconnecting client resumes from any retained seq.
// Only a peer that is stalled for stall_timeout_ms (never reads) is dropped.
class Publisher {
 public:
  struct Config {
    size_t max_backlog = 1u << 20;  // bytes queued in user space per subscriber
    size_t ring_log2 = 16;          // frames retained for resume / catch-up
    uint64_t heartbeat_ms = 1000;
    uint64_t stall_timeout_ms = 10000;
  };

  Publisher(Poller& poller, Config cfg) : poller_(poller), cfg_(cfg), ring_(cfg.ring_log2) {}

  void subscribe(int fd);  // accepted socket, waiting for its Subscribe frame
  bool is_subscriber(int fd) const { return subs_.count(fd) != 0; }
  size_t count() const { return subs_.size(); }
  uint32_t head() const { return ring_.head(); }
  uint32_t oldest() const { return ring_.oldest(); }
  const ReplayRing& ring() const { return ring_; }

  template <class M>
  void publish(const M& m) {
    ring_.append(m);
    dirty_ = true;
  }

  // Advances cursors, emits heartbeats and flushes sockets. Call every loop.
  void flush_all(uint64_t now_ns);
  void on_writable(int fd);
  void on_readable(int fd);  // parses the Subscribe frame; anything else drops

 private:
  void drop(int fd);
  bool pump(Subscriber& s, uint64_t now_ns);  // false => drop
  template <class M>
  void control(Subscriber& s, const M& m, uint64_t now_ns) {
    wire::append(s.tx, m);
    s.last_tx_ns = now_ns;
  }

  Poller& poller_;
  Config cfg_;
  ReplayRing ring_;
  std::unordered_map<int, Subscriber> subs_;
  bool dirty_ = false;
  uint64_t next_tick_ns_ = 0;
};

}  // namespace matchbox
