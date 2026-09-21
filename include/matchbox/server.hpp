#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "matchbox/engine.hpp"
#include "matchbox/net.hpp"
#include "matchbox/poller.hpp"
#include "matchbox/publisher.hpp"
#include "matchbox/spsc_queue.hpp"

namespace matchbox {

struct Session : Conn {
  uint32_t id = 0;
  uint32_t expect_seq = 1;
  bool dirty = false;
};

// Single-threaded TCP gateway: decodes client frames into Commands for the
// engine ring, turns engine Events back into frames, and runs the market-data
// publisher on a second port. Port 0 picks an ephemeral port (see port()).
class Server {
 public:
  struct Config {
    uint16_t port = 9001;
    uint16_t md_port = 9002;
    size_t max_backlog = 8u << 20;  // per order session
    Publisher::Config md;           // market-data stream
  };

  Server(Config cfg, SpscQueue<Command>& to_engine, SpscQueue<Event>& from_engine);
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  uint16_t port() const { return port_; }
  uint16_t md_port() const { return md_port_; }
  void run(const std::atomic<bool>& stop);  // blocks until stop is set

 private:
  enum class Kind : uint8_t { None, Listen, MdListen, Session, Subscriber };

  void accept_all(int lfd, Kind kind);
  void track(int fd, Kind kind, uint32_t sid);
  Session* session_by_fd(int fd);
  void read_session(Session& s);
  bool parse_frames(Session& s);  // false => close the session
  void submit(const Command& c);
  bool drain_engine();
  void on_event(const Event& e);
  void flush_dirty();
  void close_session(Session& s);

  template <class M>
  void send(Session& s, const M& m) {
    s.queue(m);
    if (!s.dirty) {
      s.dirty = true;
      dirty_.push_back(s.id);
    }
  }

  Config cfg_;
  SpscQueue<Command>& to_engine_;
  SpscQueue<Event>& from_engine_;
  Poller poller_;
  Publisher publisher_;
  int listen_fd_ = -1;
  int md_listen_fd_ = -1;
  uint16_t port_ = 0;
  uint16_t md_port_ = 0;
  std::unordered_map<uint32_t, std::unique_ptr<Session>> sessions_;
  std::vector<Kind> kind_by_fd_;
  std::vector<uint32_t> sid_by_fd_;
  std::vector<uint32_t> dirty_;
  uint32_t next_sid_ = 1;
};

}  // namespace matchbox
