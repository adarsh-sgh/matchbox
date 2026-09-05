#pragma once
#include <cstddef>
#include <unordered_map>

#include "matchbox/net.hpp"
#include "matchbox/poller.hpp"

namespace matchbox {

// Fans market-data frames out to every subscriber. A subscriber that falls
// more than max_backlog bytes behind is dropped rather than slowing the gateway.
class Publisher {
 public:
  Publisher(Poller& poller, size_t max_backlog) : poller_(poller), max_backlog_(max_backlog) {}

  void subscribe(int fd);
  bool is_subscriber(int fd) const { return subs_.count(fd) != 0; }
  size_t count() const { return subs_.size(); }

  template <class M>
  void publish(const M& m) {
    for (auto& kv : subs_) kv.second.queue(m);
    dirty_ = dirty_ || !subs_.empty();
  }

  void flush_all();
  void on_writable(int fd);
  void on_readable(int fd);  // subscribers never send; data or EOF drops them

 private:
  void drop(int fd);

  Poller& poller_;
  size_t max_backlog_;
  std::unordered_map<int, Conn> subs_;
  bool dirty_ = false;
};

}  // namespace matchbox
