#include "matchbox/publisher.hpp"

#include <unistd.h>
#include <vector>

namespace matchbox {

void Publisher::subscribe(int fd) {
  Conn c;
  c.fd = fd;
  subs_.emplace(fd, std::move(c));
  poller_.add(fd);
}

void Publisher::drop(int fd) {
  poller_.remove(fd);
  ::close(fd);
  subs_.erase(fd);
}

void Publisher::flush_all() {
  if (!dirty_) return;
  dirty_ = false;
  std::vector<int> dead;
  for (auto& kv : subs_) {
    Conn& c = kv.second;
    if (!flush_and_arm(c, poller_) || c.backlog() > max_backlog_) dead.push_back(kv.first);
  }
  for (int fd : dead) drop(fd);
}

void Publisher::on_writable(int fd) {
  auto it = subs_.find(fd);
  if (it != subs_.end() && !flush_and_arm(it->second, poller_)) drop(fd);
}

void Publisher::on_readable(int fd) {
  if (subs_.count(fd)) drop(fd);
}

}  // namespace matchbox
