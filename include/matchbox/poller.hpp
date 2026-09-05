#pragma once

namespace matchbox {

// Level-triggered readiness over kqueue (macOS/BSD) or epoll (Linux).
class Poller {
 public:
  struct Event {
    int fd;
    bool readable;
    bool writable;
    bool hangup;
  };

  Poller();
  ~Poller();
  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;

  void add(int fd);  // read interest
  void set_writable(int fd, bool on);
  void remove(int fd);
  // Returns number of events, 0 on timeout/EINTR, -1 on error.
  int wait(Event* out, int max_events, int timeout_ms);

 private:
  int fd_;
};

}  // namespace matchbox
