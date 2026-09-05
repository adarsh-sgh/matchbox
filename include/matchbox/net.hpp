#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>

#include "matchbox/poller.hpp"
#include "matchbox/protocol.hpp"

namespace matchbox {

int listen_tcp(uint16_t port, uint16_t* bound_port = nullptr);  // non-blocking; -1 on error
int connect_tcp(const char* host, uint16_t port);               // blocking, TCP_NODELAY; -1 on error
void set_nonblocking(int fd);
void set_nodelay(int fd);
bool read_full(int fd, void* buf, size_t n);
bool write_full(int fd, const void* buf, size_t n);

// One non-blocking connection with buffered output and per-direction sequence numbers.
struct Conn {
  int fd = -1;
  std::vector<uint8_t> rx;
  std::vector<uint8_t> tx;
  size_t tx_off = 0;
  uint32_t out_seq = 1;
  bool want_write = false;

  template <class M>
  void queue(M m) {
    m.hdr.seq = out_seq++;
    wire::append(tx, m);
  }
  bool pending() const { return tx_off < tx.size(); }
  size_t backlog() const { return tx.size() - tx_off; }
  bool flush();  // false on a fatal socket error
};

// Flush and (un)register write interest as needed. False if the peer is gone.
bool flush_and_arm(Conn& c, Poller& p);

}  // namespace matchbox
