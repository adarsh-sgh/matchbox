#include "matchbox/net.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace matchbox {

void set_nonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void set_nodelay(int fd) {
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

int listen_tcp(uint16_t port, uint16_t* bound_port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0 || ::listen(fd, 512) < 0) {
    ::close(fd);
    return -1;
  }
  if (bound_port) {
    socklen_t len = sizeof addr;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    *bound_port = ntohs(addr.sin_port);
  }
  set_nonblocking(fd);
  return fd;
}

int connect_tcp(const char* host, uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, host, &addr.sin_addr) != 1 ||
      ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0) {
    ::close(fd);
    return -1;
  }
  set_nodelay(fd);
  return fd;
}

bool read_full(int fd, void* buf, size_t n) {
  auto* p = static_cast<uint8_t*>(buf);
  while (n > 0) {
    const ssize_t r = ::read(fd, p, n);
    if (r == 0) return false;
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    p += r;
    n -= static_cast<size_t>(r);
  }
  return true;
}

bool write_full(int fd, const void* buf, size_t n) {
  const auto* p = static_cast<const uint8_t*>(buf);
  while (n > 0) {
    const ssize_t r = ::write(fd, p, n);
    if (r < 0) {
      if (errno == EINTR || errno == EAGAIN) continue;
      return false;
    }
    p += r;
    n -= static_cast<size_t>(r);
  }
  return true;
}

bool Conn::flush() {
  while (tx_off < tx.size()) {
    const ssize_t n = ::write(fd, tx.data() + tx_off, tx.size() - tx_off);
    if (n > 0) {
      tx_off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    if (n < 0 && errno == EINTR) continue;
    return false;
  }
  if (tx_off == tx.size()) {
    tx.clear();
    tx_off = 0;
  } else if (tx_off > (1u << 16)) {
    tx.erase(tx.begin(), tx.begin() + static_cast<std::ptrdiff_t>(tx_off));
    tx_off = 0;
  }
  return true;
}

bool flush_and_arm(Conn& c, Poller& p) {
  if (!c.flush()) return false;
  const bool need = c.pending();
  if (need != c.want_write) {
    p.set_writable(c.fd, need);
    c.want_write = need;
  }
  return true;
}

}  // namespace matchbox
