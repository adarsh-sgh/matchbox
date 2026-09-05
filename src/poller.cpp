#include "matchbox/poller.hpp"

#include <cerrno>
#include <stdexcept>
#include <unistd.h>

#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#elif defined(__linux__)
#include <sys/epoll.h>
#endif

namespace matchbox {

#if defined(__APPLE__) || defined(__FreeBSD__)

Poller::Poller() : fd_(::kqueue()) {
  if (fd_ < 0) throw std::runtime_error("kqueue() failed");
}
Poller::~Poller() { ::close(fd_); }

void Poller::add(int fd) {
  struct kevent ev;
  EV_SET(&ev, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
  ::kevent(fd_, &ev, 1, nullptr, 0, nullptr);
}

void Poller::set_writable(int fd, bool on) {
  struct kevent ev;
  EV_SET(&ev, fd, EVFILT_WRITE, on ? EV_ADD : EV_DELETE, 0, 0, nullptr);
  ::kevent(fd_, &ev, 1, nullptr, 0, nullptr);
}

void Poller::remove(int fd) {
  struct kevent ev;
  EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
  ::kevent(fd_, &ev, 1, nullptr, 0, nullptr);
  EV_SET(&ev, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
  ::kevent(fd_, &ev, 1, nullptr, 0, nullptr);
}

int Poller::wait(Event* out, int max_events, int timeout_ms) {
  struct kevent evs[256];
  if (max_events > 256) max_events = 256;
  struct timespec ts;
  ts.tv_sec = timeout_ms / 1000;
  ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
  const int n = ::kevent(fd_, nullptr, 0, evs, max_events, &ts);
  if (n < 0) return errno == EINTR ? 0 : -1;
  for (int i = 0; i < n; ++i) {
    out[i].fd = static_cast<int>(evs[i].ident);
    out[i].readable = evs[i].filter == EVFILT_READ;
    out[i].writable = evs[i].filter == EVFILT_WRITE;
    out[i].hangup = (evs[i].flags & EV_EOF) != 0;
  }
  return n;
}

#elif defined(__linux__)

Poller::Poller() : fd_(::epoll_create1(EPOLL_CLOEXEC)) {
  if (fd_ < 0) throw std::runtime_error("epoll_create1() failed");
}
Poller::~Poller() { ::close(fd_); }

void Poller::add(int fd) {
  epoll_event ev{};
  ev.events = EPOLLIN | EPOLLRDHUP;
  ev.data.fd = fd;
  ::epoll_ctl(fd_, EPOLL_CTL_ADD, fd, &ev);
}

void Poller::set_writable(int fd, bool on) {
  epoll_event ev{};
  ev.events = EPOLLIN | EPOLLRDHUP | (on ? EPOLLOUT : 0);
  ev.data.fd = fd;
  ::epoll_ctl(fd_, EPOLL_CTL_MOD, fd, &ev);
}

void Poller::remove(int fd) { ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr); }

int Poller::wait(Event* out, int max_events, int timeout_ms) {
  epoll_event evs[256];
  if (max_events > 256) max_events = 256;
  const int n = ::epoll_wait(fd_, evs, max_events, timeout_ms);
  if (n < 0) return errno == EINTR ? 0 : -1;
  for (int i = 0; i < n; ++i) {
    out[i].fd = evs[i].data.fd;
    out[i].readable = (evs[i].events & EPOLLIN) != 0;
    out[i].writable = (evs[i].events & EPOLLOUT) != 0;
    out[i].hangup = (evs[i].events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) != 0;
  }
  return n;
}

#else
#error "matchbox needs kqueue or epoll"
#endif

}  // namespace matchbox
