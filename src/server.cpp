#include "matchbox/server.hpp"

#include "matchbox/clock.hpp"

#include <cerrno>
#include <csignal>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace matchbox {

Server::Server(Config cfg, SpscQueue<Command>& to_engine, SpscQueue<Event>& from_engine)
    : cfg_(cfg), to_engine_(to_engine), from_engine_(from_engine), publisher_(poller_, cfg.md) {
  ::signal(SIGPIPE, SIG_IGN);
  listen_fd_ = listen_tcp(cfg.port, &port_);
  md_listen_fd_ = listen_tcp(cfg.md_port, &md_port_);
  if (listen_fd_ < 0 || md_listen_fd_ < 0) throw std::runtime_error("listen failed");
  track(listen_fd_, Kind::Listen, 0);
  track(md_listen_fd_, Kind::MdListen, 0);
  poller_.add(listen_fd_);
  poller_.add(md_listen_fd_);
}

Server::~Server() {
  for (auto& kv : sessions_) ::close(kv.second->fd);
  ::close(listen_fd_);
  ::close(md_listen_fd_);
}

void Server::track(int fd, Kind kind, uint32_t sid) {
  if (static_cast<size_t>(fd) >= kind_by_fd_.size()) {
    kind_by_fd_.resize(static_cast<size_t>(fd) + 64, Kind::None);
    sid_by_fd_.resize(static_cast<size_t>(fd) + 64, 0);
  }
  kind_by_fd_[static_cast<size_t>(fd)] = kind;
  sid_by_fd_[static_cast<size_t>(fd)] = sid;
}

Session* Server::session_by_fd(int fd) {
  if (static_cast<size_t>(fd) >= kind_by_fd_.size() || kind_by_fd_[static_cast<size_t>(fd)] != Kind::Session)
    return nullptr;
  auto it = sessions_.find(sid_by_fd_[static_cast<size_t>(fd)]);
  return it == sessions_.end() ? nullptr : it->second.get();
}

void Server::run(const std::atomic<bool>& stop) {
  Poller::Event evs[256];
  unsigned idle = 0;
  while (!stop.load(std::memory_order_relaxed)) {
    // Spin briefly after activity so engine responses are picked up within
    // microseconds; fall back to a 1ms sleep-poll when genuinely idle.
    const int n = poller_.wait(evs, 256, idle < 1000 ? 0 : 1);
    if (n < 0) break;
    for (int i = 0; i < n; ++i) {
      const Poller::Event& ev = evs[i];
      const int fd = ev.fd;
      switch (kind_by_fd_[static_cast<size_t>(fd)]) {
        case Kind::Listen: accept_all(fd, Kind::Session); break;
        case Kind::MdListen: accept_all(fd, Kind::Subscriber); break;
        case Kind::Session: {
          if (ev.writable) {
            Session* s = session_by_fd(fd);
            if (s && !flush_and_arm(*s, poller_)) close_session(*s);
          }
          if (ev.readable || ev.hangup) {
            Session* s = session_by_fd(fd);
            if (s) read_session(*s);
          }
          break;
        }
        case Kind::Subscriber:
          if (!publisher_.is_subscriber(fd)) break;
          if (ev.writable) publisher_.on_writable(fd);
          if (ev.readable || ev.hangup) publisher_.on_readable(fd);
          break;
        case Kind::None: break;
      }
    }
    const bool worked = drain_engine();
    flush_dirty();
    publisher_.flush_all(now_ns());
    idle = (n > 0 || worked) ? 0 : idle + 1;
  }
}

void Server::accept_all(int lfd, Kind kind) {
  for (;;) {
    const int fd = ::accept(lfd, nullptr, nullptr);
    if (fd < 0) {
      if (errno == EINTR) continue;
      break;
    }
    set_nonblocking(fd);
    set_nodelay(fd);
    if (kind == Kind::Session) {
      auto s = std::make_unique<Session>();
      s->fd = fd;
      s->id = next_sid_++;
      track(fd, Kind::Session, s->id);
      poller_.add(fd);
      sessions_.emplace(s->id, std::move(s));
    } else {
      track(fd, Kind::Subscriber, 0);
      publisher_.subscribe(fd);
    }
  }
}

void Server::close_session(Session& s) {
  poller_.remove(s.fd);
  ::close(s.fd);
  track(s.fd, Kind::None, 0);
  sessions_.erase(s.id);  // s is dangling from here
}

void Server::read_session(Session& s) {
  uint8_t buf[1 << 16];
  for (;;) {
    const ssize_t n = ::read(s.fd, buf, sizeof buf);
    if (n > 0) {
      s.rx.insert(s.rx.end(), buf, buf + n);
      if (static_cast<size_t>(n) < sizeof buf) break;
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    if (n < 0 && errno == EINTR) continue;
    close_session(s);
    return;
  }
  if (!parse_frames(s)) {
    s.flush();  // best effort: let a Reject(BadSeq) out before hanging up
    close_session(s);
  }
}

void Server::submit(const Command& c) {
  // Engine is far faster than the network path; a full ring only happens
  // under a burst, so just wait for it to drain.
  while (!to_engine_.try_push(c)) cpu_relax();
}

bool Server::parse_frames(Session& s) {
  size_t off = 0;
  while (s.rx.size() - off >= sizeof(wire::Header)) {
    const wire::Header h = wire::read_header(s.rx.data() + off);
    const auto type = static_cast<wire::MsgType>(h.type);
    if (h.version != wire::kVersion || h.len == 0 || h.len != wire::size_for(type)) return false;
    if (s.rx.size() - off < h.len) break;
    const uint8_t* p = s.rx.data() + off;
    off += h.len;

    if (h.seq != s.expect_seq) {
      auto r = wire::make<wire::Reject>();
      r.reason = static_cast<uint8_t>(RejectReason::BadSeq);
      send(s, r);
      return false;
    }
    ++s.expect_seq;

    Command c;
    c.session = s.id;
    bool ok = true;
    switch (type) {
      case wire::MsgType::NewOrder: {
        const auto m = wire::read<wire::NewOrder>(p);
        c.kind = Command::Kind::New;
        c.tag = m.tag;
        c.symbol = m.symbol;
        c.side = static_cast<Side>(m.side);
        c.type = static_cast<OrderType>(m.type);
        c.tif = static_cast<Tif>(m.tif);
        c.price = m.price;
        c.qty = m.qty;
        ok = m.side <= 1 && m.type <= 1 && m.tif <= 1;
        break;
      }
      case wire::MsgType::CancelOrder: {
        const auto m = wire::read<wire::CancelOrder>(p);
        c.kind = Command::Kind::Cancel;
        c.tag = m.tag;
        c.order_id = m.order_id;
        break;
      }
      case wire::MsgType::ReplaceOrder: {
        const auto m = wire::read<wire::ReplaceOrder>(p);
        c.kind = Command::Kind::Replace;
        c.tag = m.tag;
        c.order_id = m.order_id;
        c.price = m.price;
        c.qty = m.qty;
        break;
      }
      default: ok = false; break;
    }
    if (!ok) {
      auto r = wire::make<wire::Reject>();
      r.tag = c.tag;
      r.reason = static_cast<uint8_t>(RejectReason::BadMessage);
      send(s, r);
      continue;
    }
    submit(c);
  }
  s.rx.erase(s.rx.begin(), s.rx.begin() + static_cast<std::ptrdiff_t>(off));
  return true;
}

bool Server::drain_engine() {
  Event e;
  bool any = false;
  while (from_engine_.try_pop(e)) {
    on_event(e);
    any = true;
  }
  return any;
}

void Server::on_event(const Event& e) {
  using K = Event::Kind;
  if (e.kind == K::Trade) {
    auto m = wire::make<wire::Trade>();
    m.symbol = e.symbol;
    m.aggressor = static_cast<uint8_t>(e.side);
    m.price = e.price;
    m.qty = e.qty;
    m.ts_ns = e.ts_ns;
    publisher_.publish(m);
    return;
  }
  if (e.kind == K::Top) {
    auto m = wire::make<wire::Top>();
    m.symbol = e.symbol;
    m.bid = e.top.bid;
    m.ask = e.top.ask;
    m.bid_qty = e.top.bid_qty;
    m.ask_qty = e.top.ask_qty;
    publisher_.publish(m);
    return;
  }
  auto it = sessions_.find(e.session);
  if (it == sessions_.end()) return;  // client went away
  Session& s = *it->second;
  switch (e.kind) {
    case K::Accepted: {
      auto m = wire::make<wire::Ack>();
      m.tag = e.tag;
      m.order_id = e.order_id;
      send(s, m);
      break;
    }
    case K::Rejected: {
      auto m = wire::make<wire::Reject>();
      m.tag = e.tag;
      m.reason = static_cast<uint8_t>(e.reason);
      send(s, m);
      break;
    }
    case K::Fill: {
      auto m = wire::make<wire::Fill>();
      m.order_id = e.order_id;
      m.price = e.price;
      m.qty = e.qty;
      m.remaining = e.remaining;
      m.maker = e.maker ? 1 : 0;
      send(s, m);
      break;
    }
    case K::Cancelled: {
      auto m = wire::make<wire::Cancelled>();
      m.order_id = e.order_id;
      m.remaining = e.remaining;
      send(s, m);
      break;
    }
    case K::Replaced: {
      auto m = wire::make<wire::Replaced>();
      m.order_id = e.order_id;
      m.price = e.price;
      m.qty = e.qty;
      send(s, m);
      break;
    }
    case K::Trade:
    case K::Top: break;
  }
}

void Server::flush_dirty() {
  for (uint32_t sid : dirty_) {
    auto it = sessions_.find(sid);
    if (it == sessions_.end()) continue;
    Session& s = *it->second;
    s.dirty = false;
    if (!flush_and_arm(s, poller_) || s.backlog() > cfg_.max_backlog) close_session(s);
  }
  dirty_.clear();
}

}  // namespace matchbox
