// Load generator (`load`) and market-data tap (`md`) for a running matchbox_server.
#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <random>
#include <sys/select.h>
#include <unistd.h>
#include <vector>

#include "matchbox/args.hpp"
#include "matchbox/clock.hpp"
#include "matchbox/net.hpp"
#include "matchbox/protocol.hpp"
#include "matchbox/stats.hpp"
#include "matchbox/types.hpp"

using namespace matchbox;

namespace {

// Pulls complete frames out of a byte stream, calling fn(type, ptr) for each.
template <class Fn>
void for_each_frame(std::vector<uint8_t>& rx, Fn&& fn) {
  size_t off = 0;
  while (rx.size() - off >= sizeof(wire::Header)) {
    const wire::Header h = wire::read_header(rx.data() + off);
    if (rx.size() - off < h.len) break;
    fn(static_cast<wire::MsgType>(h.type), rx.data() + off);
    off += h.len;
  }
  rx.erase(rx.begin(), rx.begin() + static_cast<std::ptrdiff_t>(off));
}

int run_load(const Args& args) {
  const std::string host = args.str("host", "127.0.0.1");
  const auto port = static_cast<uint16_t>(args.num("port", 9001));
  const size_t n_orders = static_cast<size_t>(args.num("orders", 100000));
  const size_t inflight = static_cast<size_t>(args.num("inflight", 1));
  const auto symbol = static_cast<uint16_t>(args.num("symbol", 0));
  const Price mid = args.num("mid", 10000);
  const Price spread = args.num("spread", 5);

  const int fd = connect_tcp(host.c_str(), port);
  if (fd < 0) {
    std::perror("connect");
    return 1;
  }

  std::mt19937_64 rng(42);
  std::vector<uint64_t> sent_at(n_orders), rtt;
  rtt.reserve(n_orders);
  std::vector<uint8_t> rx, out;
  uint8_t buf[1 << 16];
  uint32_t seq = 1;
  size_t next_tag = 0, done = 0, in_flight = 0, fills = 0, rejects = 0;

  const uint64_t t_start = now_ns();
  while (done < n_orders) {
    out.clear();
    while (in_flight < inflight && next_tag < n_orders) {
      auto m = wire::make<wire::NewOrder>(seq++);
      m.tag = next_tag;
      m.symbol = symbol;
      m.side = static_cast<uint8_t>(rng() & 1);
      m.type = static_cast<uint8_t>(OrderType::Limit);
      m.tif = static_cast<uint8_t>(Tif::GTC);
      m.price = mid - spread + static_cast<Price>(rng() % static_cast<uint64_t>(2 * spread + 1));
      m.qty = 1 + static_cast<Qty>(rng() % 100);
      wire::append(out, m);
      sent_at[next_tag++] = now_ns();
      ++in_flight;
    }
    if (!out.empty() && !write_full(fd, out.data(), out.size())) {
      std::perror("write");
      return 1;
    }
    const ssize_t n = ::read(fd, buf, sizeof buf);
    if (n <= 0) {
      std::fprintf(stderr, "connection closed by server\n");
      return 1;
    }
    const uint64_t now = now_ns();
    rx.insert(rx.end(), buf, buf + n);
    for_each_frame(rx, [&](wire::MsgType t, const uint8_t* p) {
      switch (t) {
        case wire::MsgType::Ack: {
          const auto a = wire::read<wire::Ack>(p);
          rtt.push_back(now - sent_at[a.tag]);
          ++done;
          --in_flight;
          break;
        }
        case wire::MsgType::Reject: {
          ++rejects;
          ++done;
          --in_flight;
          break;
        }
        case wire::MsgType::Fill: ++fills; break;
        default: break;
      }
    });
  }
  const double secs = static_cast<double>(now_ns() - t_start) / 1e9;
  ::close(fd);

  std::printf("sent %zu orders in %.3fs: %.0f orders/s (inflight=%zu)\n", n_orders, secs, n_orders / secs, inflight);
  print_latency("ack round-trip", rtt, 1000.0, "us");
  std::printf("fills %zu  rejects %zu\n", fills, rejects);
  return 0;
}

// Resumable market-data subscriber. Tracks the stream sequence, counts gaps
// (explicit Gap frames and any seq jump), treats a missed heartbeat as a dead
// link and reconnects from the last seq seen. With --stats it prints a
// fan-out latency histogram (engine trade timestamp -> client receive) instead
// of every frame.
int run_md(const Args& args) {
  const std::string host = args.str("host", "127.0.0.1");
  const auto port = static_cast<uint16_t>(args.num("port", 9002));
  const bool stats = args.has("stats");
  const double seconds = args.real("seconds", 0);  // 0 = until EOF / Ctrl-C
  const long slow_us = args.num("slow-us", 0);     // sleep per read to act as a slow consumer
  const auto hb_timeout_ns = static_cast<uint64_t>(args.num("heartbeat-timeout-ms", 3000)) * 1000000;
  const bool reconnect = !args.has("no-reconnect");

  uint64_t from = static_cast<uint64_t>(args.num("from", 0));
  uint32_t last_seq = 0;
  uint64_t frames = 0, gaps = 0, lost = 0, heartbeats = 0, reconnects = 0;
  std::vector<uint64_t> fanout_ns;
  std::vector<uint8_t> rx;
  uint8_t buf[1 << 16];
  const uint64_t t_start = now_ns();
  const uint64_t t_end = seconds > 0 ? t_start + static_cast<uint64_t>(seconds * 1e9) : UINT64_MAX;
  unsigned backoff_ms = 50;

  while (now_ns() < t_end) {
    const int fd = connect_tcp(host.c_str(), port);
    if (fd < 0) {
      if (!reconnect) {
        std::perror("connect");
        return 1;
      }
      ::usleep(backoff_ms * 1000);
      backoff_ms = std::min(backoff_ms * 2, 2000u);
      continue;
    }
    auto sub = wire::make<wire::Subscribe>();
    sub.from_seq = from;
    if (!write_full(fd, &sub, sizeof sub)) {
      ::close(fd);
      continue;
    }
    if (!stats) std::printf("subscribed from seq %llu\n", static_cast<unsigned long long>(from));
    backoff_ms = 50;
    rx.clear();
    uint64_t last_rx_ns = now_ns();
    bool alive = true;
    while (alive && now_ns() < t_end) {
      timeval tv{};
      tv.tv_sec = 0;
      tv.tv_usec = 100000;
      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(fd, &rfds);
      const int r = ::select(fd + 1, &rfds, nullptr, nullptr, &tv);
      if (r < 0 && errno == EINTR) continue;
      if (r == 0) {
        if (now_ns() - last_rx_ns > hb_timeout_ns) {
          if (!stats) std::printf("no heartbeat for %llums, reconnecting\n", static_cast<unsigned long long>(hb_timeout_ns / 1000000));
          alive = false;
        }
        continue;
      }
      if (slow_us > 0) ::usleep(static_cast<useconds_t>(slow_us));
      const ssize_t n = ::read(fd, buf, sizeof buf);
      if (n <= 0) {
        alive = false;
        break;
      }
      const uint64_t now = now_ns();
      last_rx_ns = now;
      rx.insert(rx.end(), buf, buf + n);
      for_each_frame(rx, [&](wire::MsgType t, const uint8_t* p) {
        const wire::Header h = wire::read_header(p);
        if (t == wire::MsgType::Heartbeat) {
          const auto hb = wire::read<wire::Heartbeat>(p);
          ++heartbeats;
          last_seq = std::max<uint32_t>(last_seq, static_cast<uint32_t>(hb.head_seq));  // resume point on a quiet stream
          return;
        }
        if (t == wire::MsgType::Gap) {
          const auto g = wire::read<wire::Gap>(p);
          ++gaps;
          lost += g.resumed_at - g.from;
          last_seq = static_cast<uint32_t>(g.resumed_at - 1);
          if (!stats)
            std::printf("GAP   frames %llu..%llu lost\n", static_cast<unsigned long long>(g.from),
                        static_cast<unsigned long long>(g.resumed_at - 1));
          return;
        }
        if (last_seq != 0 && h.seq != last_seq + 1) {  // should never happen: Gap covers ring laps
          ++gaps;
          lost += h.seq - last_seq - 1;
          if (!stats) std::printf("SEQ JUMP %u -> %u\n", last_seq, h.seq);
        }
        last_seq = h.seq;
        ++frames;
        if (t == wire::MsgType::Trade) {
          const auto m = wire::read<wire::Trade>(p);
          if (stats) {
            fanout_ns.push_back(now > m.ts_ns ? now - m.ts_ns : 0);
          } else {
            std::printf("[%u] TRADE sym=%u %s %u @ %lld\n", h.seq, m.symbol, m.aggressor ? "sell" : "buy", m.qty,
                        static_cast<long long>(m.price));
          }
        } else if (t == wire::MsgType::Top && !stats) {
          const auto m = wire::read<wire::Top>(p);
          std::printf("[%u] TOP   sym=%u bid %u @ %lld | ask %u @ %lld\n", h.seq, m.symbol, m.bid_qty,
                      static_cast<long long>(m.bid), m.ask_qty, static_cast<long long>(m.ask));
        }
      });
      if (!stats) std::fflush(stdout);
    }
    ::close(fd);
    if (!reconnect || now_ns() >= t_end) break;
    ++reconnects;
    from = last_seq + 1;
  }
  const double secs = static_cast<double>(now_ns() - t_start) / 1e9;
  std::printf("md: %llu frames in %.2fs (%.0f frames/s), gaps %llu (%llu frames lost), heartbeats %llu, reconnects %llu, last seq %u\n",
              static_cast<unsigned long long>(frames), secs, frames / secs, static_cast<unsigned long long>(gaps),
              static_cast<unsigned long long>(lost), static_cast<unsigned long long>(heartbeats),
              static_cast<unsigned long long>(reconnects), last_seq);
  if (!fanout_ns.empty()) print_latency("trade fan-out (engine ts -> client rx)", fanout_ns, 1000.0, "us");
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  ::signal(SIGPIPE, SIG_IGN);
  Args args(argc, argv);
  const auto& pos = args.positional();
  const std::string mode = pos.empty() ? "" : pos[0];
  if (mode == "load") return run_load(args);
  if (mode == "md") return run_md(args);
  std::printf(
      "usage: matchbox_client load [--host 127.0.0.1] [--port 9001] [--orders 100000] [--inflight 1]\n"
      "                            [--symbol 0] [--mid 10000] [--spread 5]\n"
      "       matchbox_client md   [--host 127.0.0.1] [--port 9002] [--from 0] [--stats] [--seconds 0]\n"
      "                            [--slow-us 0] [--heartbeat-timeout-ms 3000] [--no-reconnect]\n");
  return mode.empty() ? 0 : 1;
}
