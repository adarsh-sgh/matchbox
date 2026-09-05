// Load generator (`load`) and market-data tap (`md`) for a running matchbox_server.
#include <csignal>
#include <cstdio>
#include <random>
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

int run_md(const Args& args) {
  const std::string host = args.str("host", "127.0.0.1");
  const auto port = static_cast<uint16_t>(args.num("port", 9002));
  const int fd = connect_tcp(host.c_str(), port);
  if (fd < 0) {
    std::perror("connect");
    return 1;
  }
  std::vector<uint8_t> rx;
  uint8_t buf[1 << 16];
  for (;;) {
    const ssize_t n = ::read(fd, buf, sizeof buf);
    if (n <= 0) break;
    rx.insert(rx.end(), buf, buf + n);
    for_each_frame(rx, [&](wire::MsgType t, const uint8_t* p) {
      if (t == wire::MsgType::Trade) {
        const auto m = wire::read<wire::Trade>(p);
        std::printf("TRADE sym=%u %s %u @ %lld\n", m.symbol, m.aggressor ? "sell" : "buy", m.qty,
                    static_cast<long long>(m.price));
      } else if (t == wire::MsgType::Top) {
        const auto m = wire::read<wire::Top>(p);
        std::printf("TOP   sym=%u bid %u @ %lld | ask %u @ %lld\n", m.symbol, m.bid_qty,
                    static_cast<long long>(m.bid), m.ask_qty, static_cast<long long>(m.ask));
      }
    });
    std::fflush(stdout);
  }
  ::close(fd);
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
      "       matchbox_client md   [--host 127.0.0.1] [--port 9002]\n");
  return mode.empty() ? 0 : 1;
}
