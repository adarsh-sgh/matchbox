// In-process engine benchmark: no sockets, measures Engine::process alone.
#include <cstdio>
#include <random>
#include <vector>

#include "matchbox/args.hpp"
#include "matchbox/clock.hpp"
#include "matchbox/engine.hpp"
#include "matchbox/stats.hpp"

using namespace matchbox;

namespace {

struct CountingSink : EventSink {
  std::vector<OrderId> live;  // ids accepted and not yet fully gone, for cancels
  size_t events = 0, trades = 0;
  void on_event(const Event& e) override {
    ++events;
    if (e.kind == Event::Kind::Accepted) live.push_back(e.order_id);
    if (e.kind == Event::Kind::Trade) ++trades;
  }
};

}  // namespace

int main(int argc, char** argv) {
  Args args(argc, argv);
  const size_t n = static_cast<size_t>(args.num("orders", 1000000));
  const double cancel_ratio = args.real("cancel-ratio", 0.3);
  const Price mid = args.num("mid", 10000);
  const Price spread = args.num("spread", 50);

  CountingSink sink;
  Engine::Config cfg;
  cfg.max_orders = static_cast<uint32_t>(n + 1024);
  Engine engine(cfg, sink);

  std::mt19937_64 rng(7);
  std::vector<uint64_t> lat;
  lat.reserve(n);
  size_t cancels = 0;

  const uint64_t t0 = now_ns();
  for (size_t i = 0; i < n; ++i) {
    Command c;
    c.session = 1;
    c.tag = i;
    const bool do_cancel = !sink.live.empty() && (rng() % 1000) < static_cast<uint64_t>(cancel_ratio * 1000);
    if (do_cancel) {
      const size_t k = rng() % sink.live.size();
      c.kind = Command::Kind::Cancel;
      c.order_id = sink.live[k];
      sink.live[k] = sink.live.back();
      sink.live.pop_back();
      ++cancels;
    } else {
      c.kind = Command::Kind::New;
      c.side = static_cast<Side>(rng() & 1);
      c.price = mid - spread + static_cast<Price>(rng() % static_cast<uint64_t>(2 * spread + 1));
      c.qty = 1 + static_cast<Qty>(rng() % 100);
    }
    const uint64_t a = now_ns();
    engine.process(c);
    lat.push_back(now_ns() - a);
  }
  const double secs = static_cast<double>(now_ns() - t0) / 1e9;

  std::printf("engine: %zu ops (%zu cancels) in %.3fs -> %.2fM ops/s\n", n, cancels, secs, n / secs / 1e6);
  print_latency("per-op latency", lat, 1.0, "ns");
  std::printf("trades %zu  events %zu  resting %zu  live orders %u\n", sink.trades, sink.events,
              engine.book(0).resting(), engine.pool().live());
  return 0;
}
