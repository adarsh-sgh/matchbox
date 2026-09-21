#include <atomic>
#include <csignal>
#include <cstdio>

#include "matchbox/args.hpp"
#include "matchbox/engine_thread.hpp"
#include "matchbox/server.hpp"

using namespace matchbox;

static std::atomic<bool> g_stop{false};
static void on_signal(int) { g_stop.store(true); }

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (args.has("help")) {
    std::printf(
        "usage: matchbox_server [--port 9001] [--md-port 9002] [--symbols 4] "
        "[--max-price 100000] [--max-orders 1048576]\n"
        "                       [--md-replay-log2 16] [--md-backlog 1048576] [--md-heartbeat-ms 1000] "
        "[--md-stall-ms 10000]\n");
    return 0;
  }
  Engine::Config ecfg;
  ecfg.symbols = static_cast<uint16_t>(args.num("symbols", 4));
  ecfg.max_price = args.num("max-price", 100000);
  ecfg.max_orders = static_cast<uint32_t>(args.num("max-orders", 1 << 20));
  Server::Config scfg;
  scfg.port = static_cast<uint16_t>(args.num("port", 9001));
  scfg.md_port = static_cast<uint16_t>(args.num("md-port", 9002));
  scfg.md.ring_log2 = static_cast<size_t>(args.num("md-replay-log2", 16));
  scfg.md.max_backlog = static_cast<size_t>(args.num("md-backlog", 1 << 20));
  scfg.md.heartbeat_ms = static_cast<uint64_t>(args.num("md-heartbeat-ms", 1000));
  scfg.md.stall_timeout_ms = static_cast<uint64_t>(args.num("md-stall-ms", 10000));

  EngineThread engine(ecfg);
  Server server(scfg, engine.inbox(), engine.outbox());
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  std::printf("matchbox: orders on :%u, market data on :%u (replay %zu frames, heartbeat %llums), %u symbols, prices 1..%lld\n",
              server.port(), server.md_port(), size_t(1) << scfg.md.ring_log2,
              static_cast<unsigned long long>(scfg.md.heartbeat_ms), ecfg.symbols, static_cast<long long>(ecfg.max_price));
  std::fflush(stdout);
  engine.start();
  server.run(g_stop);
  engine.stop();
  std::printf("matchbox: stopped, %u orders resting\n", engine.engine().pool().live());
  return 0;
}
