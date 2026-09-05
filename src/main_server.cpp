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
        "[--max-price 100000] [--max-orders 1048576]\n");
    return 0;
  }
  Engine::Config ecfg;
  ecfg.symbols = static_cast<uint16_t>(args.num("symbols", 4));
  ecfg.max_price = args.num("max-price", 100000);
  ecfg.max_orders = static_cast<uint32_t>(args.num("max-orders", 1 << 20));
  Server::Config scfg;
  scfg.port = static_cast<uint16_t>(args.num("port", 9001));
  scfg.md_port = static_cast<uint16_t>(args.num("md-port", 9002));

  EngineThread engine(ecfg);
  Server server(scfg, engine.inbox(), engine.outbox());
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  std::printf("matchbox: orders on :%u, market data on :%u, %u symbols, prices 1..%lld\n", server.port(),
              server.md_port(), ecfg.symbols, static_cast<long long>(ecfg.max_price));
  std::fflush(stdout);
  engine.start();
  server.run(g_stop);
  engine.stop();
  std::printf("matchbox: stopped, %u orders resting\n", engine.engine().pool().live());
  return 0;
}
