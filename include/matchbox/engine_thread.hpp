#pragma once
#include <atomic>
#include <thread>

#include "matchbox/engine.hpp"
#include "matchbox/spsc_queue.hpp"

namespace matchbox {

// Owns the engine and the two rings that connect it to the network thread.
// The engine thread spins on its inbox; after a while idle it yields.
class EngineThread : public EventSink {
 public:
  explicit EngineThread(Engine::Config cfg, size_t ring_log2 = 16)
      : inbox_(ring_log2), outbox_(ring_log2 + 2), engine_(cfg, *this) {}
  ~EngineThread() override { stop(); }

  void start() {
    stop_.store(false);
    thread_ = std::thread([this] { loop(); });
  }
  void stop() {
    stop_.store(true);
    if (thread_.joinable()) thread_.join();
  }

  SpscQueue<Command>& inbox() { return inbox_; }
  SpscQueue<Event>& outbox() { return outbox_; }
  const Engine& engine() const { return engine_; }

  void on_event(const Event& e) override {
    while (!outbox_.try_push(e)) cpu_relax();
  }

 private:
  void loop() {
    Command c;
    unsigned idle = 0;
    while (!stop_.load(std::memory_order_relaxed)) {
      if (inbox_.try_pop(c)) {
        engine_.process(c);
        idle = 0;
      } else if (++idle < 4096) {
        cpu_relax();
      } else {
        std::this_thread::yield();
      }
    }
  }

  SpscQueue<Command> inbox_;
  SpscQueue<Event> outbox_;
  Engine engine_;
  std::atomic<bool> stop_{true};
  std::thread thread_;
};

}  // namespace matchbox
