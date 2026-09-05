#pragma once
#include <chrono>
#include <cstdint>

namespace matchbox {

inline uint64_t now_ns() {
  using namespace std::chrono;
  return static_cast<uint64_t>(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
}

}  // namespace matchbox
