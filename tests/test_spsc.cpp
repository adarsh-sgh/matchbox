#include <thread>
#include <vector>

#include "doctest.h"
#include "matchbox/spsc_queue.hpp"

using namespace matchbox;

TEST_CASE("spsc: full/empty semantics and in-order transfer across threads") {
  SpscQueue<uint64_t> q(3);  // capacity 8
  CHECK(q.capacity() == 8);
  uint64_t v = 0;
  CHECK_FALSE(q.try_pop(v));
  for (uint64_t i = 0; i < 8; ++i) CHECK(q.try_push(i));
  CHECK_FALSE(q.try_push(99));
  CHECK(q.size_approx() == 8);
  for (uint64_t i = 0; i < 8; ++i) {
    CHECK(q.try_pop(v));
    CHECK(v == i);
  }
  CHECK_FALSE(q.try_pop(v));

  constexpr uint64_t kN = 2'000'000;
  SpscQueue<uint64_t> ring(10);
  std::thread producer([&] {
    for (uint64_t i = 0; i < kN; ++i)
      while (!ring.try_push(i)) cpu_relax();
  });
  uint64_t expect = 0;
  bool ordered = true;
  while (expect < kN) {
    uint64_t got;
    if (!ring.try_pop(got)) {
      cpu_relax();
      continue;
    }
    ordered = ordered && got == expect;
    ++expect;
  }
  producer.join();
  CHECK(ordered);
  CHECK(ring.size_approx() == 0);
}
