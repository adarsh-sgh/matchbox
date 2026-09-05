#pragma once
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <vector>

namespace matchbox {

struct Percentiles {
  uint64_t p50 = 0, p99 = 0, p999 = 0, max = 0;
};

inline Percentiles percentiles(std::vector<uint64_t>& samples) {
  Percentiles p;
  if (samples.empty()) return p;
  std::sort(samples.begin(), samples.end());
  auto at = [&](double q) { return samples[std::min(samples.size() - 1, static_cast<size_t>(q * samples.size()))]; };
  p.p50 = at(0.50);
  p.p99 = at(0.99);
  p.p999 = at(0.999);
  p.max = samples.back();
  return p;
}

inline void print_latency(const char* label, std::vector<uint64_t>& ns, double scale, const char* unit) {
  const Percentiles p = percentiles(ns);
  std::printf("%s: p50 %.1f%s  p99 %.1f%s  p999 %.1f%s  max %.1f%s  (n=%zu)\n", label, p.p50 / scale, unit,
              p.p99 / scale, unit, p.p999 / scale, unit, p.max / scale, unit, ns.size());
}

}  // namespace matchbox
