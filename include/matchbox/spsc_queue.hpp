#pragma once
#include <atomic>
#include <cstddef>
#include <vector>

namespace matchbox {

inline void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  __asm__ __volatile__("yield");
#endif
}

// Single-producer / single-consumer ring. Wait-free on both sides.
// Each side keeps a cached copy of the other's index so the common path
// touches only its own cache line.
template <class T>
class SpscQueue {
 public:
  explicit SpscQueue(size_t capacity_log2) : buf_(size_t(1) << capacity_log2), mask_(buf_.size() - 1) {}

  bool try_push(const T& v) {
    const size_t t = tail_.load(std::memory_order_relaxed);
    if (t - cached_head_ >= buf_.size()) {
      cached_head_ = head_.load(std::memory_order_acquire);
      if (t - cached_head_ >= buf_.size()) return false;
    }
    buf_[t & mask_] = v;
    tail_.store(t + 1, std::memory_order_release);
    return true;
  }

  bool try_pop(T& out) {
    const size_t h = head_.load(std::memory_order_relaxed);
    if (h == cached_tail_) {
      cached_tail_ = tail_.load(std::memory_order_acquire);
      if (h == cached_tail_) return false;
    }
    out = buf_[h & mask_];
    head_.store(h + 1, std::memory_order_release);
    return true;
  }

  size_t capacity() const { return buf_.size(); }
  size_t size_approx() const {
    return tail_.load(std::memory_order_acquire) - head_.load(std::memory_order_acquire);
  }

 private:
  alignas(64) std::atomic<size_t> head_{0};  // consumer owns
  alignas(64) size_t cached_tail_ = 0;       // consumer's view of tail_
  alignas(64) std::atomic<size_t> tail_{0};  // producer owns
  alignas(64) size_t cached_head_ = 0;       // producer's view of head_
  alignas(64) std::vector<T> buf_;
  const size_t mask_;
};

}  // namespace matchbox
