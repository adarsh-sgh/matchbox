#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "matchbox/protocol.hpp"

namespace matchbox {

// Fixed-capacity ring of serialized market-data frames, indexed by stream
// sequence. Appending stamps the frame's header seq. Every subscriber reads
// through a cursor into this one buffer, so fan-out costs no per-subscriber
// copies until the bytes hit that subscriber's socket buffer.
class ReplayRing {
 public:
  explicit ReplayRing(size_t capacity_log2)
      : buf_((size_t(1) << capacity_log2) * wire::kMaxFrame), mask_((size_t(1) << capacity_log2) - 1) {}

  template <class M>
  uint32_t append(M m) {
    static_assert(sizeof(M) <= wire::kMaxFrame, "frame too large for the ring");
    m.hdr.seq = ++head_;
    std::memcpy(slot(head_), &m, sizeof m);
    return head_;
  }

  uint32_t head() const { return head_; }  // newest seq, 0 when empty
  uint32_t oldest() const {                // oldest seq still retained
    const size_t cap = mask_ + 1;
    return head_ > cap ? head_ - static_cast<uint32_t>(cap) + 1 : 1;
  }
  bool holds(uint32_t seq) const { return seq >= oldest() && seq <= head_ && seq != 0; }
  size_t capacity() const { return mask_ + 1; }

  // Caller checks holds(seq). Returns the frame bytes; len comes from its header.
  const uint8_t* frame(uint32_t seq, size_t* len) const {
    const uint8_t* p = slot(seq);
    *len = wire::read_header(p).len;
    return p;
  }

 private:
  uint8_t* slot(uint32_t seq) { return buf_.data() + (seq & mask_) * wire::kMaxFrame; }
  const uint8_t* slot(uint32_t seq) const { return buf_.data() + (seq & mask_) * wire::kMaxFrame; }

  std::vector<uint8_t> buf_;
  const size_t mask_;
  uint32_t head_ = 0;
};

}  // namespace matchbox
