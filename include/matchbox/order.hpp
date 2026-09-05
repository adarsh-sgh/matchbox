#pragma once
#include <vector>

#include "matchbox/types.hpp"

namespace matchbox {

struct Order {
  OrderId id = 0;
  uint64_t tag = 0;  // client-supplied, echoed in responses
  uint32_t session = 0;
  uint16_t symbol = 0;
  Side side = Side::Buy;
  OrderType type = OrderType::Limit;
  Tif tif = Tif::GTC;
  bool live = false;
  Price price = 0;
  Qty qty = 0;  // remaining
  Qty filled = 0;
  uint32_t prev = kNull;  // intrusive per-level list; pool slot indices
  uint32_t next = kNull;  // also the free-list link while not live
  uint32_t gen = 0;
};

// Fixed-capacity slab. Ids embed slot + generation so lookup is one array
// index plus a liveness check; nothing is hashed on the hot path.
class OrderPool {
 public:
  explicit OrderPool(uint32_t capacity) : slots_(capacity) {
    for (uint32_t i = 0; i < capacity; ++i) {
      slots_[i].gen = 1;
      slots_[i].next = i + 1 < capacity ? i + 1 : kNull;
    }
    free_head_ = capacity ? 0 : kNull;
  }

  Order* alloc() {
    if (free_head_ == kNull) return nullptr;
    const uint32_t slot = free_head_;
    Order& o = slots_[slot];
    free_head_ = o.next;
    const uint32_t gen = o.gen;
    o = Order{};
    o.gen = gen;
    o.id = (uint64_t(gen) << 32) | slot;
    o.live = true;
    ++live_;
    return &o;
  }

  void free(Order& o) {
    o.live = false;
    o.gen = o.gen + 1 ? o.gen + 1 : 1;  // keep gen != 0 so ids are never 0
    o.next = free_head_;
    free_head_ = slot_of(o);
    --live_;
  }

  Order* find(OrderId id) {
    const uint32_t slot = static_cast<uint32_t>(id);
    if (slot >= slots_.size()) return nullptr;
    Order& o = slots_[slot];
    return (o.live && o.id == id) ? &o : nullptr;
  }

  const Order* find(OrderId id) const { return const_cast<OrderPool*>(this)->find(id); }

  Order& at(uint32_t slot) { return slots_[slot]; }
  const Order& at(uint32_t slot) const { return slots_[slot]; }
  uint32_t slot_of(const Order& o) const { return static_cast<uint32_t>(&o - slots_.data()); }
  uint32_t live() const { return live_; }
  uint32_t capacity() const { return static_cast<uint32_t>(slots_.size()); }

 private:
  std::vector<Order> slots_;
  uint32_t free_head_ = kNull;
  uint32_t live_ = 0;
};

}  // namespace matchbox
