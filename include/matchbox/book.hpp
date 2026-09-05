#pragma once
#include <algorithm>
#include <cstddef>
#include <vector>

#include "matchbox/order.hpp"
#include "matchbox/types.hpp"

namespace matchbox {

struct Level {
  uint32_t head = kNull;
  uint32_t tail = kNull;
  Qty qty = 0;
  uint32_t count = 0;
};

// One instrument. Levels live in a flat array indexed by price tick; each
// level is an intrusive FIFO of orders (pool slots). A bitmap over prices
// finds the next best level when one empties, so nothing here allocates or
// walks a tree.
class Book {
 public:
  Book(OrderPool& pool, Price max_price);
  Book(const Book&) = delete;
  Book& operator=(const Book&) = delete;

  bool valid_price(Price p) const { return p >= 1 && p <= max_price_; }
  Price max_price() const { return max_price_; }

  void insert(Order& o);               // rest at the back of its level
  void remove(Order& o);               // O(1) unlink
  void reduce(Order& o, Qty new_qty);  // shrink in place, keeps priority

  // Match `taker` against the opposite side in price-time order.
  // on_fill(maker, price, qty) runs after both quantities are updated; a
  // maker with qty == 0 has already been unlinked and must be freed by the caller.
  template <class OnFill>
  Qty match(Order& taker, OnFill&& on_fill) {
    const int s = idx(opposite(taker.side));
    Qty filled = 0;
    while (taker.qty > 0) {
      const Price px = best_[s];
      if (px == kNoPrice) break;
      if (taker.type == OrderType::Limit && !crosses(taker.side, taker.price, px)) break;
      Level& lvl = levels_[s][static_cast<size_t>(px)];
      while (taker.qty > 0 && lvl.head != kNull) {
        Order& maker = pool_.at(lvl.head);
        const Qty q = std::min(taker.qty, maker.qty);
        maker.qty -= q;
        maker.filled += q;
        taker.qty -= q;
        taker.filled += q;
        lvl.qty -= q;
        filled += q;
        if (maker.qty == 0) unlink(lvl, maker);
        on_fill(maker, px, q);
      }
    }
    return filled;
  }

  Price best(Side s) const { return best_[idx(s)]; }
  const Level& level(Side s, Price p) const { return levels_[idx(s)][static_cast<size_t>(p)]; }
  TopOfBook top() const;
  size_t resting() const { return resting_; }

 private:
  static bool crosses(Side taker, Price taker_px, Price book_px) {
    return taker == Side::Buy ? taker_px >= book_px : taker_px <= book_px;
  }
  void unlink(Level& lvl, Order& o);
  void level_emptied(Side side, Price p);
  Price scan_up(int s, Price from) const;    // lowest set price >= from
  Price scan_down(int s, Price from) const;  // highest set price <= from

  OrderPool& pool_;
  Price max_price_;
  size_t resting_ = 0;
  std::vector<Level> levels_[2];
  std::vector<uint64_t> bits_[2];
  Price best_[2] = {kNoPrice, kNoPrice};
};

}  // namespace matchbox
