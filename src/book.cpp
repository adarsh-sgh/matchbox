#include "matchbox/book.hpp"

namespace matchbox {

Book::Book(OrderPool& pool, Price max_price) : pool_(pool), max_price_(max_price) {
  for (int s = 0; s < 2; ++s) {
    levels_[s].resize(static_cast<size_t>(max_price) + 1);
    bits_[s].resize((static_cast<size_t>(max_price) >> 6) + 1, 0);
  }
}

void Book::insert(Order& o) {
  const int s = idx(o.side);
  Level& lvl = levels_[s][static_cast<size_t>(o.price)];
  const uint32_t slot = pool_.slot_of(o);
  o.prev = lvl.tail;
  o.next = kNull;
  if (lvl.tail != kNull)
    pool_.at(lvl.tail).next = slot;
  else
    lvl.head = slot;
  lvl.tail = slot;
  lvl.qty += o.qty;
  ++lvl.count;
  ++resting_;
  if (lvl.count == 1) {
    bits_[s][static_cast<size_t>(o.price) >> 6] |= uint64_t(1) << (o.price & 63);
    const bool better = o.side == Side::Buy ? o.price > best_[s] : o.price < best_[s];
    if (best_[s] == kNoPrice || better) best_[s] = o.price;
  }
}

void Book::remove(Order& o) { unlink(levels_[idx(o.side)][static_cast<size_t>(o.price)], o); }

void Book::reduce(Order& o, Qty new_qty) {
  Level& lvl = levels_[idx(o.side)][static_cast<size_t>(o.price)];
  lvl.qty -= o.qty - new_qty;
  o.qty = new_qty;
}

void Book::unlink(Level& lvl, Order& o) {
  if (o.prev != kNull)
    pool_.at(o.prev).next = o.next;
  else
    lvl.head = o.next;
  if (o.next != kNull)
    pool_.at(o.next).prev = o.prev;
  else
    lvl.tail = o.prev;
  lvl.qty -= o.qty;
  --lvl.count;
  --resting_;
  o.prev = o.next = kNull;
  if (lvl.count == 0) level_emptied(o.side, o.price);
}

void Book::level_emptied(Side side, Price p) {
  const int s = idx(side);
  bits_[s][static_cast<size_t>(p) >> 6] &= ~(uint64_t(1) << (p & 63));
  if (best_[s] == p) best_[s] = side == Side::Buy ? scan_down(s, p - 1) : scan_up(s, p + 1);
}

Price Book::scan_up(int s, Price from) const {
  if (from > max_price_) return kNoPrice;
  size_t w = static_cast<size_t>(from) >> 6;
  uint64_t word = bits_[s][w] & (~uint64_t(0) << (from & 63));
  for (;;) {
    if (word) return static_cast<Price>((w << 6) + __builtin_ctzll(word));
    if (++w == bits_[s].size()) return kNoPrice;
    word = bits_[s][w];
  }
}

Price Book::scan_down(int s, Price from) const {
  if (from < 1) return kNoPrice;
  size_t w = static_cast<size_t>(from) >> 6;
  uint64_t word = bits_[s][w] & (~uint64_t(0) >> (63 - (from & 63)));
  for (;;) {
    if (word) return static_cast<Price>((w << 6) + 63 - __builtin_clzll(word));
    if (w == 0) return kNoPrice;
    word = bits_[s][--w];
  }
}

TopOfBook Book::top() const {
  TopOfBook t;
  t.bid = best_[0];
  t.ask = best_[1];
  if (t.bid != kNoPrice) t.bid_qty = levels_[0][static_cast<size_t>(t.bid)].qty;
  if (t.ask != kNoPrice) t.ask_qty = levels_[1][static_cast<size_t>(t.ask)].qty;
  return t;
}

}  // namespace matchbox
