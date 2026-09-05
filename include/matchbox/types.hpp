#pragma once
#include <cstdint>

namespace matchbox {

using Price = int64_t;     // integer ticks; valid range is [1, max_price]
using Qty = uint32_t;
using OrderId = uint64_t;  // (generation << 32) | pool slot; never 0

constexpr uint32_t kNull = UINT32_MAX;
constexpr Price kNoPrice = -1;

enum class Side : uint8_t { Buy = 0, Sell = 1 };
enum class OrderType : uint8_t { Limit = 0, Market = 1 };
enum class Tif : uint8_t { GTC = 0, IOC = 1 };

enum class RejectReason : uint8_t {
  None = 0, BadSymbol, BadPrice, BadQty, BookFull, UnknownOrder, NotOwner, BadSeq, BadMessage,
};

inline Side opposite(Side s) { return s == Side::Buy ? Side::Sell : Side::Buy; }
inline int idx(Side s) { return static_cast<int>(s); }

inline const char* to_string(RejectReason r) {
  static const char* const names[] = {"none",          "bad_symbol", "bad_price", "bad_qty", "book_full",
                                      "unknown_order", "not_owner",  "bad_seq",   "bad_message"};
  return names[static_cast<int>(r)];
}

struct TopOfBook {
  Price bid = kNoPrice;
  Price ask = kNoPrice;
  Qty bid_qty = 0;
  Qty ask_qty = 0;
  bool operator==(const TopOfBook& o) const {
    return bid == o.bid && ask == o.ask && bid_qty == o.bid_qty && ask_qty == o.ask_qty;
  }
  bool operator!=(const TopOfBook& o) const { return !(*this == o); }
};

}  // namespace matchbox
