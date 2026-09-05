#pragma once
#include <memory>
#include <vector>

#include "matchbox/book.hpp"
#include "matchbox/order.hpp"
#include "matchbox/types.hpp"

namespace matchbox {

struct Command {
  enum class Kind : uint8_t { New, Cancel, Replace };
  Kind kind = Kind::New;
  Side side = Side::Buy;
  OrderType type = OrderType::Limit;
  Tif tif = Tif::GTC;
  uint16_t symbol = 0;
  uint32_t session = 0;
  uint64_t tag = 0;
  OrderId order_id = 0;  // Cancel / Replace
  Price price = 0;
  Qty qty = 0;
};

struct Event {
  enum class Kind : uint8_t { Accepted, Rejected, Fill, Cancelled, Replaced, Trade, Top };
  Kind kind = Kind::Accepted;
  Side side = Side::Buy;
  RejectReason reason = RejectReason::None;
  bool maker = false;   // Fill: this side provided liquidity
  uint16_t symbol = 0;
  uint32_t session = 0;  // 0 = market data, fan out to subscribers
  uint64_t tag = 0;
  OrderId order_id = 0;
  Price price = 0;
  Qty qty = 0;
  Qty remaining = 0;
  TopOfBook top;
  uint64_t ts_ns = 0;
};

class EventSink {
 public:
  virtual ~EventSink() = default;
  virtual void on_event(const Event& e) = 0;
};

// Single-threaded matching engine over N books. Not thread-safe by design:
// feed it from one thread (see EngineThread).
class Engine {
 public:
  struct Config {
    uint16_t symbols = 4;
    Price max_price = 100000;
    uint32_t max_orders = 1u << 20;
  };

  Engine(Config cfg, EventSink& sink);
  void process(const Command& c);

  const Book& book(uint16_t symbol) const { return *books_[symbol]; }
  const OrderPool& pool() const { return pool_; }
  uint16_t symbols() const { return static_cast<uint16_t>(books_.size()); }

 private:
  void on_new(const Command& c, Book& book);
  void on_cancel(Book& book, Order& o);
  void on_replace(const Command& c, Book& book, Order& o);
  void match_and_rest(Book& book, Order& o);
  void reject(const Command& c, RejectReason r);
  Event base(Event::Kind k, const Order& o) const;

  Config cfg_;
  EventSink& sink_;
  OrderPool pool_;
  std::vector<std::unique_ptr<Book>> books_;
  uint64_t now_ns_ = 0;
};

}  // namespace matchbox
