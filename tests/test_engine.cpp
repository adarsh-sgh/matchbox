#include <vector>

#include "doctest.h"
#include "matchbox/engine.hpp"

using namespace matchbox;

namespace {

struct Recorder : EventSink {
  std::vector<Event> events;
  void on_event(const Event& e) override { events.push_back(e); }
  void clear() { events.clear(); }
  size_t count(Event::Kind k) const {
    size_t n = 0;
    for (auto& e : events) n += e.kind == k;
    return n;
  }
  const Event* first(Event::Kind k) const {
    for (auto& e : events)
      if (e.kind == k) return &e;
    return nullptr;
  }
  // Taker-side fills in order: (price, qty)
  std::vector<std::pair<Price, Qty>> taker_fills() const {
    std::vector<std::pair<Price, Qty>> v;
    for (auto& e : events)
      if (e.kind == Event::Kind::Fill && !e.maker) v.emplace_back(e.price, e.qty);
    return v;
  }
};

struct Fixture {
  Recorder rec;
  Engine engine;
  Fixture(uint32_t max_orders = 1024) : engine(cfg(max_orders), rec) {}
  static Engine::Config cfg(uint32_t max_orders) {
    Engine::Config c;
    c.symbols = 2;
    c.max_price = 1000;
    c.max_orders = max_orders;
    return c;
  }
  OrderId limit(Side s, Price px, Qty q, Tif tif = Tif::GTC, uint32_t session = 1, uint16_t sym = 0) {
    Command c;
    c.kind = Command::Kind::New;
    c.session = session;
    c.symbol = sym;
    c.side = s;
    c.price = px;
    c.qty = q;
    c.tif = tif;
    c.tag = ++tag;
    rec.clear();
    engine.process(c);
    const Event* a = rec.first(Event::Kind::Accepted);
    return a ? a->order_id : 0;
  }
  void market(Side s, Qty q) {
    Command c;
    c.kind = Command::Kind::New;
    c.session = 1;
    c.side = s;
    c.type = OrderType::Market;
    c.qty = q;
    rec.clear();
    engine.process(c);
  }
  void cancel(OrderId id, uint32_t session = 1) {
    Command c;
    c.kind = Command::Kind::Cancel;
    c.session = session;
    c.order_id = id;
    rec.clear();
    engine.process(c);
  }
  void replace(OrderId id, Price px, Qty q) {
    Command c;
    c.kind = Command::Kind::Replace;
    c.session = 1;
    c.order_id = id;
    c.price = px;
    c.qty = q;
    rec.clear();
    engine.process(c);
  }
  uint64_t tag = 0;
};

}  // namespace

TEST_CASE("engine: price-time priority, partial fills, top-of-book events") {
  Fixture f;
  const OrderId a = f.limit(Side::Sell, 101, 10);
  const OrderId b = f.limit(Side::Sell, 101, 5);
  const OrderId c = f.limit(Side::Sell, 100, 7);
  CHECK(f.engine.book(0).top() == TopOfBook{kNoPrice, 100, 0, 7});

  // Buy 12 @ 101: best price first (c @100 x7), then oldest at 101 (a x5).
  const OrderId t = f.limit(Side::Buy, 101, 12);
  CHECK(f.rec.taker_fills() == std::vector<std::pair<Price, Qty>>{{100, 7}, {101, 5}});
  CHECK(f.rec.count(Event::Kind::Trade) == 2);
  CHECK(f.rec.count(Event::Kind::Fill) == 4);
  CHECK(f.rec.count(Event::Kind::Top) == 1);
  CHECK(f.engine.pool().live() == 2);  // a (5 left) and b
  CHECK(f.engine.book(0).top() == TopOfBook{kNoPrice, 101, 0, 10});
  CHECK(f.engine.book(0).level(Side::Sell, 101).count == 2);
  (void)t;

  // Maker fill carries remaining and the maker flag; b was never touched.
  bool saw_a = false;
  for (auto& e : f.rec.events)
    if (e.kind == Event::Kind::Fill && e.maker && e.order_id == a) {
      saw_a = true;
      CHECK(e.remaining == 5);
      CHECK(e.qty == 5);
    }
  CHECK(saw_a);
  (void)b;
  (void)c;

  // Second symbol is independent.
  f.limit(Side::Buy, 101, 1, Tif::GTC, 1, 1);
  CHECK(f.rec.count(Event::Kind::Fill) == 0);
  CHECK(f.engine.book(1).top().bid == 101);
}

TEST_CASE("engine: IOC and market orders never rest") {
  Fixture f;
  f.limit(Side::Sell, 105, 20);
  f.limit(Side::Buy, 105, 100, Tif::IOC);
  CHECK(f.rec.taker_fills() == std::vector<std::pair<Price, Qty>>{{105, 20}});
  const Event* c = f.rec.first(Event::Kind::Cancelled);
  REQUIRE(c);
  CHECK(c->remaining == 80);
  CHECK(f.engine.book(0).top() == TopOfBook{});
  CHECK(f.engine.pool().live() == 0);

  f.market(Side::Sell, 5);
  CHECK(f.rec.count(Event::Kind::Accepted) == 1);
  CHECK(f.rec.count(Event::Kind::Cancelled) == 1);
  CHECK(f.rec.count(Event::Kind::Fill) == 0);

  f.limit(Side::Buy, 90, 3);
  f.limit(Side::Buy, 95, 3);
  f.market(Side::Sell, 10);  // sweeps both bids, best first
  CHECK(f.rec.taker_fills() == std::vector<std::pair<Price, Qty>>{{95, 3}, {90, 3}});
  CHECK(f.rec.first(Event::Kind::Cancelled)->remaining == 4);
  CHECK(f.engine.pool().live() == 0);
}

TEST_CASE("engine: cancel and replace priority rules") {
  Fixture f;
  const OrderId x = f.limit(Side::Buy, 50, 10);
  const OrderId y = f.limit(Side::Buy, 50, 10);

  // Reducing quantity keeps queue position: x still fills first.
  f.replace(x, 50, 4);
  CHECK(f.rec.count(Event::Kind::Replaced) == 1);
  CHECK(f.engine.book(0).level(Side::Buy, 50).qty == 14);
  f.limit(Side::Sell, 50, 1);
  CHECK(f.rec.first(Event::Kind::Fill)->order_id == x);

  // Increasing quantity moves to the back: y fills before x.
  f.replace(x, 50, 20);
  f.limit(Side::Sell, 50, 1);
  CHECK(f.rec.first(Event::Kind::Fill)->order_id == y);

  // Replace to a crossing price executes immediately, id unchanged.
  f.limit(Side::Sell, 60, 5);
  f.replace(x, 60, 20);
  CHECK(f.rec.first(Event::Kind::Replaced)->order_id == x);
  CHECK(f.rec.taker_fills() == std::vector<std::pair<Price, Qty>>{{60, 5}});
  CHECK(f.engine.book(0).top().bid == 60);
  CHECK(f.engine.book(0).level(Side::Buy, 60).qty == 15);

  f.cancel(x);
  CHECK(f.rec.first(Event::Kind::Cancelled)->remaining == 15);
  f.cancel(x);
  CHECK(f.rec.first(Event::Kind::Rejected)->reason == RejectReason::UnknownOrder);
  CHECK(f.engine.book(0).top().bid == 50);
  CHECK(f.engine.pool().live() == 1);
}

TEST_CASE("engine: rejects") {
  Fixture f(2);
  f.limit(Side::Buy, 0, 1);
  CHECK(f.rec.first(Event::Kind::Rejected)->reason == RejectReason::BadPrice);
  f.limit(Side::Buy, 1001, 1);
  CHECK(f.rec.first(Event::Kind::Rejected)->reason == RejectReason::BadPrice);
  f.limit(Side::Buy, 10, 0);
  CHECK(f.rec.first(Event::Kind::Rejected)->reason == RejectReason::BadQty);
  f.limit(Side::Buy, 10, 1, Tif::GTC, 1, 9);
  CHECK(f.rec.first(Event::Kind::Rejected)->reason == RejectReason::BadSymbol);

  const OrderId a = f.limit(Side::Buy, 10, 1);
  f.limit(Side::Buy, 11, 1);
  f.limit(Side::Buy, 12, 1);  // pool of 2 is full
  CHECK(f.rec.first(Event::Kind::Rejected)->reason == RejectReason::BookFull);

  f.cancel(a, 2);
  CHECK(f.rec.first(Event::Kind::Rejected)->reason == RejectReason::NotOwner);
  f.cancel(a, 1);
  CHECK(f.rec.count(Event::Kind::Cancelled) == 1);
  f.limit(Side::Buy, 12, 1);  // slot recycled
  CHECK(f.rec.count(Event::Kind::Accepted) == 1);
}
