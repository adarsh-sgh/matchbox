// Randomised differential test: the flat-array book against a std::map model.
#include <deque>
#include <functional>
#include <map>
#include <random>
#include <vector>

#include "doctest.h"
#include "matchbox/engine.hpp"

using namespace matchbox;

namespace {

struct RefFill {
  OrderId maker;
  Price px;
  Qty qty;
  bool operator==(const RefFill& o) const { return maker == o.maker && px == o.px && qty == o.qty; }
};

struct RefBook {
  struct RO {
    OrderId id;
    Qty qty;
  };
  std::map<Price, std::deque<RO>, std::greater<Price>> bids;
  std::map<Price, std::deque<RO>> asks;

  template <class Map>
  static void take(Map& m, Price limit, Qty& qty, bool limit_order, std::vector<RefFill>& fills, bool buy) {
    while (qty > 0 && !m.empty()) {
      auto it = m.begin();
      if (limit_order && (buy ? limit < it->first : limit > it->first)) break;
      auto& q = it->second;
      while (qty > 0 && !q.empty()) {
        const Qty x = std::min(qty, q.front().qty);
        fills.push_back({q.front().id, it->first, x});
        qty -= x;
        q.front().qty -= x;
        if (q.front().qty == 0) q.pop_front();
      }
      if (q.empty()) m.erase(it);
    }
  }

  std::vector<RefFill> add(OrderId id, Side s, Price px, Qty qty, bool rest) {
    std::vector<RefFill> fills;
    if (s == Side::Buy)
      take(asks, px, qty, true, fills, true);
    else
      take(bids, px, qty, true, fills, false);
    if (qty > 0 && rest) {
      if (s == Side::Buy)
        bids[px].push_back({id, qty});
      else
        asks[px].push_back({id, qty});
    }
    return fills;
  }

  template <class Map>
  static bool erase_from(Map& m, OrderId id, Qty* out) {
    for (auto it = m.begin(); it != m.end(); ++it)
      for (auto q = it->second.begin(); q != it->second.end(); ++q)
        if (q->id == id) {
          if (out) *out = q->qty;
          it->second.erase(q);
          if (it->second.empty()) m.erase(it);
          return true;
        }
    return false;
  }
  bool cancel(OrderId id) { return erase_from(bids, id, nullptr) || erase_from(asks, id, nullptr); }

  template <class Map>
  static RO* find_in(Map& m, OrderId id, Price* px) {
    for (auto& kv : m)
      for (auto& o : kv.second)
        if (o.id == id) {
          *px = kv.first;
          return &o;
        }
    return nullptr;
  }

  TopOfBook top() const {
    TopOfBook t;
    if (!bids.empty()) {
      t.bid = bids.begin()->first;
      for (auto& o : bids.begin()->second) t.bid_qty += o.qty;
    }
    if (!asks.empty()) {
      t.ask = asks.begin()->first;
      for (auto& o : asks.begin()->second) t.ask_qty += o.qty;
    }
    return t;
  }
};

struct Collector : EventSink {
  std::vector<Event> events;
  void on_event(const Event& e) override { events.push_back(e); }
};

// Every level's qty/count must equal the sum over its intrusive list, and the
// best pointers must agree with a brute-force scan.
void check_invariants(const Engine& engine, const Book& book) {
  const OrderPool& pool = engine.pool();
  for (Side s : {Side::Buy, Side::Sell}) {
    Price best = kNoPrice;
    for (Price p = 1; p <= book.max_price(); ++p) {
      const Level& lvl = book.level(s, p);
      Qty sum = 0;
      uint32_t n = 0;
      uint32_t prev = kNull;
      for (uint32_t i = lvl.head; i != kNull; i = pool.at(i).next) {
        const Order& o = pool.at(i);
        REQUIRE(o.live);
        REQUIRE(o.side == s);
        REQUIRE(o.price == p);
        REQUIRE(o.prev == prev);
        sum += o.qty;
        ++n;
        prev = i;
      }
      REQUIRE(lvl.tail == prev);
      REQUIRE(lvl.qty == sum);
      REQUIRE(lvl.count == n);
      if (n && (best == kNoPrice || (s == Side::Buy ? p > best : p < best))) best = p;
    }
    REQUIRE(book.best(s) == best);
  }
  if (book.best(Side::Buy) != kNoPrice && book.best(Side::Sell) != kNoPrice)
    REQUIRE(book.best(Side::Buy) < book.best(Side::Sell));
}

}  // namespace

TEST_CASE("book: random adds/cancels/replaces match a std::map reference model") {
  Collector sink;
  Engine::Config cfg;
  cfg.symbols = 1;
  cfg.max_price = 300;
  cfg.max_orders = 4096;
  Engine engine(cfg, sink);
  RefBook ref;
  std::mt19937_64 rng(1234);
  std::vector<OrderId> live;

  for (int step = 0; step < 30000; ++step) {
    const int roll = static_cast<int>(rng() % 100);
    sink.events.clear();
    Command c;
    c.session = 1;
    c.tag = static_cast<uint64_t>(step);
    std::vector<RefFill> expect;

    if (roll < 65 || live.empty()) {
      c.kind = Command::Kind::New;
      c.side = static_cast<Side>(rng() & 1);
      c.tif = (rng() % 10 == 0) ? Tif::IOC : Tif::GTC;
      c.price = 100 + static_cast<Price>(rng() % 60);
      c.qty = 1 + static_cast<Qty>(rng() % 50);
      engine.process(c);
      const Event& acc = sink.events.front();
      REQUIRE(acc.kind == Event::Kind::Accepted);
      expect = ref.add(acc.order_id, c.side, c.price, c.qty, c.tif == Tif::GTC);
      if (engine.pool().find(acc.order_id)) live.push_back(acc.order_id);
    } else if (roll < 85) {
      const size_t k = rng() % live.size();
      c.kind = Command::Kind::Cancel;
      c.order_id = live[k];
      live[k] = live.back();
      live.pop_back();
      engine.process(c);
      REQUIRE(sink.events.front().kind == Event::Kind::Cancelled);
      REQUIRE(ref.cancel(c.order_id));
    } else {
      const size_t k = rng() % live.size();
      c.kind = Command::Kind::Replace;
      c.order_id = live[k];
      c.price = 100 + static_cast<Price>(rng() % 60);
      c.qty = 1 + static_cast<Qty>(rng() % 50);
      const Order* o = engine.pool().find(c.order_id);
      REQUIRE(o);
      const Side side = o->side;
      Price old_px = 0;
      RefBook::RO* ro = side == Side::Buy ? RefBook::find_in(ref.bids, c.order_id, &old_px)
                                          : RefBook::find_in(ref.asks, c.order_id, &old_px);
      REQUIRE(ro);
      if (old_px == c.price && c.qty <= ro->qty) {
        ro->qty = c.qty;
      } else {
        ref.cancel(c.order_id);
        expect = ref.add(c.order_id, side, c.price, c.qty, true);
      }
      engine.process(c);
      REQUIRE(sink.events.front().kind == Event::Kind::Replaced);
      if (!engine.pool().find(c.order_id)) {
        live[k] = live.back();
        live.pop_back();
      }
    }

    // Compare maker-side fills (order, price, qty) with the reference.
    std::vector<RefFill> got;
    for (auto& e : sink.events)
      if (e.kind == Event::Kind::Fill && e.maker) got.push_back({e.order_id, e.price, e.qty});
    REQUIRE(got == expect);
    // Fully filled makers are gone from the live set.
    for (auto& g : got)
      if (!engine.pool().find(g.maker))
        for (size_t i = 0; i < live.size(); ++i)
          if (live[i] == g.maker) {
            live[i] = live.back();
            live.pop_back();
            break;
          }
    REQUIRE(engine.book(0).top() == ref.top());
    if (step % 1000 == 0) check_invariants(engine, engine.book(0));
  }
  check_invariants(engine, engine.book(0));
  CHECK(engine.pool().live() == live.size());
  CHECK(engine.book(0).resting() == live.size());
}
