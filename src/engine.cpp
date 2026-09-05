#include "matchbox/engine.hpp"

#include "matchbox/clock.hpp"

namespace matchbox {

Engine::Engine(Config cfg, EventSink& sink) : cfg_(cfg), sink_(sink), pool_(cfg.max_orders) {
  for (uint16_t i = 0; i < cfg.symbols; ++i) books_.push_back(std::make_unique<Book>(pool_, cfg.max_price));
}

Event Engine::base(Event::Kind k, const Order& o) const {
  Event e;
  e.kind = k;
  e.side = o.side;
  e.symbol = o.symbol;
  e.session = o.session;
  e.tag = o.tag;
  e.order_id = o.id;
  e.price = o.price;
  e.qty = o.qty;
  e.remaining = o.qty;
  e.ts_ns = now_ns_;
  return e;
}

void Engine::reject(const Command& c, RejectReason r) {
  Event e;
  e.kind = Event::Kind::Rejected;
  e.reason = r;
  e.session = c.session;
  e.tag = c.tag;
  e.order_id = c.order_id;
  e.symbol = c.symbol;
  e.ts_ns = now_ns_;
  sink_.on_event(e);
}

void Engine::process(const Command& c) {
  now_ns_ = now_ns();
  Book* book = nullptr;
  Order* o = nullptr;
  if (c.kind == Command::Kind::New) {
    if (c.symbol >= books_.size()) return reject(c, RejectReason::BadSymbol);
    book = books_[c.symbol].get();
  } else {
    o = pool_.find(c.order_id);
    if (!o) return reject(c, RejectReason::UnknownOrder);
    if (o->session != c.session) return reject(c, RejectReason::NotOwner);
    book = books_[o->symbol].get();
  }
  const uint16_t symbol = o ? o->symbol : c.symbol;
  const TopOfBook before = book->top();

  switch (c.kind) {
    case Command::Kind::New: on_new(c, *book); break;
    case Command::Kind::Cancel: on_cancel(*book, *o); break;
    case Command::Kind::Replace: on_replace(c, *book, *o); break;
  }

  const TopOfBook after = book->top();
  if (after != before) {
    Event e;
    e.kind = Event::Kind::Top;
    e.symbol = symbol;
    e.top = after;
    e.ts_ns = now_ns_;
    sink_.on_event(e);
  }
}

void Engine::on_new(const Command& c, Book& book) {
  if (c.qty == 0) return reject(c, RejectReason::BadQty);
  if (c.type == OrderType::Limit && !book.valid_price(c.price)) return reject(c, RejectReason::BadPrice);
  Order* o = pool_.alloc();
  if (!o) return reject(c, RejectReason::BookFull);
  o->tag = c.tag;
  o->session = c.session;
  o->symbol = c.symbol;
  o->side = c.side;
  o->type = c.type;
  o->tif = c.tif;
  o->price = c.type == OrderType::Limit ? c.price : 0;
  o->qty = c.qty;
  sink_.on_event(base(Event::Kind::Accepted, *o));
  match_and_rest(book, *o);
}

void Engine::on_cancel(Book& book, Order& o) {
  book.remove(o);
  sink_.on_event(base(Event::Kind::Cancelled, o));
  pool_.free(o);
}

void Engine::on_replace(const Command& c, Book& book, Order& o) {
  if (c.qty == 0) return reject(c, RejectReason::BadQty);
  if (!book.valid_price(c.price)) return reject(c, RejectReason::BadPrice);
  if (c.price == o.price && c.qty <= o.qty) {
    // Same price, no size increase: keep queue position.
    book.reduce(o, c.qty);
    sink_.on_event(base(Event::Kind::Replaced, o));
    return;
  }
  book.remove(o);
  o.price = c.price;
  o.qty = c.qty;
  sink_.on_event(base(Event::Kind::Replaced, o));
  match_and_rest(book, o);
}

void Engine::match_and_rest(Book& book, Order& o) {
  book.match(o, [&](Order& maker, Price px, Qty q) {
    Event m = base(Event::Kind::Fill, maker);
    m.maker = true;
    m.price = px;
    m.qty = q;
    sink_.on_event(m);

    Event t = base(Event::Kind::Fill, o);
    t.price = px;
    t.qty = q;
    sink_.on_event(t);

    Event tr;
    tr.kind = Event::Kind::Trade;
    tr.side = o.side;
    tr.symbol = o.symbol;
    tr.price = px;
    tr.qty = q;
    tr.ts_ns = now_ns_;
    sink_.on_event(tr);

    if (maker.qty == 0) pool_.free(maker);
  });
  if (o.qty == 0) {
    pool_.free(o);
    return;
  }
  if (o.type == OrderType::Market || o.tif == Tif::IOC) {
    sink_.on_event(base(Event::Kind::Cancelled, o));
    pool_.free(o);
    return;
  }
  book.insert(o);
}

}  // namespace matchbox
