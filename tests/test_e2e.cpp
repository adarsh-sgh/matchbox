// Full path: TCP client -> gateway -> SPSC ring -> engine -> ring -> gateway -> client + market data.
#include <atomic>
#include <thread>
#include <unistd.h>
#include <vector>

#include "doctest.h"
#include "matchbox/engine_thread.hpp"
#include "matchbox/net.hpp"
#include "matchbox/server.hpp"

using namespace matchbox;

namespace {

struct Frame {
  wire::MsgType type;
  uint8_t bytes[wire::kMaxFrame];
  template <class M>
  M as() const {
    REQUIRE(type == M::kType);
    return wire::read<M>(bytes);
  }
};

bool read_frame(int fd, Frame& f) {
  if (!read_full(fd, f.bytes, sizeof(wire::Header))) return false;
  const wire::Header h = wire::read_header(f.bytes);
  REQUIRE(h.len <= wire::kMaxFrame);
  if (!read_full(fd, f.bytes + sizeof h, h.len - sizeof h)) return false;
  f.type = static_cast<wire::MsgType>(h.type);
  return true;
}

Frame next(int fd) {
  Frame f;
  do {
    REQUIRE(read_frame(fd, f));
  } while (f.type == wire::MsgType::Heartbeat);  // idle-stream keepalives are timing dependent
  return f;
}

template <class M>
void send(int fd, const M& m) {
  REQUIRE(write_full(fd, &m, sizeof m));
}

}  // namespace

TEST_CASE("e2e: two clients trade over TCP, market data fans out, bad seq disconnects") {
  Engine::Config ecfg;
  ecfg.symbols = 1;
  ecfg.max_price = 5000;
  ecfg.max_orders = 1024;
  EngineThread engine(ecfg, 10);
  Server::Config scfg;
  scfg.port = 0;
  scfg.md_port = 0;
  Server server(scfg, engine.inbox(), engine.outbox());
  std::atomic<bool> stop{false};
  engine.start();
  std::thread net([&] { server.run(stop); });

  const int md = connect_tcp("127.0.0.1", server.md_port());
  const int c1 = connect_tcp("127.0.0.1", server.port());
  const int c2 = connect_tcp("127.0.0.1", server.port());
  REQUIRE(md >= 0);
  REQUIRE(c1 >= 0);
  REQUIRE(c2 >= 0);
  // Subscribe live and give the gateway a moment to see it before the first trade.
  send(md, wire::make<wire::Subscribe>());
  usleep(20000);

  auto n1 = wire::make<wire::NewOrder>(1);
  n1.tag = 11;
  n1.side = static_cast<uint8_t>(Side::Buy);
  n1.price = 1000;
  n1.qty = 100;
  send(c1, n1);
  const auto ack1 = next(c1).as<wire::Ack>();
  CHECK(ack1.tag == 11);
  CHECK(ack1.hdr.seq == 1);
  const OrderId bid_id = ack1.order_id;

  auto n2 = wire::make<wire::NewOrder>(1);
  n2.tag = 22;
  n2.side = static_cast<uint8_t>(Side::Sell);
  n2.price = 999;
  n2.qty = 40;
  send(c2, n2);
  CHECK(next(c2).as<wire::Ack>().tag == 22);
  const auto taker = next(c2).as<wire::Fill>();
  CHECK(taker.price == 1000);
  CHECK(taker.qty == 40);
  CHECK(taker.remaining == 0);
  CHECK(taker.maker == 0);

  const auto maker = next(c1).as<wire::Fill>();
  CHECK(maker.order_id == bid_id);
  CHECK(maker.qty == 40);
  CHECK(maker.remaining == 60);
  CHECK(maker.maker == 1);

  // Market data: top after the bid rested, then the trade, then the new top.
  const auto top1 = next(md).as<wire::Top>();
  CHECK(top1.bid == 1000);
  CHECK(top1.bid_qty == 100);
  CHECK(top1.ask == -1);
  const auto tr = next(md).as<wire::Trade>();
  CHECK(tr.price == 1000);
  CHECK(tr.qty == 40);
  CHECK(tr.aggressor == static_cast<uint8_t>(Side::Sell));
  const auto top2 = next(md).as<wire::Top>();
  CHECK(top2.bid_qty == 60);
  CHECK(top2.hdr.seq == 3);

  // c2 may not cancel c1's order.
  auto cx = wire::make<wire::CancelOrder>(2);
  cx.tag = 33;
  cx.order_id = bid_id;
  send(c2, cx);
  const auto rj = next(c2).as<wire::Reject>();
  CHECK(rj.tag == 33);
  CHECK(rj.reason == static_cast<uint8_t>(RejectReason::NotOwner));

  // Owner replaces down in size, then a sequence gap gets c1 disconnected.
  auto rp = wire::make<wire::ReplaceOrder>(2);
  rp.tag = 44;
  rp.order_id = bid_id;
  rp.price = 1000;
  rp.qty = 10;
  send(c1, rp);
  const auto rpd = next(c1).as<wire::Replaced>();
  CHECK(rpd.qty == 10);
  CHECK(next(md).as<wire::Top>().bid_qty == 10);

  send(c1, wire::make<wire::CancelOrder>(9));
  const auto bad = next(c1).as<wire::Reject>();
  CHECK(bad.reason == static_cast<uint8_t>(RejectReason::BadSeq));
  Frame eof;
  CHECK_FALSE(read_frame(c1, eof));

  // Subscriber drops, misses the cancel's Top update, and resumes from where it stopped.
  ::close(md);
  auto cx2 = wire::make<wire::CancelOrder>(3);
  cx2.tag = 55;
  cx2.order_id = bid_id;
  send(c2, cx2);  // c2 cancelling c1's order: reject, no market data
  CHECK(next(c2).as<wire::Reject>().reason == static_cast<uint8_t>(RejectReason::NotOwner));
  usleep(20000);
  const int md2 = connect_tcp("127.0.0.1", server.md_port());
  REQUIRE(md2 >= 0);
  auto sub = wire::make<wire::Subscribe>();
  sub.from_seq = 3;  // already saw 1..3 (top, trade, top); 4 was the replace's top
  send(md2, sub);
  CHECK(next(md2).as<wire::Top>().hdr.seq == 3);
  const auto top4 = next(md2).as<wire::Top>();
  CHECK(top4.hdr.seq == 4);
  CHECK(top4.bid_qty == 10);
  ::close(md2);

  ::close(c1);
  ::close(c2);
  stop.store(true);
  net.join();
  engine.stop();
  CHECK(engine.engine().pool().live() == 1);
}
