#include <vector>

#include "doctest.h"
#include "matchbox/protocol.hpp"

using namespace matchbox;

TEST_CASE("protocol: frames round-trip through bytes and size_for covers every type") {
  auto n = wire::make<wire::NewOrder>(7);
  n.tag = 0xdeadbeef;
  n.symbol = 3;
  n.side = 1;
  n.price = -5;  // engine will reject it, but the wire carries any int64
  n.qty = 42;
  std::vector<uint8_t> buf;
  wire::append(buf, n);
  auto c = wire::make<wire::CancelOrder>(8);
  c.order_id = 1234;
  wire::append(buf, c);
  REQUIRE(buf.size() == sizeof(wire::NewOrder) + sizeof(wire::CancelOrder));

  // Little-endian on the wire: len=40 -> 0x28 0x00, then type, version, seq.
  CHECK(buf[0] == 40);
  CHECK(buf[1] == 0);
  CHECK(buf[2] == static_cast<uint8_t>(wire::MsgType::NewOrder));
  CHECK(buf[3] == wire::kVersion);
  CHECK(buf[4] == 7);

  const wire::Header h0 = wire::read_header(buf.data());
  CHECK(h0.len == sizeof(wire::NewOrder));
  CHECK(wire::size_for(static_cast<wire::MsgType>(h0.type)) == h0.len);
  const auto n2 = wire::read<wire::NewOrder>(buf.data());
  CHECK(n2.tag == 0xdeadbeef);
  CHECK(n2.symbol == 3);
  CHECK(n2.side == 1);
  CHECK(n2.price == -5);
  CHECK(n2.qty == 42);

  const wire::Header h1 = wire::read_header(buf.data() + h0.len);
  CHECK(h1.seq == 8);
  CHECK(wire::read<wire::CancelOrder>(buf.data() + h0.len).order_id == 1234);

  for (auto t : {wire::MsgType::NewOrder, wire::MsgType::CancelOrder, wire::MsgType::ReplaceOrder, wire::MsgType::Ack,
                 wire::MsgType::Reject, wire::MsgType::Fill, wire::MsgType::Cancelled, wire::MsgType::Replaced,
                 wire::MsgType::Trade, wire::MsgType::Top}) {
    CHECK(wire::size_for(t) >= sizeof(wire::Header));
    CHECK(wire::size_for(t) <= wire::kMaxFrame);
  }
  CHECK(wire::size_for(static_cast<wire::MsgType>(200)) == 0);
}
