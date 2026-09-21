#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

// Wire protocol: fixed-size packed little-endian frames. Every frame starts
// with a Header; `len` is the total frame size. On the order port `seq` is
// per-direction and per-connection, starting at 1 and incrementing by one per
// frame. On the market-data port `seq` of a Trade/Top frame is the global
// stream sequence (gap-free, shared by every subscriber) so a client can
// detect loss and resume from where it stopped; control frames (Heartbeat,
// Gap) carry seq 0.

#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
#error "matchbox wire protocol assumes a little-endian host"
#endif

namespace matchbox::wire {

constexpr uint8_t kVersion = 1;

enum class MsgType : uint8_t {
  // client -> gateway
  NewOrder = 1,
  CancelOrder = 2,
  ReplaceOrder = 3,
  // client -> market-data port
  Subscribe = 4,
  // gateway -> client
  Ack = 16,
  Reject = 17,
  Fill = 18,
  Cancelled = 19,
  Replaced = 20,
  // market data
  Trade = 32,
  Top = 33,
  Heartbeat = 34,
  Gap = 35,
};

#pragma pack(push, 1)
struct Header {
  uint16_t len;
  uint8_t type;
  uint8_t version;
  uint32_t seq;
};

struct NewOrder {
  static constexpr MsgType kType = MsgType::NewOrder;
  Header hdr;
  uint64_t tag;
  uint16_t symbol;
  uint8_t side;  // Side
  uint8_t type;  // OrderType
  uint8_t tif;   // Tif
  uint8_t pad[3];
  int64_t price;  // ignored for market orders
  uint32_t qty;
  uint32_t pad2;
};

struct CancelOrder {
  static constexpr MsgType kType = MsgType::CancelOrder;
  Header hdr;
  uint64_t tag;
  uint64_t order_id;
};

struct ReplaceOrder {
  static constexpr MsgType kType = MsgType::ReplaceOrder;
  Header hdr;
  uint64_t tag;
  uint64_t order_id;
  int64_t price;
  uint32_t qty;  // new remaining quantity
  uint32_t pad;
};

// First frame a market-data client sends. from_seq 0 means "live from now";
// otherwise the stream resumes at from_seq if the gateway still holds it,
// else at the oldest retained frame after a Gap.
struct Subscribe {
  static constexpr MsgType kType = MsgType::Subscribe;
  Header hdr;
  uint64_t from_seq;
};

struct Ack {
  static constexpr MsgType kType = MsgType::Ack;
  Header hdr;
  uint64_t tag;
  uint64_t order_id;
};

struct Reject {
  static constexpr MsgType kType = MsgType::Reject;
  Header hdr;
  uint64_t tag;
  uint8_t reason;  // RejectReason
  uint8_t pad[7];
};

struct Fill {
  static constexpr MsgType kType = MsgType::Fill;
  Header hdr;
  uint64_t order_id;
  int64_t price;
  uint32_t qty;
  uint32_t remaining;
  uint8_t maker;  // 1 if this order provided liquidity
  uint8_t pad[7];
};

struct Cancelled {
  static constexpr MsgType kType = MsgType::Cancelled;
  Header hdr;
  uint64_t order_id;
  uint32_t remaining;  // quantity removed from the book
  uint32_t pad;
};

struct Replaced {
  static constexpr MsgType kType = MsgType::Replaced;
  Header hdr;
  uint64_t order_id;
  int64_t price;
  uint32_t qty;
  uint32_t pad;
};

struct Trade {
  static constexpr MsgType kType = MsgType::Trade;
  Header hdr;
  uint16_t symbol;
  uint8_t aggressor;  // Side of the taker
  uint8_t pad[5];
  int64_t price;
  uint32_t qty;
  uint32_t pad2;
  uint64_t ts_ns;
};

struct Top {
  static constexpr MsgType kType = MsgType::Top;
  Header hdr;
  uint16_t symbol;
  uint8_t pad[6];
  int64_t bid;  // -1 when empty
  int64_t ask;
  uint32_t bid_qty;
  uint32_t ask_qty;
};

// Sent on an idle stream every heartbeat interval; head_seq is the newest
// stream sequence so a client can tell "quiet" from "stalled".
struct Heartbeat {
  static constexpr MsgType kType = MsgType::Heartbeat;
  Header hdr;
  uint64_t ts_ns;
  uint64_t head_seq;
};

// Frames [from, resumed_at) are gone: either the client resumed from a
// sequence the replay ring no longer holds, or it read too slowly and the
// ring lapped its cursor. The stream continues from resumed_at.
struct Gap {
  static constexpr MsgType kType = MsgType::Gap;
  Header hdr;
  uint64_t from;
  uint64_t resumed_at;
};
#pragma pack(pop)

static_assert(sizeof(Header) == 8, "header layout");
static_assert(sizeof(NewOrder) == 40, "NewOrder layout");
static_assert(sizeof(CancelOrder) == 24, "CancelOrder layout");
static_assert(sizeof(ReplaceOrder) == 40, "ReplaceOrder layout");
static_assert(sizeof(Ack) == 24, "Ack layout");
static_assert(sizeof(Reject) == 24, "Reject layout");
static_assert(sizeof(Fill) == 40, "Fill layout");
static_assert(sizeof(Cancelled) == 24, "Cancelled layout");
static_assert(sizeof(Replaced) == 32, "Replaced layout");
static_assert(sizeof(Trade) == 40, "Trade layout");
static_assert(sizeof(Top) == 40, "Top layout");
static_assert(sizeof(Subscribe) == 16, "Subscribe layout");
static_assert(sizeof(Heartbeat) == 24, "Heartbeat layout");
static_assert(sizeof(Gap) == 24, "Gap layout");

constexpr size_t kMaxFrame = 64;

inline size_t size_for(MsgType t) {
  switch (t) {
    case MsgType::NewOrder: return sizeof(NewOrder);
    case MsgType::CancelOrder: return sizeof(CancelOrder);
    case MsgType::ReplaceOrder: return sizeof(ReplaceOrder);
    case MsgType::Ack: return sizeof(Ack);
    case MsgType::Reject: return sizeof(Reject);
    case MsgType::Fill: return sizeof(Fill);
    case MsgType::Cancelled: return sizeof(Cancelled);
    case MsgType::Replaced: return sizeof(Replaced);
    case MsgType::Trade: return sizeof(Trade);
    case MsgType::Top: return sizeof(Top);
    case MsgType::Subscribe: return sizeof(Subscribe);
    case MsgType::Heartbeat: return sizeof(Heartbeat);
    case MsgType::Gap: return sizeof(Gap);
  }
  return 0;
}

template <class M>
M make(uint32_t seq = 0) {
  M m{};
  m.hdr.len = sizeof(M);
  m.hdr.type = static_cast<uint8_t>(M::kType);
  m.hdr.version = kVersion;
  m.hdr.seq = seq;
  return m;
}

// memcpy in/out so packed structs are never accessed through misaligned pointers.
template <class M>
M read(const uint8_t* p) {
  M m;
  std::memcpy(&m, p, sizeof m);
  return m;
}

inline Header read_header(const uint8_t* p) { return read<Header>(p); }

template <class M>
void append(std::vector<uint8_t>& buf, const M& m) {
  const auto* p = reinterpret_cast<const uint8_t*>(&m);
  buf.insert(buf.end(), p, p + sizeof m);
}

}  // namespace matchbox::wire
