#!/usr/bin/env python3
"""Replay a CSV of orders against a running matchbox_server and print every response.

CSV columns: action,tag,symbol,side,type,tif,price,qty
  new,1,0,buy,limit,gtc,10000,100
  cancel,1,,,,,,            # tag refers to an earlier `new`
  replace,2,,,,,10000,40    # price/qty are the new values

Stdlib only; speaks the binary wire protocol from include/matchbox/protocol.hpp.
"""
import argparse
import csv
import select
import socket
import struct
import sys

HDR = struct.Struct("<HBBI")
NEW = struct.Struct("<HBBIQHBBB3xqI4x")
CANCEL = struct.Struct("<HBBIQQ")
REPLACE = struct.Struct("<HBBIQQqI4x")
ACK = struct.Struct("<HBBIQQ")
REJECT = struct.Struct("<HBBIQB7x")
FILL = struct.Struct("<HBBIQqIIB7x")
CANCELLED = struct.Struct("<HBBIQI4x")
REPLACED = struct.Struct("<HBBIQqI4x")

T_NEW, T_CANCEL, T_REPLACE = 1, 2, 3
T_ACK, T_REJECT, T_FILL, T_CANCELLED, T_REPLACED = 16, 17, 18, 19, 20
VERSION = 1
REASONS = ["none", "bad_symbol", "bad_price", "bad_qty", "book_full",
           "unknown_order", "not_owner", "bad_seq", "bad_message"]
SIDES = {"buy": 0, "sell": 1}
TYPES = {"limit": 0, "market": 1}
TIFS = {"gtc": 0, "ioc": 1}


class Session:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.seq = 1
        self.buf = b""
        self.ids = {}  # tag -> order_id from Ack

    def _hdr(self, size, mtype):
        h = HDR.pack(size, mtype, VERSION, self.seq)
        self.seq += 1
        return h

    def send_new(self, tag, symbol, side, otype, tif, price, qty):
        h = self._hdr(NEW.size, T_NEW)
        body = NEW.pack(0, 0, 0, 0, tag, symbol, side, otype, tif, price, qty)[HDR.size:]
        self.sock.sendall(h + body)

    def send_cancel(self, tag, order_id):
        h = self._hdr(CANCEL.size, T_CANCEL)
        self.sock.sendall(h + CANCEL.pack(0, 0, 0, 0, tag, order_id)[HDR.size:])

    def send_replace(self, tag, order_id, price, qty):
        h = self._hdr(REPLACE.size, T_REPLACE)
        self.sock.sendall(h + REPLACE.pack(0, 0, 0, 0, tag, order_id, price, qty)[HDR.size:])

    def drain(self, timeout):
        """Print whatever the gateway sent within `timeout` seconds."""
        while True:
            r, _, _ = select.select([self.sock], [], [], timeout)
            if not r:
                return True
            chunk = self.sock.recv(65536)
            if not chunk:
                print("gateway closed the connection")
                return False
            self.buf += chunk
            while len(self.buf) >= HDR.size:
                length, mtype, _, seq = HDR.unpack_from(self.buf)
                if len(self.buf) < length:
                    break
                self.handle(mtype, seq, self.buf[:length])
                self.buf = self.buf[length:]
            timeout = 0.02  # keep draining briefly once data has started flowing

    def handle(self, mtype, seq, frame):
        if mtype == T_ACK:
            _, _, _, _, tag, oid = ACK.unpack(frame)
            self.ids[tag] = oid
            print(f"  [{seq}] ack        tag={tag} order_id={oid:#x}")
        elif mtype == T_REJECT:
            _, _, _, _, tag, reason = REJECT.unpack(frame)
            print(f"  [{seq}] reject     tag={tag} reason={REASONS[reason]}")
        elif mtype == T_FILL:
            _, _, _, _, oid, px, qty, rem, maker = FILL.unpack(frame)
            role = "maker" if maker else "taker"
            print(f"  [{seq}] fill       order_id={oid:#x} {qty} @ {px} remaining={rem} ({role})")
        elif mtype == T_CANCELLED:
            _, _, _, _, oid, rem = CANCELLED.unpack(frame)
            print(f"  [{seq}] cancelled  order_id={oid:#x} removed={rem}")
        elif mtype == T_REPLACED:
            _, _, _, _, oid, px, qty = REPLACED.unpack(frame)
            print(f"  [{seq}] replaced   order_id={oid:#x} -> {qty} @ {px}")
        else:
            print(f"  [{seq}] unknown message type {mtype}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9001)
    ap.add_argument("--wait", type=float, default=0.05, help="seconds to wait for responses per row")
    args = ap.parse_args()

    s = Session(args.host, args.port)
    with open(args.csv, newline="") as f:
        for row in csv.DictReader(f):
            action = row["action"].strip().lower()
            tag = int(row["tag"])
            if action == "new":
                print(f"new tag={tag} {row['side']} {row['qty']} @ {row['price']} {row['type']}/{row['tif']}")
                s.send_new(tag, int(row["symbol"]), SIDES[row["side"].lower()], TYPES[row["type"].lower()],
                           TIFS[row["tif"].lower()], int(row["price"] or 0), int(row["qty"]))
            elif action in ("cancel", "replace"):
                oid = s.ids.get(tag)
                if oid is None:
                    print(f"{action} tag={tag}: no ack seen for that tag, skipping")
                    continue
                if action == "cancel":
                    print(f"cancel tag={tag}")
                    s.send_cancel(tag, oid)
                else:
                    print(f"replace tag={tag} -> {row['qty']} @ {row['price']}")
                    s.send_replace(tag, oid, int(row["price"]), int(row["qty"]))
            else:
                print(f"unknown action {action!r}, skipping")
                continue
            if not s.drain(args.wait):
                return 1
    s.drain(args.wait)
    return 0


if __name__ == "__main__":
    sys.exit(main())
