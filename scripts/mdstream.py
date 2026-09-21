#!/usr/bin/env python3
"""Resumable market-data client for matchbox_server.

Subscribes on the market-data port, tracks the stream sequence, reports
gaps, treats a missed heartbeat as a dead link and reconnects with
exponential backoff from the last sequence seen, so a restart of the client
(or a network blip) loses nothing the gateway still retains.

Stdlib only; speaks the binary wire protocol from include/matchbox/protocol.hpp.
"""
import argparse
import select
import socket
import struct
import sys
import time

HDR = struct.Struct("<HBBI")
SUBSCRIBE = struct.Struct("<HBBIQ")
TRADE = struct.Struct("<HBBIHB5xqI4xQ")
TOP = struct.Struct("<HBBIH6xqqII")
HEARTBEAT = struct.Struct("<HBBIQQ")
GAP = struct.Struct("<HBBIQQ")
T_SUBSCRIBE, T_TRADE, T_TOP, T_HEARTBEAT, T_GAP = 4, 32, 33, 34, 35
VERSION = 1


class Stream:
    def __init__(self, host, port, from_seq, quiet, heartbeat_timeout):
        self.host, self.port = host, port
        self.next_seq = from_seq  # 0 = live
        self.quiet = quiet
        self.heartbeat_timeout = heartbeat_timeout
        self.frames = self.gaps = self.lost = self.heartbeats = self.reconnects = 0
        self.sock = None
        self.buf = b""

    def connect(self):
        backoff = 0.05
        while True:
            try:
                self.sock = socket.create_connection((self.host, self.port), timeout=2)
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.sock.sendall(SUBSCRIBE.pack(SUBSCRIBE.size, T_SUBSCRIBE, VERSION, 0, self.next_seq))
                self.sock.settimeout(None)
                self.buf = b""
                if not self.quiet:
                    print(f"subscribed from seq {self.next_seq}")
                return
            except OSError as e:
                if not self.quiet:
                    print(f"connect failed ({e}), retrying in {backoff:.2f}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 2.0)

    def run(self, seconds, slow):
        deadline = time.monotonic() + seconds if seconds > 0 else float("inf")
        self.connect()
        last_rx = time.monotonic()
        while time.monotonic() < deadline:
            r, _, _ = select.select([self.sock], [], [], 0.1)
            if not r:
                if time.monotonic() - last_rx > self.heartbeat_timeout:
                    self.reconnect("no heartbeat")
                    last_rx = time.monotonic()
                continue
            if slow:
                time.sleep(slow)
            chunk = self.sock.recv(65536)
            if not chunk:
                self.reconnect("gateway closed the connection")
                last_rx = time.monotonic()
                continue
            last_rx = time.monotonic()
            self.buf += chunk
            while len(self.buf) >= HDR.size:
                length, mtype, _, seq = HDR.unpack_from(self.buf)
                if len(self.buf) < length:
                    break
                self.handle(mtype, seq, self.buf[:length])
                self.buf = self.buf[length:]
        self.sock.close()

    def reconnect(self, why):
        if not self.quiet:
            print(f"{why}, reconnecting from seq {self.next_seq}")
        self.sock.close()
        self.reconnects += 1
        self.connect()

    def handle(self, mtype, seq, frame):
        if mtype == T_HEARTBEAT:
            # A quiet stream still tells us where the head is, so a resume after
            # nothing but heartbeats picks up exactly where the gateway is.
            _, _, _, _, _ts, head = HEARTBEAT.unpack(frame)
            self.heartbeats += 1
            self.next_seq = max(self.next_seq, head + 1)
            return
        if mtype == T_GAP:
            _, _, _, _, frm, resumed = GAP.unpack(frame)
            self.gaps += 1
            self.lost += resumed - frm
            self.next_seq = resumed
            if not self.quiet:
                print(f"GAP   frames {frm}..{resumed - 1} lost")
            return
        if self.next_seq and seq != self.next_seq:  # gateway always sends a Gap first; belt and braces
            self.gaps += 1
            self.lost += seq - self.next_seq
            if not self.quiet:
                print(f"SEQ JUMP expected {self.next_seq} got {seq}")
        self.next_seq = seq + 1
        self.frames += 1
        if self.quiet:
            return
        if mtype == T_TRADE:
            _, _, _, _, sym, aggr, px, qty, _ts = TRADE.unpack(frame)
            print(f"[{seq}] TRADE sym={sym} {'sell' if aggr else 'buy'} {qty} @ {px}")
        elif mtype == T_TOP:
            _, _, _, _, sym, bid, ask, bq, aq = TOP.unpack(frame)
            print(f"[{seq}] TOP   sym={sym} bid {bq} @ {bid} | ask {aq} @ {ask}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9002)
    ap.add_argument("--from", dest="from_seq", type=int, default=0, help="resume from this stream seq (0 = live)")
    ap.add_argument("--seconds", type=float, default=0, help="stop after this long (0 = run until Ctrl-C)")
    ap.add_argument("--slow", type=float, default=0, help="sleep this many seconds per read to act as a slow consumer")
    ap.add_argument("--heartbeat-timeout", type=float, default=3.0)
    ap.add_argument("--quiet", action="store_true", help="only print the summary line")
    ap.add_argument("--expect-no-loss", action="store_true", help="exit 1 if any frame was lost")
    args = ap.parse_args()

    s = Stream(args.host, args.port, args.from_seq, args.quiet, args.heartbeat_timeout)
    try:
        s.run(args.seconds, args.slow)
    except KeyboardInterrupt:
        pass
    print(f"md: {s.frames} frames, gaps {s.gaps} ({s.lost} frames lost), heartbeats {s.heartbeats}, "
          f"reconnects {s.reconnects}, next seq {s.next_seq}")
    return 1 if args.expect_no_loss and s.lost else 0


if __name__ == "__main__":
    sys.exit(main())
