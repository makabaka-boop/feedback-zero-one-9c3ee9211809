#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_fixtures.py — generate the byte fixtures and expected outputs used by
run_acceptance.py.

    fixtures/    *.bin   raw partition-prefix bytes fed to the tool
    expected/*.json  expected tool output for the success fixtures
    expected/errors.json  {fixture-name: expected error batch byte offset}

The encoder below is written independently from the tool's parser so the
acceptance run is a real cross-check (it even carries its own CRC32C).
"""

import base64
import gzip
import json
import os
import struct

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = os.path.join(HERE, "fixtures")
EXP = os.path.join(HERE, "expected")

# ---------------------------------------------------------------------------
# CRC32C (independent copy; known answer checked in main)
# ---------------------------------------------------------------------------

_POLY = 0x82F63B78
_TABLE = []
for _i in range(256):
    _c = _i
    for _ in range(8):
        _c = (_c >> 1) ^ (_POLY if _c & 1 else 0)
    _TABLE.append(_c)


def crc32c(data, value=0):
    crc = (~value) & 0xFFFFFFFF
    for b in data:
        crc = _TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return (~crc) & 0xFFFFFFFF


# ---------------------------------------------------------------------------
# Zig-zag varints
# ---------------------------------------------------------------------------

def _zz32(n):
    return ((n << 1) ^ (n >> 31)) & 0xFFFFFFFF


def _zz64(n):
    return ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF


def varint(n):
    """Encode an already zig-zagged, non-negative integer."""
    assert n >= 0
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


# ---------------------------------------------------------------------------
# Record / batch encoder
# ---------------------------------------------------------------------------

def R(delta, key=None, value=None, headers=(), attrs=0, ts=0):
    """A record spec: delta, key/value (bytes|None), headers [(str, bytes|None)]."""
    return {"delta": delta, "key": key, "value": value,
            "headers": list(headers), "attrs": attrs, "ts": ts}


def encode_record(r):
    body = bytearray()
    body.append(r["attrs"] & 0xFF)
    body += varint(_zz64(r["ts"]))
    body += varint(_zz32(r["delta"]))
    if r["key"] is None:
        body += varint(_zz32(-1))
    else:
        body += varint(_zz32(len(r["key"]))) + r["key"]
    if r["value"] is None:
        body += varint(_zz32(-1))
    else:
        body += varint(_zz32(len(r["value"]))) + r["value"]
    body += varint(_zz32(len(r["headers"])))
    for name, val in r["headers"]:
        nb = name.encode("utf-8")
        body += varint(_zz32(len(nb))) + nb
        if val is None:
            body += varint(_zz32(-1))
        else:
            body += varint(_zz32(len(val))) + val
    return varint(_zz32(len(body))) + bytes(body)


def _s16(x):
    x &= 0xFFFF
    return x - 0x10000 if x >= 0x8000 else x


def batch(base, recs, lod=None, count=None, compression=0, tx=False, ctl=False,
          pid=-1, epoch=-1, magic=2, extra_attrs=0, leader_epoch=0,
          base_ts=0, max_ts=0, base_seq=0, payload_override=None):
    """Encode one record batch (magic=2)."""
    payload = payload_override if payload_override is not None \
        else b"".join(encode_record(r) for r in recs)
    if count is None:
        count = len(recs)
    if lod is None:
        lod = recs[-1]["delta"] if recs else 0
    attrs = (compression & 0x7) | (0x10 if tx else 0) | (0x20 if ctl else 0) \
        | extra_attrs
    stored = gzip.compress(payload, mtime=0) if compression == 1 else payload
    after_crc = struct.pack(">h", _s16(attrs)) \
        + struct.pack(">i", lod) \
        + struct.pack(">q", base_ts) + struct.pack(">q", max_ts) \
        + struct.pack(">q", pid) + struct.pack(">h", epoch) \
        + struct.pack(">i", base_seq) + struct.pack(">i", count) + stored
    length = 9 + len(after_crc)
    return struct.pack(">q", base) + struct.pack(">i", length) \
        + struct.pack(">i", leader_epoch) + struct.pack(">b", magic) \
        + struct.pack(">I", crc32c(after_crc)) + after_crc


def ctl_key(ctype, version=0):
    return struct.pack(">hh", version, ctype)


# ---------------------------------------------------------------------------
# Expected-output helpers
# ---------------------------------------------------------------------------

def b64(b):
    return None if b is None else base64.b64encode(b).decode("ascii")


def row(offset, key, value, headers=()):
    return {"offset": offset, "key": b64(key), "value": b64(value),
            "headers": [[n, b64(v)] for n, v in headers]}


def out(lso, rows):
    return {"lso": lso, "rows": rows}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SUCCESS = {}  # name -> (data, expected_json_object)
ERRORS = {}   # name -> (data, expected error batch byte offset)


def build():
    # -- f01: empty input ---------------------------------------------------
    SUCCESS["f01_empty"] = (b"", out(0, []))

    # -- f02: one plain batch, nulls, duplicate/UTF-8 headers ---------------
    recs = [
        R(0, key=b"k0", value=b"v0",
          headers=[("h", b"1"), ("h", b"2"), ("n", None)]),
        R(1, key=None, value=b"v1"),
        R(2, key=b"k2", value=None,
          headers=[("utf8-é中", b"\xff\x00")]),
    ]
    SUCCESS["f02_simple"] = (batch(0, recs), out(3, [
        row(0, b"k0", b"v0", [("h", b"1"), ("h", b"2"), ("n", None)]),
        row(1, None, b"v1"),
        row(2, b"k2", None, [("utf8-é中", b"\xff\x00")]),
    ]))

    # -- f03: gzip-compressed batch -----------------------------------------
    recs = [R(0, key=b"a", value=b"1"), R(1, key=b"b", value=None)]
    SUCCESS["f03_gzip"] = (batch(0, recs, compression=1),
                           out(2, [row(0, b"a", b"1"), row(1, b"b", None)]))

    # -- f04: empty batches (plain and gzip) ---------------------------------
    b0 = batch(0, [], lod=0)
    b1 = batch(1, [], lod=0, compression=1)
    b2 = batch(2, [R(0, value=b"x")])
    SUCCESS["f04_empty_batches"] = (b0 + b1 + b2, out(3, [row(2, None, b"x")]))

    # -- f05: offset holes between and inside batches ------------------------
    b0 = batch(0, [R(0, value=b"a"), R(2, value=b"b")], lod=2)
    b1 = batch(5, [R(0, value=b"c"), R(1, value=b"d")], lod=1)
    SUCCESS["f05_holes"] = (b0 + b1, out(7, [
        row(0, None, b"a"), row(2, None, b"b"),
        row(5, None, b"c"), row(6, None, b"d"),
    ]))

    # -- f06: records trimmed inside a batch (compaction style) --------------
    b0 = batch(0, [R(0, value=b"a"), R(5, value=b"f")], lod=5)
    SUCCESS["f06_trimmed"] = (b0, out(6, [row(0, None, b"a"),
                                          row(5, None, b"f")]))

    # -- f07: the task's example — 0 opens A, 1 opens C, 2 commits C ---------
    b0 = batch(0, [R(0, value=b"A0")], tx=True, pid=100, epoch=3)
    b1 = batch(1, [R(0, value=b"C0")], tx=True, pid=200, epoch=0)
    b2 = batch(2, [R(0, key=ctl_key(1), value=b"ignored")],
               tx=True, ctl=True, pid=200, epoch=0)
    SUCCESS["f07_tx_example"] = (b0 + b1 + b2, out(0, []))

    # -- f08: commit + abort + plain records ---------------------------------
    b0 = batch(0, [R(0, value=b"free0")])
    b1 = batch(1, [R(0, value=b"t1a")], tx=True, pid=1, epoch=0)
    b2 = batch(2, [R(0, value=b"t2a")], tx=True, pid=2, epoch=0)
    b3 = batch(3, [R(0, key=ctl_key(1))], tx=True, ctl=True, pid=2, epoch=0)
    b4 = batch(4, [R(0, value=b"t1b")], tx=True, pid=1, epoch=0)
    b5 = batch(5, [R(0, key=ctl_key(0))], tx=True, ctl=True, pid=1, epoch=0)
    b6 = batch(6, [R(0, value=b"free1")])
    SUCCESS["f08_commit_abort"] = (b0 + b1 + b2 + b3 + b4 + b5 + b6, out(7, [
        row(0, None, b"free0"), row(2, None, b"t2a"), row(6, None, b"free1"),
    ]))

    # -- f09: committed records beyond the LSO are not emitted ---------------
    b0 = batch(0, [R(0, value=b"free")])
    b1 = batch(1, [R(0, value=b"open")], tx=True, pid=1, epoch=0)
    b2 = batch(2, [R(0, value=b"committed")], tx=True, pid=2, epoch=0)
    b3 = batch(3, [R(0, key=ctl_key(1))], tx=True, ctl=True, pid=2, epoch=0)
    SUCCESS["f09_committed_beyond_lso"] = (b0 + b1 + b2 + b3,
                                           out(1, [row(0, None, b"free")]))

    # -- f10: after close the producer may change epoch / go non-transactional
    b0 = batch(0, [R(0, value=b"a")], tx=True, pid=1, epoch=0)
    b1 = batch(1, [R(0, key=ctl_key(1))], tx=True, ctl=True, pid=1, epoch=0)
    b2 = batch(2, [R(0, value=b"b")], pid=1, epoch=0)
    b3 = batch(3, [R(0, value=b"c")], tx=True, pid=1, epoch=1)
    b4 = batch(4, [R(0, key=ctl_key(0))], tx=True, ctl=True, pid=1, epoch=1)
    SUCCESS["f10_reopen"] = (b0 + b1 + b2 + b3 + b4,
                             out(5, [row(0, None, b"a"), row(2, None, b"b")]))

    # -- f11: interleaved producers, both commit ------------------------------
    b0 = batch(0, [R(0, value=b"p1a")], tx=True, pid=1, epoch=0)
    b1 = batch(1, [R(0, value=b"p2a")], tx=True, pid=2, epoch=0)
    b2 = batch(2, [R(0, key=ctl_key(1))], tx=True, ctl=True, pid=1, epoch=0)
    b3 = batch(3, [R(0, value=b"p2b")], tx=True, pid=2, epoch=0)
    b4 = batch(4, [R(0, key=ctl_key(1))], tx=True, ctl=True, pid=2, epoch=0)
    SUCCESS["f11_interleave"] = (b0 + b1 + b2 + b3 + b4, out(5, [
        row(0, None, b"p1a"), row(1, None, b"p2a"), row(3, None, b"p2b"),
    ]))

    # -- f12: timestamps, sequences, reserved bits are ignored ----------------
    extra = (1 << 3) | (1 << 6) | (1 << 7) | (1 << 8) | (1 << 15)
    b0 = batch(0, [R(0, value=b"x", attrs=0x5A, ts=-7)],
               extra_attrs=extra, leader_epoch=9, base_ts=-1, max_ts=-2,
               base_seq=12345)
    SUCCESS["f12_ignored_fields"] = (b0, out(1, [row(0, None, b"x")]))

    # -- f15: empty transactional batch opens, control closes -----------------
    b0 = batch(0, [], lod=0, tx=True, pid=1, epoch=0)
    b1 = batch(1, [R(0, key=ctl_key(1))], tx=True, ctl=True, pid=1, epoch=0)
    SUCCESS["f15_tx_empty_open"] = (b0 + b1, out(2, []))

    # -- f16: trimmed records inside a committed transaction ------------------
    b0 = batch(0, [R(0, value=b"a"), R(2, value=b"b")], lod=2,
               tx=True, pid=1, epoch=0)
    b1 = batch(3, [R(0, key=ctl_key(1))], tx=True, ctl=True, pid=1, epoch=0)
    SUCCESS["f16_tx_trimmed"] = (b0 + b1, out(4, [row(0, None, b"a"),
                                                  row(2, None, b"b")]))

    # -- f17: aborted records below the LSO are not emitted -------------------
    b0 = batch(0, [R(0, value=b"dead")], tx=True, pid=1, epoch=0)
    b1 = batch(1, [R(0, key=ctl_key(0))], tx=True, ctl=True, pid=1, epoch=0)
    b2 = batch(2, [R(0, value=b"open")], tx=True, pid=2, epoch=0)
    SUCCESS["f17_aborted_below_lso"] = (b0 + b1 + b2, out(2, []))

    # ---------------------------------------------------------------- errors
    good = batch(0, [R(0, value=b"v")])

    # e01: wrong magic
    ERRORS["e01_magic"] = (batch(0, [R(0)], magic=1), 0)

    # e02: CRC mismatch (flip one byte inside the records section)
    bad = bytearray(good)
    bad[-1] ^= 0xFF
    ERRORS["e02_crc"] = (bytes(bad), 0)

    # e03: unsupported compression (zstd=4), CRC is valid
    ERRORS["e03_compression"] = (batch(0, [R(0)], compression=4), 0)

    # e04: trailing garbage after a valid batch
    ERRORS["e04_trailing"] = (good + b"\x00" * 10, len(good))

    # e05: batch length claims more than the available bytes
    over = bytearray(good)
    (length,) = struct.unpack_from(">i", over, 8)
    struct.pack_into(">i", over, 8, length + 10)
    ERRORS["e05_truncated_batch"] = (bytes(over), 0)

    # e06: record length overruns the records section
    payload = varint(_zz32(100)) + b"\x00" * 5
    ERRORS["e06_record_overrun"] = (
        batch(0, [], count=1, lod=0, payload_override=payload), 0)

    # e07: varint32 too long (6 continuation bytes)
    ERRORS["e07_bad_varint"] = (
        batch(0, [], count=1, lod=0,
              payload_override=b"\x80\x80\x80\x80\x80\x01"), 0)

    # e08: negative record count
    ERRORS["e08_negative_count"] = (
        batch(0, [], count=-1, lod=0, payload_override=b""), 0)

    # e09a: count larger than the actual records
    ERRORS["e09a_count_short"] = (batch(0, [R(0)], count=2), 0)

    # e09b: fewer records than the section holds (trailing bytes)
    ERRORS["e09b_count_trailing"] = (batch(0, [R(0), R(1)], count=1, lod=1), 0)

    # e10: offset deltas not strictly increasing
    ERRORS["e10_delta_order"] = (batch(0, [R(1), R(0)], lod=1), 0)

    # e11: offset delta beyond lastOffsetDelta
    ERRORS["e11_delta_exceed"] = (batch(0, [R(0), R(3)], lod=2), 0)

    # e12: overlapping batch coverage
    b0 = batch(0, [R(0), R(1), R(2)], lod=2)
    b1 = batch(2, [R(0)])
    ERRORS["e12_overlap"] = (b0 + b1, len(b0))

    # e13: negative base offset
    ERRORS["e13_negative_base"] = (batch(-1, [R(0)]), 0)

    # e14: control batch without the transactional bit
    ERRORS["e14_control_not_tx"] = (
        batch(0, [R(0, key=ctl_key(1))], ctl=True, pid=1, epoch=0), 0)

    # e15: control batch with two records
    ERRORS["e15_control_two_records"] = (
        batch(0, [R(0, key=ctl_key(1)), R(1, key=ctl_key(0))],
              tx=True, ctl=True, pid=1, epoch=0, lod=1), 0)

    # e16: control record version != 0
    ERRORS["e16_control_version"] = (
        batch(0, [R(0, key=ctl_key(1, version=1))],
              tx=True, ctl=True, pid=1, epoch=0), 0)

    # e17: commit without an open transaction
    ERRORS["e17_control_no_open"] = (
        batch(0, [R(0, key=ctl_key(1))], tx=True, ctl=True, pid=1, epoch=0), 0)

    # e18: control batch epoch differs from the open transaction
    b0 = batch(0, [R(0)], tx=True, pid=1, epoch=0)
    b1 = batch(1, [R(0, key=ctl_key(1))], tx=True, ctl=True, pid=1, epoch=1)
    ERRORS["e18_control_wrong_epoch"] = (b0 + b1, len(b0))

    # e19: epoch change while the transaction is still open
    b0 = batch(0, [R(0)], tx=True, pid=1, epoch=0)
    b1 = batch(1, [R(0)], tx=True, pid=1, epoch=1)
    ERRORS["e19_tx_epoch_change"] = (b0 + b1, len(b0))

    # e20: non-transactional batch from a producer with an open transaction
    b0 = batch(0, [R(0)], tx=True, pid=1, epoch=0)
    b1 = batch(1, [R(0)], pid=1, epoch=0)
    ERRORS["e20_nontx_while_open"] = (b0 + b1, len(b0))

    # e21: transactional batch with negative producer id
    ERRORS["e21_tx_negative_producer"] = (batch(0, [R(0)], tx=True), 0)

    # e22: gzip flag set but the section is not gzip
    ERRORS["e22_gzip_garbage"] = (
        batch(0, [], count=1, compression=1, payload_override=b"not gzip"), 0)

    # e23: record declares more bytes than its fields occupy
    rec = encode_record(R(0))
    body_len = len(rec) - len(varint(_zz32(0)))  # length of the record body
    payload = varint(_zz32(body_len + 1)) + rec[len(varint(_zz32(body_len))):] \
        + b"\x00"
    ERRORS["e23_record_length_mismatch"] = (
        batch(0, [], count=1, lod=0, payload_override=payload), 0)

    # e24: negative offset delta
    ERRORS["e24_negative_delta"] = (batch(0, [R(-1)], lod=0), 0)

    # e25: batch length below the 49-byte minimum
    ERRORS["e25_short_length"] = (
        struct.pack(">q", 0) + struct.pack(">i", 10) + b"\x00" * 49, 0)

    # e26: key length overruns the record
    body = b"\x00" + varint(_zz64(0)) + varint(_zz32(0)) + varint(_zz32(100)) \
        + b"ab"
    ERRORS["e26_key_overrun"] = (
        batch(0, [], count=1, lod=0,
              payload_override=varint(_zz32(len(body))) + body), 0)


def main():
    assert crc32c(b"123456789") == 0xE3069283, "fixture CRC32C self-test failed"
    build()
    os.makedirs(FIX, exist_ok=True)
    os.makedirs(EXP, exist_ok=True)
    errors = {}
    for name, (data, expected) in sorted(SUCCESS.items()):
        with open(os.path.join(FIX, name + ".bin"), "wb") as fh:
            fh.write(data)
        with open(os.path.join(EXP, name + ".json"), "w", encoding="utf-8") as fh:
            json.dump(expected, fh, ensure_ascii=False, indent=2)
            fh.write("\n")
    for name, (data, offset) in sorted(ERRORS.items()):
        with open(os.path.join(FIX, name + ".bin"), "wb") as fh:
            fh.write(data)
        errors[name] = offset
    with open(os.path.join(EXP, "errors.json"), "w", encoding="utf-8") as fh:
        json.dump(errors, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print("built %d success and %d error fixtures"
          % (len(SUCCESS), len(ERRORS)))


if __name__ == "__main__":
    main()
