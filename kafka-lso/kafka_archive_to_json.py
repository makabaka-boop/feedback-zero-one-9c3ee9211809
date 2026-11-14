#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kafka_archive_to_json.py
========================

Convert a copied Kafka partition prefix (a byte string of record batches,
message-format magic=2 as specified by Kafka 4.2) into committed-sample
JSON:

    {"lso": <int>, "rows": [{"offset", "key", "value", "headers"}, ...]}

Only uncompressed and gzip-compressed batches are accepted.  Timestamps,
sequence numbers, the partition leader epoch and all reserved attribute
bits are ignored.

Usage:

    python3 kafka_archive_to_json.py INPUT.bin OUTPUT.json

Exit codes:
    0  success; OUTPUT.json has been (atomically) replaced
    1  input rejected; stderr reports the zero-based byte offset of the
       first byte of the offending batch and OUTPUT.json is left untouched
    2  usage or I/O error

Self-contained: standard library only.  Parsing and transaction
adjudication are implemented here from scratch; gzip (stdlib) is used for
decompression and CRC32C uses a native library when one is installed,
otherwise the built-in table implementation below.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import struct
import sys
import tempfile
from collections import namedtuple

# ---------------------------------------------------------------------------
# CRC32C (Castagnoli).  Known answer: crc32c(b"123456789") == 0xE3069283.
# ---------------------------------------------------------------------------

try:  # optional native acceleration; the pure-Python fallback is equivalent
    import crc32c as _crc32c_lib  # type: ignore

    def crc32c(data: bytes, value: int = 0) -> int:
        return _crc32c_lib.crc32c(data, value)

except ImportError:
    try:
        import crcmod.predefined as _crcmod  # type: ignore

        _crc32c_fun = _crcmod.mkPredefinedCrcFun("crc-32c")

        def crc32c(data: bytes, value: int = 0) -> int:
            return _crc32c_fun(data, value)

    except ImportError:
        _CRC32C_POLY = 0x82F63B78  # reflected Castagnoli polynomial
        _CRC32C_TABLE = []
        for _i in range(256):
            _c = _i
            for _ in range(8):
                _c = (_c >> 1) ^ (_CRC32C_POLY if _c & 1 else 0)
            _CRC32C_TABLE.append(_c)

        def crc32c(data, value=0):
            crc = (~value) & 0xFFFFFFFF
            table = _CRC32C_TABLE
            for b in data:
                crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)
            return (~crc) & 0xFFFFFFFF


# ---------------------------------------------------------------------------
# Format constants (record batch v2, magic=2)
# ---------------------------------------------------------------------------

# Batch layout: baseOffset(8) length(4) partitionLeaderEpoch(4) magic(1)
# crc(4) attributes(2) lastOffsetDelta(4) baseTimestamp(8) maxTimestamp(8)
# producerId(8) producerEpoch(2) baseSequence(4) recordCount(4) records...
HEADER_SIZE = 61                     # bytes through the record-count field
MIN_BATCH_LENGTH = HEADER_SIZE - 12  # smallest legal value of the length field
MAGIC_V2 = 2

COMPRESSION_NONE = 0
COMPRESSION_GZIP = 1

ATTR_TRANSACTIONAL = 0x10  # attributes bit 4
ATTR_CONTROL = 0x20        # attributes bit 5

CONTROL_VERSION = 0
CONTROL_ABORT = 0
CONTROL_COMMIT = 1

_COMMITTED = "committed"
_ABORTED = "aborted"

Record = namedtuple("Record", ["offset_delta", "key", "value", "headers"])
Batch = namedtuple("Batch", [
    "start", "base_offset", "last_offset_delta", "is_transactional",
    "is_control", "producer_id", "producer_epoch", "records",
])


class Corrupt(Exception):
    """A format problem inside the batch currently being parsed."""


class Reject(Exception):
    """The input is rejected.

    ``batch_offset`` is the zero-based byte offset of the first byte of the
    offending batch (for trailing garbage it is the offset at which the
    garbage begins).
    """

    def __init__(self, batch_offset: int, reason: str):
        super().__init__(reason)
        self.batch_offset = batch_offset
        self.reason = reason


# ---------------------------------------------------------------------------
# Low-level readers (zig-zag varints, bounded)
# ---------------------------------------------------------------------------

def _read_varint32(buf, pos: int, limit: int):
    """Read a zig-zag encoded varint32; return (value, new_pos)."""
    result = 0
    shift = 0
    for _ in range(5):  # at most 5 bytes, like Kafka's ByteUtils.readVarint
        if pos >= limit:
            raise Corrupt("truncated varint32")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            result &= 0xFFFFFFFF
            return (result >> 1) ^ -(result & 1), pos
        shift += 7
    raise Corrupt("varint32 too long")


def _read_varlong(buf, pos: int, limit: int):
    """Read a zig-zag encoded varint64; return (value, new_pos)."""
    result = 0
    shift = 0
    for _ in range(10):  # at most 10 bytes
        if pos >= limit:
            raise Corrupt("truncated varlong")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            result &= 0xFFFFFFFFFFFFFFFF
            return (result >> 1) ^ -(result & 1), pos
        shift += 7
    raise Corrupt("varlong too long")


def _read_bytes(buf, pos: int, limit: int, what: str):
    """Read a length-prefixed byte string (-1 means null)."""
    size, pos = _read_varint32(buf, pos, limit)
    if size == -1:
        return None, pos
    if size < -1:
        raise Corrupt("invalid %s length %d" % (what, size))
    if pos + size > limit:
        raise Corrupt("%s overruns its record" % what)
    return bytes(buf[pos:pos + size]), pos + size


# ---------------------------------------------------------------------------
# Record / batch parsing
# ---------------------------------------------------------------------------

def _parse_record(buf, pos: int, limit: int):
    """Parse one record at ``pos``; return (Record, next_pos)."""
    length, pos = _read_varint32(buf, pos, limit)
    if length < 0:
        raise Corrupt("negative record length %d" % length)
    end = pos + length
    if end > limit:
        raise Corrupt("record overruns the records section")
    pos += 1  # attributes: int8, reserved, ignored
    _timestamp_delta, pos = _read_varlong(buf, pos, end)  # ignored
    offset_delta, pos = _read_varint32(buf, pos, end)
    key, pos = _read_bytes(buf, pos, end, "key")
    value, pos = _read_bytes(buf, pos, end, "value")
    header_count, pos = _read_varint32(buf, pos, end)
    if header_count < 0:
        raise Corrupt("negative header count %d" % header_count)
    headers = []
    for _ in range(header_count):
        name_len, pos = _read_varint32(buf, pos, end)
        if name_len < 0:
            raise Corrupt("negative header key length %d" % name_len)
        if pos + name_len > end:
            raise Corrupt("header key overruns its record")
        name_bytes = bytes(buf[pos:pos + name_len])
        pos += name_len
        try:
            name = name_bytes.decode("utf-8")
        except UnicodeDecodeError:
            raise Corrupt("header key is not valid UTF-8") from None
        hval, pos = _read_bytes(buf, pos, end, "header value")
        headers.append((name, hval))
    if pos != end:
        raise Corrupt("record length does not match its contents")
    return Record(offset_delta, key, value, headers), end


def _parse_records(buf, count: int):
    """Parse exactly ``count`` records; they must fill ``buf`` exactly."""
    records = []
    pos = 0
    size = len(buf)
    for _ in range(count):
        if pos >= size:
            raise Corrupt("record count exceeds the available records")
        rec, pos = _parse_record(buf, pos, size)
        records.append(rec)
    if pos != size:
        raise Corrupt("trailing bytes after the last record")
    return records


def _parse_batch(buf, pos: int):
    """Parse one batch starting at ``pos``; return (Batch, next_pos)."""
    size = len(buf)
    if size - pos < HEADER_SIZE:
        raise Corrupt("truncated batch header (%d byte(s) left)" % (size - pos))
    base_offset, length = struct.unpack_from(">qi", buf, pos)
    if length < MIN_BATCH_LENGTH:
        raise Corrupt("invalid batch length %d" % length)
    total = 12 + length  # the length field excludes baseOffset and itself
    if pos + total > size:
        raise Corrupt("truncated batch (needs %d bytes, %d available)"
                      % (total, size - pos))
    end = pos + total
    magic = buf[pos + 16]
    if magic != MAGIC_V2:
        raise Corrupt("unsupported magic %d" % magic)
    (crc_expected,) = struct.unpack_from(">I", buf, pos + 17)
    if crc32c(buf[pos + 21:end]) != crc_expected:
        raise Corrupt("CRC32C mismatch")
    (attributes,) = struct.unpack_from(">h", buf, pos + 21)
    compression = attributes & 0x07
    if compression not in (COMPRESSION_NONE, COMPRESSION_GZIP):
        raise Corrupt("unsupported compression type %d" % compression)
    (last_offset_delta,) = struct.unpack_from(">i", buf, pos + 23)
    if last_offset_delta < 0:
        raise Corrupt("negative last offset delta")
    (producer_id,) = struct.unpack_from(">q", buf, pos + 43)
    (producer_epoch,) = struct.unpack_from(">h", buf, pos + 51)
    (record_count,) = struct.unpack_from(">i", buf, pos + 57)
    if record_count < 0:
        raise Corrupt("negative record count %d" % record_count)
    section = bytes(buf[pos + HEADER_SIZE:end])
    if compression == COMPRESSION_GZIP:
        try:
            record_bytes = gzip.decompress(section)
        except Exception as exc:  # BadGzipFile, EOFError, zlib.error, ...
            raise Corrupt("invalid gzip records: %s" % exc) from None
    else:
        record_bytes = section
    records = _parse_records(record_bytes, record_count)
    prev_delta = -1
    for rec in records:
        delta = rec.offset_delta
        if delta < 0:
            raise Corrupt("negative record offset delta")
        if delta <= prev_delta:
            raise Corrupt("record offset deltas not strictly increasing")
        if delta > last_offset_delta:
            raise Corrupt("record offset delta exceeds last offset delta")
        prev_delta = delta
    batch = Batch(
        start=pos,
        base_offset=base_offset,
        last_offset_delta=last_offset_delta,
        is_transactional=bool(attributes & ATTR_TRANSACTIONAL),
        is_control=bool(attributes & ATTR_CONTROL),
        producer_id=producer_id,
        producer_epoch=producer_epoch,
        records=records,
    )
    return batch, end


# ---------------------------------------------------------------------------
# Transaction adjudication
# ---------------------------------------------------------------------------

def _adjudicate(batches):
    """Validate transaction semantics.

    Returns (open_tx, tx_status): ``open_tx`` maps producer id to
    [epoch, first batch baseOffset, [batch indexes]] for transactions still
    open at EOF; ``tx_status`` maps batch index to "committed"/"aborted"
    for transactional data batches whose transaction ended.
    """
    open_tx = {}
    tx_status = {}
    for index, batch in enumerate(batches):
        if batch.is_control:
            if not batch.is_transactional:
                raise Reject(batch.start, "control batch is not transactional")
            if batch.producer_id < 0 or batch.producer_epoch < 0:
                raise Reject(batch.start,
                             "transactional batch with negative producer id/epoch")
            if len(batch.records) != 1:
                raise Reject(batch.start,
                             "control batch must hold exactly one record")
            key = batch.records[0].key
            if key is None or len(key) != 4:
                raise Reject(batch.start, "control record key must be 4 bytes")
            version, ctype = struct.unpack(">hh", key)
            if version != CONTROL_VERSION:
                raise Reject(batch.start,
                             "unsupported control record version %d" % version)
            if ctype not in (CONTROL_ABORT, CONTROL_COMMIT):
                raise Reject(batch.start, "unknown control record type %d" % ctype)
            tx = open_tx.get(batch.producer_id)
            if tx is None or tx[0] != batch.producer_epoch:
                raise Reject(batch.start,
                             "control batch ends no open transaction of the same epoch")
            status = _COMMITTED if ctype == CONTROL_COMMIT else _ABORTED
            for i in tx[2]:
                tx_status[i] = status
            del open_tx[batch.producer_id]
        elif batch.is_transactional:
            if batch.producer_id < 0 or batch.producer_epoch < 0:
                raise Reject(batch.start,
                             "transactional batch with negative producer id/epoch")
            tx = open_tx.get(batch.producer_id)
            if tx is None:
                # first transactional data batch opens the transaction
                open_tx[batch.producer_id] = [batch.producer_epoch,
                                              batch.base_offset, [index]]
            elif tx[0] != batch.producer_epoch:
                raise Reject(batch.start,
                             "transactional batch epoch %d conflicts with the "
                             "open transaction epoch %d"
                             % (batch.producer_epoch, tx[0]))
            else:
                tx[2].append(index)
        else:
            if batch.producer_id >= 0 and batch.producer_id in open_tx:
                raise Reject(batch.start,
                             "non-transactional batch from a producer with an "
                             "open transaction")
    return open_tx, tx_status


def _last_stable_offset(batches, open_tx):
    if open_tx:
        # first batch baseOffset of the earliest unfinished transaction
        return min(tx[1] for tx in open_tx.values())
    if batches:
        last = batches[-1]
        return last.base_offset + last.last_offset_delta + 1
    return 0


# ---------------------------------------------------------------------------
# Top-level conversion
# ---------------------------------------------------------------------------

def _b64(data):
    return None if data is None else base64.b64encode(data).decode("ascii")


def convert(data: bytes) -> dict:
    """Parse ``data`` and return {"lso": int, "rows": [...]}.

    Raises Reject on any format or transaction-semantics violation.
    """
    batches = []
    pos = 0
    size = len(data)
    prev_end = -1  # last offset covered by the previous batch
    while pos < size:
        start = pos
        try:
            batch, pos = _parse_batch(data, pos)
        except Corrupt as exc:
            raise Reject(start, str(exc)) from None
        if batch.base_offset < 0:
            raise Reject(start, "negative base offset")
        if batch.base_offset <= prev_end:
            raise Reject(start, "batch coverage overlaps or does not increase")
        prev_end = batch.base_offset + batch.last_offset_delta
        batches.append(batch)

    open_tx, tx_status = _adjudicate(batches)
    lso = _last_stable_offset(batches, open_tx)

    rows = []
    for index, batch in enumerate(batches):
        if batch.is_control:
            continue  # control records are never emitted
        if batch.is_transactional and tx_status.get(index) != _COMMITTED:
            continue  # aborted or still-open transaction
        for rec in batch.records:
            offset = batch.base_offset + rec.offset_delta
            if offset < lso:
                rows.append({
                    "offset": offset,
                    "key": _b64(rec.key),
                    "value": _b64(rec.value),
                    "headers": [[name, _b64(val)] for name, val in rec.headers],
                })
    rows.sort(key=lambda r: r["offset"])
    return {"lso": lso, "rows": rows}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _atomic_write(path: str, text: str) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".kafka-lso-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main(argv) -> int:
    if len(argv) != 3:
        sys.stderr.write("usage: kafka_archive_to_json.py INPUT.bin OUTPUT.json\n")
        return 2
    src, dst = argv[1], argv[2]
    try:
        with open(src, "rb") as fh:
            data = fh.read()
    except OSError as exc:
        sys.stderr.write("error: cannot read input %s: %s\n" % (src, exc))
        return 2
    try:
        result = convert(data)
    except Reject as exc:
        # Keep any old output untouched; report the zero-based byte offset
        # of the first byte of the offending batch.
        sys.stderr.write("rejected: %s; error batch starts at byte offset %d\n"
                         % (exc.reason, exc.batch_offset))
        return 1
    payload = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    try:
        _atomic_write(dst, payload)
    except OSError as exc:
        sys.stderr.write("error: cannot write output %s: %s\n" % (dst, exc))
        return 2
    sys.stdout.write("ok: lso=%d rows=%d -> %s\n"
                     % (result["lso"], len(result["rows"]), dst))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
