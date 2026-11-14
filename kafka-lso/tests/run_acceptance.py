#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_acceptance.py — acceptance suite for kafka_archive_to_json.py.

Runs the tool as a subprocess against every fixture in tests/fixtures/:

* success fixtures (tests/expected/<name>.json): exit code 0 and the output
  JSON must equal the expected object; an old output file must be replaced;
* error fixtures (tests/expected/errors.json): exit code 1, stderr must
  report the expected zero-based byte offset of the error batch, and a
  pre-existing output file must be left untouched.

Also checks usage/I-O exit codes and the CRC32C known-answer vector.
"""

import json
import os
import re
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TOOL = os.path.join(ROOT, "kafka_archive_to_json.py")
FIX = os.path.join(HERE, "fixtures")
EXP = os.path.join(HERE, "expected")

sys.path.insert(0, ROOT)
import kafka_archive_to_json as tool  # noqa: E402

SENTINEL = "PRE-EXISTING OUTPUT — must survive rejection\n"

_failures = []


def check(cond, label, detail=""):
    status = "PASS" if cond else "FAIL"
    print("%-4s  %s%s" % (status, label, ("  -- " + detail) if detail and not cond else ""))
    if not cond:
        _failures.append(label)


def run_tool(args):
    return subprocess.run([sys.executable, "-I", TOOL] + args,
                          capture_output=True, text=True)


def main():
    # CRC32C known-answer vector (guards the pure-Python fallback too)
    check(tool.crc32c(b"123456789") == 0xE3069283, "crc32c known-answer vector")

    # usage / IO errors
    check(run_tool([]).returncode == 2, "usage error exits 2")
    with tempfile.TemporaryDirectory() as tmp:
        proc = run_tool([os.path.join(tmp, "missing.bin"),
                         os.path.join(tmp, "out.json")])
        check(proc.returncode == 2, "unreadable input exits 2")

    success = sorted(f[:-5] for f in os.listdir(EXP)
                     if f.endswith(".json") and f != "errors.json")
    with open(os.path.join(EXP, "errors.json"), encoding="utf-8") as fh:
        errors = json.load(fh)

    with tempfile.TemporaryDirectory() as tmp:
        for name in success:
            with open(os.path.join(EXP, name + ".json"), encoding="utf-8") as fh:
                expected = json.load(fh)
            inp = os.path.join(FIX, name + ".bin")
            outp = os.path.join(tmp, name + ".out.json")
            with open(outp, "w", encoding="utf-8") as fh:
                fh.write(SENTINEL)  # must be replaced on success
            proc = run_tool([inp, outp])
            ok = proc.returncode == 0
            check(ok, "%s: exit 0" % name, proc.stderr.strip())
            if not ok:
                continue
            with open(outp, encoding="utf-8") as fh:
                actual = json.load(fh)
            check(actual == expected, "%s: JSON matches" % name,
                  "expected %r, got %r" % (expected, actual))
            offsets = [r["offset"] for r in actual["rows"]]
            check(offsets == sorted(offsets), "%s: rows ascending" % name)

        for name, want_offset in sorted(errors.items()):
            inp = os.path.join(FIX, name + ".bin")
            outp = os.path.join(tmp, name + ".out.json")
            with open(outp, "w", encoding="utf-8") as fh:
                fh.write(SENTINEL)  # must survive rejection
            proc = run_tool([inp, outp])
            check(proc.returncode == 1, "%s: exit 1" % name,
                  "rc=%d stderr=%s" % (proc.returncode, proc.stderr.strip()))
            check(re.search(r"byte offset %d\b" % want_offset, proc.stderr)
                  is not None,
                  "%s: reports byte offset %d" % (name, want_offset),
                  "stderr=%s" % proc.stderr.strip())
            with open(outp, encoding="utf-8") as fh:
                check(fh.read() == SENTINEL, "%s: old output preserved" % name)

    total = "FAILURES: %d" % len(_failures) if _failures else "ALL CHECKS PASSED"
    print(total)
    return 1 if _failures else 0


if __name__ == "__main__":
    sys.exit(main())
