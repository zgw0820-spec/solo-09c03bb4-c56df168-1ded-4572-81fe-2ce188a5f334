"""Pure LSN / WAL-segment arithmetic.

PostgreSQL represents an LSN as ``XLogRecPtr``: a 64-bit integer rendered as
two hexadecimal 32-bit halves separated by ``/`` (e.g. ``0/3000100``).

WAL segment files are named ``T<timeline>LLLLLLLL`` where ``LLLLLLLL`` is the
hexadecimal segment index derived from the LSN.  With the default segment
size of 16 MiB (``XLOG_SEG_SIZE``)::

    segno = lsn // (16 * 1024 * 1024)

All functions here are side-effect free and covered by unit tests so the
failover rules can be verified without a database.
"""
from __future__ import annotations

import re

WAL_SEGMENT_SIZE = 16 * 1024 * 1024  # PostgreSQL default: 16 MiB
_LSN_RE = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]{1,8}$")
# Plain WAL file: 8 hex chars timeline + 16 hex chars segment number (24 total).
_SEG_NAME_RE = re.compile(r"^([0-9A-Fa-f]{8})([0-9A-Fa-f]{16})$")
# Timeline history style: 24 hex chars, dot, 8 hex chars timeline suffix.
_SEG_SUFFIX_RE = re.compile(r"^([0-9A-Fa-f]{24})\.([0-9A-Fa-f]{8})$")


class LsnError(ValueError):
    """Raised when an LSN or WAL segment name cannot be parsed."""


def parse_lsn(text: str) -> int:
    """Parse ``"hi/lo"`` text into an integer LSN."""
    if text is None:
        raise LsnError("LSN is required")
    cleaned = str(text).strip()
    if not _LSN_RE.match(cleaned):
        raise LsnError(f"invalid LSN {text!r}, expected format 0/16B3748")
    hi_hex, lo_hex = cleaned.split("/")
    try:
        hi = int(hi_hex, 16)
        lo = int(lo_hex, 16)
    except ValueError as exc:  # pragma: no cover - regex already guards
        raise LsnError(f"invalid LSN {text!r}") from exc
    if lo > 0xFFFFFFFF or hi > 0xFFFFFFFF:
        raise LsnError(f"LSN {text!r} exceeds 64 bits")
    return (hi << 32) | lo


def format_lsn(lsn: int) -> str:
    """Render an integer LSN back into canonical ``hi/lo`` text."""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def segment_number(lsn: int) -> int:
    """Return the WAL segment index that contains ``lsn``."""
    return lsn // WAL_SEGMENT_SIZE


def format_segment(timeline: int, segment_no: int) -> str:
    """Format a segment as PostgreSQL writes it on disk, e.g. ``000000010000000000000003``."""
    return f"{timeline:08X}{segment_no:016X}"


def parse_segment(text: str) -> tuple[int, int]:
    """Parse a WAL segment file name into ``(timeline, segment_no)``.

    Accepts both the plain form ``000000010000000000000003`` and the
    timeline-suffixed form (``...00000003.00000001``).  A bare segment number
    without a timeline prefix is rejected because failover decisions must be
    explicit about which timeline the archived WAL belongs to.
    """
    cleaned = str(text).strip()
    suffix = _SEG_SUFFIX_RE.match(cleaned)
    if suffix:
        base = suffix.group(1)
        timeline = int(base[:8], 16)
        segment_no = int(base[8:], 16)
        return timeline, segment_no
    plain = _SEG_NAME_RE.match(cleaned)
    if plain:
        return int(plain.group(1), 16), int(plain.group(2), 16)
    if re.fullmatch(r"[0-9A-Fa-f]{16}", cleaned):
        raise LsnError(
            f"segment {text!r} 缺少 8 位 timeline 前缀；"
            "期望形如 000000010000000000000003"
        )
    raise LsnError(f"invalid WAL segment name {text!r}")


def segments_between(start_lsn: int, end_lsn: int) -> list[int]:
    """Segment indexes that must be replayed to go from ``start_lsn`` to ``end_lsn``.

    A node whose replay pointer sits at the very start of segment *k* still
    needs segment *k*: replaying up to a segment's beginning means none of its
    records have been applied.
    """
    if start_lsn >= end_lsn:
        return []
    first = start_lsn // WAL_SEGMENT_SIZE
    last = end_lsn // WAL_SEGMENT_SIZE
    return list(range(first, last + 1))


def merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge inclusive integer intervals into a sorted list of disjoint ranges."""
    if not ranges:
        return []
    ordered = sorted((min(a, b), max(a, b)) for a, b in ranges)
    merged: list[tuple[int, int]] = [ordered[0]]
    for lo, hi in ordered[1:]:
        prev_lo, prev_hi = merged[-1]
        if lo <= prev_hi + 1:  # adjacent segments count as continuous
            merged[-1] = (prev_lo, max(prev_hi, hi))
        else:
            merged.append((lo, hi))
    return merged


def find_gap(required: list[int], available_ranges: list[tuple[int, int]]) -> int | None:
    """Return the first required segment not covered by any available range."""
    if not required:
        return None
    covered = merge_ranges(available_ranges)
    for seg in required:
        if not any(lo <= seg <= hi for lo, hi in covered):
            return seg
    return None
