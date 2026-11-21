"""Unit tests for pure LSN/WAL arithmetic and the failover rules."""
from __future__ import annotations

import pytest

from app import lsn
from app.failover import ArchiveRange, CandidateInput, evaluate


def test_parse_and_format_lsn_roundtrip():
    assert lsn.parse_lsn("0/3000100") == 0x03000100
    assert lsn.format_lsn(0x03000100) == "0/3000100"
    assert lsn.parse_lsn("1/FFFFFFFF") == (1 << 32) | 0xFFFFFFFF
    with pytest.raises(lsn.LsnError):
        lsn.parse_lsn("not-an-lsn")
    with pytest.raises(lsn.LsnError):
        lsn.parse_lsn("0/XYZ")


def test_segment_number_and_naming():
    # 0/3000100 = 3 * 16MiB + 0x100 -> segment 3
    assert lsn.segment_number(lsn.parse_lsn("0/3000100")) == 3
    assert lsn.format_segment(1, 3) == "000000010000000000000003"
    assert lsn.parse_segment("00000002000000000000000A") == (2, 10)
    with pytest.raises(lsn.LsnError):
        lsn.parse_segment("0000000000000003")  # no timeline prefix


def test_segments_between_boundaries():
    size = lsn.WAL_SEGMENT_SIZE
    # pointer strictly inside segment 2 needs 2..3 up to segment 3
    assert lsn.segments_between(2 * size + 0x50, 3 * size + 0x100) == [2, 3]
    # pointer exactly at the start of segment 2 still must replay segment 2
    assert lsn.segments_between(2 * size, 3 * size) == [2, 3]
    # LSN 0 is the first byte of segment 0; end at 16MiB is the first byte of 1
    assert lsn.segments_between(0, size) == [0, 1]
    assert lsn.segments_between(0, size - 1) == [0]
    # already at the watermark: nothing required
    assert lsn.segments_between(3 * size + 0x100, 3 * size + 0x100) == []


def test_find_gap_detects_missing_segment():
    ranges = [(2, 3), (5, 6)]
    assert lsn.find_gap([2, 3], ranges) is None
    assert lsn.find_gap([1, 2, 3], ranges) == 1
    assert lsn.find_gap([], ranges) is None
    # adjacent ranges merge and are continuous
    assert lsn.find_gap([2, 3, 4], [(2, 2), (3, 4)]) is None


def _decision(commit="0/3000100", archives=None):
    commit_lsn = lsn.parse_lsn(commit)
    candidates = [
        CandidateInput("r1", "r1", 1, commit_lsn),                      # at watermark
        CandidateInput("r2", "r2", 1, lsn.parse_lsn("0/2000050")),      # behind seg 2
        CandidateInput("r3", "r3", 1, lsn.parse_lsn("0/1000080")),      # behind seg 1
        CandidateInput("r4", "r4", 2, lsn.parse_lsn("0/3000080")),      # timeline 2
    ]
    return evaluate(1, commit_lsn, candidates, archives or [])


def test_verdict_all_rejected_without_archive():
    d = _decision()
    by_id = {v.node_id: v for v in d.verdicts}
    assert by_id["r1"].eligible is True
    assert by_id["r2"].eligible is False and "归档 WAL 不连续" in by_id["r2"].reasons[0]
    assert by_id["r3"].eligible is False
    assert by_id["r4"].eligible is False and "timeline" in by_id["r4"].reasons[0]
    assert len(d.eligible) == 1


def test_verdict_archive_gap_and_continuity():
    d = _decision(archives=[ArchiveRange(1, 2, 3), ArchiveRange(1, 5, 6)])
    by_id = {v.node_id: v for v in d.verdicts}
    assert by_id["r1"].eligible and by_id["r2"].eligible
    assert by_id["r3"].eligible is False
    assert by_id["r3"].missing_segment == 1
    assert by_id["r4"].eligible is False


def test_ahead_replay_is_divergent():
    commit_lsn = lsn.parse_lsn("0/3000100")
    candidates = [CandidateInput("x", "x", 1, lsn.parse_lsn("0/3000200"))]
    d = evaluate(1, commit_lsn, candidates, [])
    assert d.verdicts[0].eligible is False
    assert "超过主库最后提交水位" in d.verdicts[0].reasons[0]
