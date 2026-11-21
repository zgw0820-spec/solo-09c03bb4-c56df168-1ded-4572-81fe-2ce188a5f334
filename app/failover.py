"""Failover eligibility rules, kept free of any database dependency.

A candidate replica may take over only when it can replay, on one and the
same timeline, continuously up to the last LSN the old primary committed:

1. Its observation must be on the old primary's timeline.
2. Its replay LSN must not be ahead of the committed watermark on that
   timeline (ahead means it contains changes the old primary never committed
   in this generation — a divergent history).
3. If it is behind the watermark, every WAL segment between its replay
   position and the watermark must be present in the archive so the replay
   can be completed without a gap.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from . import lsn as lsn_mod


@dataclass(frozen=True)
class CandidateInput:
    node_id: str
    node_name: str
    timeline: int
    replay_lsn: int  # integer LSN already replayed by the replica
    observed_at: str = ""


@dataclass(frozen=True)
class ArchiveRange:
    timeline: int
    start_segment: int
    end_segment: int


@dataclass(frozen=True)
class CandidateVerdict:
    node_id: str
    node_name: str
    timeline: int
    replay_lsn: int
    eligible: bool
    reasons: list[str] = field(default_factory=list)
    required_segments: list[int] = field(default_factory=list)
    missing_segment: int | None = None


@dataclass(frozen=True)
class FailoverDecision:
    primary_timeline: int
    commit_lsn: int
    verdicts: list[CandidateVerdict]

    @property
    def eligible(self) -> list[CandidateVerdict]:
        return [v for v in self.verdicts if v.eligible]

    @property
    def rejected(self) -> list[CandidateVerdict]:
        return [v for v in self.verdicts if not v.eligible]


def evaluate(
    primary_timeline: int,
    commit_lsn: int,
    candidates: list[CandidateInput],
    archives: list[ArchiveRange],
) -> FailoverDecision:
    """Apply the takeover rules and return one verdict per candidate."""
    timeline_archives: dict[int, list[tuple[int, int]]] = {}
    for arc in archives:
        timeline_archives.setdefault(arc.timeline, []).append(
            (min(arc.start_segment, arc.end_segment), max(arc.start_segment, arc.end_segment))
        )

    verdicts: list[CandidateVerdict] = []
    for cand in candidates:
        reasons: list[str] = []
        required: list[int] = []
        missing: int | None = None

        if cand.timeline != primary_timeline:
            reasons.append(
                f"timeline {cand.timeline} 与主库 timeline {primary_timeline} 不一致，"
                "无法沿同一 timeline 重放"
            )
        elif cand.replay_lsn > commit_lsn:
            reasons.append(
                f"replay LSN {lsn_mod.format_lsn(cand.replay_lsn)} 超过主库最后提交水位 "
                f"{lsn_mod.format_lsn(commit_lsn)}，存在本代次之外的分叉记录"
            )
        elif cand.replay_lsn == commit_lsn:
            pass  # already at the watermark: immediately promotable
        else:
            required = lsn_mod.segments_between(cand.replay_lsn, commit_lsn)
            missing = lsn_mod.find_gap(required, timeline_archives.get(cand.timeline, []))
            if missing is not None:
                reasons.append(
                    "归档 WAL 不连续：缺少 segment "
                    f"{lsn_mod.format_segment(cand.timeline, missing)}，"
                    f"无法从 {lsn_mod.format_lsn(cand.replay_lsn)} 连续重放到 "
                    f"{lsn_mod.format_lsn(commit_lsn)}"
                )

        verdicts.append(
            CandidateVerdict(
                node_id=cand.node_id,
                node_name=cand.node_name,
                timeline=cand.timeline,
                replay_lsn=cand.replay_lsn,
                eligible=not reasons,
                reasons=reasons,
                required_segments=required,
                missing_segment=missing,
            )
        )
    return FailoverDecision(
        primary_timeline=primary_timeline,
        commit_lsn=commit_lsn,
        verdicts=verdicts,
    )
