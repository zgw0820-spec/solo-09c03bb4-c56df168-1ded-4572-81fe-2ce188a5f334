"""Transactional service layer.

All concurrency rules live here:

* ``expected_version`` implements optimistic concurrency.  A page rendered at
  version *n* must post *n*; once a takeover confirmation bumped the incident,
  any stale observation/archive form is rejected with HTTP 409 instead of
  overwriting the confirmed primary generation.
* ``request_id`` / ``client_request_id`` make takeover confirmations, checks
  and late recovery reports idempotent: a replayed request returns the result
  that already took effect, never a second mutation.
* Every accepted evaluation is persisted as a ``CheckRun`` with per-candidate
  verdicts, so rejection reasons remain auditable.
"""
from __future__ import annotations

import json

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import failover, lsn as lsn_mod
from .config import settings
from .models import (
    ArchiveSegment,
    CheckCandidate,
    CheckRun,
    FailoverGeneration,
    IdempotencyRecord,
    Incident,
    Node,
    Observation,
)
from .schemas import (
    ArchiveBatch,
    CheckRequest,
    IncidentCreate,
    ObservationBatch,
    PromoteRequest,
    RecoveryReport,
)


class ServiceError(Exception):
    """4xx-level error carrying an HTTP status code."""

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class NotFound(ServiceError):
    def __init__(self, message: str) -> None:
        super().__init__(404, "not_found", message)


class Conflict(ServiceError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(409, code, message)


class ValidationFailed(ServiceError):
    def __init__(self, message: str) -> None:
        super().__init__(400, "validation_error", message)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _lock_incident(session: Session, incident_id: int) -> Incident:
    incident = session.get(Incident, incident_id)
    if incident is None:
        raise NotFound(f"incident {incident_id} not found")
    if settings.schema_lock_mode == "postgresql":
        session.execute(
            select(Incident).where(Incident.id == incident_id).with_for_update()
        )
    return incident


def _check_version(incident: Incident, expected: int) -> None:
    if expected != incident.version:
        raise Conflict(
            "stale_version",
            f"页面基于版本 {expected}，但当前已是版本 {incident.version}；"
            "请刷新后重试，过期提交不会覆盖已确认的主库代次",
        )


def _idem_get(session: Session, request_id: str) -> dict | None:
    record = session.get(IdempotencyRecord, request_id)
    if record is None:
        return None
    payload = json.loads(record.response_json)
    payload["idempotent_replay"] = True
    return payload


def _idem_store(session: Session, incident_id: int, endpoint: str, request_id: str, payload: dict) -> None:
    record = IdempotencyRecord(
        request_id=request_id,
        incident_id=incident_id,
        endpoint=endpoint,
        response_json=json.dumps(payload, ensure_ascii=False, default=str),
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        # Same request id already used (the incident row lock normally
        # serializes this; a global-PK clash then means cross-incident reuse).
        raise Conflict(
            "request_id_reused",
            f"request_id {request_id!r} 已被其他请求使用，幂等键不可跨请求复用",
        ) from exc


def _parse_lsn(text: str, label: str) -> int:
    try:
        return lsn_mod.parse_lsn(text)
    except lsn_mod.LsnError as exc:
        raise ValidationFailed(f"{label}: {exc}") from exc


def _get_node(incident: Incident, node_key: str) -> Node:
    for node in incident.nodes:
        if node.node_key == node_key:
            return node
    raise ValidationFailed(f"未知节点 {node_key!r}")


def _observations(session: Session, incident_id: int, *, accepted_only: bool) -> dict[int, Observation]:
    stmt = (
        select(Observation)
        .join(Node, Node.id == Observation.node_id)
        .where(Node.incident_id == incident_id)
    )
    if accepted_only:
        stmt = stmt.where(Observation.accepted.is_(True))
    rows = session.scalars(stmt.order_by(Observation.id)).all()
    latest: dict[int, Observation] = {}
    for row in rows:  # ordered by id, so the last one wins
        latest[row.node_id] = row
    return latest


def _latest_observations(session: Session, incident_id: int) -> dict[int, Observation]:
    """All latest observations, including ignored late reports (audit view)."""
    return _observations(session, incident_id, accepted_only=False)


def _accepted_observations(session: Session, incident_id: int) -> dict[int, Observation]:
    return _observations(session, incident_id, accepted_only=True)


def _archive_inputs(session: Session, incident_id: int) -> list[failover.ArchiveRange]:
    rows = session.scalars(
        select(ArchiveSegment).where(ArchiveSegment.incident_id == incident_id)
    ).all()
    return [
        failover.ArchiveRange(r.timeline, r.start_segment, r.end_segment) for r in rows
    ]


def _watermark(
    incident: Incident, timeline_override: int | None, lsn_text_override: str | None
) -> tuple[int, int, str]:
    timeline = timeline_override if timeline_override is not None else incident.current_primary_timeline
    if lsn_text_override is not None:
        commit_lsn = _parse_lsn(lsn_text_override, "主库最后提交 LSN")
        commit_text = lsn_text_override.strip()
    else:
        if incident.current_commit_lsn is None:
            raise ValidationFailed("尚未登记主库最后提交 LSN，无法评估")
        commit_lsn = incident.current_commit_lsn
        commit_text = incident.current_commit_lsn_text or lsn_mod.format_lsn(commit_lsn)
    if timeline is None:
        raise ValidationFailed("尚未登记主库 timeline，无法评估")
    return timeline, commit_lsn, commit_text


def _persist_check(
    session: Session,
    incident: Incident,
    timeline: int,
    commit_lsn: int,
    commit_text: str,
    decision: failover.FailoverDecision,
    outcome: str,
    request_id: str | None,
    detail: str | None,
) -> CheckRun:
    check = CheckRun(
        incident_id=incident.id,
        incident_version=incident.version,
        primary_timeline=timeline,
        commit_lsn=commit_lsn,
        commit_lsn_text=commit_text,
        outcome=outcome,
        request_id=request_id,
        detail=detail,
    )
    session.add(check)
    session.flush()
    node_by_id = {n.id: n for n in incident.nodes}
    for verdict in decision.verdicts:
        node = node_by_id[verdict.node_id]
        session.add(
            CheckCandidate(
                check_run_id=check.id,
                node_id=node.id,
                timeline=verdict.timeline,
                replay_lsn=verdict.replay_lsn,
                replay_lsn_text=lsn_mod.format_lsn(verdict.replay_lsn),
                eligible=verdict.eligible,
                reasons_json=json.dumps(verdict.reasons, ensure_ascii=False),
                required_segments_json=json.dumps(verdict.required_segments),
                missing_segment=verdict.missing_segment,
            )
        )
    return check


def _evaluate_now(
    session: Session,
    incident: Incident,
    timeline: int,
    commit_lsn: int,
) -> failover.FailoverDecision:
    latest = _accepted_observations(session, incident.id)
    candidates: list[failover.CandidateInput] = []
    for node in incident.nodes:
        if node.role != "candidate":
            continue
        obs = latest.get(node.id)
        if obs is None:
            continue
        candidates.append(
            failover.CandidateInput(
                node_id=node.id,
                node_name=node.name,
                timeline=obs.timeline,
                replay_lsn=obs.replay_lsn,
                observed_at=obs.created_at.isoformat() if obs.created_at else "",
            )
        )
    return failover.evaluate(timeline, commit_lsn, candidates, _archive_inputs(session, incident.id))


# --------------------------------------------------------------------------- #
# write operations
# --------------------------------------------------------------------------- #
def create_incident(session: Session, data: IncidentCreate) -> Incident:
    commit_lsn = _parse_lsn(data.commit_lsn, "主库最后提交 LSN")
    keys = [n.node_key for n in data.nodes]
    if len(set(keys)) != len(keys):
        raise ValidationFailed("节点标识 node_key 不能重复")
    primaries = [n for n in data.nodes if n.role == "primary"]
    if len(primaries) != 1:
        raise ValidationFailed("必须恰好登记 1 个主库节点")

    incident = Incident(
        title=data.title,
        version=1,
        current_primary_timeline=data.primary_timeline,
        current_commit_lsn=commit_lsn,
        current_commit_lsn_text=data.commit_lsn.strip(),
    )
    session.add(incident)
    session.flush()
    for spec in data.nodes:
        session.add(
            Node(
                incident_id=incident.id,
                node_key=spec.node_key,
                name=spec.name,
                role=spec.role,
                generation=0,
                is_active_primary=(spec.role == "primary"),
            )
        )
    session.flush()
    return incident


def submit_observations(session: Session, incident_id: int, batch: ObservationBatch) -> dict:
    incident = _lock_incident(session, incident_id)
    # Idempotent replays are served before the version check: a request that
    # already took effect must keep returning its result from a stale page.
    if batch.client_request_id:
        replay = _idem_get(session, batch.client_request_id)
        if replay is not None:
            return replay
    _check_version(incident, batch.expected_version)

    stored: list[dict] = []
    for spec in batch.observations:
        node = _get_node(incident, spec.node_key)
        replay_lsn = _parse_lsn(spec.replay_lsn, f"副本 {node.name} 的 replay LSN")
        obs = Observation(
            node_id=node.id,
            incident_version=incident.version,
            timeline=spec.timeline,
            replay_lsn=replay_lsn,
            replay_lsn_text=spec.replay_lsn.strip(),
            source="operator",
            client_request_id=batch.client_request_id,
            accepted=True,
        )
        session.add(obs)
        session.flush()
        stored.append(_observation_dict(obs))

    incident.version += 1
    payload = {"idempotent_replay": False, "incident_version": incident.version, "observations": stored}
    if batch.client_request_id:
        _idem_store(session, incident_id, "observations", batch.client_request_id, payload)
    return payload


def add_archives(session: Session, incident_id: int, batch: ArchiveBatch) -> dict:
    incident = _lock_incident(session, incident_id)
    if batch.client_request_id:
        replay = _idem_get(session, batch.client_request_id)
        if replay is not None:
            return replay
    _check_version(incident, batch.expected_version)

    stored: list[dict] = []
    for spec in batch.archives:
        try:
            tli_start, start_seg = lsn_mod.parse_segment(spec.start_segment)
            tli_end, end_seg = lsn_mod.parse_segment(spec.end_segment)
        except lsn_mod.LsnError as exc:
            raise ValidationFailed(str(exc)) from exc
        if tli_start != spec.timeline or tli_end != spec.timeline:
            raise ValidationFailed(
                f"归档区间 timeline 前缀({tli_start}/{tli_end})与填写的 timeline {spec.timeline} 不一致"
            )
        if start_seg > end_seg:
            start_seg, end_seg = end_seg, start_seg
        row = ArchiveSegment(
            incident_id=incident.id,
            timeline=spec.timeline,
            start_segment=start_seg,
            end_segment=end_seg,
            start_text=spec.start_segment.strip(),
            end_text=spec.end_segment.strip(),
        )
        session.add(row)
        try:
            session.flush()
        except IntegrityError as exc:
            raise Conflict("duplicate_archive", "归档区间重复登记") from exc
        stored.append(
            {
                "timeline": spec.timeline,
                "start_segment": lsn_mod.format_segment(spec.timeline, start_seg),
                "end_segment": lsn_mod.format_segment(spec.timeline, end_seg),
            }
        )
    incident.version += 1
    payload = {"idempotent_replay": False, "incident_version": incident.version, "archives": stored}
    if batch.client_request_id:
        _idem_store(session, incident_id, "archives", batch.client_request_id, payload)
    return payload


def run_check(session: Session, incident_id: int, req: CheckRequest) -> dict:
    incident = _lock_incident(session, incident_id)
    if req.client_request_id:
        replay = _idem_get(session, req.client_request_id)
        if replay is not None:
            return replay
    _check_version(incident, req.expected_version)

    timeline, commit_lsn, commit_text = _watermark(
        incident, req.primary_timeline, req.commit_lsn
    )
    decision = _evaluate_now(session, incident, timeline, commit_lsn)
    check = _persist_check(
        session,
        incident,
        timeline,
        commit_lsn,
        commit_text,
        decision,
        outcome="evaluated",
        request_id=req.client_request_id,
        detail=None,
    )
    session.flush()
    payload = _check_payload(session, check, idempotent=False)
    if req.client_request_id:
        _idem_store(session, incident_id, "check", req.client_request_id, payload)
    return payload


def promote(session: Session, incident_id: int, req: PromoteRequest) -> dict:
    incident = _lock_incident(session, incident_id)

    # Idempotency: a repeated confirmation returns whatever already took
    # effect (promotion or the failed check that kept the old primary),
    # even if the version has since moved on. Checked before version control.
    replay = _idem_get(session, req.request_id)
    if replay is not None:
        return replay
    existing_gen = session.scalars(
        select(FailoverGeneration).where(FailoverGeneration.request_id == req.request_id)
    ).first()
    if existing_gen is not None:  # defensive: rows created before idempotency table
        return _promotion_payload(session, incident, existing_gen, idempotent=True)
    _check_version(incident, req.expected_version)

    timeline, commit_lsn, commit_text = _watermark(
        incident, req.primary_timeline, req.commit_lsn
    )
    decision = _evaluate_now(session, incident, timeline, commit_lsn)
    eligible = decision.eligible

    if not eligible:
        check = _persist_check(
            session,
            incident,
            timeline,
            commit_lsn,
            commit_text,
            decision,
            outcome="failed_kept_old_primary",
            request_id=req.request_id,
            detail="没有可接任副本，继续保留上次主库",
        )
        session.flush()
        active = next((n for n in incident.nodes if n.is_active_primary), None)
        payload = {
            "promoted": False,
            "idempotent_replay": False,
            "incident_version": incident.version,
            "reason": "没有可接任副本，检查失败后继续保留上次主库",
            "active_primary": _node_dict(active) if active else None,
            "check": _check_payload(session, check, idempotent=False),
        }
        # Version is intentionally NOT bumped: nothing took effect, so the
        # operator can fix observations and retry. The failed check is stored
        # under the request id so a replay returns the same result.
        _idem_store(session, incident_id, "promote", req.request_id, payload)
        return payload

    if req.node_key is not None:
        chosen = next((v for v in eligible if next(
            (n for n in incident.nodes if n.id == v.node_id), None
        ).node_key == req.node_key), None)
        if chosen is None:
            raise ValidationFailed(f"指定节点 {req.node_key!r} 不在可接任副本之列")
    else:
        # Deterministic choice: furthest replayed, lowest node id as tiebreak.
        chosen = sorted(eligible, key=lambda v: (-v.replay_lsn, v.node_id))[0]

    old_primary = next((n for n in incident.nodes if n.is_active_primary), None)
    new_primary = session.get(Node, chosen.node_id)
    next_generation = (
        max((g.generation for g in incident.generations), default=0) + 1
    )

    generation = FailoverGeneration(
        incident_id=incident.id,
        generation=next_generation,
        old_primary_id=old_primary.id if old_primary else None,
        new_primary_id=new_primary.id,
        commit_lsn=commit_lsn,
        commit_lsn_text=commit_text,
        timeline=timeline,
        request_id=req.request_id,
    )
    session.add(generation)
    if old_primary is not None:
        old_primary.is_active_primary = False
    new_primary.is_active_primary = True
    new_primary.generation = next_generation
    session.flush()

    check = _persist_check(
        session,
        incident,
        timeline,
        commit_lsn,
        commit_text,
        decision,
        outcome="promoted",
        request_id=req.request_id,
        detail=f"第 {next_generation} 代主库：{new_primary.name}",
    )
    session.flush()
    incident.version += 1
    payload = _promotion_payload(session, incident, generation, idempotent=False, check=check)
    _idem_store(session, incident_id, "promote", req.request_id, payload)
    return payload


def recovery_report(session: Session, incident_id: int, report: RecoveryReport) -> dict:
    """Accept a recovery/heartbeat report from a node.

    Reports from the currently active primary update its observation.  Reports
    arriving late from an old primary (or claiming an older generation) are
    recorded but never touch the confirmed generation; the caller receives the
    primary currently in effect.  Replayed request ids return the stored row.
    """
    incident = _lock_incident(session, incident_id)
    node = _get_node(incident, report.node_key)
    replay_lsn = _parse_lsn(report.reported_replay_lsn, "恢复回报 replay LSN")

    replay = _idem_get(session, report.request_id)
    if replay is not None:
        return replay

    current_generation = max((g.generation for g in incident.generations), default=0)
    active = next((n for n in incident.nodes if n.is_active_primary), None)
    is_current = (
        active is not None
        and active.id == node.id
        and report.generation == current_generation
    )

    obs = Observation(
        node_id=node.id,
        incident_version=incident.version,
        timeline=report.reported_primary_timeline,
        replay_lsn=replay_lsn,
        replay_lsn_text=report.reported_replay_lsn.strip(),
        source="recovery_report",
        client_request_id=report.request_id,
        accepted=is_current,
        note=(
            None
            if is_current
            else f"迟到/过期回报：节点声称第 {report.generation} 代，当前为第 {current_generation} 代，已忽略"
        ),
    )
    session.add(obs)
    session.flush()
    # Only a report from the primary actually in effect mutates state; stale
    # reports from an old primary are audit rows and must not bump the version.
    if is_current:
        incident.version += 1
    payload = {
        "idempotent_replay": False,
        "stale": not is_current,
        "accepted": is_current,
        "observation": _observation_dict(obs),
        "active_primary": _node_dict(active) if active else None,
        "current_generation": current_generation,
    }
    _idem_store(session, incident_id, "recovery_report", report.request_id, payload)
    return payload


# --------------------------------------------------------------------------- #
# read models
# --------------------------------------------------------------------------- #
def _node_dict(node: Node) -> dict:
    return {
        "id": node.id,
        "node_key": node.node_key,
        "name": node.name,
        "role": node.role,
        "generation": node.generation,
        "is_active_primary": node.is_active_primary,
    }


def _observation_dict(obs: Observation) -> dict:
    return {
        "id": obs.id,
        "node_id": obs.node_id,
        "timeline": obs.timeline,
        "replay_lsn": obs.replay_lsn,
        "replay_lsn_text": obs.replay_lsn_text,
        "source": obs.source,
        "accepted": obs.accepted,
        "note": obs.note,
        "created_at": obs.created_at.isoformat() if obs.created_at else None,
    }


def _check_payload(session: Session, check: CheckRun, idempotent: bool) -> dict:
    candidates = session.scalars(
        select(CheckCandidate).where(CheckCandidate.check_run_id == check.id)
    ).all()
    return {
        "idempotent_replay": idempotent,
        "check_id": check.id,
        "incident_version": check.incident_version,
        "primary_timeline": check.primary_timeline,
        "commit_lsn": check.commit_lsn,
        "commit_lsn_text": check.commit_lsn_text,
        "outcome": check.outcome,
        "detail": check.detail,
        "candidates": [
            {
                "node_id": c.node_id,
                "timeline": c.timeline,
                "replay_lsn_text": c.replay_lsn_text,
                "eligible": c.eligible,
                "reasons": json.loads(c.reasons_json),
                "required_segments": json.loads(c.required_segments_json),
                "missing_segment": c.missing_segment,
            }
            for c in candidates
        ],
    }


def _promotion_payload(
    session: Session,
    incident: Incident,
    generation: FailoverGeneration,
    idempotent: bool,
    check: CheckRun | None = None,
) -> dict:
    new_primary = session.get(Node, generation.new_primary_id)
    old_primary = (
        session.get(Node, generation.old_primary_id)
        if generation.old_primary_id
        else None
    )
    if check is None:
        check = session.scalars(
            select(CheckRun).where(CheckRun.request_id == generation.request_id)
        ).first()
    payload = {
        "promoted": True,
        "idempotent_replay": idempotent,
        "incident_version": incident.version,
        "generation": generation.generation,
        "new_primary": _node_dict(new_primary),
        "old_primary": _node_dict(old_primary) if old_primary else None,
        "commit_lsn_text": generation.commit_lsn_text,
        "timeline": generation.timeline,
    }
    if check is not None:
        payload["check"] = _check_payload(session, check, idempotent=idempotent)
    return payload


def incident_detail(session: Session, incident_id: int) -> dict:
    incident = session.get(Incident, incident_id)
    if incident is None:
        raise NotFound(f"incident {incident_id} not found")
    latest = _latest_observations(session, incident_id)
    active = next((n for n in incident.nodes if n.is_active_primary), None)
    current_generation = max((g.generation for g in incident.generations), default=0)
    last_check = session.scalars(
        select(CheckRun)
        .where(CheckRun.incident_id == incident_id)
        .order_by(CheckRun.id.desc())
    ).first()

    nodes_out = []
    for node in incident.nodes:
        obs = latest.get(node.id)
        nodes_out.append(
            {
                **_node_dict(node),
                "latest_observation": _observation_dict(obs) if obs else None,
            }
        )
    archives = [
        {
            "timeline": a.timeline,
            "start_segment": a.start_text,
            "end_segment": a.end_text,
        }
        for a in sorted(incident.archives, key=lambda a: (a.timeline, a.start_segment))
    ]
    generations = [
        {
            "generation": g.generation,
            "new_primary": _node_dict(session.get(Node, g.new_primary_id)),
            "old_primary": _node_dict(session.get(Node, g.old_primary_id))
            if g.old_primary_id
            else None,
            "commit_lsn_text": g.commit_lsn_text,
            "timeline": g.timeline,
            "request_id": g.request_id,
            "created_at": g.created_at.isoformat() if g.created_at else None,
        }
        for g in sorted(incident.generations, key=lambda g: g.generation)
    ]
    return {
        "id": incident.id,
        "title": incident.title,
        "version": incident.version,
        "current_primary_timeline": incident.current_primary_timeline,
        "current_commit_lsn_text": incident.current_commit_lsn_text,
        "active_primary": _node_dict(active) if active else None,
        "current_generation": current_generation,
        "nodes": nodes_out,
        "archives": archives,
        "generations": generations,
        "last_check": _check_payload(session, last_check, idempotent=False)
        if last_check
        else None,
    }


def list_incidents(session: Session) -> list[dict]:
    incidents = session.scalars(select(Incident).order_by(Incident.id)).all()
    return [
        {
            "id": i.id,
            "title": i.title,
            "version": i.version,
            "current_commit_lsn_text": i.current_commit_lsn_text,
            "current_generation": max((g.generation for g in i.generations), default=0),
            "active_primary": next(
                (_node_dict(n) for n in i.nodes if n.is_active_primary), None
            ),
        }
        for i in incidents
    ]
