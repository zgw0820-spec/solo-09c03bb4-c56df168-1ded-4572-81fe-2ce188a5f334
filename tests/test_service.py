"""Service-layer tests for the state-machine and concurrency guarantees."""
from __future__ import annotations

import pytest

from app import service
from app.schemas import CheckRequest, PromoteRequest, RecoveryReport

from .conftest import archive, make_incident, observe

WATERMARK = "0/3000100"


def _seed_observations(session, incident_id: int) -> None:
    observe(
        session,
        incident_id,
        [
            ("r1", 1, WATERMARK),
            ("r2", 1, "0/2000050"),
            ("r3", 1, "0/1000080"),
            ("r4", 2, "0/3000080"),
        ],
        version=1,
        request_id="obs-1",
    )


def test_check_persists_every_rejection_reason(session):
    iid = make_incident(session)
    _seed_observations(session, iid)
    archive(session, iid, 1, "000000010000000000000002", "000000010000000000000003", version=2)

    result = service.run_check(session, iid, CheckRequest(expected_version=3))
    assert result["outcome"] == "evaluated"
    by_node = {}
    detail = service.incident_detail(session, iid)
    node_key_by_id = {n["id"]: n["node_key"] for n in detail["nodes"]}
    for c in result["candidates"]:
        by_node[node_key_by_id[c["node_id"]]] = c
    assert by_node["r1"]["eligible"] is True
    assert by_node["r2"]["eligible"] is True
    assert by_node["r3"]["eligible"] is False and by_node["r3"]["reasons"]
    assert by_node["r4"]["eligible"] is False and "timeline" in by_node["r4"]["reasons"][0]


def test_promote_success_bumps_generation_and_version(session):
    iid = make_incident(session)
    _seed_observations(session, iid)
    archive(session, iid, 1, "000000010000000000000002", "000000010000000000000003", version=2)

    before = service.incident_detail(session, iid)
    assert before["version"] == 3
    result = service.promote(
        session, iid, PromoteRequest(expected_version=3, request_id="promo-1")
    )
    assert result["promoted"] is True
    assert result["generation"] == 1
    assert result["new_primary"]["node_key"] == "r1"  # furthest replay
    after = service.incident_detail(session, iid)
    assert after["version"] == 4
    assert after["current_generation"] == 1
    assert after["active_primary"]["node_key"] == "r1"
    assert after["generations"][0]["old_primary"]["node_key"] == "primary"


def test_promote_without_eligible_keeps_old_primary_and_version(session):
    iid = make_incident(session)
    # r4 only, wrong timeline -> nobody eligible
    observe(session, iid, [("r4", 2, "0/3000080")], version=1, request_id="obs-bad")
    result = service.promote(
        session, iid, PromoteRequest(expected_version=2, request_id="promo-fail")
    )
    assert result["promoted"] is False
    assert "继续保留上次主库" in result["reason"]
    detail = service.incident_detail(session, iid)
    assert detail["version"] == 2  # nothing took effect -> version unchanged
    assert detail["active_primary"]["node_key"] == "primary"
    assert detail["generations"] == []
    assert detail["last_check"]["outcome"] == "failed_kept_old_primary"


def test_stale_observation_cannot_overwrite_confirmed_generation(session):
    iid = make_incident(session)
    _seed_observations(session, iid)
    archive(session, iid, 1, "000000010000000000000002", "000000010000000000000003", version=2)
    service.promote(session, iid, PromoteRequest(expected_version=3, request_id="promo-x"))

    # A page still open at v3 now posts observations against the old watermark.
    with pytest.raises(service.Conflict) as exc:
        observe(session, iid, [("r2", 1, "0/2000050")], version=3, request_id="stale-obs")
    assert exc.value.code == "stale_version"
    detail = service.incident_detail(session, iid)
    assert detail["active_primary"]["node_key"] == "r1"
    assert detail["version"] == 4


def test_promote_request_id_is_idempotent(session):
    iid = make_incident(session)
    _seed_observations(session, iid)
    archive(session, iid, 1, "000000010000000000000002", "000000010000000000000003", version=2)
    req = PromoteRequest(expected_version=3, request_id="promo-once")
    first = service.promote(session, iid, req)
    # replay even with a now-stale expected version returns the same result
    replay = service.promote(session, iid, req)
    assert replay["idempotent_replay"] is True
    assert replay["generation"] == first["generation"] == 1
    assert replay["new_primary"]["node_key"] == first["new_primary"]["node_key"]
    detail = service.incident_detail(session, iid)
    assert len(detail["generations"]) == 1
    assert detail["version"] == 4  # replay did not bump again


def test_failed_promote_replay_returns_same_failure(session):
    iid = make_incident(session)
    observe(session, iid, [("r4", 2, "0/3000080")], version=1, request_id="obs-bad2")
    req = PromoteRequest(expected_version=2, request_id="promo-fail2")
    first = service.promote(session, iid, req)
    replay = service.promote(session, iid, req)
    assert first["promoted"] is False and replay["promoted"] is False
    assert replay["idempotent_replay"] is True
    assert service.incident_detail(session, iid)["generations"] == []


def test_observation_batch_idempotency_key(session):
    iid = make_incident(session)
    batch_ver = 1
    first = observe(session, iid, [("r1", 1, WATERMARK)], version=batch_ver, request_id="dup-obs")
    replay = observe(session, iid, [("r1", 1, WATERMARK)], version=2, request_id="dup-obs")
    assert replay["idempotent_replay"] is True
    assert replay["observations"][0]["id"] == first["observations"][0]["id"]
    # only one observation row stored
    detail = service.incident_detail(session, iid)
    r1 = next(n for n in detail["nodes"] if n["node_key"] == "r1")
    assert r1["latest_observation"]["id"] == first["observations"][0]["id"]


def test_late_recovery_report_from_old_primary_is_ignored(session):
    iid = make_incident(session)
    _seed_observations(session, iid)
    archive(session, iid, 1, "000000010000000000000002", "000000010000000000000003", version=2)
    service.promote(session, iid, PromoteRequest(expected_version=3, request_id="promo-y"))
    version_after = service.incident_detail(session, iid)["version"]

    # old primary claims generation 0 (its old self)
    report = service.recovery_report(
        session,
        iid,
        RecoveryReport(
            node_key="primary",
            generation=0,
            reported_primary_timeline=1,
            reported_replay_lsn=WATERMARK,
            request_id="late-1",
        ),
    )
    assert report["stale"] is True and report["accepted"] is False
    detail = service.incident_detail(session, iid)
    assert detail["active_primary"]["node_key"] == "r1"
    assert detail["version"] == version_after  # stale report changes nothing
    # the ignored row is retained for audit
    primary = next(n for n in detail["nodes"] if n["node_key"] == "primary")
    assert primary["latest_observation"]["accepted"] is False

    # replay of the same late report returns the stored result
    replay = service.recovery_report(
        session,
        iid,
        RecoveryReport(
            node_key="primary", generation=0, reported_primary_timeline=1,
            reported_replay_lsn=WATERMARK, request_id="late-1",
        ),
    )
    assert replay["idempotent_replay"] is True and replay["stale"] is True


def test_recovery_report_from_current_primary_is_accepted(session):
    iid = make_incident(session)
    report = service.recovery_report(
        session,
        iid,
        RecoveryReport(
            node_key="primary",
            generation=0,
            reported_primary_timeline=1,
            reported_replay_lsn="0/3000100",
            request_id="current-1",
        ),
    )
    assert report["stale"] is False and report["accepted"] is True
    assert service.incident_detail(session, iid)["active_primary"]["node_key"] == "primary"


def test_explicit_node_must_be_eligible(session):
    iid = make_incident(session)
    _seed_observations(session, iid)
    archive(session, iid, 1, "000000010000000000000002", "000000010000000000000003", version=2)
    with pytest.raises(service.ValidationFailed):
        service.promote(
            session,
            iid,
            PromoteRequest(expected_version=3, request_id="promo-badnode", node_key="r4"),
        )


def test_invalid_lsn_rejected(session):
    iid = make_incident(session)
    with pytest.raises(service.ValidationFailed):
        observe(session, iid, [("r1", 1, "garbage")], version=1, request_id="bad-lsn")
