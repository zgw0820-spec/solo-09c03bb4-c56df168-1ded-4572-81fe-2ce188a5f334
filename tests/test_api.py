"""End-to-end HTTP tests through the FastAPI app."""
from __future__ import annotations


def _create(client) -> int:
    resp = client.post(
        "/api/incidents",
        json={
            "title": "API 故障",
            "primary_timeline": 1,
            "commit_lsn": "0/3000100",
            "nodes": [
                {"node_key": "primary", "name": "主库", "role": "primary"},
                {"node_key": "r1", "name": "副本1", "role": "candidate"},
                {"node_key": "r2", "name": "副本2", "role": "candidate"},
                {"node_key": "r3", "name": "副本3", "role": "candidate"},
                {"node_key": "r4", "name": "副本4", "role": "candidate"},
            ],
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


def test_full_failover_flow(client):
    iid = _create(client)

    obs = client.post(
        f"/api/incidents/{iid}/observations",
        json={
            "expected_version": 1,
            "client_request_id": "api-obs-1",
            "observations": [
                {"node_key": "r1", "timeline": 1, "replay_lsn": "0/3000100"},
                {"node_key": "r2", "timeline": 1, "replay_lsn": "0/2000050"},
                {"node_key": "r3", "timeline": 1, "replay_lsn": "0/1000080"},
                {"node_key": "r4", "timeline": 2, "replay_lsn": "0/3000080"},
            ],
        },
    )
    assert obs.status_code == 200, obs.text
    assert obs.json()["incident_version"] == 2

    arc = client.post(
        f"/api/incidents/{iid}/archives",
        json={
            "expected_version": 2,
            "client_request_id": "api-arc-1",
            "archives": [
                {
                    "timeline": 1,
                    "start_segment": "000000010000000000000002",
                    "end_segment": "000000010000000000000003",
                }
            ],
        },
    )
    assert arc.status_code == 200, arc.text

    check = client.post(
        f"/api/incidents/{iid}/checks",
        json={"expected_version": 3, "client_request_id": "api-chk-1"},
    )
    assert check.status_code == 200, check.text
    body = check.json()
    eligible = {c["node_id"]: c for c in body["candidates"] if c["eligible"]}
    rejected = {c["node_id"]: c for c in body["candidates"] if not c["eligible"]}
    assert len(eligible) == 2 and len(rejected) == 2

    promote = client.post(
        f"/api/incidents/{iid}/promote",
        json={"expected_version": 3, "request_id": "api-promote-1"},
    )
    assert promote.status_code == 200, promote.text
    assert promote.json()["promoted"] is True
    assert promote.json()["generation"] == 1

    # stale page (v3) must be rejected after the promotion bumped to v4
    stale = client.post(
        f"/api/incidents/{iid}/observations",
        json={
            "expected_version": 3,
            "client_request_id": "api-stale",
            "observations": [{"node_key": "r2", "timeline": 1, "replay_lsn": "0/2000050"}],
        },
    )
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "stale_version"

    # repeated promotion returns the result already in effect
    replay = client.post(
        f"/api/incidents/{iid}/promote",
        json={"expected_version": 4, "request_id": "api-promote-1"},
    )
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    assert replay.json()["generation"] == 1

    detail = client.get(f"/api/incidents/{iid}").json()
    assert detail["version"] == 4
    assert detail["active_primary"]["node_key"] == "r1"


def test_failed_promotion_keeps_primary(client):
    iid = _create(client)
    client.post(
        f"/api/incidents/{iid}/observations",
        json={
            "expected_version": 1,
            "client_request_id": "api-obs-bad",
            "observations": [{"node_key": "r4", "timeline": 2, "replay_lsn": "0/3000080"}],
        },
    )
    resp = client.post(
        f"/api/incidents/{iid}/promote",
        json={"expected_version": 2, "request_id": "api-promote-fail"},
    )
    assert resp.status_code == 200
    assert resp.json()["promoted"] is False
    detail = client.get(f"/api/incidents/{iid}").json()
    assert detail["active_primary"]["node_key"] == "primary"
    assert detail["generations"] == []
    assert detail["last_check"]["outcome"] == "failed_kept_old_primary"


def test_late_report_and_replay(client):
    iid = _create(client)
    client.post(
        f"/api/incidents/{iid}/observations",
        json={
            "expected_version": 1,
            "client_request_id": "api-obs-ok",
            "observations": [{"node_key": "r1", "timeline": 1, "replay_lsn": "0/3000100"}],
        },
    )
    client.post(
        f"/api/incidents/{iid}/promote",
        json={"expected_version": 2, "request_id": "api-promote-2"},
    )
    payload = {
        "node_key": "primary",
        "generation": 0,
        "reported_primary_timeline": 1,
        "reported_replay_lsn": "0/3000100",
        "request_id": "api-late",
    }
    first = client.post(f"/api/incidents/{iid}/recovery-reports", json=payload)
    assert first.status_code == 200
    assert first.json()["stale"] is True
    replay = client.post(f"/api/incidents/{iid}/recovery-reports", json=payload)
    assert replay.json()["idempotent_replay"] is True

    detail = client.get(f"/api/incidents/{iid}").json()
    assert detail["active_primary"]["node_key"] == "r1"


def test_validation_errors(client):
    iid = _create(client)
    bad_lsn = client.post(
        f"/api/incidents/{iid}/observations",
        json={
            "expected_version": 1,
            "observations": [{"node_key": "r1", "timeline": 1, "replay_lsn": "nope"}],
        },
    )
    assert bad_lsn.status_code == 400

    bad_seg = client.post(
        f"/api/incidents/{iid}/archives",
        json={
            "expected_version": 1,
            "archives": [
                {"timeline": 1, "start_segment": "00000003", "end_segment": "00000003"}
            ],
        },
    )
    assert bad_seg.status_code == 400


def test_html_pages_render(client):
    iid = _create(client)
    assert client.get("/").status_code == 200
    page = client.get(f"/incidents/{iid}")
    assert page.status_code == 200
    assert "接任检查".encode() in page.content


def test_html_form_post_and_redirect(client):
    resp = client.post(
        "/incidents",
        data={
            "title": "表单建单",
            "primary_timeline": "1",
            "commit_lsn": "0/3000100",
            "node_key_0": "primary",
            "name_0": "主库",
            "role_0": "primary",
            "node_key_1": "r9",
            "name_1": "副本9",
            "role_1": "candidate",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"].startswith("/incidents/")

    # invalid LSN re-renders the index page with an error
    bad = client.post(
        "/incidents",
        data={
            "title": "坏单",
            "primary_timeline": "1",
            "commit_lsn": "not-lsn",
            "node_key_0": "primary",
            "name_0": "主库",
            "role_0": "primary",
        },
    )
    assert bad.status_code == 400
    assert "invalid LSN".encode() in bad.content


def test_html_stale_form_renders_conflict_page(client):
    iid = _create(client)
    client.post(
        f"/api/incidents/{iid}/observations",
        json={
            "expected_version": 1,
            "client_request_id": "html-obs",
            "observations": [{"node_key": "r1", "timeline": 1, "replay_lsn": "0/3000100"}],
        },
    )
    resp = client.post(
        f"/incidents/{iid}/observations",
        data={
            "expected_version": "1",  # stale after the API call bumped to v2
            "request_id": "html-stale",
            "node_key_0": "r1",
            "timeline_0": "1",
            "replay_lsn_0": "0/3000100",
        },
    )
    assert resp.status_code == 409
    assert "过期提交".encode() in resp.content
