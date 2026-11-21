"""Pytest fixtures: the whole suite runs on SQLite by default.

The service code is database-agnostic; PostgreSQL-specific row locks are
skipped outside PostgreSQL (see app.config).  A separate docker-based job can
point DATABASE_URL at the real Postgres 16 container with the same tests.
"""
from __future__ import annotations

import os
import tempfile

import pytest

_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp.name}"

from fastapi.testclient import TestClient  # noqa: E402

from app import service  # noqa: E402
from app.main import app  # noqa: E402
from app.models import Base, SessionLocal, engine, get_session  # noqa: E402
from app.schemas import (  # noqa: E402
    ArchiveBatch,
    ArchiveSpec,
    IncidentCreate,
    NodeSpec,
    ObservationBatch,
    ObservationSpec,
)


@pytest.fixture(scope="session", autouse=True)
def _schema():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def session():
    """Fresh tables per test for isolation."""
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        yield db
        db.rollback()
    finally:
        db.close()


@pytest.fixture()
def client():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    def _override():
        db = SessionLocal()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    app.dependency_overrides[get_session] = _override
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def make_incident(session, commit_lsn: str = "0/3000100", timeline: int = 1) -> int:
    incident = service.create_incident(
        session,
        IncidentCreate(
            title="测试故障",
            primary_timeline=timeline,
            commit_lsn=commit_lsn,
            nodes=[
                NodeSpec(node_key="primary", name="主库", role="primary"),
                NodeSpec(node_key="r1", name="副本1", role="candidate"),
                NodeSpec(node_key="r2", name="副本2", role="candidate"),
                NodeSpec(node_key="r3", name="副本3", role="candidate"),
                NodeSpec(node_key="r4", name="副本4", role="candidate"),
            ],
        ),
    )
    session.flush()
    return incident.id


def observe(session, incident_id: int, rows: list[tuple[str, int, str]], version: int,
            request_id: str | None = None) -> dict:
    return service.submit_observations(
        session,
        incident_id,
        ObservationBatch(
            expected_version=version,
            client_request_id=request_id,
            observations=[
                ObservationSpec(node_key=k, timeline=t, replay_lsn=l) for k, t, l in rows
            ],
        ),
    )


def archive(session, incident_id: int, timeline: int, start: str, end: str, version: int,
            request_id: str | None = None) -> dict:
    return service.add_archives(
        session,
        incident_id,
        ArchiveBatch(
            expected_version=version,
            client_request_id=request_id,
            archives=[ArchiveSpec(timeline=timeline, start_segment=start, end_segment=end)],
        ),
    )
