"""SQLAlchemy ORM models and engine/session management.

The schema deliberately keeps every user-visible value twice: LSNs/timelines
as integers used by the rules, and the original text the operator typed, so
the UI can show exactly what was entered.
"""
from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)

from .config import settings


class Base(DeclarativeBase):
    pass


class Incident(Base):
    __tablename__ = "incidents"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    # Monotonic version bumped on every *accepted* state change; stale pages
    # post the version they saw and are rejected when it no longer matches.
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    # Watermark of the generation currently being evaluated: the last LSN the
    # active primary committed before the failure.
    current_primary_timeline: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_commit_lsn: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    current_commit_lsn_text: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    nodes: Mapped[list["Node"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan"
    )
    generations: Mapped[list["FailoverGeneration"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan"
    )
    checks: Mapped[list["CheckRun"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan"
    )
    archives: Mapped[list["ArchiveSegment"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan"
    )


class Node(Base):
    __tablename__ = "nodes"
    __table_args__ = (UniqueConstraint("incident_id", "node_key", name="uq_node_key"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    incident_id: Mapped[int] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"))
    node_key: Mapped[str] = mapped_column(String(80), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    role: Mapped[str] = mapped_column(String(20), nullable=False)  # primary | candidate
    # Current generation claim for this node; 1-based, 0 means never promoted.
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_active_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    incident: Mapped[Incident] = relationship(back_populates="nodes")
    observations: Mapped[list["Observation"]] = relationship(
        back_populates="node", cascade="all, delete-orphan"
    )


class FailoverGeneration(Base):
    """One committed takeover decision (a new primary generation)."""

    __tablename__ = "failover_generations"

    id: Mapped[int] = mapped_column(primary_key=True)
    incident_id: Mapped[int] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"))
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    old_primary_id: Mapped[int | None] = mapped_column(ForeignKey("nodes.id"), nullable=True)
    new_primary_id: Mapped[int] = mapped_column(ForeignKey("nodes.id"), nullable=False)
    # Watermark recorded against the generation: last committed LSN of the
    # primary that just failed.
    commit_lsn: Mapped[int] = mapped_column(BigInteger, nullable=False)
    commit_lsn_text: Mapped[str] = mapped_column(String(32), nullable=False)
    timeline: Mapped[int] = mapped_column(Integer, nullable=False)
    request_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    incident: Mapped[Incident] = relationship(back_populates="generations")


class IdempotencyRecord(Base):
    """Stored first response for a client request id; replays return it as-is."""

    __tablename__ = "idempotency_records"

    request_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    incident_id: Mapped[int] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"))
    endpoint: Mapped[str] = mapped_column(String(60), nullable=False)
    response_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Observation(Base):
    """An operator-entered snapshot of one replica's replay state."""

    __tablename__ = "observations"

    id: Mapped[int] = mapped_column(primary_key=True)
    node_id: Mapped[int] = mapped_column(ForeignKey("nodes.id", ondelete="CASCADE"))
    incident_version: Mapped[int] = mapped_column(Integer, nullable=False)
    timeline: Mapped[int] = mapped_column(Integer, nullable=False)
    replay_lsn: Mapped[int] = mapped_column(BigInteger, nullable=False)
    replay_lsn_text: Mapped[str] = mapped_column(String(32), nullable=False)
    source: Mapped[str] = mapped_column(String(40), nullable=False, default="operator")
    # Audit-only; idempotency itself is enforced via IdempotencyRecord.
    client_request_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    # Late reports coming from a node that is no longer the active primary are
    # kept for audit but never mutate the confirmed generation.
    accepted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    node: Mapped[Node] = relationship(back_populates="observations")


class ArchiveSegment(Base):
    __tablename__ = "archive_segments"
    __table_args__ = (
        UniqueConstraint(
            "incident_id", "timeline", "start_segment", name="uq_archive_range"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    incident_id: Mapped[int] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"))
    timeline: Mapped[int] = mapped_column(Integer, nullable=False)
    start_segment: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_segment: Mapped[int] = mapped_column(BigInteger, nullable=False)
    start_text: Mapped[str] = mapped_column(String(40), nullable=False)
    end_text: Mapped[str] = mapped_column(String(40), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    incident: Mapped[Incident] = relationship(back_populates="archives")


class CheckRun(Base):
    """One evaluation pass; its candidate rows persist every rejection reason."""

    __tablename__ = "check_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    incident_id: Mapped[int] = mapped_column(ForeignKey("incidents.id", ondelete="CASCADE"))
    incident_version: Mapped[int] = mapped_column(Integer, nullable=False)
    primary_timeline: Mapped[int] = mapped_column(Integer, nullable=False)
    commit_lsn: Mapped[int] = mapped_column(BigInteger, nullable=False)
    commit_lsn_text: Mapped[str] = mapped_column(String(32), nullable=False)
    # outcome: evaluated | promoted | failed_kept_old_primary
    outcome: Mapped[str] = mapped_column(String(40), nullable=False)
    request_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    incident: Mapped[Incident] = relationship(back_populates="checks")
    candidates: Mapped[list["CheckCandidate"]] = relationship(
        back_populates="check_run", cascade="all, delete-orphan"
    )


class CheckCandidate(Base):
    __tablename__ = "check_candidates"

    id: Mapped[int] = mapped_column(primary_key=True)
    check_run_id: Mapped[int] = mapped_column(ForeignKey("check_runs.id", ondelete="CASCADE"))
    node_id: Mapped[int] = mapped_column(ForeignKey("nodes.id"), nullable=False)
    timeline: Mapped[int] = mapped_column(Integer, nullable=False)
    replay_lsn: Mapped[int] = mapped_column(BigInteger, nullable=False)
    replay_lsn_text: Mapped[str] = mapped_column(String(32), nullable=False)
    eligible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reasons_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    required_segments_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    missing_segment: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    check_run: Mapped[CheckRun] = relationship(back_populates="candidates")


engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


def get_session() -> Iterator[Session]:
    """FastAPI dependency: one transactional session per request."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
