"""Load the local demo incident.

Usage:
    python -m scripts.seed                 # uses DATABASE_URL / PG_* env vars
    DATABASE_URL=sqlite:///./demo.db python -m scripts.seed   # offline demo

The data deliberately covers every verdict of the eligibility rules:

* replica-1  already at the watermark                 -> eligible
* replica-2  behind, archive covers the needed WAL    -> eligible
* replica-3  behind, archive has a gap                -> rejected
* replica-4  on a different timeline                  -> rejected
"""
from __future__ import annotations

import sys

from sqlalchemy import select

from app import service
from app.models import Incident, SessionLocal, init_db
from app.schemas import (
    ArchiveBatch,
    ArchiveSpec,
    IncidentCreate,
    NodeSpec,
    ObservationBatch,
    ObservationSpec,
)

DEMO_TITLE = "本地样例：主库磁盘故障"

# Watermark 0/3000100 -> segment 3 (0x3000100 / 16MiB = 3).
WATERMARK = "0/3000100"
TIMELINE = 1


def seed() -> int:
    init_db()
    session = SessionLocal()
    try:
        existing = session.scalars(
            select(Incident).where(Incident.title == DEMO_TITLE)
        ).first()
        if existing is not None:
            print(f"样例事件已存在：incident id={existing.id}，跳过重复写入")
            return existing.id

        incident = service.create_incident(
            session,
            IncidentCreate(
                title=DEMO_TITLE,
                primary_timeline=TIMELINE,
                commit_lsn=WATERMARK,
                nodes=[
                    NodeSpec(node_key="primary", name="主库 pg-primary", role="primary"),
                    NodeSpec(node_key="replica-1", name="副本 replica-1（已追平）", role="candidate"),
                    NodeSpec(node_key="replica-2", name="副本 replica-2（落后·归档可补齐）", role="candidate"),
                    NodeSpec(node_key="replica-3", name="副本 replica-3（落后·归档有缺口）", role="candidate"),
                    NodeSpec(node_key="replica-4", name="副本 replica-4（timeline 分叉）", role="candidate"),
                ],
            ),
        )
        session.flush()

        service.submit_observations(
            session,
            incident.id,
            ObservationBatch(
                expected_version=1,
                client_request_id="seed-observations-v1",
                observations=[
                    ObservationSpec(node_key="replica-1", timeline=1, replay_lsn=WATERMARK),
                    # 0/2000050 sits in segment 2; needs segments 2..3
                    ObservationSpec(node_key="replica-2", timeline=1, replay_lsn="0/2000050"),
                    # 0/1000080 sits in segment 1; needs segments 1..3
                    ObservationSpec(node_key="replica-3", timeline=1, replay_lsn="0/1000080"),
                    ObservationSpec(node_key="replica-4", timeline=2, replay_lsn="0/3000080"),
                ],
            ),
        )

        # replica-2 needs segments 2..3 and this range covers both.
        # replica-3 needs segments 1..3: segment 1 stays missing so it fails
        # the continuity rule.
        service.add_archives(
            session,
            incident.id,
            ArchiveBatch(
                expected_version=2,
                client_request_id="seed-archive-1",
                archives=[
                    ArchiveSpec(
                        timeline=1,
                        start_segment="000000010000000000000002",
                        end_segment="000000010000000000000003",
                    ),
                ],
            ),
        )
        # A second range further along; still no segment 1 for replica-3.
        service.add_archives(
            session,
            incident.id,
            ArchiveBatch(
                expected_version=3,
                client_request_id="seed-archive-2",
                archives=[
                    ArchiveSpec(
                        timeline=1,
                        start_segment="000000010000000000000005",
                        end_segment="000000010000000000000006",
                    ),
                ],
            ),
        )
        session.commit()
        print(f"样例事件已创建：incident id={incident.id}")
        print("  replica-1 可接任（已追平水位）")
        print("  replica-2 可接任（segment 2..3 归档齐全）")
        print("  replica-3 淘汰（缺少 segment 1，归档不连续）")
        print("  replica-4 淘汰（timeline 2 ≠ 主库 timeline 1）")
        return incident.id
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(0 if seed() else 1)
