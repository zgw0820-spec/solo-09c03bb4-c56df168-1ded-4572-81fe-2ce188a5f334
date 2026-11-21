"""FastAPI application: JSON API plus the operator web pages."""
from __future__ import annotations

import uuid

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from . import service
from .models import get_session, init_db
from .schemas import (
    ArchiveBatch,
    CheckRequest,
    IncidentCreate,
    ObservationBatch,
    PromoteRequest,
    RecoveryReport,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title="PostgreSQL 故障切换值班台", version="1.0.0", lifespan=lifespan)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@app.exception_handler(service.ServiceError)
async def _service_error_handler(_request: Request, exc: service.ServiceError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


# --------------------------------------------------------------------------- #
# JSON API
# --------------------------------------------------------------------------- #
@app.get("/api/incidents")
def api_list_incidents(session: Session = Depends(get_session)) -> dict:
    return {"incidents": service.list_incidents(session)}


@app.post("/api/incidents", status_code=201)
def api_create_incident(data: IncidentCreate, session: Session = Depends(get_session)) -> dict:
    incident = service.create_incident(session, data)
    return service.incident_detail(session, incident.id)


@app.get("/api/incidents/{incident_id}")
def api_incident(incident_id: int, session: Session = Depends(get_session)) -> dict:
    return service.incident_detail(session, incident_id)


@app.post("/api/incidents/{incident_id}/observations")
def api_observations(
    incident_id: int, data: ObservationBatch, session: Session = Depends(get_session)
) -> dict:
    return service.submit_observations(session, incident_id, data)


@app.post("/api/incidents/{incident_id}/archives")
def api_archives(
    incident_id: int, data: ArchiveBatch, session: Session = Depends(get_session)
) -> dict:
    return service.add_archives(session, incident_id, data)


@app.post("/api/incidents/{incident_id}/checks")
def api_checks(incident_id: int, data: CheckRequest, session: Session = Depends(get_session)) -> dict:
    return service.run_check(session, incident_id, data)


@app.post("/api/incidents/{incident_id}/promote")
def api_promote(
    incident_id: int, data: PromoteRequest, session: Session = Depends(get_session)
) -> dict:
    return service.promote(session, incident_id, data)


@app.post("/api/incidents/{incident_id}/recovery-reports")
def api_recovery(
    incident_id: int, data: RecoveryReport, session: Session = Depends(get_session)
) -> dict:
    return service.recovery_report(session, incident_id, data)


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# Web pages (server-rendered forms, same service layer underneath)
# --------------------------------------------------------------------------- #
def _form_error(exc: Exception) -> int:
    return getattr(exc, "status_code", 400)


@app.get("/", response_class=HTMLResponse)
def page_index(request: Request, session: Session = Depends(get_session)) -> HTMLResponse:
    incidents = service.list_incidents(session)
    return templates.TemplateResponse(
        request, "index.html", {"incidents": incidents}
    )


@app.post("/incidents", response_class=HTMLResponse)
async def page_create_incident(
    request: Request, session: Session = Depends(get_session)
):
    form = await request.form()
    try:
        nodes = []
        i = 0
        while f"node_key_{i}" in form:
            nodes.append(
                {
                    "node_key": form[f"node_key_{i}"],
                    "name": form[f"name_{i}"],
                    "role": form[f"role_{i}"],
                }
            )
            i += 1
        payload = IncidentCreate(
            title=form["title"],
            primary_timeline=int(form["primary_timeline"]),
            commit_lsn=form["commit_lsn"],
            nodes=nodes,
        )
        incident = service.create_incident(session, payload)
        return RedirectResponse(url=f"/incidents/{incident.id}", status_code=303)
    except Exception as exc:  # noqa: BLE001 - re-render with message
        incidents = service.list_incidents(session)
        return templates.TemplateResponse(
            request,
            "index.html",
            {"incidents": incidents, "error": str(getattr(exc, "message", exc))},
            status_code=_form_error(exc),
        )


@app.get("/incidents/{incident_id}", response_class=HTMLResponse)
def page_incident(
    request: Request, incident_id: int, session: Session = Depends(get_session)
) -> HTMLResponse:
    detail = service.incident_detail(session, incident_id)
    return templates.TemplateResponse(
        request,
        "incident.html",
        {"d": detail, "new_request_id": uuid.uuid4().hex},
    )


def _render_incident(
    request: Request, session: Session, incident_id: int, error: str | None, status: int = 200
) -> HTMLResponse:
    detail = service.incident_detail(session, incident_id)
    return templates.TemplateResponse(
        request,
        "incident.html",
        {"d": detail, "new_request_id": uuid.uuid4().hex, "error": error},
        status_code=status,
    )


@app.post("/incidents/{incident_id}/observations", response_class=HTMLResponse)
async def page_observations(
    request: Request, incident_id: int, session: Session = Depends(get_session)
):
    form = await request.form()
    try:
        observations = []
        i = 0
        while f"timeline_{i}" in form:
            observations.append(
                {
                    "node_key": form[f"node_key_{i}"],
                    "timeline": int(form[f"timeline_{i}"]),
                    "replay_lsn": form[f"replay_lsn_{i}"],
                }
            )
            i += 1
        payload = ObservationBatch(
            expected_version=int(form["expected_version"]),
            observations=observations,
            client_request_id=form.get("request_id") or None,
        )
        service.submit_observations(session, incident_id, payload)
        return RedirectResponse(url=f"/incidents/{incident_id}", status_code=303)
    except Exception as exc:  # noqa: BLE001
        return _render_incident(
            request, session, incident_id, str(getattr(exc, "message", exc)), _form_error(exc)
        )


@app.post("/incidents/{incident_id}/archives", response_class=HTMLResponse)
async def page_archives(
    request: Request, incident_id: int, session: Session = Depends(get_session)
):
    form = await request.form()
    try:
        payload = ArchiveBatch(
            expected_version=int(form["expected_version"]),
            archives=[
                {
                    "timeline": int(form["timeline"]),
                    "start_segment": form["start_segment"],
                    "end_segment": form["end_segment"],
                }
            ],
            client_request_id=form.get("request_id") or None,
        )
        service.add_archives(session, incident_id, payload)
        return RedirectResponse(url=f"/incidents/{incident_id}", status_code=303)
    except Exception as exc:  # noqa: BLE001
        return _render_incident(
            request, session, incident_id, str(getattr(exc, "message", exc)), _form_error(exc)
        )


@app.post("/incidents/{incident_id}/checks", response_class=HTMLResponse)
async def page_checks(
    request: Request, incident_id: int, session: Session = Depends(get_session)
):
    form = await request.form()
    try:
        payload = CheckRequest(
            expected_version=int(form["expected_version"]),
            client_request_id=form.get("request_id") or None,
        )
        service.run_check(session, incident_id, payload)
        return RedirectResponse(url=f"/incidents/{incident_id}", status_code=303)
    except Exception as exc:  # noqa: BLE001
        return _render_incident(
            request, session, incident_id, str(getattr(exc, "message", exc)), _form_error(exc)
        )


@app.post("/incidents/{incident_id}/promote", response_class=HTMLResponse)
async def page_promote(
    request: Request, incident_id: int, session: Session = Depends(get_session)
):
    form = await request.form()
    try:
        payload = PromoteRequest(
            expected_version=int(form["expected_version"]),
            request_id=form["request_id"],
            node_key=form.get("node_key") or None,
        )
        result = service.promote(session, incident_id, payload)
        if not result["promoted"]:
            return _render_incident(
                request, session, incident_id, result["reason"], status_code=409
            )
        return RedirectResponse(url=f"/incidents/{incident_id}", status_code=303)
    except Exception as exc:  # noqa: BLE001
        return _render_incident(
            request, session, incident_id, str(getattr(exc, "message", exc)), _form_error(exc)
        )


@app.post("/incidents/{incident_id}/recovery-reports", response_class=HTMLResponse)
async def page_recovery(
    request: Request, incident_id: int, session: Session = Depends(get_session)
):
    form = await request.form()
    try:
        payload = RecoveryReport(
            node_key=form["node_key"],
            generation=int(form["generation"]),
            reported_primary_timeline=int(form["reported_primary_timeline"]),
            reported_replay_lsn=form["reported_replay_lsn"],
            request_id=form["request_id"],
        )
        result = service.recovery_report(session, incident_id, payload)
        if result["stale"]:
            return _render_incident(
                request,
                session,
                incident_id,
                "旧主库迟到回报已忽略：已确认的主库代次不受影响",
                status_code=409,
            )
        return RedirectResponse(url=f"/incidents/{incident_id}", status_code=303)
    except Exception as exc:  # noqa: BLE001
        return _render_incident(
            request, session, incident_id, str(getattr(exc, "message", exc)), _form_error(exc)
        )
