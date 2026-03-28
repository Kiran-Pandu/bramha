from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.db import (
    backfill_operational_data,
    fetch_entities,
    fetch_entity,
    fetch_graph,
    fetch_summary,
    ingest_records,
    init_db,
    list_investigation_subgraphs,
    list_ingestion_runs,
    list_log_stream_runs,
    list_recent_log_events,
    list_sources,
    preview_ingest_file,
    save_investigation_subgraph,
)

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Bramha System", version="0.2.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class IngestRequest(BaseModel):
    source_name: str = Field(min_length=2, max_length=120)
    file_name: str = Field(min_length=3, max_length=200)
    file_type: str = Field(default="", max_length=10)
    content: str = Field(min_length=2)
    mapping: dict[str, str] = Field(default_factory=dict)


class IngestPreviewRequest(BaseModel):
    file_name: str = Field(min_length=3, max_length=200)
    file_type: str = Field(default="", max_length=10)
    content: str = Field(min_length=2)


class InvestigationSubgraphRequest(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    focus_entity_id: str = Field(default="", max_length=64)
    filters: dict = Field(default_factory=dict)
    graph: dict = Field(default_factory=dict)


@app.on_event("startup")
def startup() -> None:
    init_db()
    backfill_operational_data()


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/summary")
def summary() -> dict:
    return fetch_summary()


@app.get("/api/entities")
def entities(search: str | None = Query(default=None, max_length=80)) -> list[dict]:
    return fetch_entities(search)


@app.get("/api/entities/{entity_id}")
def entity_detail(entity_id: str) -> dict:
    entity = fetch_entity(entity_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return entity


@app.get("/api/graph")
def graph() -> dict:
    return fetch_graph()


@app.get("/api/sources")
def sources() -> list[dict]:
    return list_sources()


@app.get("/api/ingest/runs")
def ingestion_runs() -> list[dict]:
    return list_ingestion_runs()


@app.get("/api/logs/runs")
def log_stream_runs(limit: int = Query(default=10, ge=1, le=100)) -> list[dict]:
    return list_log_stream_runs(limit)


@app.get("/api/logs/recent")
def recent_logs(limit: int = Query(default=50, ge=1, le=200)) -> list[dict]:
    return list_recent_log_events(limit)


@app.get("/api/investigations/subgraphs")
def investigation_subgraphs() -> list[dict]:
    return list_investigation_subgraphs()


@app.post("/api/investigations/subgraphs")
def create_investigation_subgraph(request: InvestigationSubgraphRequest) -> dict:
    return save_investigation_subgraph(
        name=request.name.strip(),
        focus_entity_id=request.focus_entity_id.strip(),
        filters=request.filters,
        graph=request.graph,
    )


@app.post("/api/ingest/preview")
def ingest_preview(request: IngestPreviewRequest) -> dict:
    try:
        return preview_ingest_file(
            file_name=request.file_name.strip(),
            file_type=request.file_type.strip().lower(),
            content=request.content,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ingest/run")
def run_ingestion(request: IngestRequest) -> dict:
    try:
        return ingest_records(
            source_name=request.source_name.strip(),
            file_name=request.file_name.strip(),
            file_type=request.file_type.strip().lower(),
            content=request.content,
            mapping=request.mapping,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
