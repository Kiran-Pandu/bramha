from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "Bramha.db"
SEED_PATH = DATA_DIR / "seed_case.json"

CANONICAL_FIELDS = [
    "entity_name",
    "entity_kind",
    "entity_description",
    "entity_location",
    "entity_risk_score",
    "entity_aliases",
    "organization_name",
    "organization_location",
    "organization_risk_score",
    "organization_relationship_type",
    "event_title",
    "event_kind",
    "event_date",
    "event_location",
    "event_summary",
    "target_name",
    "target_kind",
    "target_relationship_type",
    "target_location",
]

FIELD_ALIASES = {
    "entity_name": ["entity_name", "name", "person_name", "subject_name", "subject", "person"],
    "entity_kind": ["entity_kind", "kind", "type", "person_kind"],
    "entity_description": ["entity_description", "description", "entity_summary", "notes", "details"],
    "entity_location": ["entity_location", "location", "city", "site"],
    "entity_risk_score": ["entity_risk_score", "risk_score", "score", "risk"],
    "entity_aliases": ["entity_aliases", "aliases"],
    "organization_name": ["organization_name", "org_name", "company_name", "organization", "company"],
    "organization_location": ["organization_location", "org_location"],
    "organization_risk_score": ["organization_risk_score", "org_risk_score"],
    "organization_relationship_type": ["organization_relationship_type", "relationship_type", "org_relationship"],
    "event_title": ["event_title", "title", "case_title", "incident_title"],
    "event_kind": ["event_kind", "incident_type", "event_type"],
    "event_date": ["event_date", "date", "incident_date"],
    "event_location": ["event_location", "event_site", "event_city"],
    "event_summary": ["event_summary", "summary", "event_details"],
    "target_name": ["target_name", "related_name", "asset_name", "target"],
    "target_kind": ["target_kind", "related_kind", "asset_kind"],
    "target_relationship_type": ["target_relationship_type", "related_relationship_type", "target_relationship"],
    "target_location": ["target_location", "related_location", "asset_location"],
}


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def make_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:10].upper()}"


def canonicalize(value: str | None) -> str:
    if not value:
        return ""
    return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in value).split())


def parse_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).replace("|", ",").replace(";", ",").split(",") if item.strip()]


def text_value(row: dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def int_value(row: dict[str, Any], *keys: str, default: int = 0) -> int:
    for key in keys:
        value = row.get(key)
        if value is None or str(value).strip() == "":
            continue
        try:
            return max(0, min(100, int(float(str(value).strip()))))
        except ValueError:
            continue
    return default


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    with get_connection() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS entities (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                kind TEXT NOT NULL,
                description TEXT NOT NULL,
                risk_score INTEGER NOT NULL,
                location TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                kind TEXT NOT NULL,
                event_date TEXT NOT NULL,
                location TEXT NOT NULL,
                summary TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event_entities (
                event_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                role TEXT NOT NULL,
                PRIMARY KEY (event_id, entity_id, role),
                FOREIGN KEY (event_id) REFERENCES events(id),
                FOREIGN KEY (entity_id) REFERENCES entities(id)
            );

            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                file_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_run_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_type TEXT NOT NULL,
                status TEXT NOT NULL,
                record_count INTEGER NOT NULL DEFAULT 0,
                entity_count INTEGER NOT NULL DEFAULT 0,
                entity_existing_count INTEGER NOT NULL DEFAULT 0,
                event_count INTEGER NOT NULL DEFAULT 0,
                event_existing_count INTEGER NOT NULL DEFAULT 0,
                relationship_count INTEGER NOT NULL DEFAULT 0,
                relationship_existing_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                completed_at TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (source_id) REFERENCES sources(id)
            );

            CREATE TABLE IF NOT EXISTS raw_records (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                record_index INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                normalized_json TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (run_id) REFERENCES ingestion_runs(id)
            );

            CREATE TABLE IF NOT EXISTS entity_aliases (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                alias TEXT NOT NULL,
                alias_key TEXT NOT NULL,
                source_id TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL DEFAULT 1.0,
                UNIQUE (entity_id, alias_key),
                FOREIGN KEY (entity_id) REFERENCES entities(id),
                FOREIGN KEY (source_id) REFERENCES sources(id)
            );

            CREATE TABLE IF NOT EXISTS relationships (
                id TEXT PRIMARY KEY,
                source_entity_id TEXT NOT NULL,
                target_entity_id TEXT NOT NULL,
                relationship_type TEXT NOT NULL,
                strength INTEGER NOT NULL DEFAULT 1,
                source_event_id TEXT NOT NULL DEFAULT '',
                source_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                UNIQUE (
                    source_entity_id,
                    target_entity_id,
                    relationship_type,
                    source_event_id,
                    source_id
                ),
                FOREIGN KEY (source_entity_id) REFERENCES entities(id),
                FOREIGN KEY (target_entity_id) REFERENCES entities(id),
                FOREIGN KEY (source_event_id) REFERENCES events(id),
                FOREIGN KEY (source_id) REFERENCES sources(id)
            );

            CREATE TABLE IF NOT EXISTS entity_sources (
                entity_id TEXT NOT NULL,
                source_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                PRIMARY KEY (entity_id, source_id),
                FOREIGN KEY (entity_id) REFERENCES entities(id),
                FOREIGN KEY (source_id) REFERENCES sources(id),
                FOREIGN KEY (run_id) REFERENCES ingestion_runs(id)
            );

            CREATE TABLE IF NOT EXISTS event_sources (
                event_id TEXT NOT NULL,
                source_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                PRIMARY KEY (event_id, source_id),
                FOREIGN KEY (event_id) REFERENCES events(id),
                FOREIGN KEY (source_id) REFERENCES sources(id),
                FOREIGN KEY (run_id) REFERENCES ingestion_runs(id)
            );

            CREATE TABLE IF NOT EXISTS investigation_subgraphs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                focus_entity_id TEXT NOT NULL DEFAULT '',
                filters_json TEXT NOT NULL,
                graph_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS log_stream_runs (
                id TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                topic TEXT NOT NULL,
                status TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                created_entity_count INTEGER NOT NULL DEFAULT 0,
                created_event_count INTEGER NOT NULL DEFAULT 0,
                created_relationship_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS raw_log_events (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                topic TEXT NOT NULL,
                partition_id INTEGER NOT NULL DEFAULT 0,
                offset_value INTEGER NOT NULL DEFAULT 0,
                line_number INTEGER NOT NULL DEFAULT 0,
                raw_line TEXT NOT NULL,
                parsed_json TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES log_stream_runs(id)
            );
            """
        )
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(ingestion_runs)").fetchall()
        }
        if "entity_existing_count" not in existing_columns:
            connection.execute(
                "ALTER TABLE ingestion_runs ADD COLUMN entity_existing_count INTEGER NOT NULL DEFAULT 0"
            )
        if "event_existing_count" not in existing_columns:
            connection.execute(
                "ALTER TABLE ingestion_runs ADD COLUMN event_existing_count INTEGER NOT NULL DEFAULT 0"
            )
        if "relationship_existing_count" not in existing_columns:
            connection.execute(
                "ALTER TABLE ingestion_runs ADD COLUMN relationship_existing_count INTEGER NOT NULL DEFAULT 0"
            )
        connection.commit()


def seed_db() -> None:
    with get_connection() as connection:
        existing = connection.execute("SELECT COUNT(*) AS count FROM entities").fetchone()
        if existing["count"] > 0:
            return

        seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))
        seed_source_id = make_id("SRC")
        seed_run_id = make_id("RUN")
        now = utc_now()

        connection.execute(
            """
            INSERT INTO sources (id, name, file_type, created_at, last_run_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (seed_source_id, "Seed Case File", "json", now, now),
        )
        connection.execute(
            """
            INSERT INTO ingestion_runs (
                id, source_id, source_name, file_name, file_type, status, record_count,
                entity_count, entity_existing_count, event_count, event_existing_count,
                relationship_count, relationship_existing_count, error_count, error_message,
                created_at, completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                seed_run_id,
                seed_source_id,
                "Seed Case File",
                "seed_case.json",
                "json",
                "completed",
                len(seed["events"]),
                len(seed["entities"]),
                0,
                len(seed["events"]),
                0,
                len(seed["event_entities"]),
                0,
                0,
                "",
                now,
                now,
            ),
        )

        connection.executemany(
            """
            INSERT INTO entities (id, name, kind, description, risk_score, location, metadata_json)
            VALUES (:id, :name, :kind, :description, :risk_score, :location, :metadata_json)
            """,
            [
                {
                    **entity,
                    "metadata_json": json.dumps(entity.get("metadata", {}), sort_keys=True),
                }
                for entity in seed["entities"]
            ],
        )
        connection.executemany(
            """
            INSERT INTO events (id, title, kind, event_date, location, summary)
            VALUES (:id, :title, :kind, :event_date, :location, :summary)
            """,
            seed["events"],
        )
        connection.executemany(
            """
            INSERT INTO event_entities (event_id, entity_id, role)
            VALUES (:event_id, :entity_id, :role)
            """,
            seed["event_entities"],
        )

        for entity in seed["entities"]:
            connection.execute(
                """
                INSERT INTO entity_sources (entity_id, source_id, run_id, first_seen_at)
                VALUES (?, ?, ?, ?)
                """,
                (entity["id"], seed_source_id, seed_run_id, now),
            )
            for alias in parse_list(entity.get("metadata", {}).get("aliases")):
                upsert_alias(connection, entity["id"], alias, seed_source_id)

        for event in seed["events"]:
            connection.execute(
                """
                INSERT INTO event_sources (event_id, source_id, run_id, first_seen_at)
                VALUES (?, ?, ?, ?)
                """,
                (event["id"], seed_source_id, seed_run_id, now),
            )

        event_entity_map: dict[str, list[str]] = {}
        for item in seed["event_entities"]:
            event_entity_map.setdefault(item["event_id"], []).append(item["entity_id"])

        for event_id, entity_ids in event_entity_map.items():
            for source_entity_id in entity_ids:
                for target_entity_id in entity_ids:
                    if source_entity_id == target_entity_id:
                        continue
                    create_relationship(
                        connection,
                        source_entity_id,
                        target_entity_id,
                        "co_involved",
                        seed_source_id,
                        event_id,
                        now,
                    )

        connection.commit()


def backfill_operational_data() -> None:
    with get_connection() as connection:
        entity_count = connection.execute("SELECT COUNT(*) AS count FROM entities").fetchone()["count"]
        if entity_count == 0:
            return

        source_count = connection.execute("SELECT COUNT(*) AS count FROM sources").fetchone()["count"]
        if source_count == 0:
            source_id = make_id("SRC")
            run_id = make_id("RUN")
            now = utc_now()
            connection.execute(
                """
                INSERT INTO sources (id, name, file_type, created_at, last_run_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (source_id, "Legacy Seed Data", "json", now, now),
            )
            connection.execute(
                """
                INSERT INTO ingestion_runs (
                    id, source_id, source_name, file_name, file_type, status, record_count,
                    entity_count, entity_existing_count, event_count, event_existing_count,
                    relationship_count, relationship_existing_count, error_count, error_message,
                    created_at, completed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    source_id,
                    "Legacy Seed Data",
                    "legacy-bootstrap.json",
                    "json",
                    "completed",
                    0,
                    entity_count,
                    0,
                    connection.execute("SELECT COUNT(*) AS count FROM events").fetchone()["count"],
                    0,
                    0,
                    0,
                    0,
                    "",
                    now,
                    now,
                ),
            )

            for row in connection.execute("SELECT id, metadata_json FROM entities").fetchall():
                connection.execute(
                    """
                    INSERT OR IGNORE INTO entity_sources (entity_id, source_id, run_id, first_seen_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (row["id"], source_id, run_id, now),
                )
                metadata = json.loads(row["metadata_json"])
                for alias in parse_list(metadata.get("aliases")):
                    upsert_alias(connection, row["id"], alias, source_id)

            for row in connection.execute("SELECT id FROM events").fetchall():
                connection.execute(
                    """
                    INSERT OR IGNORE INTO event_sources (event_id, source_id, run_id, first_seen_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (row["id"], source_id, run_id, now),
                )

        relationship_count = connection.execute("SELECT COUNT(*) AS count FROM relationships").fetchone()["count"]
        if relationship_count == 0:
            event_entity_map: dict[str, list[str]] = {}
            for item in connection.execute("SELECT event_id, entity_id FROM event_entities").fetchall():
                event_entity_map.setdefault(item["event_id"], []).append(item["entity_id"])

            source_row = connection.execute("SELECT id FROM sources ORDER BY created_at ASC LIMIT 1").fetchone()
            source_id = source_row["id"] if source_row else ""
            now = utc_now()
            for event_id, entity_ids in event_entity_map.items():
                for source_entity_id in entity_ids:
                    for target_entity_id in entity_ids:
                        if source_entity_id == target_entity_id:
                            continue
                        create_relationship(
                            connection,
                            source_entity_id,
                            target_entity_id,
                            "co_involved",
                            source_id,
                            event_id,
                            now,
                        )

        connection.commit()


def fetch_entities(query: str | None = None) -> list[dict[str, Any]]:
    sql = """
        SELECT id, name, kind, description, risk_score, location, metadata_json
        FROM entities
    """
    params: list[Any] = []
    if query:
        like = f"%{query}%"
        alias_like = canonicalize(query)
        sql += """
            WHERE
                name LIKE ?
                OR description LIKE ?
                OR kind LIKE ?
                OR location LIKE ?
                OR id IN (
                    SELECT entity_id FROM entity_aliases WHERE alias_key LIKE ?
                )
        """
        params.extend([like, like, like, like, f"%{alias_like}%"])
    sql += " ORDER BY risk_score DESC, name ASC"

    with get_connection() as connection:
        rows = connection.execute(sql, params).fetchall()
    return [serialize_entity(row) for row in rows]


def fetch_entity(entity_id: str) -> dict[str, Any] | None:
    with get_connection() as connection:
        entity_row = connection.execute(
            """
            SELECT id, name, kind, description, risk_score, location, metadata_json
            FROM entities
            WHERE id = ?
            """,
            (entity_id,),
        ).fetchone()
        if entity_row is None:
            return None

        event_rows = connection.execute(
            """
            SELECT e.id, e.title, e.kind, e.event_date, e.location, e.summary, ee.role
            FROM events e
            JOIN event_entities ee ON ee.event_id = e.id
            WHERE ee.entity_id = ?
            ORDER BY e.event_date DESC, e.title ASC
            """,
            (entity_id,),
        ).fetchall()

        relationship_rows = connection.execute(
            """
            SELECT
                linked.id,
                linked.name,
                linked.kind,
                linked.risk_score,
                r.relationship_type,
                r.strength,
                r.source_event_id
            FROM relationships r
            JOIN entities linked ON linked.id = r.target_entity_id
            WHERE r.source_entity_id = ?
            ORDER BY r.strength DESC, linked.risk_score DESC, linked.name ASC
            """,
            (entity_id,),
        ).fetchall()

        source_rows = connection.execute(
            """
            SELECT s.id, s.name, s.file_type, es.first_seen_at
            FROM entity_sources es
            JOIN sources s ON s.id = es.source_id
            WHERE es.entity_id = ?
            ORDER BY es.first_seen_at DESC
            """,
            (entity_id,),
        ).fetchall()

        alias_rows = connection.execute(
            """
            SELECT alias
            FROM entity_aliases
            WHERE entity_id = ?
            ORDER BY alias ASC
            """,
            (entity_id,),
        ).fetchall()

    entity = serialize_entity(entity_row)
    entity["events"] = [dict(row) for row in event_rows]
    entity["links"] = [
        {
            "id": row["id"],
            "name": row["name"],
            "kind": row["kind"],
            "risk_score": row["risk_score"],
            "relationship_type": row["relationship_type"],
            "strength": row["strength"],
            "source_event_id": row["source_event_id"],
        }
        for row in relationship_rows
    ]
    entity["sources"] = [dict(row) for row in source_rows]
    entity["aliases"] = [row["alias"] for row in alias_rows]
    return entity


def fetch_summary() -> dict[str, Any]:
    with get_connection() as connection:
        stats = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM entities) AS entity_count,
                (SELECT COUNT(*) FROM events) AS event_count,
                (SELECT COUNT(*) FROM entities WHERE risk_score >= 75) AS high_risk_count,
                (SELECT COUNT(*) FROM sources) AS source_count,
                (SELECT COUNT(*) FROM ingestion_runs) AS ingestion_run_count
            """
        ).fetchone()
        hottest = connection.execute(
            """
            SELECT id, name, kind, risk_score, location
            FROM entities
            ORDER BY risk_score DESC, name ASC
            LIMIT 5
            """
        ).fetchall()
        latest_events = connection.execute(
            """
            SELECT id, title, kind, event_date, location, summary
            FROM events
            ORDER BY event_date DESC, title ASC
            LIMIT 6
            """
        ).fetchall()
        latest_runs = connection.execute(
            """
            SELECT id, source_name, status, record_count, created_at, completed_at
            FROM ingestion_runs
            ORDER BY created_at DESC
            LIMIT 4
            """
        ).fetchall()

    return {
        "entity_count": stats["entity_count"],
        "event_count": stats["event_count"],
        "high_risk_count": stats["high_risk_count"],
        "source_count": stats["source_count"],
        "ingestion_run_count": stats["ingestion_run_count"],
        "priority_entities": [dict(row) for row in hottest],
        "latest_events": [dict(row) for row in latest_events],
        "latest_runs": [dict(row) for row in latest_runs],
    }


def fetch_graph() -> dict[str, Any]:
    with get_connection() as connection:
        entities = connection.execute(
            """
            SELECT id, name, kind, risk_score
            FROM entities
            ORDER BY risk_score DESC, name ASC
            """
        ).fetchall()
        links = connection.execute(
            """
            SELECT
                source_entity_id AS source,
                target_entity_id AS target,
                relationship_type,
                SUM(strength) AS weight
            FROM relationships
            GROUP BY source_entity_id, target_entity_id, relationship_type
            ORDER BY weight DESC, source ASC, target ASC
            """
        ).fetchall()

    return {
        "nodes": [dict(row) for row in entities],
        "edges": [dict(row) for row in links],
    }


def list_sources() -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                s.id,
                s.name,
                s.file_type,
                s.created_at,
                s.last_run_at,
                COUNT(DISTINCT es.entity_id) AS entity_count,
                COUNT(DISTINCT evs.event_id) AS event_count
            FROM sources s
            LEFT JOIN entity_sources es ON es.source_id = s.id
            LEFT JOIN event_sources evs ON evs.source_id = s.id
            GROUP BY s.id, s.name, s.file_type, s.created_at, s.last_run_at
            ORDER BY s.last_run_at DESC, s.name ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def list_ingestion_runs() -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                id,
                source_id,
                source_name,
                file_name,
                file_type,
                status,
                record_count,
                entity_count,
                entity_existing_count,
                event_count,
                event_existing_count,
                relationship_count,
                relationship_existing_count,
                error_count,
                error_message,
                created_at,
                completed_at
            FROM ingestion_runs
            ORDER BY created_at DESC
            LIMIT 20
            """
        ).fetchall()
    return [dict(row) for row in rows]


def list_log_stream_runs(limit: int = 10) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                id,
                source_name,
                topic,
                status,
                message_count,
                created_entity_count,
                created_event_count,
                created_relationship_count,
                created_at,
                updated_at
            FROM log_stream_runs
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (max(1, min(limit, 100)),),
        ).fetchall()
    return [dict(row) for row in rows]


def list_recent_log_events(limit: int = 50) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                id,
                run_id,
                source_name,
                topic,
                partition_id,
                offset_value,
                line_number,
                raw_line,
                parsed_json,
                status,
                error_message,
                created_at
            FROM raw_log_events
            ORDER BY created_at DESC, offset_value DESC
            LIMIT ?
            """,
            (max(1, min(limit, 200)),),
        ).fetchall()
    return [
        {
            **dict(row),
            "parsed": json.loads(row["parsed_json"]),
        }
        for row in rows
    ]


def list_investigation_subgraphs() -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, name, focus_entity_id, filters_json, graph_json, created_at
            FROM investigation_subgraphs
            ORDER BY created_at DESC
            LIMIT 20
            """
        ).fetchall()
    return [
        {
            **dict(row),
            "filters": json.loads(row["filters_json"]),
            "graph": json.loads(row["graph_json"]),
        }
        for row in rows
    ]


def save_investigation_subgraph(
    name: str,
    focus_entity_id: str,
    filters: dict[str, Any],
    graph: dict[str, Any],
) -> dict[str, Any]:
    snapshot_id = make_id("SGR")
    created_at = utc_now()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO investigation_subgraphs (id, name, focus_entity_id, filters_json, graph_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                name,
                focus_entity_id,
                json.dumps(filters, sort_keys=True),
                json.dumps(graph, sort_keys=True),
                created_at,
            ),
        )
        connection.commit()
    return next(item for item in list_investigation_subgraphs() if item["id"] == snapshot_id)


def preview_ingest_file(file_name: str, file_type: str, content: str) -> dict[str, Any]:
    normalized_type = (file_type or "").strip().lower() or detect_file_type(file_name)
    if normalized_type not in {"csv", "json"}:
        raise ValueError("Only CSV and JSON ingestion are supported right now.")

    records = parse_records(content, normalized_type)
    if not records:
        raise ValueError("No records were found in the uploaded file.")

    fields = sorted({key for record in records for key in record.keys()})
    return {
        "file_type": normalized_type,
        "record_count": len(records),
        "fields": fields,
        "suggested_mapping": suggest_mapping(fields),
        "sample_records": records[:3],
    }


def ingest_records(
    source_name: str,
    file_name: str,
    file_type: str,
    content: str,
    mapping: dict[str, str] | None = None,
) -> dict[str, Any]:
    normalized_type = (file_type or "").strip().lower() or detect_file_type(file_name)
    if normalized_type not in {"csv", "json"}:
        raise ValueError("Only CSV and JSON ingestion are supported right now.")

    records = parse_records(content, normalized_type)
    if not records:
        raise ValueError("No records were found in the uploaded file.")

    run_id = make_id("RUN")
    now = utc_now()

    with get_connection() as connection:
        source_row = connection.execute(
            "SELECT id, name FROM sources WHERE name = ?",
            (source_name,),
        ).fetchone()
        if source_row is None:
            source_id = make_id("SRC")
            connection.execute(
                """
                INSERT INTO sources (id, name, file_type, created_at, last_run_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (source_id, source_name, normalized_type, now, now),
            )
        else:
            source_id = source_row["id"]
            connection.execute(
                "UPDATE sources SET file_type = ?, last_run_at = ? WHERE id = ?",
                (normalized_type, now, source_id),
            )

        connection.execute(
            """
              INSERT INTO ingestion_runs (
                  id, source_id, source_name, file_name, file_type, status, record_count,
                  entity_count, entity_existing_count, event_count, event_existing_count,
                  relationship_count, relationship_existing_count, error_count, error_message,
                  created_at, completed_at
              )
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                source_id,
                source_name,
                file_name,
                normalized_type,
                "running",
                len(records),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                "",
                now,
                "",
            ),
        )

        stats = {
            "entity_count": 0,
            "entity_existing_count": 0,
            "event_count": 0,
            "event_existing_count": 0,
            "relationship_count": 0,
            "relationship_existing_count": 0,
            "error_count": 0,
        }

        try:
            for index, payload in enumerate(records, start=1):
                normalized = normalize_record(apply_mapping(payload, mapping or {}))
                try:
                    counts = ingest_normalized_record(connection, normalized, source_id, run_id)
                    stats["entity_count"] += counts["entity_count"]
                    stats["entity_existing_count"] += counts["entity_existing_count"]
                    stats["event_count"] += counts["event_count"]
                    stats["event_existing_count"] += counts["event_existing_count"]
                    stats["relationship_count"] += counts["relationship_count"]
                    stats["relationship_existing_count"] += counts["relationship_existing_count"]
                    raw_status = "processed"
                    error_message = ""
                except ValueError as exc:
                    raw_status = "skipped"
                    error_message = str(exc)
                    stats["error_count"] += 1

                connection.execute(
                    """
                    INSERT INTO raw_records (
                        id, run_id, record_index, payload_json, normalized_json, status, error_message
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        make_id("RAW"),
                        run_id,
                        index,
                        json.dumps(payload, sort_keys=True),
                        json.dumps(normalized, sort_keys=True),
                        raw_status,
                        error_message,
                    ),
                )

            completed_at = utc_now()
            final_status = "completed" if stats["error_count"] < len(records) else "failed"
            final_message = "" if final_status == "completed" else "All records failed validation."
            connection.execute(
                """
                UPDATE ingestion_runs
                SET status = ?, entity_count = ?, entity_existing_count = ?, event_count = ?, event_existing_count = ?,
                    relationship_count = ?, relationship_existing_count = ?, error_count = ?, error_message = ?, completed_at = ?
                WHERE id = ?
                """,
                (
                    final_status,
                    stats["entity_count"],
                    stats["entity_existing_count"],
                    stats["event_count"],
                    stats["event_existing_count"],
                    stats["relationship_count"],
                    stats["relationship_existing_count"],
                    stats["error_count"],
                    final_message,
                    completed_at,
                    run_id,
                ),
            )
            connection.commit()
        except Exception as exc:
            connection.execute(
                """
                UPDATE ingestion_runs
                SET status = ?, error_count = ?, error_message = ?, completed_at = ?
                WHERE id = ?
                """,
                ("failed", len(records), str(exc), utc_now(), run_id),
            )
            connection.commit()
            raise

    return next(run for run in list_ingestion_runs() if run["id"] == run_id)


def detect_file_type(file_name: str) -> str:
    lowered = file_name.lower()
    if lowered.endswith(".csv"):
        return "csv"
    if lowered.endswith(".json"):
        return "json"
    return ""


def parse_records(content: str, file_type: str) -> list[dict[str, Any]]:
    if file_type == "csv":
        reader = csv.DictReader(io.StringIO(content))
        return [dict(row) for row in reader]

    payload = json.loads(content)
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("records"), list):
            return [dict(item) for item in payload["records"] if isinstance(item, dict)]
        return [payload]
    raise ValueError("JSON files must contain either an object or a list of objects.")


def suggest_mapping(fields: list[str]) -> dict[str, str]:
    lowered = {field.lower(): field for field in fields}
    mapping: dict[str, str] = {}
    for canonical in CANONICAL_FIELDS:
        for alias in FIELD_ALIASES.get(canonical, []):
            field = lowered.get(alias.lower())
            if field:
                mapping[canonical] = field
                break
    return mapping


def apply_mapping(payload: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    if not mapping:
        return payload

    remapped = dict(payload)
    for canonical, source_field in mapping.items():
        if source_field and source_field in payload:
            remapped[canonical] = payload[source_field]
    return remapped


def normalize_record(payload: dict[str, Any]) -> dict[str, Any]:
    primary_name = text_value(payload, "entity_name", "name", "person_name", "subject_name")
    primary_kind = text_value(payload, "entity_kind", "kind", "person_kind", default="Person") or "Person"
    primary = {
        "name": primary_name,
        "kind": primary_kind.title(),
        "description": text_value(payload, "entity_description", "description", "entity_summary", "notes"),
        "risk_score": int_value(payload, "entity_risk_score", "risk_score", "score", default=50),
        "location": text_value(payload, "entity_location", "location", "city", "site"),
        "aliases": parse_list(payload.get("entity_aliases") or payload.get("aliases")),
    }

    related_entities: list[dict[str, Any]] = []
    org_name = text_value(payload, "organization_name", "org_name", "company_name")
    if org_name:
        related_entities.append(
            {
                "name": org_name,
                "kind": "Organization",
                "description": text_value(payload, "organization_description", "org_description"),
                "risk_score": int_value(payload, "organization_risk_score", "org_risk_score", default=primary["risk_score"]),
                "location": text_value(payload, "organization_location", "org_location", "location"),
                "aliases": parse_list(payload.get("organization_aliases") or payload.get("org_aliases")),
                "relationship_type": text_value(payload, "organization_relationship_type", "relationship_type", default="associated_with"),
                "role": text_value(payload, "organization_role", "org_role", default="associated organization"),
            }
        )

    target_name = text_value(payload, "target_name", "related_name", "asset_name")
    if target_name:
        related_entities.append(
            {
                "name": target_name,
                "kind": text_value(payload, "target_kind", "related_kind", "asset_kind", default="Asset").title(),
                "description": text_value(payload, "target_description", "related_description", "asset_description"),
                "risk_score": int_value(payload, "target_risk_score", "related_risk_score", default=primary["risk_score"]),
                "location": text_value(payload, "target_location", "related_location", "asset_location", "location"),
                "aliases": parse_list(payload.get("target_aliases") or payload.get("related_aliases")),
                "relationship_type": text_value(payload, "target_relationship_type", "related_relationship_type", default="linked_to"),
                "role": text_value(payload, "target_role", "related_role", default="related entity"),
            }
        )

    event_title = text_value(payload, "event_title", "title", "case_title")
    event = {
        "title": event_title,
        "kind": text_value(payload, "event_kind", "incident_type", default="Observation"),
        "event_date": text_value(payload, "event_date", "date", default=utc_now()[:10]),
        "location": text_value(payload, "event_location", "location", "site"),
        "summary": text_value(payload, "event_summary", "summary", "details", "description"),
    }

    return {
        "primary_entity": primary,
        "primary_role": text_value(payload, "entity_role", "role", default="subject"),
        "related_entities": related_entities,
        "event": event,
        "raw_keys": sorted(payload.keys()),
    }


def ingest_normalized_record(
    connection: sqlite3.Connection,
    normalized: dict[str, Any],
    source_id: str,
    run_id: str,
) -> dict[str, int]:
    primary = normalized["primary_entity"]
    if not primary["name"]:
        raise ValueError("Record skipped: missing entity_name or name.")
    if not normalized["event"]["title"]:
        raise ValueError("Record skipped: missing event_title or title.")

    now = utc_now()
    entity_count = 0
    entity_existing_count = 0
    event_count = 0
    event_existing_count = 0
    relationship_count = 0
    relationship_existing_count = 0

    primary_id, created_primary = ensure_entity(connection, primary, source_id, run_id, now)
    entity_count += int(created_primary)
    entity_existing_count += int(not created_primary)
    event_id, created_event = ensure_event(connection, normalized["event"], source_id, run_id, now)
    event_count += int(created_event)
    event_existing_count += int(not created_event)
    add_event_entity(connection, event_id, primary_id, normalized["primary_role"])

    for related in normalized["related_entities"]:
        related_id, created_related = ensure_entity(connection, related, source_id, run_id, now)
        entity_count += int(created_related)
        entity_existing_count += int(not created_related)
        add_event_entity(connection, event_id, related_id, related["role"])
        created_forward = create_relationship(
            connection,
            primary_id,
            related_id,
            related["relationship_type"],
            source_id,
            event_id,
            now,
        )
        relationship_count += created_forward
        relationship_existing_count += int(not created_forward)
        created_reverse = create_relationship(
            connection,
            related_id,
            primary_id,
            f"reverse_{related['relationship_type']}",
            source_id,
            event_id,
            now,
        )
        relationship_count += created_reverse
        relationship_existing_count += int(not created_reverse)

    return {
        "entity_count": entity_count,
        "entity_existing_count": entity_existing_count,
        "event_count": event_count,
        "event_existing_count": event_existing_count,
        "relationship_count": relationship_count,
        "relationship_existing_count": relationship_existing_count,
    }


def ensure_entity(
    connection: sqlite3.Connection,
    entity: dict[str, Any],
    source_id: str,
    run_id: str,
    now: str,
) -> tuple[str, bool]:
    entity_id = find_entity_id(connection, entity["name"], entity["kind"], entity.get("aliases", []))
    created = False
    metadata = {"ingested": True}
    if entity_id is None:
        entity_id = make_id("ENT")
        connection.execute(
            """
            INSERT INTO entities (id, name, kind, description, risk_score, location, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity_id,
                entity["name"],
                entity["kind"],
                entity.get("description", ""),
                entity.get("risk_score", 0),
                entity.get("location", ""),
                json.dumps(metadata, sort_keys=True),
            ),
        )
        created = True
    else:
        row = connection.execute(
            "SELECT description, risk_score, location, metadata_json FROM entities WHERE id = ?",
            (entity_id,),
        ).fetchone()
        existing_metadata = json.loads(row["metadata_json"])
        existing_metadata["ingested"] = True
        description = row["description"] if len(row["description"]) >= len(entity.get("description", "")) else entity.get("description", "")
        location = row["location"] or entity.get("location", "")
        risk_score = max(row["risk_score"], entity.get("risk_score", 0))
        connection.execute(
            """
            UPDATE entities
            SET description = ?, risk_score = ?, location = ?, metadata_json = ?
            WHERE id = ?
            """,
            (
                description,
                risk_score,
                location,
                json.dumps(existing_metadata, sort_keys=True),
                entity_id,
            ),
        )

    connection.execute(
        """
        INSERT OR IGNORE INTO entity_sources (entity_id, source_id, run_id, first_seen_at)
        VALUES (?, ?, ?, ?)
        """,
        (entity_id, source_id, run_id, now),
    )
    upsert_alias(connection, entity_id, entity["name"], source_id)
    for alias in entity.get("aliases", []):
        upsert_alias(connection, entity_id, alias, source_id)

    return entity_id, created


def find_entity_id(
    connection: sqlite3.Connection,
    name: str,
    kind: str,
    aliases: list[str],
) -> str | None:
    if not canonicalize(name):
        return None

    row = connection.execute(
        """
        SELECT id
        FROM entities
        WHERE kind = ? AND name = ?
        LIMIT 1
        """,
        (kind, name),
    ).fetchone()
    if row:
        return row["id"]

    return None


def ensure_event(
    connection: sqlite3.Connection,
    event: dict[str, Any],
    source_id: str,
    run_id: str,
    now: str,
) -> tuple[str, bool]:
    row = connection.execute(
        """
        SELECT id, summary
        FROM events
        WHERE title = ? AND event_date = ? AND location = ?
        LIMIT 1
        """,
        (event["title"], event["event_date"], event["location"]),
    ).fetchone()
    created = False
    if row is None:
        event_id = make_id("EVT")
        connection.execute(
            """
            INSERT INTO events (id, title, kind, event_date, location, summary)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                event["title"],
                event["kind"],
                event["event_date"],
                event["location"],
                event["summary"],
            ),
        )
        created = True
    else:
        event_id = row["id"]
        summary = row["summary"] if len(row["summary"]) >= len(event["summary"]) else event["summary"]
        connection.execute(
            """
            UPDATE events
            SET kind = ?, summary = ?
            WHERE id = ?
            """,
            (event["kind"], summary, event_id),
        )

    connection.execute(
        """
        INSERT OR IGNORE INTO event_sources (event_id, source_id, run_id, first_seen_at)
        VALUES (?, ?, ?, ?)
        """,
        (event_id, source_id, run_id, now),
    )
    return event_id, created


def add_event_entity(connection: sqlite3.Connection, event_id: str, entity_id: str, role: str) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO event_entities (event_id, entity_id, role)
        VALUES (?, ?, ?)
        """,
        (event_id, entity_id, role),
    )


def upsert_alias(connection: sqlite3.Connection, entity_id: str, alias: str, source_id: str) -> None:
    alias = alias.strip()
    if not alias:
        return
    connection.execute(
        """
        INSERT OR IGNORE INTO entity_aliases (id, entity_id, alias, alias_key, source_id, confidence)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (make_id("ALS"), entity_id, alias, canonicalize(alias), source_id, 1.0),
    )


def create_relationship(
    connection: sqlite3.Connection,
    source_entity_id: str,
    target_entity_id: str,
    relationship_type: str,
    source_id: str,
    source_event_id: str,
    now: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO relationships (
            id, source_entity_id, target_entity_id, relationship_type, strength,
            source_event_id, source_id, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            make_id("REL"),
            source_entity_id,
            target_entity_id,
            relationship_type,
            1,
            source_event_id,
            source_id,
            now,
        ),
    )
    return int(cursor.rowcount > 0)


def ensure_log_stream_run(source_name: str, topic: str) -> dict[str, Any]:
    now = utc_now()
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, source_name, topic, status, message_count, created_entity_count,
                   created_event_count, created_relationship_count, created_at, updated_at
            FROM log_stream_runs
            WHERE source_name = ? AND topic = ? AND status = 'running'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (source_name, topic),
        ).fetchone()
        if row:
            return dict(row)

        run_id = make_id("LSR")
        connection.execute(
            """
            INSERT INTO log_stream_runs (
                id, source_name, topic, status, message_count, created_entity_count,
                created_event_count, created_relationship_count, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, source_name, topic, "running", 0, 0, 0, 0, now, now),
        )
        connection.commit()
        return {
            "id": run_id,
            "source_name": source_name,
            "topic": topic,
            "status": "running",
            "message_count": 0,
            "created_entity_count": 0,
            "created_event_count": 0,
            "created_relationship_count": 0,
            "created_at": now,
            "updated_at": now,
        }


def ingest_stream_log_event(
    source_name: str,
    topic: str,
    partition_id: int,
    offset_value: int,
    line_number: int,
    raw_line: str,
    parsed: dict[str, Any],
) -> dict[str, Any]:
    run = ensure_log_stream_run(source_name, topic)
    run_id = run["id"]
    now = utc_now()

    with get_connection() as connection:
        existing = connection.execute(
            """
            SELECT id
            FROM raw_log_events
            WHERE topic = ? AND partition_id = ? AND offset_value = ?
            LIMIT 1
            """,
            (topic, partition_id, offset_value),
        ).fetchone()
        if existing:
            return {"status": "duplicate", "created_entity_count": 0, "created_event_count": 0, "created_relationship_count": 0}

        host_entity = {
            "name": parsed["host"],
            "kind": "Asset",
            "description": f"Host observed in Linux syslog stream {source_name}.",
            "risk_score": parsed.get("risk_score", 35),
            "location": parsed["host"],
            "aliases": [],
        }
        service_entity = {
            "name": parsed["service"],
            "kind": "Asset",
            "description": f"Service/process observed in Linux syslog stream {source_name}.",
            "risk_score": parsed.get("risk_score", 25),
            "location": parsed["host"],
            "aliases": [],
        }

        host_id, created_host = ensure_entity(connection, host_entity, "", run_id, now)
        service_id, created_service = ensure_entity(connection, service_entity, "", run_id, now)

        event_id = make_id("EVT")
        connection.execute(
            """
            INSERT INTO events (id, title, kind, event_date, location, summary)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                parsed["title"],
                "Log Entry",
                parsed["timestamp"],
                parsed["host"],
                parsed["message"],
            ),
        )
        add_event_entity(connection, event_id, host_id, "host")
        add_event_entity(connection, event_id, service_id, "service")

        relationship_count = 0
        relationship_count += create_relationship(connection, host_id, service_id, "runs_service", "", event_id, now)
        relationship_count += create_relationship(connection, service_id, host_id, "service_runs_on", "", event_id, now)

        connection.execute(
            """
            INSERT INTO raw_log_events (
                id, run_id, source_name, topic, partition_id, offset_value, line_number,
                raw_line, parsed_json, status, error_message, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                make_id("LOG"),
                run_id,
                source_name,
                topic,
                partition_id,
                offset_value,
                line_number,
                raw_line,
                json.dumps(parsed, sort_keys=True),
                "processed",
                "",
                now,
            ),
        )

        connection.execute(
            """
            UPDATE log_stream_runs
            SET message_count = message_count + 1,
                created_entity_count = created_entity_count + ?,
                created_event_count = created_event_count + 1,
                created_relationship_count = created_relationship_count + ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                int(created_host) + int(created_service),
                relationship_count,
                now,
                run_id,
            ),
        )
        connection.commit()

    return {
        "status": "processed",
        "created_entity_count": int(created_host) + int(created_service),
        "created_event_count": 1,
        "created_relationship_count": relationship_count,
    }


def serialize_entity(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "kind": row["kind"],
        "description": row["description"],
        "risk_score": row["risk_score"],
        "location": row["location"],
        "metadata": json.loads(row["metadata_json"]),
    }
