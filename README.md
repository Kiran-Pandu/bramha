# Bramha System

A starter Bramha-style intelligence platform for correlating entities, events, links, and ingested source data in a single local dashboard.

## What this MVP includes

- FastAPI backend with SQLite persistence
- Seeded investigation dataset
- CSV and JSON ingestion from the dashboard
- Preview and column mapping for arbitrary CSV and JSON files
- Source tracking, run logs, and raw record capture
- Basic entity resolution using names and aliases
- Entity search and detail views
- Event timeline, provenance, and an interactive node-link graph view
- Graph filters, clustering modes, edge labels, and saved investigation subgraphs
- Lightweight dashboard served from the same app

## Run locally

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m uvicorn app.main:app --reload
```

Then open <http://127.0.0.1:8000>.

## Try ingestion immediately

Use the dashboard upload panel and import [data/sample_ingest.csv](data/sample_ingest.csv). The app will:

- create or update a source
- log an ingestion run
- preview detected fields and suggest a canonical mapping
- normalize each row into entities, events, aliases, and relationships
- merge matching entities by name or alias
- refresh the dashboard with the new graph data

## Graph workspace

The graph view now supports:

- pan, zoom, drag, and click-to-focus
- filtering by kind and minimum risk score
- clustering by entity kind or risk band
- edge labels for relationship types
- saving the current filtered graph as an investigation subgraph

## API surface

- `GET /api/summary` for dashboard stats
- `GET /api/entities?search=term` for filtered entity search
- `GET /api/entities/{id}` for investigation detail
- `GET /api/graph` for the shared-event network
- `GET /api/sources` for tracked data sources
- `GET /api/ingest/runs` for ingestion history
- `POST /api/ingest/preview` for field detection and mapping suggestions
- `POST /api/ingest/run` for CSV or JSON ingestion
- `GET /api/investigations/subgraphs` for saved graph snapshots
- `POST /api/investigations/subgraphs` to save the current graph view

## Where to take it next

- Add authentication and case-based access control
- Add column mapping UI for arbitrary source schemas
- Bring in live ingestion from APIs, queues, or scheduled pulls
- Add alert rules, analyst notes, and audit logging
