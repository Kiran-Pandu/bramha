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

## Kafka log pipeline

This repo now includes a Kafka-based Linux log stream:

- producer: [scripts/kafka_linux_producer.py](scripts/kafka_linux_producer.py)
- consumer: [scripts/kafka_log_consumer.py](scripts/kafka_log_consumer.py)
- parser: [app/log_parser.py](app/log_parser.py)
- graph ingestion: [app/db.py](app/db.py)

### Topic flow

- raw topic: `logs.raw.linux`
- producer sends one log line at a time
- consumer parses each syslog line
- parsed logs create or reuse graph entities for host and service
- each consumed line creates an event and relationships in the graph store

### Environment variables

- `KAFKA_BOOTSTRAP_SERVERS` default `localhost:9092`
- `KAFKA_RAW_TOPIC` default `logs.raw.linux`
- `KAFKA_CONSUMER_GROUP` default `bramha-log-consumer`
- `KAFKA_PRODUCER_DELAY_MS` default `200`
- `LINUX_LOG_PATH` default `data/Linux.log`

### Run the Kafka workers

Start the local Kafka broker first:

```powershell
.\scripts\start_kafka_stack.ps1
```

This starts a local KRaft-mode Kafka broker on `localhost:9092`.

Start the consumer:

```powershell
.\.venv\Scripts\Activate.ps1
python scripts\kafka_log_consumer.py
```

In another terminal:

```powershell
.\.venv\Scripts\Activate.ps1
python scripts\kafka_linux_producer.py
```

Useful dev options:

- set `$env:KAFKA_PRODUCER_DELAY_MS="1"` to stream quickly
- set `$env:KAFKA_MAX_LINES="300"` to send only the first 300 lines while testing

When you are done:

```powershell
.\scripts\stop_kafka_stack.ps1
```

### One-click local streaming

To start both the broker and Kafka consumer in the background:

```powershell
.\scripts\start_streaming_stack.ps1
```

To stop both:

```powershell
.\scripts\stop_streaming_stack.ps1
```

You can also control just the consumer:

```powershell
.\scripts\start_kafka_consumer.ps1
.\scripts\stop_kafka_consumer.ps1
```

## Where to take it next

- Add authentication and case-based access control
- Add column mapping UI for arbitrary source schemas
- Bring in live ingestion from APIs, queues, or scheduled pulls
- Add alert rules, analyst notes, and audit logging
