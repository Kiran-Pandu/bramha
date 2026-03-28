from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from aiokafka import AIOKafkaConsumer

from app.db import ingest_stream_log_event, init_db
from app.log_parser import parse_linux_syslog_line
from app.settings import get_kafka_settings


async def main() -> None:
    kafka = get_kafka_settings()
    init_db()

    consumer = AIOKafkaConsumer(
        kafka.raw_topic,
        bootstrap_servers=kafka.bootstrap_servers,
        group_id=kafka.consumer_group,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    await consumer.start()
    print(f"consuming topic {kafka.raw_topic}")
    try:
        async for message in consumer:
            payload = message.value
            raw_line = payload["raw_line"]
            try:
                parsed = parse_linux_syslog_line(raw_line)
                result = ingest_stream_log_event(
                    source_name=payload.get("source_name", kafka.source_name),
                    topic=message.topic,
                    partition_id=message.partition,
                    offset_value=message.offset,
                    line_number=payload.get("line_number", 0),
                    raw_line=raw_line,
                    parsed=parsed,
                )
                print(
                    f"processed offset={message.offset} entities={result['created_entity_count']} "
                    f"events={result['created_event_count']} relationships={result['created_relationship_count']}"
                )
            except Exception as exc:
                print(f"failed offset={message.offset}: {exc}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
