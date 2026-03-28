from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from aiokafka import AIOKafkaProducer

from app.settings import get_kafka_settings, get_log_stream_settings


async def main() -> None:
    kafka = get_kafka_settings()
    log_settings = get_log_stream_settings()
    log_path = Path(log_settings.default_log_path)
    if not log_path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")

    producer = AIOKafkaProducer(
        bootstrap_servers=kafka.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    await producer.start()
    try:
        for line_number, raw_line in enumerate(log_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
            if kafka.max_lines and line_number > kafka.max_lines:
                break
            payload = {
                "source_name": kafka.source_name,
                "file_name": log_path.name,
                "line_number": line_number,
                "raw_line": raw_line,
            }
            await producer.send_and_wait(kafka.raw_topic, payload)
            print(f"sent line {line_number}")
            await asyncio.sleep(kafka.producer_delay_ms / 1000)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
