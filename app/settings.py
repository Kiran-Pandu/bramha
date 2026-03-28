from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    raw_topic: str = os.getenv("KAFKA_RAW_TOPIC", "logs.raw.linux")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "bramha-log-consumer")
    producer_delay_ms: int = int(os.getenv("KAFKA_PRODUCER_DELAY_MS", "200"))
    source_name: str = os.getenv("KAFKA_SOURCE_NAME", "Linux Kafka Stream")
    max_lines: int = int(os.getenv("KAFKA_MAX_LINES", "0"))


@dataclass(frozen=True)
class LogStreamSettings:
    default_log_path: Path = Path(os.getenv("LINUX_LOG_PATH", "data/Linux.log"))


def get_kafka_settings() -> KafkaSettings:
    return KafkaSettings()


def get_log_stream_settings() -> LogStreamSettings:
    return LogStreamSettings()
