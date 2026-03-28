from __future__ import annotations

import re
from datetime import datetime
from typing import Any

SYSLOG_PATTERN = re.compile(
    r"^(?P<month>[A-Z][a-z]{2})\s+(?P<day>\d{1,2})\s+(?P<time>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<host>\S+)\s+(?P<service>[^:]+):\s*(?P<message>.*)$"
)


def parse_linux_syslog_line(raw_line: str, year: int | None = None) -> dict[str, Any]:
    line = raw_line.rstrip("\n")
    match = SYSLOG_PATTERN.match(line)
    if not match:
        raise ValueError("Unsupported Linux syslog format")

    year = year or datetime.now().year
    timestamp = datetime.strptime(
        f"{year} {match.group('month')} {int(match.group('day')):02d} {match.group('time')}",
        "%Y %b %d %H:%M:%S",
    ).isoformat()

    host = match.group("host")
    service = normalize_service(match.group("service"))
    message = match.group("message").strip()

    return {
        "timestamp": timestamp,
        "host": host,
        "service": service,
        "message": message,
        "title": f"{service} log on {host}",
        "kind": infer_kind(service, message),
        "risk_score": infer_risk(service, message),
        "raw_line": line,
    }


def normalize_service(service: str) -> str:
    return " ".join(service.strip().split())


def infer_kind(service: str, message: str) -> str:
    lowered = f"{service} {message}".lower()
    if "kernel" in lowered:
        return "kernel"
    if "error" in lowered or "failed" in lowered:
        return "error"
    if "restart" in lowered or "startup" in lowered or "started" in lowered:
        return "startup"
    return "system"


def infer_risk(service: str, message: str) -> int:
    lowered = f"{service} {message}".lower()
    if "failed" in lowered or "denied" in lowered or "panic" in lowered:
        return 78
    if "kernel" in lowered:
        return 62
    if "restart" in lowered or "startup" in lowered:
        return 35
    return 28
