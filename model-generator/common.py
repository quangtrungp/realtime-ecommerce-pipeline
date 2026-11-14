
from datetime import datetime, timezone
from uuid import uuid4
from decimal import Decimal
from typing import Any


def generate_event_id() -> str:
    return str(uuid4())


def get_current_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def decimal_to_string(value: Decimal) -> str:
    return format(value, "f")


def create_event(
    event_type: str,
    source: str,
    payload: dict[str, Any],
    event_time: str | None = None
) -> dict:

    produced_at = get_current_timestamp()

    return {
        "event_id": generate_event_id(),
        "event_type": event_type,
        "event_version": 1,
        "event_time": event_time or produced_at,
        "produced_at": produced_at,
        "source": source,
        "payload": payload
    }
