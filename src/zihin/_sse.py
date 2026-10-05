"""Minimal Server-Sent Events parser."""

from __future__ import annotations

import json
from typing import Any, Iterable, Iterator, List, Tuple


def parse_sse(lines: Iterable[str]) -> Iterator[Tuple[str, Any]]:
    """Turn an iterable of text lines into ``(event, data)`` pairs.

    ``data`` is decoded from JSON when possible and left as a string otherwise. Events with
    no data are skipped; comment lines (keep-alives) are ignored.
    """
    event = ""
    data: List[str] = []

    def flush() -> Iterator[Tuple[str, Any]]:
        nonlocal event, data
        raw = "\n".join(data)
        name = event or "message"
        event, data = "", []
        if raw == "":
            return
        try:
            yield name, json.loads(raw)
        except ValueError:
            yield name, raw

    for line in lines:
        line = line.rstrip("\r\n")
        if line == "":
            yield from flush()
            continue
        if line.startswith(":"):
            continue
        field, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        if field == "event":
            event = value
        elif field == "data":
            data.append(value)
        # `id` and `retry` are ignored: this client does not reconnect.

    yield from flush()
