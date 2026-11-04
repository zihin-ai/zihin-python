"""Server-Sent Events parser (port of ``@zihin/agent-client`` ``sse.ts``).

Robustness:
- size limits (``max_event_bytes`` / ``max_total_bytes``) raise ``protocol`` (memory anti-DoS);
- incremental parsing over raw byte chunks;
- the caller closes the response in a ``finally`` so the connection is released on break/error.
"""

from __future__ import annotations

import codecs
import json
from typing import Any, Iterable, Iterator, List, Tuple

from ._errors import ZihinError

DEFAULT_MAX_EVENT_BYTES = 1 * 1024 * 1024  # 1 MB per event/line
DEFAULT_MAX_TOTAL_BYTES = 64 * 1024 * 1024  # 64 MB per stream


def _utf8_len(text: str) -> int:
    return len(text.encode("utf-8", errors="surrogatepass"))


def parse_sse(
    chunks: Iterable[bytes],
    *,
    max_event_bytes: int = DEFAULT_MAX_EVENT_BYTES,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
) -> Iterator[Tuple[str, Any]]:
    """Turn an iterable of raw byte chunks into ``(event, data)`` pairs.

    ``data`` is decoded from JSON when possible and left as a string otherwise. Events with no
    data are skipped; comment lines (keep-alives) are ignored; ``id``/``retry`` are ignored (this
    client does not reconnect).
    """
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    buffer = ""
    event = ""
    data: List[str] = []
    data_bytes = 0
    total = 0

    def flush() -> Iterator[Tuple[str, Any]]:
        nonlocal event, data, data_bytes
        had = bool(data) or event != ""
        raw = "\n".join(data)
        name = event or "message"
        event, data, data_bytes = "", [], 0
        if not had or raw == "":
            return
        try:
            yield name, json.loads(raw)
        except ValueError:
            yield name, raw

    def field_of(line: str) -> Tuple[str, str]:
        name, sep, value = line.partition(":")
        if not sep:
            return line, ""
        if value.startswith(" "):
            value = value[1:]
        return name, value

    for chunk in chunks:
        if not chunk:
            continue
        total += len(chunk)
        if total > max_total_bytes:
            raise ZihinError("protocol", f"SSE stream exceeded {max_total_bytes} bytes.")
        buffer += decoder.decode(chunk)
        while True:
            nl = buffer.find("\n")
            if nl == -1:
                break
            line = buffer[:nl]
            buffer = buffer[nl + 1 :]
            if line.endswith("\r"):
                line = line[:-1]
            if line == "":
                yield from flush()
                continue
            if line.startswith(":"):
                continue  # comment / keep-alive
            name, value = field_of(line)
            if name == "event":
                event = value
            elif name == "data":
                data_bytes += _utf8_len(value) + 1
                if data_bytes > max_event_bytes:
                    raise ZihinError("protocol", f"SSE event exceeded {max_event_bytes} bytes.")
                data.append(value)
        # A line still without `\n` in the buffer cannot grow without a ceiling.
        if _utf8_len(buffer) > max_event_bytes:
            raise ZihinError("protocol", f"SSE line exceeded {max_event_bytes} bytes.")

    # Final decoder flush (a multibyte character split at the end) and a last `data:`/`event:` line
    # that arrived without the terminating `\n`.
    buffer += decoder.decode(b"", final=True)
    if buffer:
        line = buffer[:-1] if buffer.endswith("\r") else buffer
        if line and not line.startswith(":"):
            name, value = field_of(line)
            if name == "data":
                data.append(value)
            elif name == "event":
                event = value
    yield from flush()
