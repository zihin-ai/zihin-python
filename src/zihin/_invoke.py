"""Direct agent invocation, POST /api/v2/agents/:id/stream (port of ``invoke.ts``).

``stream_events`` yields normalized events live; ``invoke`` buffers the stream and consolidates
the ``response`` event (content/sources/session_id/blocks) with ``metrics`` (usage/model).
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence
from urllib.parse import quote

from ._errors import ZihinError, error_from_response
from ._http import DEADLINE_GRACE_MS, Transport, normalize_timeout, parse_body, to_json_body
from ._security import safe_id, safe_label, sanitize_message, validate_base_url, validate_prompt_size
from ._sse import parse_sse
from ._types import AgentResult, ErrorKind, IgnoredField, StreamEvent, Usage


def _d(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _s(value: Any) -> Optional[str]:
    return value if isinstance(value, str) else None


def _n(value: Any) -> Any:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _first(*values: Any) -> Any:
    """First value that is not ``None`` (JavaScript's ``??``)."""
    for v in values:
        if v is not None:
            return v
    return None


def parse_usage(value: Any) -> Optional[Usage]:
    u = _d(value)
    if not u:
        return None
    return Usage(
        input_tokens=_first(_n(u.get("input_tokens")), _n(u.get("prompt_tokens"))),
        output_tokens=_first(_n(u.get("output_tokens")), _n(u.get("completion_tokens"))),
        total_tokens=_n(u.get("total_tokens")),
        cost_usd=_n(u.get("cost_usd")),
    )


def _js_len(text: str) -> int:
    """Length as JavaScript counts it (UTF-16 code units): the server measures limits that way."""
    return len(text.encode("utf-16-le", errors="surrogatepass")) // 2


# ---------------------------------------------------------------------------------------------
# Event mapping
# ---------------------------------------------------------------------------------------------


def _field_name(x: Any) -> Optional[str]:
    return safe_id(x if isinstance(x, str) else _first(_s(_d(x).get("field")), _s(_d(x).get("name"))))


def _parse_accepted_fields(v: Any) -> Optional[List[str]]:
    """``accepted_fields``: array of names (tolerates ``{field}``)."""
    if not isinstance(v, list):
        return None
    return [f for f in (_field_name(x) for x in v) if f]


def _parse_ignored_fields(v: Any) -> Optional[List[IgnoredField]]:
    """``ignored_fields``: names or ``{field|name, reason}``."""
    if not isinstance(v, list):
        return None
    out: List[IgnoredField] = []
    for x in v:
        # Name and reason go into the error message: only the expected format (hostile server).
        name = _field_name(x)
        if name:
            out.append(IgnoredField(name) if isinstance(x, str) else IgnoredField(name, safe_label(_d(x).get("reason"))))
    return out


# Defensive ceiling (hostile server): the server sends up to 4 labels of up to 60 characters.
_MAX_SUGGESTIONS = 10
_MAX_SUGGESTION_CHARS = 200


def suggestions_from_blocks(blocks: Any) -> Optional[List[str]]:
    """Reply suggestions: labels of the LAST ``quick_replies`` block, filtered and capped."""
    if not isinstance(blocks, list):
        return None
    qr = next((b for b in reversed(blocks) if _d(b).get("type") == "quick_replies"), None)
    labels = _d(_d(qr).get("data")).get("labels")
    if not isinstance(labels, list):
        return None
    out = [
        label
        for label in labels
        if isinstance(label, str) and label.strip() and _js_len(label) <= _MAX_SUGGESTION_CHARS
    ][:_MAX_SUGGESTIONS]
    return out or None


def _turn_ids(d: Dict[str, Any]) -> Dict[str, Any]:
    """Turn ids the server spreads over ``metadata``/``response``/``done`` (since 2026-10-17)."""
    return {
        "execution_id": safe_id(d.get("execution_id")),
        "client_message_id": safe_id(d.get("client_message_id")),
        "idempotent_replay": True if d.get("idempotent_replay") is True else None,
        "user_message_id": safe_id(d.get("user_message_id")),
        "assistant_message_id": safe_id(d.get("assistant_message_id")),
    }


def _b(value: Any) -> Optional[bool]:
    return value if isinstance(value, bool) else None


def map_event(name: str, data: Any) -> StreamEvent:
    """Map a raw SSE event to the normalized :class:`StreamEvent`."""
    d = _d(data)
    if name == "metadata":
        return StreamEvent(
            "metadata",
            data,
            event=name,
            request_id=safe_id(d.get("request_id")),
            agent_id=_s(d.get("agent_id")),
            session_id=safe_id(d.get("session_id")),
            accepted_fields=_parse_accepted_fields(d.get("accepted_fields")),
            ignored_fields=_parse_ignored_fields(d.get("ignored_fields")),
            **_turn_ids(d),
        )
    if name == "status":
        return StreamEvent("status", data, event=name, phase=_s(d.get("phase")), blocked=safe_label(d.get("blocked")))
    if name == "attachments":
        return StreamEvent("attachments", data, event=name)
    if name == "phase":
        return StreamEvent("phase", data, event=name, phase=_s(d.get("phase")))
    if name == "thinking":
        return StreamEvent("thinking", data, event=name, thought=_s(d.get("thought")))
    if name == "tool":
        return StreamEvent(
            "tool", data, event=name, tool=_s(d.get("tool")), call_id=_s(d.get("call_id")), success=_b(d.get("success"))
        )
    if name == "action_proposed":
        return StreamEvent("action_proposed", data, event=name)
    if name == "action_forwarded":
        return StreamEvent("action_forwarded", data, event=name, action_id=safe_id(d.get("action_id")))
    if name == "token":
        return StreamEvent("token", data, event=name, content=_first(_s(d.get("content")), ""))
    if name == "warning":
        return StreamEvent("warning", data, event=name, message=_s(d.get("message")))
    if name == "session":
        return StreamEvent("session", data, event=name, session_id=safe_id(d.get("session_id")))
    if name == "done":
        return StreamEvent(
            "done",
            data,
            event=name,
            outcome=safe_label(d.get("outcome")),
            timed_out=_b(d.get("timed_out")),
            timeout_clock=safe_label(d.get("timeout_clock")),
            truncated=_b(d.get("truncated")),
            finish_reason=_s(d.get("finish_reason")),
            **_turn_ids(d),
        )
    if name == "error":
        err = _d(d.get("error"))
        return StreamEvent(
            "error",
            data,
            event=name,
            code=safe_label(err.get("code")),
            message=_s(err.get("message")),
            request_id=safe_id(err.get("request_id")),
            timed_out=_b(err.get("timed_out")),
        )
    if name == "response":
        return StreamEvent(
            "response",
            data,
            event=name,
            content=_first(_s(d.get("content")), ""),
            session_id=safe_id(d.get("session_id")),
            gated=True if d.get("gated") is True else None,
            sources=d.get("sources") if isinstance(d.get("sources"), list) else None,
            blocks=d.get("blocks") if isinstance(d.get("blocks"), list) else None,
            suggestions=suggestions_from_blocks(d.get("blocks")),
            **_turn_ids(d),
        )
    if name == "metrics":
        model_config = _d(d.get("model_config"))
        return StreamEvent(
            "metrics",
            data,
            event=name,
            usage=parse_usage(d.get("usage")),
            model=_first(_s(model_config.get("model")), _s(d.get("model"))),
            iterations=_n(d.get("iterations")),
            execution_time_ms=_n(d.get("execution_time_ms")),
            execution_id=safe_id(d.get("execution_id")),
        )
    return StreamEvent("unknown", data, event=name)


# ---------------------------------------------------------------------------------------------
# Structured input: validation and request body
# ---------------------------------------------------------------------------------------------

# The server's ceilings (`utils/declared-turn-input.js`, zihin-agent-builder#669). Repeated here to
# fail before opening the stream; the server remains the source of truth. Totals are in characters
# of the JSON serialization, as the server measures. `context` has a smaller ceiling because the
# block is repeated in every user message (prompt cache); `bindings` does not go to the prompt.
MAX_FIELDS = 50
MAX_VALUE_CHARS = 2000
MAX_TOTAL_CHARS = {"context": 2000, "bindings": 16000}
MAX_SESSION_FROM_FIELDS = 5
_FIELD_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,63}")
# The server drops these names silently (no `ignored_fields` entry): refuse them here.
_PROTOTYPE_KEYS = {"__proto__", "constructor", "prototype"}
MAX_CONSUMER_KEY_CHARS = 200
MAX_CONSUMER_NAME_CHARS = 255
MAX_CLIENT_BLOCKS = 10
# Same regex as the server (`services/runtime/stream-idempotency.js`): a UUID of any version, any case.
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)


@dataclass
class InvokeParams:
    agent_id: str
    message: Optional[str] = None
    session_id: Optional[str] = None
    client_message_id: Optional[str] = None
    consumer_key: Optional[str] = None
    consumer_name: Optional[str] = None
    capabilities: Optional[Mapping[str, Any]] = None
    context: Optional[Mapping[str, Any]] = None
    bindings: Optional[Mapping[str, Any]] = None
    session_from: Optional[Sequence[str]] = None
    attachments: Optional[Sequence[Mapping[str, Any]]] = None
    timeout_ms: Optional[float] = None
    metadata: Optional[Mapping[str, Any]] = None
    include_raw: bool = False

    def snapshot(self) -> "InvokeParams":
        """One copy of the structured input: validation and body read the SAME objects."""
        copy = InvokeParams(**self.__dict__)
        if isinstance(self.context, dict):
            copy.context = dict(self.context)
        if isinstance(self.bindings, dict):
            copy.bindings = dict(self.bindings)
        if isinstance(self.session_from, (list, tuple)):
            copy.session_from = list(self.session_from)
        if isinstance(self.capabilities, dict):
            caps = dict(self.capabilities)
            if isinstance(caps.get("blocks"), (list, tuple)):
                caps["blocks"] = list(caps["blocks"])
            copy.capabilities = caps
        return copy


def _is_scalar(v: Any) -> bool:
    if v is None or isinstance(v, (str, bool, int)):
        return True
    return isinstance(v, float) and math.isfinite(v)


def _validate_structured_input(p: InvokeParams) -> None:
    if p.client_message_id is not None and (
        not isinstance(p.client_message_id, str) or not _UUID_RE.fullmatch(p.client_message_id)
    ):
        # Fail here, not on the server: there it becomes `VALIDATION_ERROR` already inside the
        # stream, and the caller who sent the key precisely to be able to retry would lose it.
        raise ZihinError("config", "`client_message_id` must be a UUID (e.g. `str(uuid.uuid4())`).")
    if p.consumer_key is not None and (not isinstance(p.consumer_key, str) or not p.consumer_key.strip()):
        raise ZihinError("config", "`consumer_key` must be a non-empty string.")
    if isinstance(p.consumer_key, str) and _js_len(p.consumer_key) > MAX_CONSUMER_KEY_CHARS:
        raise ZihinError("config", f"`consumer_key` is longer than {MAX_CONSUMER_KEY_CHARS} characters.")
    if p.consumer_name is not None:
        if p.consumer_key is None:
            raise ZihinError("config", "`consumer_name` requires `consumer_key` (the name has nothing to attach to without it).")
        n = p.consumer_name
        if not isinstance(n, str) or not n.strip() or _js_len(n) > MAX_CONSUMER_NAME_CHARS:
            raise ZihinError(
                "config", f"`consumer_name` must be a non-empty string of up to {MAX_CONSUMER_NAME_CHARS} characters."
            )
    if p.capabilities is not None:
        caps = p.capabilities if isinstance(p.capabilities, dict) else None
        blocks = caps.get("blocks", ...) if caps is not None else None
        if (
            caps is None
            or blocks is None
            or (
                blocks is not ...
                and (not isinstance(blocks, list) or len(blocks) > MAX_CLIENT_BLOCKS or any(not isinstance(b, str) for b in blocks))
            )
        ):
            raise ZihinError(
                "config",
                f'`capabilities` must be `{{"blocks": [...]}}` with up to {MAX_CLIENT_BLOCKS} block names '
                '(e.g. `["quick_replies"]`).',
            )
    for name in ("context", "bindings"):
        v = getattr(p, name)
        if v is None:
            continue
        if not isinstance(v, dict):
            raise ZihinError("config", f"`{name}` must be a flat dict.")
        if len(v) > MAX_FIELDS:
            raise ZihinError("config", f"`{name}` accepts at most {MAX_FIELDS} fields.")
        for k, val in v.items():
            if not isinstance(k, str) or not _FIELD_NAME_RE.fullmatch(k) or k in _PROTOTYPE_KEYS:
                raise ZihinError(
                    "config",
                    f"Invalid field name in `{name}`: use letters, digits, `_` or `-`, starting with a letter "
                    "or `_` (max 64 characters; `constructor`, `prototype` and `__proto__` are not allowed).",
                )
            if isinstance(val, str) and _js_len(val) > MAX_VALUE_CHARS:
                raise ZihinError("config", f"`{name}.{k}` is longer than {MAX_VALUE_CHARS} characters.")
            if not _is_scalar(val):
                raise ZihinError(
                    "config",
                    f"`{name}.{k}` must be a string, number, boolean or None — `{name}` is flat "
                    "(no nested dicts or lists).",
                )
        serialized = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
        if _js_len(serialized) > MAX_TOTAL_CHARS[name]:
            raise ZihinError("config", f"`{name}` is longer than {MAX_TOTAL_CHARS[name]} characters in total (as JSON).")
    if p.session_from is not None:
        sf = p.session_from
        if (
            not isinstance(sf, list)
            or len(sf) > MAX_SESSION_FROM_FIELDS
            or any(not isinstance(f, str) or not _FIELD_NAME_RE.fullmatch(f) or f in _PROTOTYPE_KEYS for f in sf)
        ):
            raise ZihinError("config", f"`session_from` must be a list of up to {MAX_SESSION_FROM_FIELDS} field names.")
        if sf and p.session_id:
            raise ZihinError("config", "Use either `session_id` or `session_from`, not both.")
        # Same rule as the server: `bindings` wins over `context` on a collision, and the webhook
        # derivation (`value || ''` + `.filter(Boolean)`) treats `0`/`False`/`''` as ABSENT.
        available: Dict[str, Any] = {**(p.context or {}), **(p.bindings or {})}
        if sf and not any(f in available and bool(available[f]) for f in sf):
            names = ", ".join(f'"{f}"' for f in sf)
            raise ZihinError(
                "config", f"`session_from` names {names}, but none of them has a value in `context` or `bindings`."
            )


def validate_params(p: InvokeParams) -> None:
    if not p.agent_id:
        raise ZihinError("config", "agent_id is required.")
    if not p.message and not p.attachments:
        raise ZihinError("config", "Either `message` or `attachments` is required.")
    if p.message is not None and not isinstance(p.message, str):
        raise ZihinError("config", "`message` must be a string.")
    if p.message:
        size_error = validate_prompt_size(p.message)
        if size_error:
            raise ZihinError("config", size_error)
    _validate_structured_input(p)
    if p.timeout_ms is not None:
        normalize_timeout(p.timeout_ms, "timeout_ms", unit=1000)
    # Defense in depth: attachments come from the user and the server fetches them (server-side
    # fetch). HTTPS + no private IP; any domain.
    for att in p.attachments or []:
        check = validate_base_url(_s(_d(att).get("url")) or "\x00", allow_custom_domains=True)
        if not check.valid:
            raise ZihinError("config", f"Invalid attachment URL: {check.error}")


def build_request_body(p: InvokeParams, normalized_timeout_ms: Optional[int]) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    if p.message is not None:
        body["message"] = p.message
    if p.session_id:
        body["session_id"] = p.session_id
    if p.client_message_id:
        body["client_message_id"] = p.client_message_id
    if p.consumer_key:
        body["consumer_key"] = p.consumer_key
    if p.consumer_name is not None:
        body["consumer_display_name"] = p.consumer_name
    blocks = _d(p.capabilities).get("blocks")
    if isinstance(blocks, list) and blocks:
        body["capabilities"] = {"blocks": list(blocks)}
    if p.context is not None:
        body["context"] = dict(p.context)
    if p.bindings is not None:
        body["bindings"] = dict(p.bindings)
    if p.session_from:
        body["session_from"] = list(p.session_from)
    if p.attachments:
        body["attachments"] = [{"url": _d(a).get("url"), "type": _d(a).get("type")} for a in p.attachments]
    if normalized_timeout_ms is not None:
        body["options"] = {"timeout_ms": normalized_timeout_ms}
    if p.metadata is not None:
        body["metadata"] = dict(p.metadata)
    return body


def _requested_input_fields(p: InvokeParams) -> List[str]:
    """Input-contract fields the caller sent (wire names)."""
    out = []
    if p.context is not None:
        out.append("context")
    if p.bindings is not None:
        out.append("bindings")
    if p.session_from:
        out.append("session_from")
    # Idempotency the server did not apply is not "it just doesn't happen": the caller will retry
    # counting on it and duplicate the turn. It enters the check like the structured fields.
    if p.client_message_id:
        out.append("client_message_id")
    return out


def _check_input_capability(p: InvokeParams, meta: Optional[StreamEvent]) -> None:
    """Capability check: the caller sent context/bindings/session_from/client_message_id and the
    server has to say it applied them. Without it the turn runs WITHOUT them; never go on silently.

    - ``accepted_fields`` absent = server older than the input contract (drops the fields).
    - a field in ``ignored_fields`` = received and not applied: ``requires_admin_api_key`` or, per
      key, ``reserved_key``. ``redacted`` is a warning (applied with a redacted value).
    ``consumer_key`` predates the contract; it only fails when explicitly ignored, or missing from
    ``accepted_fields`` once the server publishes that list.
    """
    requested = _requested_input_fields(p)
    ignored = (meta.ignored_fields if meta else None) or []
    sent = requested + (["consumer_key"] if p.consumer_key else [])
    request_id = meta.request_id if meta else None
    blocking = [
        f
        for f in ignored
        if f.reason != "redacted" and any(f.field == n or f.field.startswith(n + ".") for n in sent)
    ]
    # Closing the stream requests the cancellation, but does not undo what the server already did
    # before the client dropped: the message may be recorded in the session.
    tail = "the stream was closed to cancel the turn, but the message may already be recorded in the session."
    if blocking:
        # Short list: a hostile server may send thousands of entries.
        shown = blocking[:5]
        more = f" (+{len(blocking) - len(shown)} more)" if len(blocking) > len(shown) else ""
        what = ", ".join(f"`{f.field}` ({f.reason})" if f.reason else f"`{f.field}`" for f in shown) + more
        if any(f.reason == "reserved_key" for f in blocking):
            hint = " Reserved keys are names the agent engine uses internally; rename them."
        elif any(f.reason == "requires_admin_api_key" for f in blocking):
            hint = " These fields only apply with an admin/owner API key."
        else:
            hint = ""
        raise ZihinError("config", f"The server ignored {what}.{hint} The answer is not used: {tail}", request_id=request_id)
    accepted = meta.accepted_fields if meta else None
    if requested and accepted is None:
        names = ", ".join(f"`{f}`" for f in requested)
        raise ZihinError(
            "config",
            f"This Zihin server does not support {names} on /stream yet (no `accepted_fields` in the stream "
            f"metadata) — {tail}",
            request_id=request_id,
        )
    # Explicit acceptance, not just absence of refusal: a partial server may publish
    # `accepted_fields` before turning `context` on.
    must_accept = requested + ["consumer_key"] if accepted is not None and p.consumer_key else requested
    not_accepted = [f for f in must_accept if f not in (accepted or [])]
    if not_accepted:
        names = ", ".join(f"`{f}`" for f in not_accepted)
        raise ZihinError(
            "config", f"The server did not accept {names} (missing from `accepted_fields`) — {tail}", request_id=request_id
        )


# Events that only exist after the turn started: one of them arriving without `metadata` is late.
_TURN_EVENTS = {
    "phase", "thinking", "tool", "action_proposed", "action_forwarded", "token", "response", "metrics", "done",
}


def stream_events(t: Transport, raw: InvokeParams) -> Iterator[StreamEvent]:
    """Validate eagerly, then return a generator that opens the stream on first iteration."""
    p = raw.snapshot()
    validate_params(p)
    # The caller's `timeout_ms` is the ceiling the server must respect (sent as
    # `options.timeout_ms`). The LOCAL deadline adds a grace; messages cite the caller's number.
    caller_ms = int(normalize_timeout(p.timeout_ms, "timeout_ms", unit=1000)) if p.timeout_ms is not None else None
    timeout = (caller_ms + DEADLINE_GRACE_MS) / 1000 if caller_ms is not None else t.resolve_timeout("stream")
    reported_ms = caller_ms if caller_ms is not None else round(timeout * 1000)
    body = to_json_body(build_request_body(p, caller_ms), "request body")
    url = f"{t.base_url}/api/v2/agents/{quote(p.agent_id, safe='')}/stream"
    return _stream(t, p, url, body, timeout, reported_ms)


def _stream(
    t: Transport, p: InvokeParams, url: str, body: bytes, timeout: float, reported_ms: int
) -> Iterator[StreamEvent]:
    # The check runs once: on `metadata`, or on the first turn event if it did not come.
    needs_capability = bool(_requested_input_fields(p)) or bool(p.consumer_key)
    # After a `status: GATED` without `metadata` (older server), turn events do not require the
    # check; a `metadata` that still arrives is checked.
    gated = False

    # POST retry only BEFORE the headers arrive. What makes it safe is the server's order: the SSE
    # flushes its headers before reserving the id and before any work of the turn, so failing
    # without headers almost always means the handler did not start. The key covers the rest: if
    # the first POST did run, the resend gets SESSION_BUSY (in flight) or a replay (COMPLETED/LENGTH).
    # The key does NOT protect a turn cut mid-stream: the server cancels and releases the id, and
    # the resend runs again with its tools — that is why the mid-stream error is not retryable.
    # This is on before `accepted_fields` is seen (it comes in `metadata`): against an old server
    # the pre-header retry happens without the key being honoured — acceptable by the same order,
    # and documented in the README. Unverified residue: a proxy that buffers headers and answers
    # 502/504 after the server already started.
    res, deadline_at, endpoint = t.open_stream(
        url, body=body, timeout=timeout, reported_ms=reported_ms, idempotent=bool(p.client_message_id)
    )
    try:
        if res.status_code >= 400:
            try:
                text = res.read().decode("utf-8", errors="replace")
            except Exception:  # noqa: BLE001
                text = ""
            raise error_from_response(res.status_code, parse_body(text), endpoint=endpoint)
        chunks = t.iter_stream(res, deadline_at, reported_ms, endpoint)
        for name, data in parse_sse(
            chunks, max_event_bytes=t.max_stream_event_bytes, max_total_bytes=t.max_stream_total_bytes
        ):
            event = map_event(name, data)
            if needs_capability:
                # Raise BEFORE delivering: leaving the loop closes the connection, and the server
                # cancels the turn when the client drops.
                # GATED (handoff/denylist/suspension): a canned answer without a model, nothing ran
                # with missing context. Current servers open that branch with `metadata` (checked
                # above, before `status`); this detour is for older servers that skipped it.
                if event.type == "metadata":
                    _check_input_capability(p, event)
                elif event.type == "status" and event.phase == "GATED":
                    gated = True
                elif event.type in _TURN_EVENTS and not gated:
                    _check_input_capability(p, None)
                if event.type == "metadata" or (event.type in _TURN_EVENTS and not gated):
                    needs_capability = False
            yield event
    finally:
        res.close()


# ---------------------------------------------------------------------------------------------
# Outcome classification
# ---------------------------------------------------------------------------------------------

#: Codes the server emits as `event: error` (not via `done`) that are NOT a server failure, verified
#: in `routes/agents-v2.js` @ 9ec79cfc (2026-11-03). The server's `sse.error()` drops `status` and
#: `details`: only `code` + `message` reach the wire, so classification is by code.
#:
#: - SESSION_BUSY (one turn per session, on by default since 2026-10-18): the turn did not even
#:   start, so it is retryable. Two cases indistinguishable on the wire: another turn in flight in
#:   the session, or the SAME client_message_id still in flight (the resend becomes a replay when it ends).
#: - IDEMPOTENCY_CONFLICT: the client_message_id was already used with ANOTHER message (caller bug).
#: - CONTEXT_LIMIT: the conversation gathered more context than the model processes. Repeating in
#:   the same session gives the same; ask for less or open another session.
#: - CONFLICT: declared identity differs from the session's.
#: - NOT_FOUND: agent missing/inaccessible for this key (same rule as 403 AGENT_ACCESS_DENIED).
#: - SERVICE_UNAVAILABLE: idempotent replay without a recoverable answer.
#: - Quota/subscription: the same kinds as the HTTP commercial blocks.
_STREAM_ERROR_CODES: Dict[str, Dict[str, Any]] = {
    "SESSION_BUSY": {
        "kind": "blocked",
        "message": "Another turn is still running in this session (or the same client_message_id is still in "
        "flight); retry after it finishes.",
        "retryable": True,
    },
    "IDEMPOTENCY_CONFLICT": {
        "kind": "config",
        "message": "This `client_message_id` was already used with a different message — use a new UUID per message.",
    },
    # `keep_server_message`: the server's sentence says what was and was NOT done (tools that ran);
    # the integrator needs it to decide whether to resend.
    "CONTEXT_LIMIT": {
        "kind": "blocked",
        "message": "The conversation gathered more context than the model can process; send a smaller request "
        "or start a new session.",
        "keep_server_message": True,
    },
    "CONFLICT": {
        "kind": "blocked",
        "message": "The declared consumer identity does not match the one already bound to this session.",
    },
    "NOT_FOUND": {
        "kind": "not_found",
        "message": "Agent not found, inactive, or not accessible with this API key — check the agent ID and that "
        "the key belongs to the same Zihin workspace.",
    },
    # Idempotent replay without a recoverable answer. It can last 24 h (turn recorded without the
    # assistant message): retrying with the SAME id hits the same error, so it is not retryable;
    # the way out is a new client_message_id.
    "SERVICE_UNAVAILABLE": {
        "kind": "server",
        "message": "This message was already answered, but the stored answer could not be read; resend with a new "
        "client_message_id.",
        "retryable": False,
    },
    # Defensive: no in-stream emission point observed on the server @ 9ec79cfc.
    "RATE_LIMIT_EXCEEDED": {"kind": "rate_limit", "retryable": True},
    "QUOTA_EXCEEDED": {"kind": "quota_exceeded"},
    "TRIAL_EXPIRED": {"kind": "trial_expired"},
    "NO_ACTIVE_SUBSCRIPTION": {"kind": "no_subscription"},
}

_TIMEOUT_NAME_RE = re.compile(r"TIMEOUT", re.I)


def classify_stream_error_kind(code: str, timed_out: Optional[bool] = None) -> ErrorKind:
    """Kind of a terminal ``event: error``.

    The explicit ``error.timed_out`` field is authoritative both ways: ``True`` is a timeout
    whatever the code; ``False`` is never a timeout even if the code name contains "TIMEOUT" (the
    server is renaming those codes). The name regex is only a FALLBACK for servers without the field.
    """
    if timed_out is True:
        return "timeout"
    # /stream validates the input after opening the SSE: a malformed request (config).
    if code == "VALIDATION_ERROR":
        return "config"
    known = _STREAM_ERROR_CODES.get(code)
    if known:
        kind: ErrorKind = known["kind"]
        return kind
    if timed_out is False:
        return "server"
    return "timeout" if _TIMEOUT_NAME_RE.search(code) else "server"


_TAG_RE = re.compile(r"<[^>]*>")


def error_from_stream_event(
    event: StreamEvent, *, execution_id: Optional[str] = None, session_id: Optional[str] = None
) -> ZihinError:
    """Convert an ``error`` stream event into a :class:`ZihinError`, the same rule
    ``invoke_agent`` applies. Pass the ids already seen in ``metadata`` (the error carries none)."""
    code = event.code or "AGENT_ERROR"
    kind = classify_stream_error_kind(code, event.timed_out)
    known = None if event.timed_out is True else _STREAM_ERROR_CODES.get(code)
    # Our message when the code is known: the server's is in Portuguese and changes without notice.
    # Only secrets are redacted by the sanitizer; this sentence may reach a UI, so tags are stripped
    # and the size is capped.
    server_msg = _TAG_RE.sub("", sanitize_message(event.message))[:300] if event.message else ""
    known_msg = known.get("message") if known else None
    if known and known_msg:
        message = f"{known_msg} Server: {server_msg}" if known.get("keep_server_message") and server_msg else known_msg
    else:
        message = server_msg or code
    return ZihinError(
        kind,
        message,
        code=code,
        request_id=event.request_id,
        execution_id=safe_id(execution_id),
        session_id=safe_id(session_id),
        retryable=bool(known.get("retryable", False)) if known else False,
    )


#: Outcomes where the ``response`` content is NOT an agent answer: the server emits the engine's
#: failure/refusal text as ``response`` and only ``done`` says what happened. Not here:
#: COMPLETED/LENGTH (success), PAUSED (pending approval OR paused agent, see below) and any new
#: outcome (additive; passed through without raising).
_FAILED_OUTCOMES: Dict[str, Dict[str, Any]] = {
    "ERROR": {"kind": "server", "message": "The agent turn failed on the server; its text is the engine failure notice, not an answer."},
    # `server`, not `aborted`: `aborted` is the caller's OWN cancellation.
    "CANCELLED": {"kind": "server", "message": "The agent turn was cancelled on the server (by an operator or a platform guard)."},
    # Process mode draining/shutting down; the engine resumes later.
    "SUSPENDED": {"kind": "blocked", "message": "The agent turn was suspended by the server and did not answer; retry later."},
    "BUDGET_EXCEEDED": {"kind": "blocked", "message": "The agent's spending cap was reached; raise the agent budget to keep invoking it."},
    "QUOTA_EXCEEDED": {"kind": "quota_exceeded", "message": "Your Zihin token quota is exhausted. Upgrade the plan or wait for the next cycle."},
    "SESSION_BUSY": {"kind": "blocked", "message": "Another turn is still running in this session; retry after it finishes.", "retryable": True},
    # Today the route sends CONFLICT as `event: error`; mapped defensively.
    "CONFLICT": {"kind": "blocked", "message": "The declared consumer identity does not match the one already bound to this session."},
    # The model provider refused under its policy (server 4cea8d53, 2026-10-08). The content is a
    # FIXED neutral platform text, not an answer: the server records the execution as an error.
    # Publishing it as an answer was the silent failure of 0.1.0.
    "REFUSED": {"kind": "blocked", "message": "The model provider refused this request under its content policy; the agent did not answer."},
    # On /stream it arrives as `event: error`; on the sync webhook and future servers it may be an outcome.
    "CONTEXT_LIMIT": {"kind": "blocked", "message": "The conversation gathered more context than the model can process; send a smaller request or start a new session."},
}

#: PAUSED is ambiguous on the server: a pending human approval (legitimate turn) OR the admission
#: gate refusing a paused/archived agent. Only the ``status {blocked}`` emitted before ``response``
#: disambiguates.
_PAUSED_BLOCKS = {"agent_paused", "agent_archived"}

#: ERROR with a preceding ``status.blocked`` is NOT a server failure: it is a gate whose reason falls
#: into the ERROR fallback.
_ERROR_BLOCKS: Dict[str, Dict[str, Any]] = {
    "ownership_denied": {"kind": "not_found", "message": "Session not found for this API key — start a new conversation (omit session_id) instead of reusing this one."},
    "session_load_failed": {"kind": "blocked", "message": "The server could not load this conversation right now; retry shortly.", "retryable": True},
    "approval_resolve_failed": {"kind": "blocked", "message": "The server could not route the required approval right now; retry shortly.", "retryable": True},
    "approval_no_approver": {"kind": "blocked", "message": "This action requires approval, but the approval policy has no approver configured."},
    "blocked_schedule": {"kind": "blocked", "message": "The agent is outside its allowed schedule (security policy)."},
    "owner_inactive": {"kind": "blocked", "message": "The agent's owner is inactive; the agent cannot be invoked."},
}


def error_from_outcome(
    outcome: Optional[str],
    *,
    execution_id: Optional[str] = None,
    session_id: Optional[str] = None,
    status: Optional[int] = None,
    endpoint: Optional[str] = None,
    attempts: Optional[int] = None,
) -> Optional[ZihinError]:
    """Convert a raw ``outcome`` (outside /stream: the sync webhook's ``outcome``) into a
    :class:`ZihinError`, or ``None`` when the accompanying text IS an answer."""
    label = safe_label(outcome)
    if not label:
        return None
    ctx: Dict[str, Any] = {
        "execution_id": safe_id(execution_id),
        "session_id": safe_id(session_id),
        "status": status,
        "endpoint": endpoint,
        "attempts": attempts,
    }
    if label == "TIMEOUT":
        return ZihinError("timeout", "The agent turn timed out on the server.", code="TURN_TIMEOUT", retryable=False, **ctx)
    failed = _FAILED_OUTCOMES.get(label)
    if not failed:
        return None
    return ZihinError(failed["kind"], failed["message"], code=label, retryable=failed.get("retryable", False), **ctx)


_CLOCK_TEXT = {
    "turn": "the turn deadline",
    "first_signal": "the model provider did not start responding in time",
    "idle": "the model provider stream went silent",
    "call": "the model call took too long",
}


def error_from_done_event(
    event: StreamEvent,
    *,
    execution_id: Optional[str] = None,
    session_id: Optional[str] = None,
    blocked: Optional[str] = None,
) -> Optional[ZihinError]:
    """Convert the ``done`` of a turn that produced NO answer into a :class:`ZihinError`; ``None``
    when the content is an answer. The same rule ``invoke_agent`` applies, for ``stream_agent`` users.

    Why an error and not a result field: in these turns the server already emitted a ``response``
    with the engine's failure text. As a field, every caller would have to remember to check it, and
    the one that forgot would publish the failure as an answer. As an error, safe is the default.
    None is retryable by default (the turn may have run tools with side effects), except SESSION_BUSY.
    """
    meta: Dict[str, Any] = {"execution_id": execution_id, "session_id": session_id}
    blocked_reason = safe_label(blocked)
    if event.outcome == "TIMEOUT" or event.timed_out is True:
        clock = event.timeout_clock
        what = _CLOCK_TEXT.get(clock or "", "a server-side clock")
        return ZihinError(
            "timeout", f"The agent turn timed out ({what}).", code="TURN_TIMEOUT", retryable=False, timeout_clock=clock, **meta
        )
    if event.outcome == "PAUSED" and blocked_reason in _PAUSED_BLOCKS:
        return ZihinError("blocked", "The agent is paused or archived and did not answer.", code="PAUSED", retryable=False, **meta)
    if event.outcome == "ERROR" and blocked_reason:
        gate = _ERROR_BLOCKS.get(blocked_reason, {})
        return ZihinError(
            gate.get("kind", "blocked"),
            gate.get("message", f"The agent turn was blocked by the server ({blocked_reason})."),
            code=blocked_reason.upper(),
            retryable=gate.get("retryable", False),
            **meta,
        )
    failed = _FAILED_OUTCOMES.get(event.outcome or "")
    if not failed:
        return None
    return ZihinError(failed["kind"], failed["message"], code=event.outcome, retryable=failed.get("retryable", False), **meta)


# ---------------------------------------------------------------------------------------------
# Buffered invocation
# ---------------------------------------------------------------------------------------------


def invoke(t: Transport, p: InvokeParams) -> AgentResult:
    raw: List[StreamEvent] = []
    response: Optional[StreamEvent] = None
    metrics: Optional[StreamEvent] = None
    metadata: Optional[StreamEvent] = None
    metadata_session: Optional[str] = None
    minted_session: Optional[str] = None
    done: Optional[StreamEvent] = None
    blocked: Optional[str] = None
    accepted_fields: Optional[List[str]] = None
    ignored_fields: Optional[List[IgnoredField]] = None
    tokens: List[str] = []

    for event in stream_events(t, p):
        if p.include_raw:
            raw.append(event)
        if event.type == "error":
            # Terminal: the server closes the stream right after. Never fall back to tokens: what
            # arrived is the deliberation of a turn that DIED, not an answer.
            raise error_from_stream_event(
                event,
                execution_id=metadata.execution_id if metadata else None,
                session_id=_first(minted_session, metadata_session, p.session_id),
            )
        if event.type == "response":
            response = event
        elif event.type == "metrics":
            metrics = event
        elif event.type == "token":
            tokens.append(event.content or "")
        elif event.type == "metadata":
            metadata = event
            if event.session_id:
                metadata_session = event.session_id
            accepted_fields = event.accepted_fields
            ignored_fields = event.ignored_fields
        elif event.type == "session" and event.session_id:
            minted_session = event.session_id
        elif event.type == "done":
            done = event
        elif event.type == "status" and event.blocked:
            blocked = event.blocked

    session_id = _first(minted_session, response.session_id if response else None, metadata_session, p.session_id)
    # `metadata` carries the execution_id since 2026-10-17, and `metrics` does not come on a replay
    # nor when the turn dies before it; `done`/`response` echo it too.
    execution_id = _first(
        metadata.execution_id if metadata else None,
        done.execution_id if done else None,
        response.execution_id if response else None,
        metrics.execution_id if metrics else None,
    )

    def pick(attr: str, *events: Optional[StreamEvent]) -> Any:
        return _first(*(getattr(e, attr) if e else None for e in events))

    ids = {
        "client_message_id": pick("client_message_id", done, response, metadata),
        "idempotent_replay": pick("idempotent_replay", done, response, metadata),
        "user_message_id": pick("user_message_id", done, response),
        "assistant_message_id": pick("assistant_message_id", done, response),
    }
    # Before looking at `response`: in a TIMEOUT/ERROR/blocked turn it exists, but its content is the
    # engine's failure or refusal, not an answer.
    if done is not None:
        outcome_error = error_from_done_event(done, execution_id=execution_id, session_id=session_id, blocked=blocked)
        if outcome_error:
            raise outcome_error
    # The server's idempotency scope is key + consumer, NOT the session, and the fingerprint is only
    # message + attachments: the same client_message_id reused in another conversation returns the
    # answer (and session_id) of the ORIGINAL conversation, and the caller would go on in the wrong
    # one. It is a caller bug (fixed/reused id), not an answer. The server lowercases the session_id
    # and the replay returns that form: compare case-insensitively, or a legitimate retry with an
    # uppercase UUID would be stuck in this error for 24 h.
    replay_session = _first(response.session_id if response else None, metadata_session)
    if ids["idempotent_replay"] and p.session_id and replay_session and replay_session.lower() != p.session_id.lower():
        raise ZihinError(
            "config",
            "This `client_message_id` was already used in a different session: the server replayed that "
            "session's answer. Use a new UUID per message (never a fixed value).",
            code="IDEMPOTENT_REPLAY_OTHER_SESSION",
            execution_id=execution_id,
            session_id=p.session_id,
            retryable=False,
        )

    common = {
        "session_id": session_id,
        "model": metrics.model if metrics else None,
        "usage": metrics.usage if metrics else None,
        "iterations": metrics.iterations if metrics else None,
        "execution_time_ms": metrics.execution_time_ms if metrics else None,
        "execution_id": execution_id,
        "accepted_fields": accepted_fields,
        "ignored_fields": ignored_fields,
        "raw": raw,
        **ids,
    }

    if response is None:
        # `done` without `response`: the stream ended CLEANLY and the server, on purpose, sent no
        # answer. Returning the token buffer would deliver deliberation as the answer it chose not to give.
        if done is not None:
            raise ZihinError("protocol", "Stream ended (done) without a `response` event.", execution_id=execution_id, session_id=session_id)
        # Stream CUT before the final event (no `done`). The token buffer is the ONLY thing left, so
        # it is returned, marked as `tokens`: it is the raw concatenation of every iteration's
        # deltas, deliberation included. Whoever publishes externally must check `content_source`.
        if tokens and "".join(tokens):
            return AgentResult(content="".join(tokens), content_source="tokens", **common)
        raise ZihinError("protocol", "Stream ended without a `response` event.", execution_id=execution_id, session_id=session_id)

    # The `response` event IS the text the server decided to deliver. An empty content is the
    # server's decision, not a gap to fill: replacing it with tokens would swap the answer for
    # the deliberation.
    return AgentResult(
        content=response.content or "",
        content_source="response",
        sources=response.sources,
        blocks=response.blocks,
        suggestions=response.suggestions,
        gated=response.gated,
        outcome=done.outcome if done else None,
        truncated=done.truncated if done else None,
        finish_reason=done.finish_reason if done else None,
        **common,
    )
