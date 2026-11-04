"""Async/decoupled invocation through webhook triggers (port of ``triggers.ts``).

- ``create_trigger``: provision (once) a webhook bound to an agent (POST /api/triggers).
- ``dispatch_agent``: fire the trigger (POST /api/triggers/webhook/:id), 202 (async) or JSON (sync).
- ``get_execution``: read the result (GET /api/triggers/:id/executions/:exec_id).

Requires an admin/editor key (triggers:create / telemetry:read); ``member`` keys get 403.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import quote, urlencode

from ._errors import ZihinError, error_from_response
from ._http import Transport, parse_body, to_json_body
from ._invoke import error_from_outcome, parse_usage
from ._security import safe_id, safe_label, sanitize_message, validate_base_url
from ._types import DispatchResult, ExecutionStatus, FindOrCreateResult, TriggerInfo, TriggerRef


def _d(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _s(value: Any) -> Optional[str]:
    return value if isinstance(value, str) else None


def _n(value: Any) -> Any:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _q(value: str) -> str:
    return quote(value, safe="")


#: Statuses that mean "still running". The NON-terminal ones are listed on purpose: any new value
#: the server starts recording is treated as terminal, and polling STOPS instead of spinning forever.
_RUNNING_STATUSES = {
    "running", "pending", "queued", "processing", "in_progress", "waiting", "retrying", "scheduled",
    "awaiting_approval",  # HITL: a turn paused waiting for a human decision has not finished
    "paused", "awaiting_input",
}


def is_terminal_status(status: str) -> bool:
    """Hyphen/case normalized: ``in-progress`` and ``IN_PROGRESS`` are the same in-flight state."""
    return status.lower().replace("-", "_") not in _RUNNING_STATUSES


def _query_extraction_body(extraction: Optional[Mapping[str, Any]], query_field: Optional[str]) -> Dict[str, Any]:
    """``query_extraction`` of the trigger_config. ``query_extraction`` wins; ``query_field`` is the
    legacy sugar (equivalent to mode=field)."""
    if extraction is None:
        return {"mode": "field", "field": query_field or "message"}
    mode = _d(extraction).get("mode")
    if mode == "field":
        if not _d(extraction).get("field"):
            raise ZihinError("config", 'query_extraction["field"] is required for mode "field".')
        return {"mode": "field", "field": extraction["field"]}
    if mode == "template":
        if not _d(extraction).get("template"):
            raise ZihinError("config", 'query_extraction["template"] is required for mode "template".')
        return {"mode": "template", "template": extraction["template"]}
    if mode == "full_body":
        return {"mode": "full_body"}
    raise ZihinError("config", "Unknown query_extraction mode (use field|template|full_body).")


def _check_callback_url(url: Optional[str]) -> None:
    # The callback URL is registered on the server, which calls it: HTTPS + no private IP.
    if url:
        check = validate_base_url(url, allow_custom_domains=True)
        if not check.valid:
            raise ZihinError("config", f"Invalid callback_url: {check.error}")


def create_trigger(
    t: Transport,
    agent_id: str,
    name: str,
    *,
    mode: str = "async",
    callback_url: Optional[str] = None,
    callback_method: str = "POST",
    query_field: Optional[str] = None,
    query_extraction: Optional[Mapping[str, Any]] = None,
    dispatch_auth: str = "api_key",
    response_transform: Optional[str] = None,
) -> TriggerRef:
    if not agent_id:
        raise ZihinError("config", "agent_id is required.")
    if not name:
        raise ZihinError("config", "name is required.")
    if mode == "async" and not callback_url:
        raise ZihinError("config", "callback_url is required for async triggers.")
    _check_callback_url(callback_url)
    # The server reads the response mode from INSIDE the callback
    # (`trigger_config.execution.callback.response_transform.mode`, also on the SYNC path), and its
    # validator requires `callback.url` whenever `callback` exists. Without callback_url the mode
    # cannot be declared, so fail here with the explanation.
    if response_transform and not callback_url:
        raise ZihinError(
            "config",
            "response_transform requires callback_url: the backend only reads it from "
            "trigger_config.execution.callback.response_transform, and a callback without a url is rejected.",
        )
    execution: Dict[str, Any] = {"mode": mode}
    if callback_url:
        callback: Dict[str, Any] = {"url": callback_url, "method": callback_method or "POST"}
        if response_transform:
            callback["response_transform"] = {"mode": response_transform}
        execution["callback"] = callback
    body = {
        "agent_id": agent_id,
        "name": name,
        "trigger_type": "webhook",
        "trigger_config": {
            "query_extraction": _query_extraction_body(query_extraction, query_field),
            "auth": {"type": dispatch_auth or "api_key"},
            "execution": execution,
        },
    }
    j = _d(t.request_json("POST", "/api/triggers", body=body, policy="side-effecting"))
    trigger_id = _s(_d(j.get("data")).get("id")) or _s(j.get("id"))
    webhook_url = _s(j.get("webhook_url")) or (f"{t.base_url}/api/triggers/webhook/{trigger_id}" if trigger_id else None)
    if not trigger_id or not webhook_url:
        raise ZihinError("protocol", "create_trigger: missing trigger id / webhook url in response.")
    return TriggerRef(trigger_id=trigger_id, webhook_url=webhook_url)


def _webhook_url_for(t: Transport, raw: Dict[str, Any]) -> Optional[str]:
    url = _s(raw.get("webhook_url"))
    if url:
        return url
    tid = _s(raw.get("id"))
    return f"{t.base_url}/api/triggers/webhook/{tid}" if raw.get("trigger_type") == "webhook" and tid else None


def _trigger_info(t: Transport, raw: Any) -> TriggerInfo:
    r = _d(raw)
    return TriggerInfo(
        id=_s(r.get("id")) or "",
        agent_id=_s(r.get("agent_id")),
        name=_s(r.get("name")),
        description=_s(r.get("description")),
        trigger_type=_s(r.get("trigger_type")),
        enabled=r.get("enabled") if isinstance(r.get("enabled"), bool) else None,
        trigger_count=_n(r.get("trigger_count")),
        last_error=_s(r.get("last_error")),
        webhook_url=_webhook_url_for(t, r),
    )


def list_triggers(
    t: Transport, *, agent_id: Optional[str] = None, trigger_type: Optional[str] = None, enabled_only: bool = False
) -> List[TriggerInfo]:
    qs: Dict[str, str] = {}
    if agent_id:
        qs["agentId"] = agent_id
    if trigger_type:
        qs["triggerType"] = trigger_type
    if enabled_only:
        qs["enabledOnly"] = "true"
    path = "/api/triggers" + (f"?{urlencode(qs)}" if qs else "")
    j = t.request_json("GET", path, policy="idempotent")
    items = j if isinstance(j, list) else _d(j).get("data")
    return [_trigger_info(t, x) for x in (items if isinstance(items, list) else [])]


def get_trigger(t: Transport, trigger_id: str) -> TriggerInfo:
    if not trigger_id:
        raise ZihinError("config", "trigger_id is required.")
    j = t.request_json("GET", f"/api/triggers/{_q(trigger_id)}", policy="idempotent")
    data = _d(j).get("data")
    return _trigger_info(t, data if data is not None else j)


def update_trigger(
    t: Transport,
    trigger_id: str,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    enabled: Optional[bool] = None,
    query_extraction: Optional[Mapping[str, Any]] = None,
    callback_url: Optional[str] = None,
    callback_method: Optional[str] = None,
    mode: Optional[str] = None,
    response_transform: Optional[str] = None,
) -> TriggerInfo:
    """PATCH /api/triggers/:id. The server REPLACES ``trigger_config`` entirely, so when a config
    field changes the current config is read (GET) and merged before the PATCH. A concurrent write
    in the GET->PATCH window can be overwritten; acceptable for integration reconfiguration."""
    if not trigger_id:
        raise ZihinError("config", "trigger_id is required.")
    _check_callback_url(callback_url)
    body: Dict[str, Any] = {}
    if name is not None:
        body["name"] = name
    if description is not None:
        body["description"] = description
    if enabled is not None:
        body["enabled"] = enabled

    touches_callback = callback_url is not None or callback_method is not None or response_transform is not None
    if query_extraction is not None or mode is not None or touches_callback:
        path = f"/api/triggers/{_q(trigger_id)}"
        current_json = _d(t.request_json("GET", path, policy="idempotent"))
        current = _d(current_json.get("data") if current_json.get("data") is not None else current_json)
        config = current.get("trigger_config")
        if not isinstance(config, dict) or not config:
            # Without the current config the PATCH would write a PARTIAL trigger_config and
            # silently erase auth/execution.
            raise ZihinError(
                "protocol", "update_trigger: current trigger_config unavailable — refusing to overwrite it partially."
            )
        merged = dict(config)
        if query_extraction is not None:
            merged["query_extraction"] = _query_extraction_body(query_extraction, None)
        if mode is not None or touches_callback:
            execution = _d(config.get("execution"))
            callback = _d(execution.get("callback"))
            new_execution = dict(execution)
            if mode is not None:
                new_execution["mode"] = mode
            if touches_callback:
                url = callback_url if callback_url is not None else _s(callback.get("url"))
                if not url:
                    raise ZihinError(
                        "config",
                        "response_transform requires a callback: the backend only reads it from "
                        "trigger_config.execution.callback.response_transform, and a callback without a url is rejected."
                        if response_transform
                        else "callback_url is required: this trigger has no callback configured yet.",
                    )
                # Preserve the current callback: changing only the URL must not erase auth/headers/
                # body_template/content_type/response_transform.
                new_callback = dict(callback)
                new_callback["url"] = url
                new_callback["method"] = callback_method or _s(callback.get("method")) or "POST"
                if response_transform:
                    new_callback["response_transform"] = {**_d(callback.get("response_transform")), "mode": response_transform}
                new_execution["callback"] = new_callback
            # Same invariant as create_trigger: async without a callback never delivers the result.
            if new_execution.get("mode") == "async" and not _d(new_execution.get("callback")).get("url"):
                raise ZihinError("config", "callback_url is required for async triggers.")
            merged["execution"] = new_execution
        body["trigger_config"] = merged

    if not body:
        raise ZihinError("config", "update_trigger: no fields to update.")
    # PATCH here is replay-safe: reapplying the same values produces the same state.
    j = t.request_json("PATCH", f"/api/triggers/{_q(trigger_id)}", body=body, policy="idempotent")
    data = _d(j).get("data")
    return _trigger_info(t, data if data is not None else j)


def delete_trigger(t: Transport, trigger_id: str) -> bool:
    """Delete a trigger (admin/owner key; editor gets 403). Returns whether it was deleted."""
    if not trigger_id:
        raise ZihinError("config", "trigger_id is required.")
    status, text, endpoint, attempts = t.request_buffered(
        "DELETE",
        f"{t.base_url}/api/triggers/{_q(trigger_id)}",
        timeout=t.resolve_timeout("short"),
        policy="idempotent",  # DELETE is idempotent
    )
    if status >= 400:
        raise error_from_response(status, parse_body(text), endpoint=endpoint, attempts=attempts)
    return _d(parse_body(text)).get("deleted") is True or status == 200


def find_or_create_trigger(t: Transport, agent_id: str, name: str, **kwargs: Any) -> FindOrCreateResult:
    """Reuse a trigger of the same agent + name, or create one (the server has no name
    uniqueness). A same-name trigger with a different config is REUSED as is."""
    if not agent_id:
        raise ZihinError("config", "agent_id is required.")
    if not name:
        raise ZihinError("config", "name is required.")
    match = next((x for x in list_triggers(t, agent_id=agent_id) if x.name == name), None)
    if match and match.id:
        url = match.webhook_url or f"{t.base_url}/api/triggers/webhook/{match.id}"
        return FindOrCreateResult(trigger_id=match.id, webhook_url=url, created=False)
    ref = create_trigger(t, agent_id, name, **kwargs)
    return FindOrCreateResult(trigger_id=ref.trigger_id, webhook_url=ref.webhook_url, created=True)


def dispatch_agent(t: Transport, trigger_id: str, payload: Optional[Mapping[str, Any]] = None) -> DispatchResult:
    """Fire a trigger. Async returns ``accepted`` + ``execution_id`` (202); sync also returns
    ``agent_response``/``usage``/``outcome`` (200)."""
    if not trigger_id:
        raise ZihinError("config", "trigger_id is required.")
    status, text, endpoint, attempts = t.request_buffered(
        "POST",
        f"{t.base_url}/api/triggers/webhook/{_q(trigger_id)}",
        body=to_json_body(dict(payload) if payload is not None else {}, "payload"),
        timeout=t.resolve_timeout("dispatch"),
        policy="side-effecting",  # never duplicates a dispatch on a network error
    )
    if status >= 400:
        raise error_from_response(status, parse_body(text), endpoint=endpoint, attempts=attempts)
    body = parse_body(text)
    j = _d(body)
    ctx: Dict[str, Any] = {"status": status, "endpoint": endpoint, "attempts": attempts, "execution_id": safe_id(j.get("execution_id"))}

    # The server answers HTTP 200 even when the TURN failed: `{success: false, error, agent_response:
    # null}`. `success: false` only comes from a pre-turn exception, and since 2026-10-14 carries
    # `outcome` (`CONFLICT`, `QUOTA_EXCEEDED`...). Classify by it first: flattening into `server`
    # would lose the commercial kind and CONFLICT would stop being `blocked`. A generic ERROR falls
    # into the branch below, which keeps the detail.
    pre_turn = safe_label(j.get("outcome"))
    if j.get("success") is False and pre_turn and pre_turn != "ERROR":
        classified = error_from_outcome(pre_turn, **ctx)
        if classified:
            raise classified
    if j.get("success") is False:
        # `error` is free text and may carry a failed tool's message (signed callback URL, echoed
        # key, stack): sanitize the STRING.
        detail = sanitize_message(_s(j.get("error")) or "")
        # Not transient: the turn ran and failed. Repeating would spend another execution.
        raise ZihinError("server", detail or "The agent run failed.", retryable=False, **ctx)

    # `success` still means only "delivered": REFUSED, CONTEXT_LIMIT, TIMEOUT, SESSION_BUSY... arrive
    # with `success: true` and `agent_response` = the platform's sentence. Same rule as /stream.
    outcome_error = error_from_outcome(_s(j.get("outcome")), **ctx)
    if outcome_error:
        raise outcome_error

    return DispatchResult(
        accepted=True,
        outcome=safe_label(j.get("outcome")),
        trigger_id=_s(j.get("trigger_id")) or trigger_id,
        agent_id=_s(j.get("agent_id")),
        execution_id=safe_id(j.get("execution_id")),
        agent_response=_s(j.get("agent_response")),
        usage=parse_usage(j.get("usage")),
        iterations=_n(j.get("iterations")),
        latency_ms=_n(j.get("latency_ms")),
        query_extracted=_s(j.get("query_extracted")),
        raw=body,
    )


def get_execution(t: Transport, trigger_id: str, execution_id: str) -> ExecutionStatus:
    """Status/result of a trigger execution; poll until ``terminal``."""
    if not trigger_id or not execution_id:
        raise ZihinError("config", "trigger_id and execution_id are required.")
    j = _d(t.request_json("GET", f"/api/triggers/{_q(trigger_id)}/executions/{_q(execution_id)}", policy="idempotent"))
    # The server nests under the `data` envelope: { success, data: { execution, metrics } }.
    data = _d(j.get("data"))
    exe = _d(data.get("execution") if data.get("execution") is not None else j.get("execution"))
    metrics = _d(data.get("metrics") if data.get("metrics") is not None else j.get("metrics"))
    # An ABSENT status is not an unknown one: absent means we could not read the envelope (or the row
    # has no status yet), so keep waiting. Only an unrecognized VALUE counts as terminal.
    raw_status = _s(exe.get("status"))
    usage_src = metrics.get("usage") if metrics.get("usage") is not None else metrics
    return ExecutionStatus(
        execution_id=_s(exe.get("id")) or execution_id,
        status=raw_status if raw_status is not None else "unknown",
        terminal=False if raw_status is None else is_terminal_status(raw_status),
        started_at=_s(exe.get("started_at")),
        finished_at=_s(exe.get("finished_at")),
        execution_time_ms=_n(exe.get("execution_time_ms")),
        usage=parse_usage(usage_src) or parse_usage(exe.get("usage")),
        result=_s(exe.get("agent_response")) if _s(exe.get("agent_response")) is not None else _s(exe.get("result")),
        raw=j,
    )
