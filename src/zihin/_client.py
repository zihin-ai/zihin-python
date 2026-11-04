"""Zihin client facade: agents, direct invocation (buffered/streaming) and webhook triggers."""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

import httpx

from . import _triggers
from ._errors import ZihinError, error_from_response
from ._http import DEFAULT_MAX_RETRIES, RetryInfo, Transport, normalize_timeout, parse_body
from ._invoke import AgentStream, InvokeParams, invoke, stream_events
from ._security import DEFAULT_BASE_URL, validate_api_key_format, validate_base_url
from ._sse import DEFAULT_MAX_EVENT_BYTES, DEFAULT_MAX_TOTAL_BYTES
from ._types import (
    Agent,
    AgentResult,
    Attachment,
    Capabilities,
    ContextValue,
    DispatchResult,
    ExecutionStatus,
    FindOrCreateResult,
    QueryExtraction,
    TriggerInfo,
    TriggerRef,
)


def _d(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _s(value: Any) -> Optional[str]:
    return value if isinstance(value, str) else None


class Zihin:
    """Client for the Zihin.ai agent API.

    ``api_key`` defaults to the ``ZIHIN_API_KEY`` environment variable. Use the client as a
    context manager, or call ``close()`` when done.

    ``base_url`` must be a Zihin domain unless ``allow_custom_base_url=True`` (an explicit toggle:
    it is never derived from ``base_url`` being set). ``allow_http=True`` allows ``http://`` (and
    localhost) for local development only.

    ``timeout`` (seconds) overrides the deadline of EVERY operation; leave it unset to use each
    operation's default (stream 200 s, trigger dispatch 260 s, short requests 35 s), each above the
    server's own budget so the server's structured outcome arrives before the client gives up.
    ``stream_timeout`` overrides only the agent stream.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        allow_custom_base_url: bool = False,
        allow_http: bool = False,
        timeout: Optional[float] = None,
        stream_timeout: Optional[float] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        max_stream_event_bytes: int = DEFAULT_MAX_EVENT_BYTES,
        max_stream_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
        on_retry: Optional[Callable[[RetryInfo], None]] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        key = api_key if api_key is not None else os.environ.get("ZIHIN_API_KEY", "")
        if not key:
            raise ZihinError("config", "No API key. Pass api_key or set the ZIHIN_API_KEY environment variable.")
        key_error = validate_api_key_format(key)
        if key_error:
            raise ZihinError("config", key_error)
        url_check = validate_base_url(
            base_url,
            allow_custom_domains=allow_custom_base_url is True,
            allow_http=allow_http is True,
            allow_localhost=allow_http is True,
        )
        if not url_check.valid:
            raise ZihinError("config", url_check.error or "Invalid base URL.")
        try:
            # httpx applies IDNA 2008, stricter than the check above: fail here, not on first use.
            httpx.URL(url_check.sanitized_url or DEFAULT_BASE_URL)
        except Exception as exc:  # httpx.InvalidURL, idna errors
            raise ZihinError("config", "Invalid URL format") from exc
        if not isinstance(max_retries, int) or isinstance(max_retries, bool) or max_retries < 0:
            raise ZihinError("config", "max_retries must be a non-negative integer.")
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client()
        self._t = Transport(
            self._http,
            api_key=key.strip(),
            base_url=url_check.sanitized_url or DEFAULT_BASE_URL,
            timeout=normalize_timeout(timeout, "timeout") if timeout is not None else None,
            stream_timeout=normalize_timeout(stream_timeout, "stream_timeout") if stream_timeout is not None else None,
            max_retries=max_retries,
            max_stream_event_bytes=max_stream_event_bytes,
            max_stream_total_bytes=max_stream_total_bytes,
            on_retry=on_retry,
        )

    def __enter__(self) -> "Zihin":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    @property
    def base_url(self) -> str:
        """Effective base URL (after SSRF validation)."""
        return self._t.base_url

    def __repr__(self) -> str:
        # The API key is deliberately left out.
        return f"Zihin(base_url={self._t.base_url!r})"

    # -- agents -------------------------------------------------------------------------------

    def list_agents(self) -> List[Agent]:
        """List the agents accessible with the API key."""
        status, text, endpoint, attempts = self._t.request_buffered(
            "GET", f"{self._t.base_url}/api/v2/agents", timeout=self._t.resolve_timeout("short"), policy="idempotent"
        )
        body = parse_body(text)
        if status >= 400:
            raise error_from_response(status, body, endpoint=endpoint, attempts=attempts)
        items = _d(body).get("data")
        if not isinstance(items, list):
            raise ZihinError("protocol", "Expected a list of agents from GET /api/v2/agents.", status=status, endpoint=endpoint)
        agents = []
        for item in items:
            a = _d(item)
            tools_count = a.get("tools_count")
            agents.append(
                Agent(
                    id=_s(a.get("id")) or "",
                    name=_s(a.get("name")),
                    commercial_name=_s(a.get("commercial_name")),
                    bio=_s(a.get("bio")),
                    type=_s(a.get("type")),
                    status=_s(a.get("status")),
                    tags=[t for t in a.get("tags") or [] if isinstance(t, str)] if isinstance(a.get("tags"), list) else [],
                    tools_count=tools_count if isinstance(tools_count, int) and not isinstance(tools_count, bool) else None,
                )
            )
        return agents

    # -- invocation ---------------------------------------------------------------------------

    def stream_agent(
        self,
        agent_id: str,
        message: Optional[str] = None,
        *,
        session_id: Optional[str] = None,
        client_message_id: Optional[str] = None,
        consumer_key: Optional[str] = None,
        consumer_name: Optional[str] = None,
        context: Optional[Mapping[str, ContextValue]] = None,
        bindings: Optional[Mapping[str, ContextValue]] = None,
        session_from: Optional[Sequence[str]] = None,
        capabilities: Optional[Capabilities] = None,
        attachments: Optional[Sequence[Attachment]] = None,
        timeout_ms: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> AgentStream:
        """Run one agent turn and yield its normalized events as they arrive.

        Arguments are validated immediately; the connection opens on the first iteration. Use it
        as a context manager (``with zihin.stream_agent(...) as events:``) or call
        ``events.close()``: closing closes the connection, which cancels the turn on the server. An
        ``error`` event is yielded like any other; use :func:`zihin.error_from_stream_event` and
        :func:`zihin.error_from_done_event` to apply the same rules as :meth:`invoke_agent`.
        The capability check (fields the server did not accept) raises ``ZihinError('config')``
        and closes the stream. See :meth:`invoke_agent` for the arguments.
        """
        return stream_events(
            self._t,
            InvokeParams(
                agent_id=agent_id,
                message=message,
                session_id=session_id,
                client_message_id=client_message_id,
                consumer_key=consumer_key,
                consumer_name=consumer_name,
                context=context,
                bindings=bindings,
                session_from=session_from,
                capabilities=capabilities,
                attachments=attachments,
                timeout_ms=timeout_ms,
                metadata=metadata,
            ),
        )

    def invoke_agent(
        self,
        agent_id: str,
        message: Optional[str] = None,
        *,
        session_id: Optional[str] = None,
        client_message_id: Optional[str] = None,
        consumer_key: Optional[str] = None,
        consumer_name: Optional[str] = None,
        context: Optional[Mapping[str, ContextValue]] = None,
        bindings: Optional[Mapping[str, ContextValue]] = None,
        session_from: Optional[Sequence[str]] = None,
        capabilities: Optional[Capabilities] = None,
        attachments: Optional[Sequence[Attachment]] = None,
        timeout_ms: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        include_raw: bool = False,
    ) -> AgentResult:
        """Run one agent turn and return the consolidated result.

        - ``session_id``: continue a conversation (pass the returned ``session_id``).
        - ``client_message_id``: idempotency key, a NEW UUID per message (resend the SAME one to
          retry). Enables retrying the POST on network errors/5xx before the stream opens.
        - ``consumer_key`` / ``consumer_name``: who the person is (admin/owner key only).
        - ``context``: flat dict the MODEL reads (up to 50 fields, 2000 chars as JSON).
        - ``bindings``: flat dict only the TOOLS receive (up to 50 fields, 16000 chars as JSON).
        - ``session_from``: derive the session id from these 1-5 field names of context/bindings.
        - ``capabilities``: what your UI renders, e.g. ``{"blocks": ["quick_replies"]}``.
        - ``timeout_ms``: turn ceiling sent to the server as ``options.timeout_ms``; the local
          deadline is 30 s above it.
        - ``include_raw``: keep every stream event in ``AgentResult.raw``.

        Turns whose content is not an answer (``TIMEOUT``, ``ERROR``, ``REFUSED``,
        ``CONTEXT_LIMIT``, blocks...) raise :class:`ZihinError` instead of returning that text.
        """
        return invoke(
            self._t,
            InvokeParams(
                agent_id=agent_id,
                message=message,
                session_id=session_id,
                client_message_id=client_message_id,
                consumer_key=consumer_key,
                consumer_name=consumer_name,
                context=context,
                bindings=bindings,
                session_from=session_from,
                capabilities=capabilities,
                attachments=attachments,
                timeout_ms=timeout_ms,
                metadata=metadata,
                include_raw=include_raw,
            ),
        )

    # -- triggers -----------------------------------------------------------------------------

    def create_trigger(
        self,
        agent_id: str,
        name: str,
        *,
        mode: str = "async",
        callback_url: Optional[str] = None,
        callback_method: str = "POST",
        query_field: Optional[str] = None,
        query_extraction: Optional[QueryExtraction] = None,
        dispatch_auth: str = "api_key",
        response_transform: Optional[str] = None,
    ) -> TriggerRef:
        """Provision (once) a webhook trigger bound to an agent. ``mode`` is ``async`` (202 +
        callback, ``callback_url`` required) or ``sync`` (200 + JSON). Admin/editor key."""
        return _triggers.create_trigger(
            self._t,
            agent_id,
            name,
            mode=mode,
            callback_url=callback_url,
            callback_method=callback_method,
            query_field=query_field,
            query_extraction=query_extraction,
            dispatch_auth=dispatch_auth,
            response_transform=response_transform,
        )

    def find_or_create_trigger(
        self,
        agent_id: str,
        name: str,
        *,
        mode: str = "async",
        callback_url: Optional[str] = None,
        callback_method: str = "POST",
        query_field: Optional[str] = None,
        query_extraction: Optional[QueryExtraction] = None,
        dispatch_auth: str = "api_key",
        response_transform: Optional[str] = None,
    ) -> FindOrCreateResult:
        """Reuse the trigger with this agent + name (as it is), or create it."""
        return _triggers.find_or_create_trigger(
            self._t,
            agent_id,
            name,
            mode=mode,
            callback_url=callback_url,
            callback_method=callback_method,
            query_field=query_field,
            query_extraction=query_extraction,
            dispatch_auth=dispatch_auth,
            response_transform=response_transform,
        )

    def list_triggers(
        self, *, agent_id: Optional[str] = None, trigger_type: Optional[str] = None, enabled_only: bool = False
    ) -> List[TriggerInfo]:
        """List the workspace's triggers, optionally filtered."""
        return _triggers.list_triggers(self._t, agent_id=agent_id, trigger_type=trigger_type, enabled_only=enabled_only)

    def get_trigger(self, trigger_id: str) -> TriggerInfo:
        """Get one trigger (with its webhook URL)."""
        return _triggers.get_trigger(self._t, trigger_id)

    def update_trigger(
        self,
        trigger_id: str,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        enabled: Optional[bool] = None,
        query_extraction: Optional[QueryExtraction] = None,
        callback_url: Optional[str] = None,
        callback_method: Optional[str] = None,
        mode: Optional[str] = None,
        response_transform: Optional[str] = None,
    ) -> TriggerInfo:
        """Update a trigger; config changes are merged into the current ``trigger_config``."""
        return _triggers.update_trigger(
            self._t,
            trigger_id,
            name=name,
            description=description,
            enabled=enabled,
            query_extraction=query_extraction,
            callback_url=callback_url,
            callback_method=callback_method,
            mode=mode,
            response_transform=response_transform,
        )

    def delete_trigger(self, trigger_id: str) -> bool:
        """Delete a trigger (admin/owner key)."""
        return _triggers.delete_trigger(self._t, trigger_id)

    def dispatch_agent(self, trigger_id: str, payload: Optional[Mapping[str, Any]] = None) -> DispatchResult:
        """Fire a trigger. A sync turn whose ``outcome`` is not an answer raises, even with
        ``success: true`` in the body."""
        return _triggers.dispatch_agent(self._t, trigger_id, payload)

    def get_execution(self, trigger_id: str, execution_id: str) -> ExecutionStatus:
        """Status/result of a trigger execution (polling alternative to the callback)."""
        return _triggers.get_execution(self._t, trigger_id, execution_id)
