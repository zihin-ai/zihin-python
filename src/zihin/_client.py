"""Zihin client: list agents, invoke an agent, stream an agent turn."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import quote

import httpx

from ._errors import ZihinError, error_from_response
from ._sse import parse_sse

DEFAULT_BASE_URL = "https://llm.zihin.ai"
# The server has its own turn deadline; the stream timeout stays above it so the client
# receives the server's structured outcome instead of cutting the connection first.
DEFAULT_STREAM_TIMEOUT = 200.0
DEFAULT_TIMEOUT = 35.0
_KEY_PREFIXES = ("zhn_live_", "zhn_test_", "zhn_dev_")


@dataclass
class Agent:
    """An agent visible to the API key."""

    id: str
    name: Optional[str] = None
    commercial_name: Optional[str] = None
    bio: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class Usage:
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    cost_usd: Optional[float] = None


@dataclass
class StreamEvent:
    """One event of an agent turn.

    ``type`` is the server event name (``metadata``, ``status``, ``thinking``, ``tool``,
    ``token``, ``response``, ``metrics``, ``done``, ``error`` and others). ``data`` is the
    decoded payload. ``content`` is filled for ``token`` and ``response`` events.
    """

    type: str
    data: Any
    content: Optional[str] = None


@dataclass
class AgentResult:
    """The consolidated result of an agent turn."""

    content: str
    session_id: Optional[str] = None
    model: Optional[str] = None
    usage: Optional[Usage] = None
    execution_id: Optional[str] = None
    outcome: Optional[str] = None
    sources: Optional[List[Any]] = None


def _d(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _s(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def _n(value: Any) -> Any:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _usage(value: Any) -> Optional[Usage]:
    u = _d(value)
    if not u:
        return None
    return Usage(
        input_tokens=_n(u.get("input_tokens")) if _n(u.get("input_tokens")) is not None else _n(u.get("prompt_tokens")),
        output_tokens=_n(u.get("output_tokens"))
        if _n(u.get("output_tokens")) is not None
        else _n(u.get("completion_tokens")),
        total_tokens=_n(u.get("total_tokens")),
        cost_usd=_n(u.get("cost_usd")),
    )


class Zihin:
    """Client for the Zihin.ai agent API.

    ``api_key`` defaults to the ``ZIHIN_API_KEY`` environment variable. Use the client as a
    context manager, or call ``close()`` when done.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        stream_timeout: float = DEFAULT_STREAM_TIMEOUT,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        key = api_key if api_key is not None else os.environ.get("ZIHIN_API_KEY", "")
        if not key:
            raise ZihinError("config", "No API key. Pass api_key or set the ZIHIN_API_KEY environment variable.")
        if not key.startswith(_KEY_PREFIXES):
            raise ZihinError("config", "Invalid API key format. Zihin keys start with zhn_live_, zhn_test_ or zhn_dev_.")
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._stream_timeout = stream_timeout
        self._headers = {"Content-Type": "application/json", "Accept": "application/json", "X-Api-Key": key}
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client()

    def __enter__(self) -> "Zihin":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __repr__(self) -> str:
        # The API key is deliberately left out.
        return f"Zihin(base_url={self._base_url!r})"

    # -- agents ---------------------------------------------------------------------------

    def list_agents(self) -> List[Agent]:
        """List the agents accessible with the API key."""
        try:
            res = self._http.get(f"{self._base_url}/api/v2/agents", headers=self._headers, timeout=self._timeout)
        except httpx.TimeoutException as exc:
            raise ZihinError("timeout", "The request timed out.") from exc
        except httpx.HTTPError as exc:
            raise ZihinError("network", f"Could not reach the Zihin server: {exc}") from exc
        body = self._json_or_none(res)
        if res.status_code >= 400:
            raise error_from_response(res.status_code, body)
        items = _d(body).get("data")
        if not isinstance(items, list):
            raise ZihinError("protocol", "Expected a list of agents from GET /api/v2/agents.", status=res.status_code)
        agents = []
        for item in items:
            a = _d(item)
            agents.append(
                Agent(
                    id=_s(a.get("id")) or "",
                    name=_s(a.get("name")),
                    commercial_name=_s(a.get("commercial_name")),
                    bio=_s(a.get("bio")),
                    type=_s(a.get("type")),
                    status=_s(a.get("status")),
                    tags=[t for t in a.get("tags") or [] if isinstance(t, str)],
                )
            )
        return agents

    # -- invocation -----------------------------------------------------------------------

    def stream_agent(self, agent_id: str, message: str, *, session_id: Optional[str] = None) -> Iterator[StreamEvent]:
        """Run one agent turn and yield its events as they arrive.

        Closing the iterator early closes the connection, which cancels the turn on the server.
        An ``error`` event is yielded like any other; ``invoke_agent`` is the method that turns
        it into an exception.
        """
        if not agent_id:
            raise ZihinError("config", "agent_id is required.")
        if not message:
            raise ZihinError("config", "message is required.")
        body: Dict[str, Any] = {"message": message}
        if session_id:
            body["session_id"] = session_id
        url = f"{self._base_url}/api/v2/agents/{quote(agent_id, safe='')}/stream"
        try:
            with self._http.stream(
                "POST", url, headers=self._headers, json=body, timeout=self._stream_timeout
            ) as res:
                if res.status_code >= 400:
                    res.read()
                    raise error_from_response(res.status_code, self._json_or_none(res))
                for name, data in parse_sse(res.iter_lines()):
                    payload = _d(data)
                    content = _s(payload.get("content")) if name in ("token", "response") else None
                    yield StreamEvent(type=name, data=data, content=content)
        except httpx.TimeoutException as exc:
            raise ZihinError("timeout", "The agent stream timed out on the client side.") from exc
        except httpx.HTTPError as exc:
            raise ZihinError("network", f"The connection to the Zihin server failed: {exc}") from exc

    def invoke_agent(self, agent_id: str, message: str, *, session_id: Optional[str] = None) -> AgentResult:
        """Run one agent turn and return the final answer.

        Pass the returned ``session_id`` to the next call to continue the conversation.
        """
        response: Optional[dict] = None
        metrics: dict = {}
        done: Optional[dict] = None
        meta_session: Optional[str] = None
        minted_session: Optional[str] = None

        for event in self.stream_agent(agent_id, message, session_id=session_id):
            payload = _d(event.data)
            if event.type == "error":
                err = _d(payload.get("error"))
                timed_out = err.get("timed_out") is True
                raise ZihinError(
                    "timeout" if timed_out else "agent",
                    _s(err.get("message")) or "The agent turn failed.",
                    code=_s(err.get("code")),
                    request_id=_s(err.get("request_id")),
                )
            if event.type == "response":
                response = payload
            elif event.type == "metrics":
                metrics = payload
            elif event.type == "metadata":
                meta_session = _s(payload.get("session_id")) or meta_session
            elif event.type == "session":
                minted_session = _s(payload.get("session_id")) or minted_session
            elif event.type == "done":
                done = payload

        resolved_session = (
            minted_session or _s(_d(response).get("session_id")) or meta_session or session_id
        )
        execution_id = _s(metrics.get("execution_id"))

        if done is not None and (done.get("outcome") == "TIMEOUT" or done.get("timed_out") is True):
            # The response event of a timed-out turn carries the failure, not an answer.
            raise ZihinError(
                "timeout",
                "The agent turn timed out on the server.",
                code="TURN_TIMEOUT",
                execution_id=execution_id,
                session_id=resolved_session,
            )
        if response is None:
            # Token events are the model's working text across iterations, not the answer the
            # server chose to deliver, so they are never returned in its place.
            raise ZihinError(
                "protocol",
                "The stream ended without a response event.",
                execution_id=execution_id,
                session_id=resolved_session,
            )

        model = _s(_d(metrics.get("model_config")).get("model")) or _s(metrics.get("model"))
        sources = response.get("sources")
        return AgentResult(
            content=_s(response.get("content")) or "",
            session_id=resolved_session,
            model=model,
            usage=_usage(metrics.get("usage")),
            execution_id=execution_id,
            outcome=_s(_d(done).get("outcome")),
            sources=sources if isinstance(sources, list) else None,
        )

    # -- internals ------------------------------------------------------------------------

    @staticmethod
    def _json_or_none(res: httpx.Response) -> Any:
        try:
            return res.json()
        except ValueError:
            return None
