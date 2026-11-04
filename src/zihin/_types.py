"""Public result types (port of the result shapes in ``@zihin/agent-client`` ``types.ts``)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from typing import Literal, TypedDict

ErrorKind = Literal[
    "config",
    "auth",
    "trial_expired",
    "no_subscription",
    "quota_exceeded",
    "not_found",
    "rate_limit",
    "timeout",
    "aborted",
    "server",
    "network",
    "protocol",
    "blocked",
    "unknown",
]

#: Known turn outcomes (``done.outcome``). The server may add new ones, so result fields are typed
#: ``Optional[str]``; compare against these values.
AgentOutcome = Literal[
    "COMPLETED",
    "LENGTH",
    "TIMEOUT",
    "ERROR",
    "CANCELLED",
    "PAUSED",
    "SUSPENDED",
    "BUDGET_EXCEEDED",
    "QUOTA_EXCEEDED",
    "SESSION_BUSY",
    "CONFLICT",
    "REFUSED",
    "CONTEXT_LIMIT",
]

#: Which server clock ended a timed-out turn.
TimeoutClock = Literal["turn", "first_signal", "idle", "call"]

ContentSource = Literal["response", "tokens"]

#: Normalized stream event types. Unrecognized server events arrive as ``unknown``.
StreamEventType = Literal[
    "metadata",
    "status",
    "attachments",
    "phase",
    "thinking",
    "tool",
    "action_proposed",
    "action_forwarded",
    "token",
    "warning",
    "session",
    "done",
    "error",
    "response",
    "metrics",
    "unknown",
]

#: A value accepted in ``context``/``bindings``: flat and scalar.
ContextValue = Union[str, int, float, bool, None]


class Capabilities(TypedDict, total=False):
    """What your interface can render, e.g. ``{"blocks": ["quick_replies"]}``."""

    blocks: List[str]


class Attachment(TypedDict, total=False):
    url: str
    type: str


class QueryExtraction(TypedDict, total=False):
    """How a webhook trigger extracts the agent message from the dispatch payload.

    ``{"mode": "field", "field": "message"}``, ``{"mode": "template", "template": "..."}`` or
    ``{"mode": "full_body"}``.
    """

    mode: Literal["field", "template", "full_body"]
    field: str
    template: str


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
    tools_count: Optional[int] = None


@dataclass
class Usage:
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    cost_usd: Optional[float] = None


@dataclass
class IgnoredField:
    """A request field the server received and did NOT apply (``metadata.ignored_fields``)."""

    field: str
    reason: Optional[str] = None


@dataclass
class StreamEvent:
    """One normalized event of an agent turn.

    ``type`` is the normalized event type (``unknown`` for events this client does not know; the
    raw name is in ``event``). ``data`` is the decoded payload. The other attributes are filled
    only for the event types that carry them; ids coming from the server are sanitized.
    """

    type: str
    data: Any
    event: str = ""
    content: Optional[str] = None
    # metadata
    request_id: Optional[str] = None
    agent_id: Optional[str] = None
    session_id: Optional[str] = None
    accepted_fields: Optional[List[str]] = None
    ignored_fields: Optional[List[IgnoredField]] = None
    # turn ids (metadata/response/done)
    execution_id: Optional[str] = None
    client_message_id: Optional[str] = None
    idempotent_replay: Optional[bool] = None
    user_message_id: Optional[str] = None
    assistant_message_id: Optional[str] = None
    # status / phase
    phase: Optional[str] = None
    blocked: Optional[str] = None
    # thinking
    thought: Optional[str] = None
    # tool
    tool: Optional[str] = None
    call_id: Optional[str] = None
    success: Optional[bool] = None
    # action_forwarded
    action_id: Optional[str] = None
    # warning / error
    message: Optional[str] = None
    code: Optional[str] = None
    timed_out: Optional[bool] = None
    # done
    outcome: Optional[str] = None
    timeout_clock: Optional[str] = None
    truncated: Optional[bool] = None
    finish_reason: Optional[str] = None
    # response
    gated: Optional[bool] = None
    sources: Optional[List[Any]] = None
    blocks: Optional[List[Dict[str, Any]]] = None
    suggestions: Optional[List[str]] = None
    # metrics
    usage: Optional[Usage] = None
    model: Optional[str] = None
    iterations: Optional[int] = None
    execution_time_ms: Optional[float] = None


@dataclass
class AgentResult:
    """The consolidated result of an agent turn.

    ``content_source`` says where ``content`` came from: ``response`` is the server's final event,
    the text that is safe to publish; ``tokens`` means the stream ended without it and ``content``
    is the raw concatenation of the deltas of every iteration, internal deliberation included.
    Treat ``tokens`` as a diagnostic partial, never publish it to an external channel unreviewed.
    """

    content: str
    content_source: ContentSource = "response"
    session_id: Optional[str] = None
    model: Optional[str] = None
    usage: Optional[Usage] = None
    sources: Optional[List[Any]] = None
    blocks: Optional[List[Dict[str, Any]]] = None
    suggestions: Optional[List[str]] = None
    #: True when ``content`` is a canned platform-gate answer (human handoff, denylist, suspended
    #: session): no model ran.
    gated: Optional[bool] = None
    iterations: Optional[int] = None
    execution_time_ms: Optional[float] = None
    execution_id: Optional[str] = None
    client_message_id: Optional[str] = None
    #: True when this is the STORED answer of an earlier turn with the same ``client_message_id``:
    #: the agent did not run again (no ``usage``/``model``/``iterations``).
    idempotent_replay: Optional[bool] = None
    user_message_id: Optional[str] = None
    assistant_message_id: Optional[str] = None
    #: Only outcomes whose content IS an answer reach here: ``COMPLETED``, ``LENGTH``, the
    #: ``PAUSED`` of a pending approval, and new outcomes. ``None`` = older server.
    outcome: Optional[str] = None
    truncated: Optional[bool] = None
    finish_reason: Optional[str] = None
    accepted_fields: Optional[List[str]] = None
    ignored_fields: Optional[List[IgnoredField]] = None
    #: Every stream event, only when ``include_raw=True``.
    raw: List[StreamEvent] = field(default_factory=list)


@dataclass
class TriggerRef:
    trigger_id: str
    #: Full dispatch URL: POST it.
    webhook_url: str


@dataclass
class FindOrCreateResult(TriggerRef):
    created: bool = False


@dataclass
class TriggerInfo:
    id: str
    agent_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    trigger_type: Optional[str] = None
    enabled: Optional[bool] = None
    trigger_count: Optional[int] = None
    last_error: Optional[str] = None
    webhook_url: Optional[str] = None


@dataclass
class DispatchResult:
    """Result of :meth:`Zihin.dispatch_agent`. A turn that FAILED never gets here: it raises."""

    accepted: bool
    trigger_id: Optional[str] = None
    agent_id: Optional[str] = None
    #: The TRIGGER execution id (``trigger_executions``), not the agent turn's execution id.
    execution_id: Optional[str] = None
    #: Present for ``sync`` triggers: the agent's immediate answer.
    agent_response: Optional[str] = None
    #: Sync outcome; only outcomes whose ``agent_response`` is an answer reach here.
    outcome: Optional[str] = None
    usage: Optional[Usage] = None
    iterations: Optional[int] = None
    latency_ms: Optional[float] = None
    #: The text the server extracted from the payload (exposes a misconfigured query_extraction).
    query_extracted: Optional[str] = None
    raw: Any = None


@dataclass
class ExecutionStatus:
    execution_id: str
    #: Raw server status. Not a closed enum: use ``terminal`` to decide when to stop polling.
    status: str
    #: False only while the execution is in progress; an unknown status counts as terminal.
    terminal: bool
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    execution_time_ms: Optional[float] = None
    usage: Optional[Usage] = None
    result: Optional[str] = None
    raw: Any = None
