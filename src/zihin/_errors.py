"""Error type raised by the Zihin client (port of ``@zihin/agent-client`` ``errors.ts``).

Errors are sanitized (no API key, Bearer token or stack trace leaks) but COMPLETE: they carry the
endpoint, the number of attempts and the server's request_id for debugging in production.
"""

from __future__ import annotations

import socket
import ssl
from typing import Any, Dict, Optional

from ._security import safe_id, sanitize_api_error
from ._types import ErrorKind


class ZihinError(Exception):
    """Raised for every failure of a Zihin API call.

    Branch on ``kind`` (and ``code`` for the server's exact reason), never on the message:

    - ``config``: invalid client configuration or arguments, or a field the server did not apply
    - ``auth``: 401/403, the key was rejected or its role does not allow the action
    - ``trial_expired``: the workspace trial has ended (the key is fine)
    - ``no_subscription``: the workspace has no active subscription (the key is fine)
    - ``quota_exceeded``: the token quota is exhausted (retrying does not help)
    - ``not_found``: the agent, trigger or execution does not exist or is not accessible
    - ``rate_limit``: too many requests
    - ``timeout``: the request, the stream or the agent turn timed out
    - ``aborted``: cancelled by the caller (reserved for parity; the sync client does not raise it)
    - ``server``: the Zihin server or the agent turn failed
    - ``network``: the connection failed
    - ``protocol``: the response was not what the API contract describes
    - ``blocked``: the turn was refused (spending cap, busy session, paused agent, provider refusal,
      context limit, plan limit)
    - ``unknown``: an unexpected HTTP status
    """

    def __init__(
        self,
        kind: ErrorKind,
        message: str,
        *,
        status: Optional[int] = None,
        code: Optional[str] = None,
        retryable: Optional[bool] = None,
        request_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        session_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        attempts: Optional[int] = None,
        timeout_clock: Optional[str] = None,
    ) -> None:
        full = _build_message(message, endpoint, attempts, request_id, execution_id)
        super().__init__(full)
        self.kind: ErrorKind = kind
        self.message = full
        self.status = status
        self.code = code
        self.request_id = request_id
        self.execution_id = execution_id
        self.session_id = session_id
        self.endpoint = endpoint
        self.attempts = attempts
        # Only on a `timeout` that came from the turn outcome (`done.timeout_clock`): which server
        # clock ended the turn (`turn`, `first_signal`, `idle` or `call`).
        self.timeout_clock = timeout_clock
        self.retryable: bool = (
            retryable if retryable is not None else kind in ("rate_limit", "server", "network")
        )

    def __repr__(self) -> str:
        return (
            f"ZihinError(kind={self.kind!r}, code={self.code!r}, status={self.status!r}, "
            f"retryable={self.retryable!r}, message={self.message!r})"
        )


def _build_message(
    base: str,
    endpoint: Optional[str],
    attempts: Optional[int],
    request_id: Optional[str],
    execution_id: Optional[str],
) -> str:
    s = base
    if endpoint:
        s += f" [{endpoint}]"
    if attempts and attempts > 1:
        s += f" (after {attempts} attempts)"
    if request_id:
        s += f" (request_id: {request_id})"
    if execution_id:
        s += f" (execution_id: {execution_id})"
    return s


def _d(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _s(value: Any) -> Optional[str]:
    return value if isinstance(value, str) else None


# Commercial blocks (verified in the server's routes/triggers.js, 2026-08-31):
#   trial expired        -> 403 { error: 'TRIAL_EXPIRED' }                    (no `reason`)
#   inactive subscription-> 429 { error: 'QUOTA_EXCEEDED', reason: 'no_active_subscription' }
#   quota exhausted      -> 429 { error: 'QUOTA_EXCEEDED', reason: 'quota_exhausted' }
# None improves with time (retrying only burns attempts) and none is fixed by changing the key:
# flattening them into `auth` would send the user to change the one thing that is right.
_COMMERCIAL_BLOCK_CODES = {"TRIAL_EXPIRED", "QUOTA_EXCEEDED", "NO_ACTIVE_SUBSCRIPTION"}


def _error_code_of(body: Any) -> Optional[str]:
    b = _d(body)
    for candidate in (_s(b.get("error")), _s(_d(b.get("error")).get("code")), _s(b.get("code"))):
        if candidate is not None:
            return candidate
    return None


def is_commercial_block(body: Any) -> bool:
    """True when the body is a commercial block. Checked by the transport BEFORE retrying: the
    quota 429 is indistinguishable from a rate limit by status, only the body separates them."""
    code = _error_code_of(body)
    return code is not None and code in _COMMERCIAL_BLOCK_CODES


def _commercial_kind(body: Any) -> Optional[ErrorKind]:
    code = _error_code_of(body)
    if code == "TRIAL_EXPIRED":
        return "trial_expired"
    if code == "NO_ACTIVE_SUBSCRIPTION":
        return "no_subscription"
    if code == "QUOTA_EXCEEDED":
        # Same code for an inactive subscription and an exhausted quota; only `reason` separates them.
        return "no_subscription" if _s(_d(body).get("reason")) == "no_active_subscription" else "quota_exceeded"
    return None


def _extract_request_id(body: Any) -> Optional[str]:
    b = _d(body)
    # `meta.request_id` is the server's new canonical envelope; older routes still use the
    # top-level field. The shapes coexist.
    for candidate in (
        b.get("request_id"),
        b.get("requestId"),
        _d(b.get("error")).get("request_id"),
        _d(b.get("meta")).get("request_id"),
    ):
        sid = safe_id(candidate)
        if sid:
            return sid
    return None


def error_from_response(
    status: int, body: Any, *, endpoint: Optional[str] = None, attempts: Optional[int] = None
) -> ZihinError:
    """Map an HTTP status + parsed body to a complete :class:`ZihinError`."""
    message, code = sanitize_api_error(body)
    base: Dict[str, Any] = {
        "status": status,
        "code": code,
        "request_id": _extract_request_id(body),
        "execution_id": safe_id(_d(body).get("execution_id")),
        "endpoint": endpoint,
        "attempts": attempts,
    }

    # Before the status mapping: a commercial block arrives as 403 or 429 and would be flattened
    # into `auth` (change the key) or `rate_limit` (retryable), both the wrong advice.
    commercial = _commercial_kind(body)
    if commercial:
        return ZihinError(commercial, message or "Request blocked by your plan.", retryable=False, **base)

    # A feature outside the plan (e.g. a trigger type): 400 `PLAN_LIMIT_REACHED` is a commercial
    # block, not an invalid request.
    if code == "PLAN_LIMIT_REACHED":
        return ZihinError(
            "blocked", message or "This feature is not available on your Zihin plan.", retryable=False, **base
        )
    if status == 401:
        return ZihinError("auth", message or "Authentication failed.", **base)
    # The tenant gate answers 403 FORBIDDEN + `details.reason: 'AGENT_ACCESS_DENIED'` both for an agent
    # that does not exist and for one of another tenant/inactive, indistinguishable by design (no
    # existence oracle). The problem is the agent ID, not the key. `details` is NOT passed on: it
    # carries the tenant_id.
    if status == 403 and _s(_d(_d(_d(body).get("error")).get("details")).get("reason")) == "AGENT_ACCESS_DENIED":
        return ZihinError(
            "not_found",
            "Agent not found, inactive, or not accessible with this API key — check the agent ID and "
            "that the key belongs to the same Zihin workspace.",
            retryable=False,
            **base,
        )
    if status == 403:
        # Roles are not enumerated: the required role varies per route.
        return ZihinError(
            "auth",
            message
            or "This API key's role does not allow this action. The required role depends on the "
            "endpoint — check the key's role in Zihin → Settings → API Keys.",
            **base,
        )
    if status == 404:
        return ZihinError("not_found", message or "Resource not found.", **base)
    # 408 is what the server's global 30 s Express timeout returns. It is transient.
    if status == 408:
        return ZihinError("timeout", message or "The server timed out handling the request.", retryable=True, **base)
    if status == 429:
        return ZihinError("rate_limit", message or "Rate limit exceeded.", retryable=True, **base)
    if status >= 500:
        return ZihinError("server", message or f"Server error ({status}).", retryable=True, **base)
    return ZihinError("unknown", message or f"Request failed ({status}).", **base)


def _root_cause_is_permanent(exc: BaseException) -> bool:
    """DNS and TLS failures do not improve by retrying (TS: ENOTFOUND/EAI_AGAIN/CERT_/TLS)."""
    seen = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, (socket.gaierror, ssl.SSLError, ssl.CertificateError)):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def error_from_network_exception(
    exc: BaseException, *, endpoint: Optional[str] = None, attempts: Optional[int] = None
) -> ZihinError:
    """Normalize a transport failure (connection refused, DNS, TLS...) to a ``network`` error."""
    if isinstance(exc, ZihinError):
        return exc
    detail = sanitize_api_error(str(exc) or type(exc).__name__)[0]
    return ZihinError(
        "network",
        f"Network error: {detail}",
        endpoint=endpoint,
        attempts=attempts,
        retryable=not _root_cause_is_permanent(exc),
    )
