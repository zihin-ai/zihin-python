"""Error type raised by the Zihin client."""

from __future__ import annotations

from typing import Any, Optional


class ZihinError(Exception):
    """Raised for every failure of a Zihin API call.

    ``kind`` says what went wrong, so callers can branch without parsing messages:

    - ``config``: invalid client configuration or arguments
    - ``auth``: the API key was rejected or its role does not allow the action
    - ``not_found``: the agent does not exist or is not accessible with this key
    - ``rate_limit``: too many requests
    - ``timeout``: the request or the agent turn timed out
    - ``server``: the Zihin server failed (5xx)
    - ``network``: the connection failed
    - ``protocol``: the response was not what the API contract describes
    - ``agent``: the agent turn ended with an error reported by the server
    """

    def __init__(
        self,
        kind: str,
        message: str,
        *,
        status: Optional[int] = None,
        code: Optional[str] = None,
        request_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.status = status
        self.code = code
        self.request_id = request_id
        self.execution_id = execution_id
        self.session_id = session_id

    def __repr__(self) -> str:
        return f"ZihinError(kind={self.kind!r}, message={self.message!r}, status={self.status!r}, code={self.code!r})"


def _as_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _as_str(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def error_from_response(status: int, body: Any) -> ZihinError:
    """Map an HTTP error response to a ``ZihinError``."""
    root = _as_dict(body)
    err = _as_dict(root.get("error"))
    message = _as_str(err.get("message")) or _as_str(root.get("message"))
    code = _as_str(err.get("code")) or _as_str(root.get("code"))
    meta = {
        "status": status,
        "code": code,
        "request_id": _as_str(err.get("request_id")) or _as_str(root.get("request_id")),
        "execution_id": _as_str(root.get("execution_id")),
    }

    if status == 401:
        return ZihinError("auth", message or "Authentication failed. Check the API key.", **meta)
    if status == 403:
        # The tenant gate answers 403 for an agent that does not exist as well as for one that
        # belongs to another workspace, so the useful advice is about the agent ID, not the key.
        if _as_str(_as_dict(err.get("details")).get("reason")) == "AGENT_ACCESS_DENIED":
            return ZihinError(
                "not_found",
                "Agent not found, inactive, or not accessible with this API key. "
                "Check the agent ID and that the key belongs to the same Zihin workspace.",
                **meta,
            )
        return ZihinError("auth", message or "This API key's role does not allow this action.", **meta)
    if status == 404:
        return ZihinError("not_found", message or "Not found.", **meta)
    if status == 429:
        return ZihinError("rate_limit", message or "Too many requests.", **meta)
    if status >= 500:
        return ZihinError("server", message or f"Zihin server error ({status}).", **meta)
    return ZihinError("protocol", message or f"Unexpected HTTP status {status}.", **meta)
