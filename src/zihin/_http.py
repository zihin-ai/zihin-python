"""HTTP transport (port of ``@zihin/agent-client`` ``http.ts``).

Base URL validation (SSRF), auth headers, a deadline that covers the WHOLE request (headers and
body/stream), and retry with exponential backoff + jitter.

Retry policy (avoids duplicating side effects):

- ``idempotent`` (GET): retries on network errors, 429, 408 and 5xx.
- ``side-effecting`` (POST: invoke/dispatch/create): retries ONLY on 429/503 (the server said "try
  later" without processing). NEVER on a network error: the request may have been received and
  processed even though the socket dropped on the way back.
"""

from __future__ import annotations

import json
import math
import random
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, Iterator, Optional, Tuple
from urllib.parse import urlsplit

import httpx

from ._errors import ZihinError, error_from_network_exception, error_from_response, is_commercial_block

#: Default deadline PER OPERATION, in seconds. Each value sits ABOVE the server's budget for that
#: route, because the server was designed to deliver the outcome to the client: giving up before
#: it trades a structured error (TURN_TIMEOUT with execution_id/session_id) for a blind abort.
#:
#: - ``stream``: POST /api/v2/agents/:id/stream. The SSE closes at 180 s and the ``chat`` channel
#:   turn dies at 150 s. The stream outlives the turn ON PURPOSE, to deliver the outcome; 200 s
#:   lets that tail arrive.
#: - ``dispatch``: POST /api/triggers/webhook/:id. Outside the global Express timeout; the
#:   ``async`` channel turn is 240 s.
#: - ``short``: listing GETs and trigger CRUD. Global 30 s timeout, which answers 408.
OPERATION_TIMEOUT = {"stream": 200.0, "dispatch": 260.0, "short": 35.0}

#: Grace added to the caller's ``timeout_ms`` to form the LOCAL deadline. The same number goes to
#: the server as ``options.timeout_ms``; without a grace both clocks fire together and the server's
#: error, which has a request_id, loses the race to our abort. 30 s, not 10 s: the server's turn
#: clock only starts AFTER loading (config, tools, gates; a cold MCP takes tens of seconds).
DEADLINE_GRACE_MS = 30_000

#: Ceiling of the time spent on FAILED attempts and the backoff between them, separate from the
#: deadline of each attempt. Shrinking the deadline of the attempt that succeeds to "what is left of
#: the budget" would put it BELOW the server's 180 s SSE after a single ``Retry-After: 30``, and the
#: client would abort blind exactly where the server was about to deliver ``TURN_TIMEOUT``. So every
#: attempt gets the full timeout and it is the NUMBER of retries that this window limits.
RETRY_OVERHEAD = 60.0

#: Floor and ceiling of any external timeout (seconds).
MIN_TIMEOUT = 1.0
MAX_TIMEOUT = 900.0

DEFAULT_MAX_RETRIES = 2

#: Replaced in tests to avoid real sleeps / control the clock; always looked up at call time.
_sleep: Callable[[float], None] = time.sleep
_now: Callable[[], float] = time.monotonic


def normalize_timeout(value: Any, label: str, *, unit: float = 1.0) -> float:
    """Validate a timeout and clamp it to the usable range. ``unit`` converts to seconds
    (``unit=1000`` for milliseconds); the result is in the same unit as the input.

    Fails loudly at the input instead of aborting on its own mid-call (``0``, ``NaN`` or a huge
    number would otherwise invert the meaning of the setting).
    """
    if isinstance(value, bool):
        n = float("nan")
    else:
        try:
            n = float(value)
        except (TypeError, ValueError):
            n = float("nan")
    if not math.isfinite(n) or n <= 0:
        raise ZihinError("config", f"{label} must be a positive number (got {value!r}).")
    return float(min(max(n, MIN_TIMEOUT * unit), MAX_TIMEOUT * unit))


def endpoint_label(method: str, url: str) -> str:
    """``"POST host/path"`` for error messages and observability."""
    try:
        parts = urlsplit(url)
        host = parts.netloc.rsplit("@", 1)[-1]
        return f"{method} {host}{parts.path}"
    except ValueError:
        return method


def _retry_after(headers: httpx.Headers, cap: float = 30.0) -> Optional[float]:
    """``Retry-After`` (seconds or HTTP date) in seconds, capped."""
    raw = headers.get("retry-after")
    if not raw:
        return None
    try:
        secs = float(raw)
        if math.isfinite(secs):
            return min(max(0.0, secs), cap)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
        return min(max(0.0, when.timestamp() - time.time()), cap)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None


def _backoff(attempt: int) -> float:
    """Exponential backoff 1 s -> 10 s with jitter (half fixed, half random)."""
    base = min(1.0 * float(2**attempt), 10.0)
    return base / 2 + random.random() * (base / 2)


def _should_retry_status(status: int, policy: str) -> bool:
    if policy == "idempotent":
        return status >= 500 or status in (429, 408)
    return status in (429, 503)  # side-effecting: only "try later"


def parse_body(text: str) -> Any:
    """Parse a body safely: JSON when possible, otherwise ``{"message": text}``."""
    if not text:
        return {}
    try:
        return json.loads(text)
    except ValueError:
        return {"message": text}


def to_json_body(value: Any, label: str = "body") -> bytes:
    """Serialize a request body; a value that is not JSON-serializable is a ``config`` error."""
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ZihinError("config", f"{label} is not JSON-serializable: {exc}.") from exc


def _redirect_error(endpoint: str, attempts: int) -> ZihinError:
    # A 3xx to another host would leak the X-Api-Key, so redirects are never followed.
    return ZihinError(
        "network",
        "The server answered with a redirect, which is not followed (it could leak the API key).",
        endpoint=endpoint,
        attempts=attempts,
        retryable=False,
    )


@dataclass
class RetryInfo:
    endpoint: str
    attempt: int
    delay_s: float
    reason: str  # "status" | "network"
    status: Optional[int] = None


class _Budget:
    """Retry window for one operation: how much may still be spent on failed attempts + backoff."""

    def __init__(self, timeout: float) -> None:
        self._started = _now()
        # On a short operation, spending 60 s only on retries would be disproportionate.
        self._cap = min(timeout, RETRY_OVERHEAD)

    def overhead_left(self) -> float:
        return self._cap - (_now() - self._started)


class Transport:
    """Shared transport state: one per :class:`zihin.Zihin` client."""

    def __init__(
        self,
        http: httpx.Client,
        *,
        api_key: str,
        base_url: str,
        timeout: Optional[float],
        stream_timeout: Optional[float],
        max_retries: int,
        max_stream_event_bytes: int,
        max_stream_total_bytes: int,
        on_retry: Optional[Callable[[RetryInfo], None]],
    ) -> None:
        self.http = http
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.stream_timeout = stream_timeout
        self.max_retries = max_retries
        self.max_stream_event_bytes = max_stream_event_bytes
        self.max_stream_total_bytes = max_stream_total_bytes
        self.on_retry = on_retry

    # -- helpers ------------------------------------------------------------------------------

    def resolve_timeout(self, operation: str) -> float:
        """Effective deadline (seconds) of an operation: an explicit override wins, else the default."""
        if operation == "stream" and self.stream_timeout is not None:
            return self.stream_timeout
        return self.timeout if self.timeout is not None else OPERATION_TIMEOUT[operation]

    def headers(self, accept: str = "application/json") -> Dict[str, str]:
        return {"Content-Type": "application/json", "Accept": accept, "X-Api-Key": self.api_key}

    def _notify_retry(self, info: RetryInfo) -> None:
        if self.on_retry is None:
            return
        try:
            self.on_retry(info)
        except Exception:  # noqa: BLE001 - observability must never break the call
            pass

    def _send(self, method: str, url: str, body: Optional[bytes], timeout: float, accept: str) -> httpx.Response:
        request = self.http.build_request(
            method, url, headers=self.headers(accept), content=body, timeout=httpx.Timeout(timeout)
        )
        return self.http.send(request, stream=True, follow_redirects=False)

    @staticmethod
    def _read_text(res: httpx.Response, deadline_at: float, on_expired: Callable[[], ZihinError]) -> str:
        parts = []
        for chunk in res.iter_bytes():
            parts.append(chunk)
            if _now() > deadline_at:
                raise on_expired()
        return b"".join(parts).decode("utf-8", errors="replace")

    # -- buffered request ---------------------------------------------------------------------

    def request_buffered(
        self,
        method: str,
        url: str,
        *,
        body: Optional[bytes] = None,
        timeout: float,
        policy: str,
    ) -> Tuple[int, str, str, int]:
        """Run a request reading the whole body inside the deadline. Returns
        ``(status, text, endpoint, attempts)`` also for non-2xx (the caller maps the error)."""
        endpoint = endpoint_label(method, url)
        budget = _Budget(timeout)
        reported_ms = round(timeout * 1000)
        last_error: Optional[ZihinError] = None

        for attempt in range(self.max_retries + 1):
            attempts = attempt + 1

            def expired(n: int = attempts) -> ZihinError:
                return ZihinError("timeout", f"Request timed out after {reported_ms}ms.", endpoint=endpoint, attempts=n)

            # FULL deadline on every attempt: it keeps the client above the server's budget even
            # after a retry. The loop only comes back here if the retry window allowed it.
            deadline_at = _now() + timeout
            wait: Optional[float] = None
            try:
                res = self._send(method, url, body, timeout, "application/json")
                try:
                    if 300 <= res.status_code < 400:
                        raise _redirect_error(endpoint, attempts)
                    if _should_retry_status(res.status_code, policy) and attempt < self.max_retries:
                        wait = _retry_after(res.headers)
                        if wait is None:
                            wait = _backoff(attempt)
                    # A 429 can be a rate limit (passes) OR a commercial block (trial/subscription/
                    # quota). Only the body separates them; without peeking, an exhausted quota
                    # would burn every attempt with backoff before delivering the error.
                    peeked: Optional[str] = None
                    if wait is not None and res.status_code == 429:
                        peeked = self._read_text(res, deadline_at, expired)
                        if is_commercial_block(parse_body(peeked)):
                            wait = None
                    # Retry only if the backoff FITS the retry window; sleeping until the window ends
                    # to then time out would replace the typed error in hand with a generic timeout.
                    if wait is not None and budget.overhead_left() > wait:
                        self._notify_retry(RetryInfo(endpoint, attempts, wait, "status", res.status_code))
                    else:
                        text = peeked if peeked is not None else self._read_text(res, deadline_at, expired)
                        return res.status_code, text, endpoint, attempts
                finally:
                    res.close()
            except ZihinError:
                # Errors WE classified inside the try pass straight through.
                raise
            except httpx.TimeoutException as exc:
                raise expired() from exc
            except httpx.HTTPError as exc:
                last_error = error_from_network_exception(exc, endpoint=endpoint, attempts=attempts)
                # A network error is only retryable on an idempotent method.
                if not (policy == "idempotent" and last_error.retryable and attempt < self.max_retries):
                    raise last_error from exc
                wait = _backoff(attempt)
                if budget.overhead_left() <= wait:
                    raise last_error from exc
                self._notify_retry(RetryInfo(endpoint, attempts, wait, "network"))
            assert wait is not None
            _sleep_now(wait)

        raise last_error or ZihinError("unknown", "Max retries exceeded.", endpoint=endpoint)

    # -- streaming request --------------------------------------------------------------------

    def open_stream(
        self,
        url: str,
        *,
        body: bytes,
        timeout: float,
        reported_ms: int,
        idempotent: bool,
    ) -> Tuple[httpx.Response, float, str]:
        """Open an SSE POST. Returns ``(response, deadline_at, endpoint)`` with the response still
        open: the caller reads the stream inside the same deadline and closes it.

        ``idempotent=True`` (the POST carries ``client_message_id``) applies the idempotent policy
        (retry on network errors and 5xx) only BEFORE the headers arrive. Default: side-effecting
        (429/503 only).
        """
        endpoint = endpoint_label("POST", url)
        budget = _Budget(timeout)
        policy = "idempotent" if idempotent else "side-effecting"

        for attempt in range(self.max_retries + 1):
            attempts = attempt + 1

            def expired(n: int = attempts) -> ZihinError:
                return ZihinError("timeout", f"Request timed out after {reported_ms}ms.", endpoint=endpoint, attempts=n)

            # FULL deadline per attempt; on the attempt that succeeds, this same clock keeps
            # covering the read of the whole stream.
            deadline_at = _now() + timeout
            try:
                res = self._send("POST", url, body, timeout, "text/event-stream")
            except httpx.TimeoutException as exc:
                raise expired() from exc
            except httpx.HTTPError as exc:
                # POST does not retry on a network error, unless it carries an idempotency key (the
                # server dedupes), and then only if the backoff still fits the budget.
                net = error_from_network_exception(exc, endpoint=endpoint, attempts=attempts)
                wait = _backoff(attempt)
                if (
                    policy == "idempotent"
                    and net.retryable
                    and attempt < self.max_retries
                    and budget.overhead_left() > wait
                ):
                    self._notify_retry(RetryInfo(endpoint, attempts, wait, "network"))
                    _sleep_now(wait)
                    continue
                raise net from exc

            keep_open = False
            try:
                status = res.status_code
                if 300 <= status < 400:
                    raise _redirect_error(endpoint, attempts)
                wait_s: Optional[float] = None
                if _should_retry_status(status, policy) and attempt < self.max_retries:
                    wait_s = _retry_after(res.headers)
                    if wait_s is None:
                        wait_s = _backoff(attempt)
                # Same peek as the buffered path. A 429 never becomes a useful stream, so the error
                # leaves here already typed.
                if status == 429:
                    try:
                        text = self._read_text(res, deadline_at, expired)
                    except httpx.HTTPError:
                        text = ""
                    parsed = parse_body(text)
                    if wait_s is None or is_commercial_block(parsed) or budget.overhead_left() <= wait_s:
                        raise error_from_response(status, parsed, endpoint=endpoint, attempts=attempts)
                elif wait_s is None or budget.overhead_left() <= wait_s:
                    keep_open = True
                    return res, deadline_at, endpoint  # NOT closed: the stream is still to be read
                self._notify_retry(RetryInfo(endpoint, attempts, wait_s, "status", status))
            except httpx.TimeoutException as exc:
                raise expired() from exc
            finally:
                if not keep_open:
                    res.close()
            _sleep_now(wait_s)

        raise ZihinError("unknown", "Max retries exceeded.", endpoint=endpoint)

    def iter_stream(
        self, res: httpx.Response, deadline_at: float, reported_ms: int, endpoint: str
    ) -> Iterator[bytes]:
        """Yield the stream's raw chunks inside the deadline, translating transport failures."""
        it = res.iter_bytes()
        while True:
            try:
                chunk = next(it)
            except StopIteration:
                return
            except httpx.TimeoutException as exc:
                raise ZihinError("timeout", f"Stream timed out after {reported_ms}ms.", endpoint=endpoint) from exc
            except httpx.HTTPError as exc:
                # Not retryable: the server cancels the turn when it loses the connection and
                # RELEASES the client_message_id, so a resend (even with the key) runs the turn, and
                # its tools, again.
                raise ZihinError(
                    "network", "Stream connection interrupted.", endpoint=endpoint, retryable=False
                ) from exc
            if _now() > deadline_at:
                raise ZihinError("timeout", f"Stream timed out after {reported_ms}ms.", endpoint=endpoint)
            yield chunk

    # -- JSON helper --------------------------------------------------------------------------

    def request_json(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        policy: str,
        operation: str = "short",
    ) -> Any:
        """JSON request against the API; raises :class:`ZihinError` on non-2xx."""
        status, text, endpoint, attempts = self.request_buffered(
            method,
            f"{self.base_url}{path}",
            body=to_json_body(body) if body is not None else None,
            timeout=self.resolve_timeout(operation),
            policy=policy,
        )
        if status >= 400:
            raise error_from_response(status, parse_body(text), endpoint=endpoint, attempts=attempts)
        try:
            return json.loads(text)
        except ValueError as exc:
            raise ZihinError("protocol", "Expected JSON response.", endpoint=endpoint) from exc


def _sleep_now(seconds: Optional[float]) -> None:
    if seconds:
        _sleep(seconds)
