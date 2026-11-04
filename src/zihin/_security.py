"""Security primitives, ported from ``@zihin/agent-client`` ``security.ts`` / ``internal.ts``.

SSRF prevention for the base URL, payload limits, error-message sanitization, API key format and
the sanitizers for identifiers that come from the server.
"""

from __future__ import annotations

import re
import socket
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple
from urllib.parse import urlsplit

DEFAULT_BASE_URL = "https://llm.zihin.ai"

# ---------------------------------------------------------------------------------------------
# Server-provided identifiers
# ---------------------------------------------------------------------------------------------

_SAFE_ID_RE = re.compile(r"[\w.:-]{1,128}", re.ASCII)
_SAFE_LABEL_RE = re.compile(r"[A-Za-z0-9_]{1,64}")


def safe_id(value: Any) -> Optional[str]:
    """An identifier coming from the SERVER (execution_id, session_id): bounded, no line breaks.

    A hostile server (custom base URL) could send 500 KB with ``\\n`` and a fake "Bearer ...",
    which would end up in the error message and in the integrator's logs.
    """
    return value if isinstance(value, str) and _SAFE_ID_RE.fullmatch(value) else None


def safe_label(value: Any) -> Optional[str]:
    """An enumerated label from the server (``outcome``, ``status.blocked``): ``[A-Za-z0-9_]``, up to 64."""
    return value if isinstance(value, str) and _SAFE_LABEL_RE.fullmatch(value) else None


# ---------------------------------------------------------------------------------------------
# 1. URL validation (SSRF prevention)
# ---------------------------------------------------------------------------------------------

_PRIVATE_IP_PATTERNS = [
    re.compile(r"^127\."),  # 127.0.0.0/8 (localhost)
    re.compile(r"^10\."),  # 10.0.0.0/8 (private)
    re.compile(r"^172\.(1[6-9]|2[0-9]|3[0-1])\."),  # 172.16.0.0/12 (private)
    re.compile(r"^192\.168\."),  # 192.168.0.0/16 (private)
    re.compile(r"^169\.254\."),  # 169.254.0.0/16 (link-local)
    re.compile(r"^0\."),  # 0.0.0.0/8
]

_BLOCKED_HOSTNAMES = [
    "localhost",
    "localtest.me",
    "metadata.google.internal",  # GCP metadata
    "169.254.169.254",  # AWS/Azure/GCP metadata
    "metadata.azure.com",
]

ALLOWED_ZIHIN_DOMAINS = [
    "llm.zihin.ai",
    "api.zihin.ai",
    "staging.llm.zihin.ai",
    "dev.llm.zihin.ai",
]

_HEX_GROUP_RE = re.compile(r"[0-9a-f]{1,4}", re.I)
_V4_TAIL_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3})$")


def _parse_ipv6(host: str) -> Optional[List[int]]:
    """Expand an IPv6 literal (without brackets) into 8 16-bit words; ``None`` if not IPv6.

    Accepts the embedded-IPv4 form (``::ffff:127.0.0.1``).
    """
    if ":" not in host:
        return None
    text = host
    tail: List[int] = []
    m = _V4_TAIL_RE.search(text)
    if m:
        octets = [int(o) for o in m.group(1).split(".")]
        if any(o < 0 or o > 255 for o in octets):
            return None
        tail = [(octets[0] << 8) | octets[1], (octets[2] << 8) | octets[3]]
        text = text[: m.start()]
        if text.endswith(":"):
            text = text[:-1]
        text = text or "::"

    halves = text.split("::")
    if len(halves) > 2:
        return None

    def to_words(part: str) -> Optional[List[int]]:
        if not part:
            return []
        out = []
        for group in part.split(":"):
            if not _HEX_GROUP_RE.fullmatch(group):
                return None
            out.append(int(group, 16))
        return out

    head = to_words(halves[0])
    rest = to_words(halves[1]) if len(halves) == 2 else []
    if head is None or rest is None:
        return None
    if len(halves) == 2:
        fill = 8 - (len(head) + len(rest) + len(tail))
        if fill < 0:
            return None
        words = head + [0] * fill + rest + tail
    else:
        words = head + tail
    return words if len(words) == 8 else None


def _is_private_ipv6(words: List[int]) -> bool:
    """IPv6 ranges that must never be fetched server-side, compared by BITS.

    A textual prefix (``fd00:``/``fe80:``) would let through almost every real ULA, which is
    ``fd`` + 40 random bits (e.g. ``fd12:3456:789a::1``), and link-locals such as ``fe9a::1``.
    """
    w0, w1, w2, w3, w4, w5 = words[:6]
    if all(w == 0 for w in words):
        return True  # :: (unspecified)
    if all(w == 0 for w in words[:7]) and words[7] == 1:
        return True  # ::1 loopback
    # Embedded IPv4: ::ffff:a.b.c.d (mapped) and ::a.b.c.d (compatible) -> validated as IPv4.
    if w0 == 0 and w1 == 0 and w2 == 0 and w3 == 0 and w4 == 0 and w5 in (0, 0xFFFF):
        v4 = f"{words[6] >> 8}.{words[6] & 0xFF}.{words[7] >> 8}.{words[7] & 0xFF}"
        return any(p.search(v4) for p in _PRIVATE_IP_PATTERNS)
    if ((w0 >> 8) & 0xFE) == 0xFC:
        return True  # fc00::/7  unique local (fc00-fdff)
    if (w0 & 0xFFC0) == 0xFE80:
        return True  # fe80::/10 link-local (fe80-febf)
    if (w0 & 0xFFC0) == 0xFEC0:
        return True  # fec0::/10 site-local (deprecated, still routable internally)
    return False


_IPV4_NUMBERISH_RE = re.compile(r"[0-9a-fx.]+", re.I)
_IPV4_LAST_PART_RE = re.compile(r"(0x[0-9a-f]*|[0-9]+)", re.I)


def _normalize_ipv4(host: str) -> str:
    """Normalize IPv4 written in octal/hex/decimal/short form (``0177.0.0.1``, ``0x7f.1``,
    ``2130706433``) to dotted decimal, as the WHATWG URL parser does in the TS client. Without
    this, Python's ``urlsplit`` would hand the private-range check the raw spelling."""
    if not _IPV4_NUMBERISH_RE.fullmatch(host):
        return host
    last = host.rstrip(".").rsplit(".", 1)[-1]
    if not _IPV4_LAST_PART_RE.fullmatch(last):
        return host
    try:
        return socket.inet_ntoa(socket.inet_aton(host.rstrip(".")))
    except (OSError, ValueError):
        return host


@dataclass(frozen=True)
class UrlValidationResult:
    valid: bool
    error: Optional[str] = None
    sanitized_url: Optional[str] = None


def _rebuild(scheme: str, host: str, port: Optional[int], path: str) -> str:
    """origin + path without trailing slash; default ports dropped like a WHATWG origin."""
    shown = f"[{host}]" if ":" in host else host
    default = {"https": 443, "http": 80}.get(scheme)
    netloc = shown if port is None or port == default else f"{shown}:{port}"
    return f"{scheme}://{netloc}{path.rstrip('/')}"


def validate_base_url(
    url: Optional[str],
    *,
    allow_custom_domains: bool = False,
    allow_http: bool = False,
    allow_localhost: bool = False,
) -> UrlValidationResult:
    """Validate a base URL for SSRF prevention.

    The check is by host NAME, not by resolved IP: with ``allow_custom_domains=True`` a host that
    resolves to a private IP (DNS rebinding) passes. In a sensitive environment keep the
    whitelist (default) or check egress at the network edge.
    """
    if url is None or url.strip() == "":
        return UrlValidationResult(True, sanitized_url=DEFAULT_BASE_URL)
    try:
        parts = urlsplit(url.strip())
        scheme = parts.scheme.lower()
        raw_host = parts.hostname  # lowercased, brackets removed
        port = parts.port  # raises ValueError on an invalid port
    except ValueError:
        return UrlValidationResult(False, "Invalid URL format")
    if not scheme or not raw_host:
        return UrlValidationResult(False, "Invalid URL format")

    # Trailing dot: `localhost.` and `metadata.google.internal.` are valid FQDNs that resolve to the
    # same host but would escape the block list by string equality. It is also dropped from the
    # effective URL: a Host/SNI with a trailing dot can fail certificate validation.
    hostname = raw_host.rstrip(".") if len(raw_host) > 1 and raw_host.endswith(".") else raw_host
    hostname = _normalize_ipv4(hostname)
    path = parts.path or ""

    is_localhost = hostname in ("localhost", "127.0.0.1", "::1")
    if is_localhost and allow_localhost:
        return UrlValidationResult(True, sanitized_url=_rebuild(scheme, hostname, port, path))

    # 1. Protocol (HTTPS only by default)
    if not allow_http and scheme != "https":
        return UrlValidationResult(False, "Only HTTPS URLs are allowed for security reasons")
    if scheme not in ("https", "http"):
        return UrlValidationResult(False, "Invalid URL format")

    # 2. Private ranges (IPv4 by textual pattern; IPv6 by bits)
    words = _parse_ipv6(hostname)
    if words is not None:
        if _is_private_ipv6(words):
            return UrlValidationResult(False, "Private IP addresses are not allowed")
    elif any(p.search(hostname) for p in _PRIVATE_IP_PATTERNS):
        return UrlValidationResult(False, "Private IP addresses are not allowed")

    # 3. Known dangerous hostnames
    for blocked in _BLOCKED_HOSTNAMES:
        if hostname == blocked or hostname.endswith("." + blocked):
            return UrlValidationResult(False, "This hostname is not allowed")

    # 4. Whitelist (only Zihin domains) unless custom domains are allowed
    if not allow_custom_domains and not any(
        hostname == d or hostname.endswith("." + d) for d in ALLOWED_ZIHIN_DOMAINS
    ):
        return UrlValidationResult(
            False, "Only Zihin domains are allowed. Use 'allow_custom_base_url' for custom endpoints."
        )

    return UrlValidationResult(True, sanitized_url=_rebuild(scheme, hostname, port, path))


# ---------------------------------------------------------------------------------------------
# 2. Payload size limits
# ---------------------------------------------------------------------------------------------

MAX_PROMPT_BYTES = 100 * 1024


def validate_prompt_size(prompt: str, max_bytes: int = MAX_PROMPT_BYTES) -> Optional[str]:
    """Return an error message when ``prompt`` exceeds ``max_bytes`` (UTF-8), else ``None``."""
    size = len(prompt.encode("utf-8"))
    if size > max_bytes:
        return (
            f"Prompt size ({round(size / 1024)}KB) exceeds maximum allowed "
            f"({round(max_bytes / 1024)}KB)"
        )
    return None


# ---------------------------------------------------------------------------------------------
# 3. Error sanitization
# ---------------------------------------------------------------------------------------------

# Only REAL secrets are redacted. request_id/trace_id/correlation_id are NOT: they are not secret
# and they are exactly what support/observability needs to correlate.
_SENSITIVE_PATTERNS = [
    re.compile(r"zhn_(?:live|test|dev)_[a-zA-Z0-9]+"),  # API keys
    re.compile(r"Bearer\s+[a-zA-Z0-9._-]+", re.I),  # Bearer tokens
    re.compile(r"at\s+[\w./<>]+\s*\(.*:\d+:\d+\)"),  # stack trace lines
    re.compile(r"^\s+at\s+.+$", re.M),  # stack traces
    re.compile(r"Error:\s*.*\n\s+at"),  # error stack beginnings
]

_ERROR_CODE_MAP = {
    "RATE_LIMIT_EXCEEDED": "Rate limit exceeded. Please wait before retrying.",
    "UNAUTHORIZED": "Authentication failed. Please check your API credentials.",
    "FORBIDDEN": "This API key's role does not allow this action. The required role depends on the "
    "endpoint — check the key's role in Zihin → Settings → API Keys.",
    "INVALID_MODEL": "The specified model is not available.",
    "INTERNAL_ERROR": "An internal error occurred. Please try again later.",
    "TIMEOUT": "Request timed out. Try increasing the timeout.",
    "INVALID_REQUEST": "The request was malformed. Please check your input.",
    "INSUFFICIENT_CREDITS": "Insufficient credits. Please check your account balance.",
    "CONTEXT_LENGTH_EXCEEDED": "The input exceeds the model context length limit.",
    "AGENT_NOT_FOUND": "Agent not found or not active for this tenant.",
    "NOT_FOUND": "Resource not found — check the ID (agent, trigger or execution) and that it "
    "belongs to this workspace.",
    # Commercial blocks. The server's messages are in Portuguese, and each case needs a DIFFERENT
    # action that the text has to name.
    "TRIAL_EXPIRED": "Your Zihin trial has ended. Upgrade the plan to keep invoking agents — the API "
    "key is fine.",
    "QUOTA_EXCEEDED": "Your Zihin token quota is exhausted. Upgrade the plan or wait for the next "
    "cycle — retrying will not help, and the API key is fine.",
    "NO_ACTIVE_SUBSCRIPTION": "This tenant has no active Zihin subscription. Activate a plan to "
    "invoke agents — the API key is fine.",
}


def _redact(message: str) -> str:
    # Bound the size BEFORE the regexes (bounds cost/ReDoS on large messages).
    if len(message) > 2000:
        message = message[:2000]
    for pattern in _SENSITIVE_PATTERNS:
        message = pattern.sub("[REDACTED]", message)
    return message


def sanitize_api_error(error: Any) -> Tuple[str, Optional[str]]:
    """Return ``(message, code)`` for an error body or string, with secrets redacted."""
    message = "An unexpected error occurred"
    code: Optional[str] = None
    if isinstance(error, dict):
        nested = error.get("error") if isinstance(error.get("error"), dict) else None
        if nested is not None and nested.get("code") is not None:
            code_raw = nested.get("code")
        elif isinstance(error.get("error"), str):
            code_raw = error.get("error")
        else:
            code_raw = error.get("code")
        code = code_raw if isinstance(code_raw, str) else None
        raw_message = nested.get("message") if nested is not None and nested.get("message") is not None else error.get("message")
        # The server sends the SAME code `QUOTA_EXCEEDED` for an exhausted quota and for an inactive
        # subscription; only `reason` separates them, and each case has a different action.
        reason = error.get("reason") if isinstance(error.get("reason"), str) else None
        effective = "NO_ACTIVE_SUBSCRIPTION" if code == "QUOTA_EXCEEDED" and reason == "no_active_subscription" else code
        if effective and effective in _ERROR_CODE_MAP:
            message = _ERROR_CODE_MAP[effective]
        elif isinstance(raw_message, str) and raw_message:
            message = raw_message
        message = _redact(message)
    elif isinstance(error, str):
        message = _redact(error)
    if len(message) > 500:
        message = message[:500] + "..."
    return message, code


def sanitize_message(text: str) -> str:
    """Redact secrets from a free-form string (same rules as :func:`sanitize_api_error`)."""
    return sanitize_api_error(text)[0]


# ---------------------------------------------------------------------------------------------
# 4. API key format
# ---------------------------------------------------------------------------------------------

_API_KEY_RE = re.compile(r"zhn_(live|test|dev)_[a-zA-Z0-9]{20,}")


def validate_api_key_format(api_key: str) -> Optional[str]:
    """Return an error message when the key is not ``zhn_(live|test|dev)_<20+ alphanumerics>``."""
    if not api_key or not api_key.strip():
        return "API key is required"
    if not _API_KEY_RE.fullmatch(api_key.strip()):
        return "Invalid API key format. Expected: zhn_live_xxx, zhn_test_xxx or zhn_dev_xxx"
    return None
