"""SSRF guard, API key format, error mapping and sanitization."""

import httpx
import pytest

from zihin import Zihin, ZihinError, is_commercial_block
from zihin._errors import error_from_response
from zihin._security import safe_id, safe_label, sanitize_api_error, validate_base_url

from conftest import KEY, Recorder, make_client


@pytest.mark.parametrize(
    "url, kwargs",
    [
        ("https://example.com", {}),  # outside the whitelist without the toggle
        ("https://llm.zihin.ai.evil.com", {}),
        ("http://llm.zihin.ai", {}),  # http without allow_http
        ("https://10.0.0.1", {"allow_custom_base_url": True}),
        ("https://192.168.1.10", {"allow_custom_base_url": True}),
        ("https://172.16.5.4", {"allow_custom_base_url": True}),
        ("https://127.0.0.1", {"allow_custom_base_url": True}),
        ("https://169.254.169.254", {"allow_custom_base_url": True}),
        ("https://0x7f.0.0.1", {"allow_custom_base_url": True}),  # hex IPv4
        ("https://0177.0.0.1", {"allow_custom_base_url": True}),  # octal IPv4
        ("https://2130706433", {"allow_custom_base_url": True}),  # decimal IPv4
        ("https://[::1]", {"allow_custom_base_url": True}),
        ("https://[fd12:3456:789a::1]", {"allow_custom_base_url": True}),  # ULA by bits
        ("https://[fe9a::1]", {"allow_custom_base_url": True}),  # link-local by bits
        ("https://[::ffff:10.0.0.1]", {"allow_custom_base_url": True}),  # IPv4-mapped
        ("https://localhost.", {"allow_custom_base_url": True}),  # trailing dot
        ("https://metadata.google.internal", {"allow_custom_base_url": True}),
        ("ftp://llm.zihin.ai", {}),
        ("not a url", {}),
    ],
)
def test_unsafe_base_urls_are_rejected(url, kwargs):
    with pytest.raises(ZihinError) as exc:
        Zihin(KEY, base_url=url, **kwargs)
    assert exc.value.kind == "config"


def test_allowed_base_urls():
    assert Zihin(KEY).base_url == "https://llm.zihin.ai"
    assert Zihin(KEY, base_url="https://staging.llm.zihin.ai/").base_url == "https://staging.llm.zihin.ai"
    assert Zihin(KEY, base_url="https://llm.zihin.ai.:443/").base_url == "https://llm.zihin.ai"
    assert Zihin(KEY, base_url="https://gw.example.com/zihin", allow_custom_base_url=True).base_url == "https://gw.example.com/zihin"
    assert Zihin(KEY, base_url="http://localhost:3000", allow_http=True).base_url == "http://localhost:3000"


def test_custom_base_url_is_used_for_requests():
    rec = Recorder(httpx.Response(200, json={"data": []}))
    make_client(rec, base_url="https://gw.example.com/zihin", allow_custom_base_url=True).list_agents()
    assert str(rec.requests[0].url) == "https://gw.example.com/zihin/api/v2/agents"


def test_toggle_is_explicit_not_derived_from_base_url():
    # A truthy non-bool must not open the whitelist.
    with pytest.raises(ZihinError):
        Zihin(KEY, base_url="https://example.com", allow_custom_base_url="yes")  # type: ignore[arg-type]


@pytest.mark.parametrize("key", ["zhn_live_short", "zhn_prod_" + "a" * 24, "zhn_live_" + "a" * 19 + "-", "x"])
def test_api_key_format(key):
    with pytest.raises(ZihinError) as exc:
        Zihin(key)
    assert exc.value.kind == "config"


def test_error_messages_redact_keys_and_bearer_but_keep_request_id():
    # Like the TS core, only SECRETS are redacted. request_id is a support handle, not a secret: it
    # is kept on the error (and appended to the message) so support can correlate.
    body = {
        "error": {
            "code": "SOMETHING",
            "message": "failed with zhn_live_" + "a" * 30 + " and Bearer tok.en-1\n    at handler (/srv/app.js:10:5)",
            "request_id": "req-42",
        }
    }
    err = error_from_response(500, body)
    assert "zhn_live_" not in err.message and "tok.en-1" not in err.message and "/srv/app.js" not in err.message
    assert err.message.count("[REDACTED]") >= 2
    assert err.request_id == "req-42" and "(request_id: req-42)" in err.message
    assert len(sanitize_api_error({"message": "x" * 5000})[0]) == 503


@pytest.mark.parametrize(
    "status, body, kind, retryable",
    [
        (401, {}, "auth", False),
        (403, {"error": {"code": "FORBIDDEN"}}, "auth", False),
        (403, {"error": "TRIAL_EXPIRED"}, "trial_expired", False),
        (404, {}, "not_found", False),
        (408, {}, "timeout", True),
        (429, {"error": {"code": "RATE_LIMIT_EXCEEDED"}}, "rate_limit", True),
        (400, {"error": {"code": "PLAN_LIMIT_REACHED"}}, "blocked", False),
        (400, {"error": {"code": "BAD"}}, "unknown", False),
        (503, {}, "server", True),
    ],
)
def test_http_status_mapping(status, body, kind, retryable):
    err = error_from_response(status, body)
    assert (err.kind, err.retryable, err.status) == (kind, retryable, status)


def test_is_commercial_block():
    assert is_commercial_block({"error": "QUOTA_EXCEEDED"}) and not is_commercial_block({"error": "RATE_LIMIT_EXCEEDED"})


def test_safe_id_and_label():
    assert safe_id("e-1:abc.def") == "e-1:abc.def"
    assert safe_id("e-1\n") is None and safe_id("a" * 129) is None and safe_id("é") is None and safe_id(5) is None
    assert safe_label("REFUSED") == "REFUSED" and safe_label("REFUSED ") is None and safe_label("a-b") is None


def test_validate_base_url_result():
    assert validate_base_url("").sanitized_url == "https://llm.zihin.ai"
    assert not validate_base_url("https://[fc00::1]", allow_custom_domains=True).valid
    assert validate_base_url("https://[2001:db8::1]", allow_custom_domains=True).valid


# The TS client validates with the WHATWG URL parser and the server reads the URL the same way;
# each of these resolves to a private/blocked host there, so all must be refused here too.
WHATWG_BYPASSES = [
    "https://localhost。",
    "https://metadata.google.internal。",
    "https://127。0。0。1",
    "https://ｌｏｃａｌｈｏｓｔ",
    "https://０x7f.1",
    "https://169.254.169.254\\@public.com/",
    "https://127。0。0。1/",
    "https://%31%32%37.0.0.1/",
    "https://xn--llm-zihin-ai.com",
    "https://1.2.3.256/",
]


@pytest.mark.parametrize("url", WHATWG_BYPASSES)
def test_whatwg_host_tricks_are_refused_as_custom_base_url(url):
    with pytest.raises(ZihinError) as exc:
        Zihin(KEY, base_url=url, allow_custom_base_url=True)
    assert exc.value.kind == "config"


@pytest.mark.parametrize("url", WHATWG_BYPASSES)
def test_whatwg_host_tricks_are_refused_as_attachment(url):
    rec = Recorder(httpx.Response(200))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi", attachments=[{"url": url, "type": "image"}])
    assert exc.value.kind == "config" and rec.requests == []


@pytest.mark.parametrize("url", WHATWG_BYPASSES)
def test_whatwg_host_tricks_are_refused_as_callback_url(url):
    rec = Recorder(httpx.Response(201, json={"data": {"id": "t"}}))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).create_trigger("ag", "n", callback_url=url)
    assert exc.value.kind == "config" and rec.requests == []
    with pytest.raises(ZihinError) as exc:
        make_client(rec).update_trigger("t", callback_url=url)
    assert exc.value.kind == "config" and rec.requests == []


def test_idna_host_is_mapped_like_whatwg():
    # U+24DB maps to "l" under IDNA, as in WHATWG: the effective host is the ASCII one.
    z = Zihin(KEY, base_url="https://ⓛlm.example.com", allow_custom_base_url=True)
    assert z.base_url == "https://llm.example.com"
    assert validate_base_url("https://LLM.zihin.ai").sanitized_url == "https://llm.zihin.ai"


def test_zihin_error_pickles():
    import pickle

    err = ZihinError("blocked", "refused", code="REFUSED", status=200, request_id="r-1", attempts=2, retryable=False)
    back = pickle.loads(pickle.dumps(err))
    assert type(back) is ZihinError
    assert (back.kind, back.code, back.status, back.request_id, back.retryable, back.message) == (
        err.kind, err.code, err.status, err.request_id, err.retryable, err.message
    )
    assert str(back) == str(err)
