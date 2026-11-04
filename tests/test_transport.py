"""Transport: retry policy, pre-header retry with an idempotency key, deadline, redirects, limits."""

import httpx
import pytest

import zihin._http as zhttp
from zihin import ZihinError

from conftest import UUID, CutStream, Recorder, make_client, sse

ACCEPTS_KEY = ("metadata", {"accepted_fields": ["client_message_id"], "client_message_id": UUID})


def good_stream():
    return httpx.Response(200, content=sse(ACCEPTS_KEY, ("response", {"content": "ok"}), ("done", {"outcome": "COMPLETED"})))


def test_pre_header_retry_with_key_network_then_500_then_ok(no_sleep):
    rec = Recorder(httpx.ConnectError("refused"), httpx.Response(500, json={}), good_stream())
    r = make_client(rec).invoke_agent("a", "Hi", client_message_id=UUID)
    assert r.content == "ok"
    assert len(rec.requests) == 3 and all(b["client_message_id"] == UUID for b in rec.bodies)
    assert len(no_sleep) == 2


def test_no_network_retry_without_key():
    rec = Recorder(httpx.ConnectError("refused"), good_stream())
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi")
    assert exc.value.kind == "network" and len(rec.requests) == 1


def test_no_5xx_retry_without_key_except_503():
    rec = Recorder(httpx.Response(500, json={}), good_stream())
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi")
    assert exc.value.kind == "server" and len(rec.requests) == 1
    rec = Recorder(httpx.Response(503, json={}), httpx.Response(200, content=sse(("response", {"content": "ok"}))))
    assert make_client(rec).invoke_agent("a", "Hi").content == "ok" and len(rec.requests) == 2


def test_mid_stream_cut_with_key_is_one_post_and_not_retryable():
    first = sse(ACCEPTS_KEY, ("token", {"content": "par"}))
    rec = Recorder(httpx.Response(200, stream=CutStream(first)), good_stream())
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi", client_message_id=UUID)
    assert (exc.value.kind, exc.value.retryable) == ("network", False)
    assert len(rec.requests) == 1


def test_4xx_is_not_retried_even_with_key():
    rec = Recorder(httpx.Response(400, json={"error": {"code": "BAD", "message": "nope"}}), good_stream())
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi", client_message_id=UUID)
    assert exc.value.status == 400 and len(rec.requests) == 1


def test_429_honours_retry_after(no_sleep):
    rec = Recorder(httpx.Response(429, headers={"Retry-After": "7"}, json={"error": {"code": "RATE_LIMIT_EXCEEDED"}}), good_stream())
    make_client(rec).invoke_agent("a", "Hi", client_message_id=UUID)
    assert no_sleep == [7.0] and len(rec.requests) == 2


def test_commercial_429_is_not_retried():
    rec = Recorder(httpx.Response(429, json={"error": "QUOTA_EXCEEDED", "reason": "quota_exhausted"}), good_stream())
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi")
    assert (exc.value.kind, exc.value.retryable) == ("quota_exceeded", False) and len(rec.requests) == 1
    rec = Recorder(httpx.Response(429, json={"error": "QUOTA_EXCEEDED", "reason": "no_active_subscription"}))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).list_agents()
    assert exc.value.kind == "no_subscription" and len(rec.requests) == 1


def test_get_retries_5xx_and_records_attempts(no_sleep):
    seen = []
    rec = Recorder(httpx.Response(502, json={}), httpx.Response(502, json={}), httpx.Response(200, json={"data": []}))
    assert make_client(rec, on_retry=seen.append).list_agents() == []
    assert len(rec.requests) == 3 and [i.attempt for i in seen] == [1, 2]
    rec = Recorder(httpx.Response(500, json={"request_id": "req-1"}))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).list_agents()
    assert exc.value.attempts == 3 and "after 3 attempts" in exc.value.message and exc.value.request_id == "req-1"


def test_redirect_is_rejected_and_not_followed():
    rec = Recorder(httpx.Response(302, headers={"Location": "https://evil.example/steal"}))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).list_agents()
    assert (exc.value.kind, exc.value.retryable) == ("network", False)
    assert len(rec.requests) == 1
    rec = Recorder(httpx.Response(307, headers={"Location": "https://evil.example/"}))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi")
    assert (exc.value.kind, exc.value.retryable) == ("network", False)
    assert [str(r.url.host) for r in rec.requests] == ["llm.zihin.ai"]


def test_redirect_rejected_even_when_the_injected_client_follows_redirects():
    rec = Recorder(httpx.Response(302, headers={"Location": "https://evil.example/steal"}))
    from zihin import Zihin
    from conftest import KEY

    z = Zihin(KEY, http_client=httpx.Client(transport=httpx.MockTransport(rec), follow_redirects=True))
    with pytest.raises(ZihinError) as exc:
        z.list_agents()
    assert exc.value.kind == "network" and len(rec.requests) == 1


def test_stream_deadline_covers_the_whole_stream(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(zhttp, "_now", lambda: clock[0])

    class SlowStream(httpx.SyncByteStream):
        def __iter__(self):
            yield sse(("token", {"content": "a"}))
            clock[0] += 500  # far beyond the 200 s default stream deadline
            yield sse(("response", {"content": "late"}))

    rec = Recorder(httpx.Response(200, stream=SlowStream()))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi")
    assert exc.value.kind == "timeout" and "200000ms" in exc.value.message


def test_caller_timeout_is_sent_and_deadline_adds_grace(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(zhttp, "_now", lambda: clock[0])

    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            yield sse(("metadata", {"accepted_fields": ["options.timeout_ms"]}))
            clock[0] += 34  # past timeout_ms (5 s), still inside the 30 s local grace (35 s)
            yield sse(("response", {"content": "ok"}), ("done", {"outcome": "COMPLETED"}))

    rec = Recorder(httpx.Response(200, stream=Stream()))
    assert make_client(rec).invoke_agent("a", "Hi", timeout_ms=5000).content == "ok"
    assert rec.bodies[0]["options"] == {"timeout_ms": 5000}


def test_sse_event_size_limit_is_protocol():
    rec = Recorder(httpx.Response(200, content=sse(("token", {"content": "x" * 5000}))))
    with pytest.raises(ZihinError) as exc:
        make_client(rec, max_stream_event_bytes=1024).invoke_agent("a", "Hi")
    assert exc.value.kind == "protocol"
    rec = Recorder(httpx.Response(200, content=sse(*[("token", {"content": "x" * 100})] * 50)))
    with pytest.raises(ZihinError) as exc:
        make_client(rec, max_stream_total_bytes=2048).invoke_agent("a", "Hi")
    assert exc.value.kind == "protocol"


def test_dns_failure_is_not_retryable(no_sleep):
    import socket

    def handler(request):
        try:
            raise socket.gaierror(8, "nodename nor servname provided")
        except socket.gaierror as exc:
            raise httpx.ConnectError("dns") from exc

    rec = Recorder(handler)
    with pytest.raises(ZihinError) as exc:
        make_client(rec).list_agents()
    assert (exc.value.kind, exc.value.retryable) == ("network", False) and len(rec.requests) == 1
