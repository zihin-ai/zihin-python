"""The 0.1.0 suite, adapted to 0.2.0 where the contract changed (see comments)."""

import json

import httpx
import pytest

from zihin import Zihin, ZihinError
from zihin._sse import parse_sse

from conftest import KEY, make_client as client, sse


def test_requires_api_key(monkeypatch):
    monkeypatch.delenv("ZIHIN_API_KEY", raising=False)
    with pytest.raises(ZihinError) as exc:
        Zihin()
    assert exc.value.kind == "config"


def test_rejects_key_with_unknown_prefix():
    with pytest.raises(ZihinError) as exc:
        Zihin("sk-not-a-zihin-key")
    assert exc.value.kind == "config"


def test_reads_key_from_environment(monkeypatch):
    monkeypatch.setenv("ZIHIN_API_KEY", KEY)
    assert "zhn_" not in repr(Zihin())


def test_list_agents_sends_key_and_maps_fields():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["key"] = request.headers.get("x-api-key")
        return httpx.Response(
            200, json={"data": [{"id": "a1", "name": "Support", "commercial_name": "Ana", "tags": ["x", 3], "tools_count": 4}]}
        )

    agents = client(handler).list_agents()
    assert seen == {"url": "https://llm.zihin.ai/api/v2/agents", "key": KEY}
    assert agents[0].id == "a1" and agents[0].commercial_name == "Ana" and agents[0].tags == ["x"]
    assert agents[0].tools_count == 4


def test_list_agents_maps_401_to_auth():
    with pytest.raises(ZihinError) as exc:
        client(lambda r: httpx.Response(401, json={"error": {"code": "UNAUTHORIZED", "message": "bad key"}})).list_agents()
    assert (exc.value.kind, exc.value.status, exc.value.code) == ("auth", 401, "UNAUTHORIZED")


def test_invoke_agent_consolidates_response_and_metrics():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            content=sse(
                ("metadata", {"session_id": "s-meta"}),
                ("token", {"content": "thinking out loud"}),
                ("response", {"content": "Hello!", "session_id": "s-1", "sources": []}),
                ("metrics", {"usage": {"input_tokens": 10, "output_tokens": 2, "cost_usd": 0.001}, "model_config": {"model": "m-1"}, "execution_id": "e-1"}),
                ("done", {"outcome": "COMPLETED"}),
            ),
        )

    result = client(handler).invoke_agent("agent/1", "Hi", session_id="s-0")
    assert seen["url"] == "https://llm.zihin.ai/api/v2/agents/agent%2F1/stream"
    assert seen["body"] == {"message": "Hi", "session_id": "s-0"}
    assert result.content == "Hello!"
    assert result.content_source == "response"
    assert result.session_id == "s-1"
    assert (result.model, result.execution_id, result.outcome) == ("m-1", "e-1", "COMPLETED")
    assert (result.usage.input_tokens, result.usage.output_tokens, result.usage.cost_usd) == (10, 2, 0.001)


def test_invoke_agent_raises_on_error_event():
    # 0.2.0: the `agent` kind is gone. An unknown in-stream code is classified like the TS core
    # (`server`, not retryable); known codes get their own kind (see test_invoke.py).
    body = sse(("token", {"content": "partial"}), ("error", {"error": {"code": "ENGINE_ERROR", "message": "boom", "request_id": "r-1"}}))
    with pytest.raises(ZihinError) as exc:
        client(lambda r: httpx.Response(200, content=body)).invoke_agent("a", "Hi")
    assert (exc.value.kind, exc.value.code, exc.value.request_id) == ("server", "ENGINE_ERROR", "r-1")
    assert exc.value.retryable is False


def test_invoke_agent_raises_timeout_even_with_a_response_event():
    body = sse(("response", {"content": "the turn timed out", "session_id": "s-1"}), ("done", {"outcome": "TIMEOUT", "timed_out": True}))
    with pytest.raises(ZihinError) as exc:
        client(lambda r: httpx.Response(200, content=body)).invoke_agent("a", "Hi")
    assert (exc.value.kind, exc.value.code, exc.value.session_id) == ("timeout", "TURN_TIMEOUT", "s-1")


def test_invoke_agent_does_not_return_tokens_as_the_answer():
    body = sse(("token", {"content": "deliberation"}), ("done", {"outcome": "COMPLETED"}))
    with pytest.raises(ZihinError) as exc:
        client(lambda r: httpx.Response(200, content=body)).invoke_agent("a", "Hi")
    assert exc.value.kind == "protocol"


def test_agent_access_denied_is_reported_as_not_found():
    def handler(request):
        return httpx.Response(403, json={"error": {"code": "FORBIDDEN", "details": {"reason": "AGENT_ACCESS_DENIED"}}})

    with pytest.raises(ZihinError) as exc:
        client(handler).invoke_agent("a", "Hi")
    assert (exc.value.kind, exc.value.status) == ("not_found", 403)


def test_stream_agent_yields_events_in_order():
    body = sse(("token", {"content": "He"}), ("token", {"content": "llo"}), ("response", {"content": "Hello"}))
    events = list(client(lambda r: httpx.Response(200, content=body)).stream_agent("a", "Hi"))
    assert [e.type for e in events] == ["token", "token", "response"]
    assert "".join(e.content for e in events if e.type == "token") == "Hello"


def test_network_failure_is_reported_as_network(no_sleep):
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ConnectError("refused")

    with pytest.raises(ZihinError) as exc:
        client(handler).list_agents()
    assert exc.value.kind == "network"
    # 0.2.0: GET is idempotent, so a network error is retried (default max_retries=2).
    assert len(calls) == 3 and len(no_sleep) == 2


def test_parse_sse_handles_comments_multiline_data_and_plain_text():
    # 0.2.0: the parser consumes raw byte chunks (to enforce size limits) instead of text lines.
    lines = [": keep-alive", "event: a", 'data: {"x":', "data: 1}", "", "data: plain", "", "event: empty", ""]
    raw = "\n".join(lines).encode()
    chunks = [raw[i : i + 7] for i in range(0, len(raw), 7)]
    assert list(parse_sse(chunks)) == [("a", {"x": 1}), ("message", "plain")]
