"""Webhook triggers: create/find/update/delete, dispatch outcome handling, execution polling."""

import json

import httpx
import pytest

from zihin import ZihinError

from conftest import Recorder, make_client


def test_create_trigger_body_and_ref():
    rec = Recorder(httpx.Response(201, json={"data": {"id": "t-1"}}))
    ref = make_client(rec).create_trigger(
        "ag-1", "Orders", callback_url="https://hooks.example.com/z", response_transform="final",
        query_extraction={"mode": "template", "template": "{{body.text}}"},
    )
    assert (ref.trigger_id, ref.webhook_url) == ("t-1", "https://llm.zihin.ai/api/triggers/webhook/t-1")
    assert rec.bodies[0] == {
        "agent_id": "ag-1",
        "name": "Orders",
        "trigger_type": "webhook",
        "trigger_config": {
            "query_extraction": {"mode": "template", "template": "{{body.text}}"},
            "auth": {"type": "api_key"},
            "execution": {"mode": "async", "callback": {"url": "https://hooks.example.com/z", "method": "POST", "response_transform": {"mode": "final"}}},
        },
    }


@pytest.mark.parametrize(
    "kwargs",
    [
        {},  # async without callback
        {"callback_url": "https://10.0.0.1/hook"},
        {"mode": "sync", "response_transform": "final"},
        {"mode": "sync", "query_extraction": {"mode": "jmespath"}},
    ],
)
def test_create_trigger_invalid(kwargs):
    rec = Recorder(httpx.Response(201, json={"data": {"id": "t"}}))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).create_trigger("ag", "n", **kwargs)
    assert exc.value.kind == "config" and rec.requests == []


def test_find_or_create_reuses_by_name():
    rec = Recorder(httpx.Response(200, json={"data": [{"id": "t-9", "name": "Orders", "trigger_type": "webhook"}]}))
    r = make_client(rec).find_or_create_trigger("ag-1", "Orders", mode="sync")
    assert (r.trigger_id, r.created) == ("t-9", False)
    assert rec.requests[0].url.params["agentId"] == "ag-1"
    rec = Recorder(httpx.Response(200, json={"data": []}), httpx.Response(201, json={"data": {"id": "t-new"}}))
    r = make_client(rec).find_or_create_trigger("ag-1", "Orders", mode="sync")
    assert (r.trigger_id, r.created) == ("t-new", True)


def test_update_trigger_merges_the_current_config():
    current = {
        "data": {
            "id": "t-1",
            "trigger_config": {
                "auth": {"type": "api_key"},
                "execution": {"mode": "async", "callback": {"url": "https://old.example.com", "method": "PUT", "headers": {"X": "1"}}},
            },
        }
    }
    rec = Recorder(httpx.Response(200, json=current), httpx.Response(200, json={"data": {"id": "t-1", "name": "N"}}))
    info = make_client(rec).update_trigger("t-1", name="N", callback_url="https://new.example.com")
    assert info.name == "N"
    patch = rec.bodies[1]
    assert rec.requests[1].method == "PATCH"
    assert patch["trigger_config"]["auth"] == {"type": "api_key"}
    assert patch["trigger_config"]["execution"]["callback"] == {"url": "https://new.example.com", "method": "PUT", "headers": {"X": "1"}}


def test_update_trigger_refuses_partial_overwrite():
    rec = Recorder(httpx.Response(200, json={"data": {"id": "t-1"}}))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).update_trigger("t-1", mode="sync")
    assert exc.value.kind == "protocol" and len(rec.requests) == 1


def test_delete_trigger():
    rec = Recorder(httpx.Response(200, json={"deleted": True}))
    assert make_client(rec).delete_trigger("t-1") is True and rec.requests[0].method == "DELETE"


def test_dispatch_sync_success_returns_outcome():
    rec = Recorder(httpx.Response(200, json={"success": True, "outcome": "COMPLETED", "agent_response": "Done", "execution_id": "x-1", "usage": {"total_tokens": 9}, "query_extracted": "hi"}))
    r = make_client(rec).dispatch_agent("t-1", {"message": "hi"})
    assert (r.accepted, r.outcome, r.agent_response, r.execution_id, r.usage.total_tokens) == (True, "COMPLETED", "Done", "x-1", 9)
    assert json.loads(rec.requests[0].content) == {"message": "hi"}


def test_dispatch_async_202():
    rec = Recorder(httpx.Response(202, json={"success": True, "execution_id": "x-2"}))
    r = make_client(rec).dispatch_agent("t-1")
    assert (r.accepted, r.execution_id, r.outcome) == (True, "x-2", None)


@pytest.mark.parametrize(
    "outcome, kind, code",
    [("TIMEOUT", "timeout", "TURN_TIMEOUT"), ("REFUSED", "blocked", "REFUSED"), ("CONTEXT_LIMIT", "blocked", "CONTEXT_LIMIT"), ("ERROR", "server", "ERROR"), ("SESSION_BUSY", "blocked", "SESSION_BUSY")],
)
def test_dispatch_success_true_with_bad_outcome_raises(outcome, kind, code):
    body = {"success": True, "outcome": outcome, "agent_response": "I cannot help with that request.", "execution_id": "x-3"}
    with pytest.raises(ZihinError) as exc:
        make_client(Recorder(httpx.Response(200, json=body))).dispatch_agent("t-1", {"message": "hi"})
    assert (exc.value.kind, exc.value.code, exc.value.status, exc.value.execution_id) == (kind, code, 200, "x-3")


def test_dispatch_success_false_reads_outcome_first():
    body = {"success": False, "outcome": "CONFLICT", "error": "identidade diferente"}
    with pytest.raises(ZihinError) as exc:
        make_client(Recorder(httpx.Response(200, json=body))).dispatch_agent("t-1")
    assert (exc.value.kind, exc.value.code) == ("blocked", "CONFLICT")
    body = {"success": False, "outcome": "QUOTA_EXCEEDED"}
    with pytest.raises(ZihinError) as exc:
        make_client(Recorder(httpx.Response(200, json=body))).dispatch_agent("t-1")
    assert exc.value.kind == "quota_exceeded"


def test_dispatch_success_false_generic_error_is_sanitized_server_error():
    body = {"success": False, "outcome": "ERROR", "error": "tool failed with zhn_live_" + "b" * 30}
    with pytest.raises(ZihinError) as exc:
        make_client(Recorder(httpx.Response(200, json=body))).dispatch_agent("t-1")
    assert (exc.value.kind, exc.value.retryable) == ("server", False)
    assert "zhn_live_" not in exc.value.message and "[REDACTED]" in exc.value.message


def test_dispatch_is_not_retried_on_network_errors():
    rec = Recorder(httpx.ConnectError("refused"), httpx.Response(200, json={"success": True}))
    with pytest.raises(ZihinError):
        make_client(rec).dispatch_agent("t-1")
    assert len(rec.requests) == 1


def test_get_execution_terminal_logic():
    body = {"success": True, "data": {"execution": {"id": "x-1", "status": "in-progress"}, "metrics": {"usage": {"total_tokens": 3}}}}
    s = make_client(Recorder(httpx.Response(200, json=body))).get_execution("t-1", "x-1")
    assert (s.status, s.terminal, s.usage.total_tokens) == ("in-progress", False, 3)
    body = {"data": {"execution": {"id": "x-1", "status": "quota_exhausted", "agent_response": "r"}}}
    s = make_client(Recorder(httpx.Response(200, json=body))).get_execution("t-1", "x-1")
    assert (s.terminal, s.result) == (True, "r")
    s = make_client(Recorder(httpx.Response(200, json={}))).get_execution("t-1", "x-1")
    assert (s.status, s.terminal) == ("unknown", False)
