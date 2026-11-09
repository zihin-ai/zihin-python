"""Direct invocation: structured input, capability check, outcomes, stream errors, results."""

import httpx
import pytest

from zihin import ZihinError, error_from_done_event, error_from_stream_event

from conftest import UUID, Recorder, make_client, sse


def ok_stream(*events):
    return httpx.Response(200, content=sse(*events))


def raises(events, **invoke_kwargs):
    rec = Recorder(ok_stream(*events))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi", **invoke_kwargs)
    return exc.value


# -- structured input ----------------------------------------------------------------------------


def test_structured_input_goes_on_the_wire_with_server_names():
    accepted = ["context", "bindings", "session_from", "client_message_id", "consumer_key", "options.timeout_ms"]
    rec = Recorder(
        ok_stream(
            ("metadata", {"session_id": "s-d", "accepted_fields": accepted}),
            ("response", {"content": "ok", "session_id": "s-d"}),
            ("done", {"outcome": "COMPLETED"}),
        )
    )
    result = make_client(rec).invoke_agent(
        "a",
        "Status?",
        client_message_id=UUID,
        consumer_key="5511999990000",
        consumer_name="Ana",
        context={"cliente": "Ana", "vip": True, "n": 3, "x": None},
        bindings={"hashFornecedor": "f-81c2"},
        session_from=["hashFornecedor"],
        capabilities={"blocks": ["quick_replies"]},
        timeout_ms=60_000,
        metadata={"trace_id": "t-1"},
    )
    assert rec.bodies[0] == {
        "message": "Status?",
        "client_message_id": UUID,
        "consumer_key": "5511999990000",
        "consumer_display_name": "Ana",
        "capabilities": {"blocks": ["quick_replies"]},
        "context": {"cliente": "Ana", "vip": True, "n": 3, "x": None},
        "bindings": {"hashFornecedor": "f-81c2"},
        "session_from": ["hashFornecedor"],
        "options": {"timeout_ms": 60000},
        "metadata": {"trace_id": "t-1"},
    }
    assert result.accepted_fields == accepted and result.session_id == "s-d"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"client_message_id": "not-a-uuid"},
        {"client_message_id": "0b8f5e9c3f1a4c559d276e2a1b7c4d10"},
        {"context": {"nested": {"a": 1}}},
        {"context": {"list": [1]}},
        {"context": {f"k{i}": i for i in range(51)}},
        {"context": {"big": "x" * 2001}},
        {"context": {"a": "x" * 1000, "b": "y" * 1000}},  # > 2000 chars as JSON
        {"context": {"1bad": "v"}},
        {"context": {"constructor": "v"}},
        {"context": {"nan": float("nan")}},
        {"context": "flat?"},
        {"bindings": {"a": "x" * 2000, **{f"b{i}": "y" * 1990 for i in range(8)}}},  # > 16000
        {"session_from": ["a", "b", "c", "d", "e", "f"]},
        {"session_from": ["k"], "session_id": "s-1", "bindings": {"k": "v"}},
        {"session_from": ["k"], "bindings": {"k": 0}},  # 0/False/'' count as absent
        {"consumer_name": "Ana"},  # name without key
        {"consumer_key": "   "},
        {"consumer_key": "k" * 201},
        {"capabilities": {"blocks": "quick_replies"}},
        {"capabilities": {"blocks": None}},
        {"capabilities": {"blocks": ["b"] * 11}},
        {"timeout_ms": 0},
        {"timeout_ms": "soon"},
        {"attachments": [{"url": "https://10.0.0.5/file.png", "type": "image"}]},
    ],
)
def test_invalid_input_is_config_without_a_request(kwargs):
    rec = Recorder(ok_stream(("response", {"content": "x"})))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi", **kwargs)
    assert exc.value.kind == "config"
    assert rec.requests == []


def test_bindings_accept_up_to_16000_chars():
    rec = Recorder(
        ok_stream(("metadata", {"accepted_fields": ["bindings"]}), ("response", {"content": "ok"}), ("done", {"outcome": "COMPLETED"}))
    )
    big = {f"b{i}": "y" * 1900 for i in range(8)}  # ~15.3k chars as JSON
    assert make_client(rec).invoke_agent("a", "Hi", bindings=big).content == "ok"


def test_stream_agent_validates_before_iterating():
    rec = Recorder(ok_stream(("response", {"content": "x"})))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).stream_agent("a", "Hi", client_message_id="nope")
    assert exc.value.kind == "config" and rec.requests == []


# -- capability check ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "metadata, kwargs",
    [
        ({}, {"context": {"a": 1}}),  # older server: no accepted_fields at all
        ({"accepted_fields": []}, {"context": {"a": 1}}),
        ({"accepted_fields": ["context"]}, {"context": {"a": 1}, "client_message_id": UUID}),
        ({"accepted_fields": [], "ignored_fields": [{"field": "context", "reason": "requires_admin_api_key"}]}, {"context": {"a": 1}}),
        ({"accepted_fields": ["context"], "ignored_fields": [{"field": "context.branch", "reason": "reserved_key"}]}, {"context": {"branch": "x"}}),
        ({"accepted_fields": []}, {"consumer_key": "5511"}),  # consumer_key once the list is published
        ({"accepted_fields": ["context"], "ignored_fields": ["consumer_key"]}, {"consumer_key": "5511", "context": {}}),
    ],
)
def test_field_not_accepted_is_config_and_closes_the_stream(metadata, kwargs):
    rec = Recorder(ok_stream(("metadata", metadata), ("response", {"content": "ran without the fields"}), ("done", {"outcome": "COMPLETED"})))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi", **kwargs)
    assert exc.value.kind == "config"


def test_redacted_field_is_only_a_warning():
    rec = Recorder(
        ok_stream(
            ("metadata", {"accepted_fields": ["context"], "ignored_fields": [{"field": "context.token", "reason": "redacted"}]}),
            ("response", {"content": "ok"}),
            ("done", {"outcome": "COMPLETED"}),
        )
    )
    result = make_client(rec).invoke_agent("a", "Hi", context={"token": "x"})
    assert result.ignored_fields[0].field == "context.token" and result.ignored_fields[0].reason == "redacted"


def test_capability_checked_on_first_turn_event_when_metadata_is_missing():
    rec = Recorder(ok_stream(("token", {"content": "x"}), ("response", {"content": "ok"})))
    events = make_client(rec).stream_agent("a", "Hi", context={"a": 1})
    with pytest.raises(ZihinError) as exc:
        list(events)
    assert exc.value.kind == "config"


def test_gated_turn_without_metadata_does_not_require_the_check():
    rec = Recorder(ok_stream(("status", {"phase": "GATED"}), ("response", {"content": "canned", "gated": True}), ("done", {"outcome": "COMPLETED"})))
    result = make_client(rec).invoke_agent("a", "Hi", context={"a": 1})
    assert result.gated is True and result.content == "canned"


# -- done outcomes -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "outcome, kind, code, retryable",
    [
        ("TIMEOUT", "timeout", "TURN_TIMEOUT", False),
        ("ERROR", "server", "ERROR", False),
        ("CANCELLED", "server", "CANCELLED", False),
        ("SUSPENDED", "blocked", "SUSPENDED", False),
        ("BUDGET_EXCEEDED", "blocked", "BUDGET_EXCEEDED", False),
        ("QUOTA_EXCEEDED", "quota_exceeded", "QUOTA_EXCEEDED", False),
        ("SESSION_BUSY", "blocked", "SESSION_BUSY", True),
        ("CONFLICT", "blocked", "CONFLICT", False),
        ("REFUSED", "blocked", "REFUSED", False),
        ("CONTEXT_LIMIT", "blocked", "CONTEXT_LIMIT", False),
    ],
)
def test_failed_outcomes_raise_instead_of_returning_the_neutral_text(outcome, kind, code, retryable):
    neutral = "I cannot help with that request."
    err = raises(
        [
            ("metadata", {"execution_id": "e-7", "session_id": "s-7"}),
            ("response", {"content": neutral, "session_id": "s-7"}),
            ("done", {"outcome": outcome}),
        ]
    )
    assert (err.kind, err.code, err.retryable) == (kind, code, retryable)
    assert (err.execution_id, err.session_id) == ("e-7", "s-7")
    assert neutral not in str(err)


def test_timeout_clock_is_carried():
    err = raises([("response", {"content": "x"}), ("done", {"outcome": "TIMEOUT", "timeout_clock": "idle"})])
    assert err.timeout_clock == "idle" and "went silent" in err.message


def test_paused_agent_is_blocked_but_pending_approval_is_an_answer():
    err = raises([("status", {"blocked": "agent_paused"}), ("response", {"content": "paused"}), ("done", {"outcome": "PAUSED"})])
    assert (err.kind, err.code) == ("blocked", "PAUSED")
    rec = Recorder(ok_stream(("response", {"content": "awaiting approval"}), ("done", {"outcome": "PAUSED"})))
    assert make_client(rec).invoke_agent("a", "Hi").outcome == "PAUSED"


def test_error_after_gate_uses_the_gate_reason():
    err = raises([("status", {"blocked": "ownership_denied"}), ("response", {"content": "x"}), ("done", {"outcome": "ERROR"})])
    assert (err.kind, err.code) == ("not_found", "OWNERSHIP_DENIED")
    err = raises([("status", {"blocked": "session_load_failed"}), ("response", {"content": "x"}), ("done", {"outcome": "ERROR"})])
    assert (err.kind, err.retryable) == ("blocked", True)


def test_unknown_outcome_passes_through():
    rec = Recorder(ok_stream(("response", {"content": "ok"}), ("done", {"outcome": "SOMETHING_NEW"})))
    assert make_client(rec).invoke_agent("a", "Hi").outcome == "SOMETHING_NEW"


def test_error_from_done_event_is_exported_for_stream_users():
    rec = Recorder(ok_stream(("response", {"content": "x"}), ("done", {"outcome": "REFUSED"})))
    done = [e for e in make_client(rec).stream_agent("a", "Hi") if e.type == "done"][0]
    assert error_from_done_event(done).code == "REFUSED"


# -- event: error ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code, kind, retryable",
    [
        ("SESSION_BUSY", "blocked", True),
        ("IDEMPOTENCY_CONFLICT", "config", False),
        ("CONTEXT_LIMIT", "blocked", False),
        ("CONFLICT", "blocked", False),
        ("NOT_FOUND", "not_found", False),
        ("SERVICE_UNAVAILABLE", "server", False),
        ("RATE_LIMIT_EXCEEDED", "rate_limit", True),
        ("QUOTA_EXCEEDED", "quota_exceeded", False),
        ("TRIAL_EXPIRED", "trial_expired", False),
        ("NO_ACTIVE_SUBSCRIPTION", "no_subscription", False),
        ("VALIDATION_ERROR", "config", False),
        ("ENGINE_ERROR", "server", False),
        ("LLM_TIMEOUT", "timeout", False),  # name fallback for servers without `timed_out`
    ],
)
def test_stream_error_codes_are_classified(code, kind, retryable):
    err = raises(
        [
            ("metadata", {"execution_id": "e-1", "session_id": "s-1"}),
            ("error", {"error": {"code": code, "message": "mensagem do servidor", "request_id": "r-1"}}),
        ]
    )
    assert (err.kind, err.code, err.retryable) == (kind, code, retryable)
    assert (err.execution_id, err.session_id, err.request_id) == ("e-1", "s-1", "r-1")


def test_timed_out_flag_takes_precedence_over_the_code():
    err = raises([("error", {"error": {"code": "SESSION_BUSY", "message": "x", "timed_out": True}})])
    assert (err.kind, err.retryable) == ("timeout", False)
    err = raises([("error", {"error": {"code": "LLM_TIMEOUT", "message": "x", "timed_out": False}})])
    assert err.kind == "server"


def test_only_context_limit_keeps_the_server_message_stripped_and_capped():
    server = "<b>Rodei 2 tools</b> " + "x" * 600
    err = raises([("error", {"error": {"code": "CONTEXT_LIMIT", "message": server}})])
    assert "Server: Rodei 2 tools" in err.message and "<b>" not in err.message
    assert len(err.message.split("Server: ", 1)[1]) <= 300
    err = raises([("error", {"error": {"code": "SESSION_BUSY", "message": "frase do BE"}})])
    assert "frase do BE" not in err.message


def test_session_from_mint_event_is_used_on_errors():
    err = raises([("session", {"session_id": "s-new"}), ("error", {"error": {"code": "ENGINE_ERROR"}})])
    assert err.session_id == "s-new"


def test_error_from_stream_event_is_exported():
    rec = Recorder(ok_stream(("error", {"error": {"code": "SESSION_BUSY"}})))
    ev = list(make_client(rec).stream_agent("a", "Hi"))[0]
    assert ev.type == "error" and error_from_stream_event(ev, session_id="s").retryable is True


# -- results ---------------------------------------------------------------------------------------


def test_result_carries_turn_ids_and_metadata():
    rec = Recorder(
        ok_stream(
            ("metadata", {"execution_id": "e-meta", "client_message_id": UUID, "accepted_fields": ["client_message_id"], "ignored_fields": ["options.temperature"]}),
            ("tool", {"tool": "crm", "call_id": "c1", "success": True}),
            ("phase", {"phase": "answering"}),
            ("action_proposed", {"action": "x"}),
            ("action_forwarded", {"action_id": "act-1"}),
            (
                "response",
                {
                    "content": "Pick one",
                    "session_id": "s-1",
                    "user_message_id": "u-1",
                    "assistant_message_id": "as-1",
                    "sources": [{"url": "https://x"}],
                    "blocks": [{"type": "quick_replies", "data": {"labels": ["A", "B", "", 3]}}],
                },
            ),
            ("metrics", {"execution_id": "e-metrics", "iterations": 2, "execution_time_ms": 1234}),
            ("done", {"outcome": "LENGTH", "truncated": True, "finish_reason": "length", "execution_id": "e-done"}),
        )
    )
    r = make_client(rec).invoke_agent("a", "Hi", client_message_id=UUID, include_raw=True)
    assert r.execution_id == "e-meta"  # metadata -> done -> response -> metrics
    assert (r.client_message_id, r.user_message_id, r.assistant_message_id) == (UUID, "u-1", "as-1")
    assert (r.outcome, r.truncated, r.finish_reason) == ("LENGTH", True, "length")
    assert (r.iterations, r.execution_time_ms) == (2, 1234)
    assert r.suggestions == ["A", "B"] and r.blocks[0]["type"] == "quick_replies"
    assert r.sources == [{"url": "https://x"}] and r.content_source == "response"
    assert [f.field for f in r.ignored_fields] == ["options.temperature"]
    types = [e.type for e in r.raw]
    assert types == ["metadata", "tool", "phase", "action_proposed", "action_forwarded", "response", "metrics", "done"]
    tool = r.raw[1]
    assert (tool.tool, tool.call_id, tool.success) == ("crm", "c1", True)
    assert r.raw[4].action_id == "act-1"


def test_raw_is_empty_by_default():
    rec = Recorder(ok_stream(("response", {"content": "ok"}), ("done", {"outcome": "COMPLETED"})))
    assert make_client(rec).invoke_agent("a", "Hi").raw == []


def test_execution_id_falls_back_to_metrics():
    rec = Recorder(ok_stream(("response", {"content": "ok"}), ("metrics", {"execution_id": "e-m"})))
    assert make_client(rec).invoke_agent("a", "Hi").execution_id == "e-m"


def test_stream_cut_without_done_returns_tokens_marked_as_tokens():
    rec = Recorder(ok_stream(("token", {"content": "par"}), ("token", {"content": "tial"})))
    r = make_client(rec).invoke_agent("a", "Hi")
    assert (r.content, r.content_source) == ("partial", "tokens")


def test_unknown_events_are_typed_unknown():
    rec = Recorder(ok_stream(("brand_new", {"x": 1}), ("response", {"content": "ok"})))
    events = list(make_client(rec).stream_agent("a", "Hi"))
    assert (events[0].type, events[0].event) == ("unknown", "brand_new")


# -- idempotent replay guard -----------------------------------------------------------------------


def _replay(session):
    return ok_stream(
        ("metadata", {"session_id": session, "accepted_fields": ["client_message_id"], "idempotent_replay": True}),
        ("response", {"content": "stored answer", "session_id": session, "idempotent_replay": True}),
        ("done", {"outcome": "COMPLETED", "idempotent_replay": True}),
    )


def test_replay_from_another_session_is_config():
    rec = Recorder(_replay("s-other"))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi", session_id="S-ABC", client_message_id=UUID)
    assert (exc.value.kind, exc.value.code, exc.value.session_id) == ("config", "IDEMPOTENT_REPLAY_OTHER_SESSION", "S-ABC")


def test_replay_in_the_same_session_with_different_case_passes():
    rec = Recorder(_replay("s-abc"))
    r = make_client(rec).invoke_agent("a", "Hi", session_id="S-ABC", client_message_id=UUID.upper())
    assert r.idempotent_replay is True and r.content == "stored answer"


# -- server ids are sanitized ----------------------------------------------------------------------


def test_hostile_ids_are_dropped():
    rec = Recorder(
        ok_stream(
            ("metadata", {"execution_id": "e-1\nBearer abc.def", "session_id": "s" * 129}),
            ("error", {"error": {"code": "ENGINE ERROR\n", "request_id": "r 1", "message": "key zhn_live_" + "a" * 24}}),
        )
    )
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", "Hi")
    err = exc.value
    assert err.execution_id is None and err.session_id is None and err.request_id is None
    assert err.code == "AGENT_ERROR"  # unsafe label dropped
    assert "zhn_live_" not in err.message and "[REDACTED]" in err.message


# -- no raw exceptions ---------------------------------------------------------------------------


@pytest.mark.parametrize("agent_id", [123, None, "", b"a"])
def test_non_string_agent_id_is_config(agent_id):
    rec = Recorder(ok_stream(("response", {"content": "x"})))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent(agent_id, "Hi")
    assert exc.value.kind == "config" and rec.requests == []


def test_non_string_trigger_ids_are_config():
    z = make_client(Recorder(httpx.Response(200, json={})))
    for call in (lambda: z.dispatch_agent(123), lambda: z.get_execution("t", 5), lambda: z.get_trigger(None), lambda: z.create_trigger(1, "n", mode="sync")):
        with pytest.raises(ZihinError) as exc:
            call()
        assert exc.value.kind == "config"


def test_build_request_failure_is_config(monkeypatch):
    z = make_client(Recorder(ok_stream(("response", {"content": "x"}))))

    def boom(*a, **k):
        raise httpx.InvalidURL("bad host")

    monkeypatch.setattr(z._http, "build_request", boom)
    with pytest.raises(ZihinError) as exc:
        z.invoke_agent("a", "Hi")
    assert exc.value.kind == "config"
    with pytest.raises(ZihinError) as exc:
        z.list_agents()
    assert exc.value.kind == "config"


def test_non_httpx_transport_failure_is_wrapped():
    def handler(request):
        raise UnicodeError("idna failure inside a custom transport")

    with pytest.raises(ZihinError) as exc:
        make_client(handler).invoke_agent("a", "Hi")
    assert (exc.value.kind, exc.value.retryable) == ("network", False)


# -- P2 ------------------------------------------------------------------------------------------


def test_attachment_type_is_required_and_never_null():
    rec = Recorder(ok_stream(("response", {"content": "x"})))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a", None, attachments=[{"url": "https://files.example.com/a.png"}])
    assert exc.value.kind == "config" and rec.requests == []
    make_client(rec).invoke_agent("a", None, attachments=[{"url": "https://files.example.com/a.png", "type": "image"}])
    assert rec.bodies[0]["attachments"] == [{"url": "https://files.example.com/a.png", "type": "image"}]


def test_error_body_read_is_capped():
    sent = []

    class Huge(httpx.SyncByteStream):
        def __iter__(self):
            for _ in range(256):
                sent.append(1)
                yield b"x" * 8192  # 2 MiB in total if fully read

    with pytest.raises(ZihinError) as exc:
        make_client(Recorder(httpx.Response(400, stream=Huge()))).invoke_agent("a", "Hi")
    assert exc.value.status == 400
    assert len(sent) * 8192 <= 64 * 1024 + 8192


def test_sse_with_bom_is_parsed():
    body = b"\xef\xbb\xbf" + sse(("response", {"content": "ok"}), ("done", {"outcome": "COMPLETED"}))
    assert make_client(Recorder(httpx.Response(200, content=body))).invoke_agent("a", "Hi").content == "ok"


# -- second review: remaining raw exceptions -----------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"metadata": "abc"},
        {"metadata": [1]},
        {"message": "\ud800"},
        {"session_id": "s-\udfff"},
        {"context": {"a": "\ud800"}},
        {"attachments": [{"url": "https://files.example.com/\ud800", "type": "image"}]},
    ],
)
def test_more_invalid_input_is_config(kwargs):
    rec = Recorder(ok_stream(("response", {"content": "x"})))
    with pytest.raises(ZihinError) as exc:
        args = {"message": "Hi", **kwargs}
        make_client(rec).invoke_agent("a", **args)
    assert exc.value.kind == "config" and rec.requests == []


def test_agent_id_with_surrogate_is_config():
    rec = Recorder(ok_stream(("response", {"content": "x"})))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).invoke_agent("a\ud800", "Hi")
    assert exc.value.kind == "config" and rec.requests == []


@pytest.mark.parametrize("payload", [[1], "abc", 5])
def test_dispatch_payload_must_be_a_mapping(payload):
    rec = Recorder(httpx.Response(200, json={"success": True}))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).dispatch_agent("t-1", payload)  # type: ignore[arg-type]
    assert exc.value.kind == "config" and rec.requests == []


def test_trigger_id_with_surrogate_is_config():
    rec = Recorder(httpx.Response(200, json={"success": True}))
    with pytest.raises(ZihinError) as exc:
        make_client(rec).dispatch_agent("t\ud800")
    assert exc.value.kind == "config" and rec.requests == []
