# zihin

Official Python client for [Zihin.ai](https://zihin.ai), a platform for building and running hosted
AI agents. Use it to invoke an agent from your backend (buffered or streaming) or through webhook
triggers.

It follows the same contract as the Node.js client
[`@zihin/agent-client`](https://www.npmjs.com/package/@zihin/agent-client) 0.7: structured input,
idempotency, turn outcomes, error kinds, retry policy and SSRF guard behave the same way. Durable
tasks (`/api/v1/tasks`) and request signing are not implemented here yet.

## Install

```bash
pip install zihin
```

Requires Python 3.9 or later. The only dependency is `httpx`.

## Usage

Create an API key in the Zihin console. Keys look like `zhn_live_…`, `zhn_test_…` or `zhn_dev_…`.
The client reads `ZIHIN_API_KEY` from the environment when no key is passed.

```python
from zihin import Zihin

with Zihin() as zihin:
    for agent in zihin.list_agents():
        print(agent.id, agent.name)

    result = zihin.invoke_agent("AGENT_ID", "What is the status of order 1234?")
    print(result.content)       # the agent's answer
    print(result.usage)         # input/output/total tokens and cost
    print(result.execution_id)  # correlate with the Zihin console

    # Continue the same conversation
    follow_up = zihin.invoke_agent("AGENT_ID", "And when does it ship?", session_id=result.session_id)
```

### Streaming

```python
for event in zihin.stream_agent("AGENT_ID", "Hello"):
    if event.type == "token":
        print(event.content, end="", flush=True)
    elif event.type == "tool":
        print("\n[tool]", event.tool, event.success)
    elif event.type == "action_proposed":
        print("\n[needs approval]", event.data)
```

Events are normalized: `metadata`, `status`, `phase`, `thinking`, `tool`, `action_proposed`,
`action_forwarded`, `token`, `warning`, `session`, `response`, `metrics`, `done`, `error`, and
`unknown` for event types this version does not know (the raw name is in `event.event`).

`token` events are the model's working text across iterations. The answer the agent delivers is
the `response` event, which is what `invoke_agent` returns. Stopping the iteration early closes the
connection and cancels the turn on the server. `stream_agent` yields the `error` event instead of
raising; apply the same rules as `invoke_agent` with `error_from_stream_event(event, ...)` and
`error_from_done_event(event, ...)`.

## Structured input, identity and derived sessions

```python
zihin.invoke_agent(
    "AGENT_ID",
    "What is the status of my order?",
    consumer_key="5511999990000",               # who the person is
    consumer_name="Ana",                        # shown in the console's contacts
    context={"customer": "Ana", "plan": "pro"}, # the MODEL reads it
    bindings={"supplier_hash": "f-81c2"},       # only the TOOLS receive it, never the prompt
    session_from=["supplier_hash"],             # derive the session id from these fields
    capabilities={"blocks": ["quick_replies"]}, # my UI renders reply suggestions
)
```

| Field | Who sees it | Use it for |
|---|---|---|
| `context` | model + tools | data the agent should consider in the answer |
| `bindings` | tools only | internal ids, lookup keys, nothing the model needs to read |
| `metadata` | nobody in the turn | opaque tracing (`trace_id`, `correlation_id`) |

- **Never put credentials in any of them.** Use a tenant secret.
- `context` is **stored** with the message and repeated in every turn; the server redacts secrets in
  it by key name and value format, and reports `{"field": "context.<name>", "reason": "redacted"}`
  in `ignored_fields` (the turn still runs). `bindings` is neither stored nor redacted.
- `context` and `bindings` are **flat** (string, number, boolean or `None`). The server's limits are
  checked locally before spending a turn: up to 50 fields; names match
  `^[A-Za-z_][A-Za-z0-9_-]{0,63}$`; text values up to 2,000 characters; total (as JSON) up to
  **2,000** for `context` and 16,000 for `bindings`. `session_from` takes up to 5 names, does not
  combine with `session_id`, and at least one named field needs a value (`0`, `False` and `""` count
  as absent).
- `context`, `bindings`, `session_from` and `consumer_key` only apply with an **admin/owner** API key.
- **A dropped field never passes silently.** If you sent `context`/`bindings`/`session_from`/
  `client_message_id` and the stream's `metadata` has no `accepted_fields` (older server), lists the
  field in `ignored_fields` (`requires_admin_api_key`, `reserved_key`), or does not accept it, the
  client raises `ZihinError` with `kind == "config"` and closes the stream to cancel the turn. The
  answer is not used, but the message may already be recorded in the session, so resending after
  fixing it can create two entries in the conversation history.
- `consumer_key` must be the **same value** a webhook used if you want to continue its conversations.
  Use one `consumer_key` per person: changing only the name does not separate people.
- `capabilities` is a **request**, not a requirement: a server that does not know it just lists it in
  `ignored_fields`. `result.suggestions` holds the labels of the `quick_replies` block; tapping one
  means sending its text as the next `message`.

## Idempotency

```python
import uuid

result = zihin.invoke_agent(
    "AGENT_ID",
    "Close order 42",
    client_message_id=str(uuid.uuid4()),  # a NEW UUID per message; resend the SAME one to retry
)
result.idempotent_replay  # True = the server replayed the stored answer (the agent did not run again)
```

- With `client_message_id` (a UUID, validated locally), the client also **retries the POST** on
  network errors and 5xx **before the stream opens**. Without it, a POST is never retried on a network
  error, since that could duplicate the turn.
- Resending the same id with the same message: turn still in flight → `SESSION_BUSY` (`blocked`,
  retryable); turn finished with `COMPLETED`/`LENGTH` → the server replays the stored answer for 24 h,
  with `idempotent_replay=True` and no `usage`. Same id with a **different** message →
  `IDEMPOTENCY_CONFLICT` (`config`).
- **The guarantee covers only a finished turn.** A turn cut mid-stream, cancelled, failed or timed out
  releases the id, and a resend runs again, tools included. That is why the automatic retry happens
  only before the stream opens, and why a stream cut mid-way raises `network` with
  `retryable=False`. It is best-effort (no dedupe if the server's Redis is down).
- The scope is the API key + `consumer_key` (not the session) and only message + attachments are
  compared: **never use a fixed value** (a scheduled automation would get the same answer for 24 h).
  Reusing the id in another session raises `config` with `code == "IDEMPOTENT_REPLAY_OTHER_SESSION"`
  (the session comparison is case-insensitive: the server returns ids in lowercase). That guard only
  exists in `invoke_agent` **with an explicit `session_id`**: with `session_from`, without a session,
  or in `stream_agent`, a reused id silently returns the original conversation's answer and session.
- `SERVICE_UNAVAILABLE` (replay without a recoverable answer) is **not** retryable: it can last 24 h,
  and only a new `client_message_id` gets out of it.
- Against a server that does not list `client_message_id` in `accepted_fields`, the turn is refused
  (`config`): the guarantee you asked for would not exist. Before that `metadata` arrives, the
  pre-stream retry may already have happened without the server honouring the key; it is still safe
  because the server sends the stream headers before doing any work.

## What is not an answer

`AgentResult.content_source` says where `content` came from:

- `"response"`: the server's final event. This is the text that is safe to publish. An empty
  `content` here is the server's decision; the client never fills it with tokens.
- `"tokens"`: the stream ended without the final event and only the raw concatenation of the deltas
  was left, **with the model's internal deliberation**. Treat it as a diagnostic partial.

When the server closes a turn whose text is a failure or refusal notice, `invoke_agent` **raises**
instead of returning that text. The `code` is the outcome:

| Outcome | `kind` | Retryable |
|---|---|---|
| `TIMEOUT` (`code == "TURN_TIMEOUT"`, with `timeout_clock`) | `timeout` | no |
| `ERROR`, `CANCELLED` | `server` | no |
| `REFUSED` (the model provider refused under its policy) | `blocked` | no |
| `CONTEXT_LIMIT` (more context than the model can process) | `blocked` | no |
| `SUSPENDED`, `BUDGET_EXCEEDED`, `CONFLICT` | `blocked` | no |
| `SESSION_BUSY` (the turn did not start) | `blocked` | **yes** |
| `QUOTA_EXCEEDED` | `quota_exceeded` | no |
| `PAUSED` of a paused/archived agent | `blocked` | no |

`PAUSED` of a pending approval, `COMPLETED`, `LENGTH` (with `truncated`/`finish_reason`) and new
outcomes are returned in `AgentResult.outcome`. A canned platform-gate answer (human handoff,
denylist) returns with `gated=True`: no model ran.

Errors the server sends as `event: error` are classified by code: `SESSION_BUSY` → `blocked`
(retryable), `IDEMPOTENCY_CONFLICT`/`VALIDATION_ERROR` → `config`, `CONTEXT_LIMIT`/`CONFLICT` →
`blocked`, `NOT_FOUND` → `not_found`, `SERVICE_UNAVAILABLE` → `server` (not retryable),
`RATE_LIMIT_EXCEEDED` → `rate_limit`, and the commercial codes below. The explicit `timed_out` flag
wins over the code. The error carries the turn's `execution_id` and `session_id`.

## Errors

Every failure raises `ZihinError`. Branch on `kind` and `code`, never on the message:

```python
from zihin import Zihin, ZihinError

try:
    Zihin().invoke_agent("AGENT_ID", "Hi")
except ZihinError as err:
    if err.kind == "blocked" and err.code == "REFUSED":
        ...  # the provider refused; do not show the text as an answer
    elif err.retryable:
        ...  # rate_limit, server, network before the stream, SESSION_BUSY
    print(err.kind, err.code, err.status, err.request_id, err.execution_id)
```

| `kind` | Meaning |
|---|---|
| `config` | Invalid key, arguments or base URL, or a field the server did not apply |
| `auth` | The key was rejected, or its role does not allow the action |
| `trial_expired` | The trial has ended: upgrade the plan (the key is fine) |
| `no_subscription` | No active subscription: activate a plan (the key is fine) |
| `quota_exceeded` | Token quota exhausted: wait for the next cycle or upgrade (retrying does not help) |
| `not_found` | The agent, trigger or execution does not exist or is not accessible with this key |
| `rate_limit` | Too many requests |
| `timeout` | The request, the stream or the agent turn timed out |
| `blocked` | The turn was refused (provider refusal, context limit, spending cap, busy session, paused agent, plan limit) |
| `server` | The Zihin server or the agent turn failed |
| `network` | The connection failed |
| `protocol` | The response did not match the API contract |
| `unknown` | An unexpected HTTP status |
| `aborted` | Reserved for parity with the Node.js client; the sync client does not raise it |

The error also has `status`, `retryable`, `request_id`, `execution_id`, `session_id`, `endpoint`
(`"POST host/path"`), `attempts` and, for a turn timeout, `timeout_clock`. Messages redact secrets
(API keys, Bearer tokens, stack traces); `request_id` is kept, since it is what support needs to
find the request. A 403 with `AGENT_ACCESS_DENIED` is reported as `not_found`: the problem is the
agent ID, not the key.

## Webhook triggers

```python
ref = zihin.find_or_create_trigger("AGENT_ID", "orders-sync", mode="sync")

result = zihin.dispatch_agent(ref.trigger_id, {"message": "Summarize order 42"})
print(result.agent_response, result.outcome, result.usage)

# Async: the server calls your callback; poll as an alternative
ref = zihin.create_trigger("AGENT_ID", "orders-async", callback_url="https://hooks.example.com/zihin")
accepted = zihin.dispatch_agent(ref.trigger_id, {"message": "Hi"})
status = zihin.get_execution(ref.trigger_id, accepted.execution_id)
if status.terminal:
    print(status.status, status.result)
```

- Requires an admin or editor key (deleting requires admin/owner).
- `query_extraction` picks the message from the payload: `{"mode": "field", "field": "message"}`
  (default), `{"mode": "template", "template": "..."}` or `{"mode": "full_body"}`.
- A sync dispatch reads `outcome` before `success`: `REFUSED`, `CONTEXT_LIMIT`, `TIMEOUT`, `ERROR`…
  arrive with `success: true` and the platform's sentence in `agent_response`, and the client raises
  the same classification as the stream (with `status == 200` and the trigger's `execution_id`).
- `update_trigger` merges config changes into the current `trigger_config`, since the server replaces
  it as a whole; `list_triggers`, `get_trigger` and `delete_trigger` are also available.
- A dispatch is never retried on a network error (it could run twice).

## Configuration

```python
Zihin(
    api_key="zhn_live_...",          # default: ZIHIN_API_KEY
    base_url="https://llm.zihin.ai", # default; must be a Zihin domain unless allowed below
    allow_custom_base_url=False,     # explicit toggle for a gateway/proxy URL
    allow_http=False,                # http:// and localhost, for local development only
    timeout=None,                    # seconds; overrides EVERY operation's default
    stream_timeout=None,             # seconds; overrides only the agent stream
    max_retries=2,
    on_retry=lambda info: print(info),
)
```

- **Deadlines cover the whole operation**, stream included. Defaults are per operation: stream
  200 s, trigger dispatch 260 s, short requests 35 s, each above the server's own budget, so the
  server's structured outcome (`TURN_TIMEOUT` with ids) arrives before the client gives up. Per call,
  `timeout_ms=` goes to the server as `options.timeout_ms` (it can only shorten the platform's
  deadline), and the local deadline is 30 s above it, because the server's turn clock starts only
  after loading tools. Retries do not shrink the deadline of the next attempt; the time spent on
  retries is capped at 60 s.
- **Retries never duplicate side effects.** GET retries network errors, 408, 429 and 5xx; a POST
  (invoke, dispatch, create) retries only 429/503, plus network errors/5xx before the stream opens
  when it carries `client_message_id`. Backoff is exponential (1 s → 10 s) with jitter; `Retry-After`
  is honoured. Commercial 429s (quota, subscription) are never retried.
- **Redirects are rejected**, not followed: a 3xx to another host would leak the API key.
- **SSRF guard**: the base URL must be a Zihin domain unless `allow_custom_base_url=True`, and
  private/loopback/link-local addresses are always refused (IPv4 in any notation, IPv6 by range,
  IPv4-mapped IPv6, cloud metadata hosts, trailing-dot names). The check is by host name, not by the
  resolved IP: with a custom base URL, a name that resolves to a private IP passes, so keep the
  whitelist in sensitive environments.
- **Memory limits**: `max_stream_event_bytes` (1 MB) and `max_stream_total_bytes` (64 MB); exceeding
  them raises `protocol`.
- `include_raw=True` keeps every stream event in `AgentResult.raw` (empty by default).

An agent turn costs LLM tokens on your Zihin workspace.

## Upgrading from 0.1.0

0.2.0 changes behaviour (pre-1.0, breaking changes bump the minor version):

- **`REFUSED` and `CONTEXT_LIMIT` now raise** `ZihinError(kind="blocked")`. In 0.1.0 the provider's
  neutral refusal text came back as `result.content`. The same applies to every outcome in the table
  above (only `TIMEOUT` raised before).
- **The `agent` kind is gone.** `event: error` is classified by code; `SESSION_BUSY` is now `blocked`
  and **retryable**. Unknown codes are `server`. HTTP statuses without a mapping are `unknown`
  (was `protocol`).
- **`base_url` outside the Zihin domains requires `allow_custom_base_url=True`**, and `http://`
  requires `allow_http=True`.
- **Redirects are rejected**: a 3xx raises a non-retryable `network` error (0.1.0 reported it as
  `protocol`, or followed it when the injected `httpx.Client` had `follow_redirects=True`).
- API keys must match `zhn_(live|test|dev)_` followed by at least 20 letters or digits.
- `timeout` now defaults to `None` and, when set, overrides every operation (it was the short-request
  timeout). `stream_timeout` still overrides only the stream.
- GET requests are retried (network errors, 408, 429, 5xx); POSTs only on 429/503.
- A stream that ends cleanly without `done` or `response` returns the token text with
  `content_source == "tokens"` instead of raising `protocol`.
- `StreamEvent.type` is `unknown` for unrecognized events (the raw name is in `event`), and
  `ZihinError.message` now includes the endpoint, attempts and ids.

## Development

```bash
pip install -e ".[dev]"
pytest
```

The tests are offline: every request is answered by an `httpx.MockTransport`.

## License

MIT
