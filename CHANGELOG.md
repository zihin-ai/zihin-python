# Changelog

## 0.2.0 (2026-11-04)

Contract parity with the Node.js client `@zihin/agent-client` 0.7.0 (server `server-llm` of
October 2026). Minor because behaviour changes: turns that used to return now raise.

### Behaviour changes

- `REFUSED` (the model provider refused) and `CONTEXT_LIMIT` now raise `ZihinError(kind="blocked")`
  with the outcome in `code`; the platform's neutral text is never returned as an answer. Every
  failed outcome of `done` raises: `ERROR`/`CANCELLED` → `server`, `SUSPENDED`/`BUDGET_EXCEEDED`/
  `CONFLICT`/`SESSION_BUSY` and the `PAUSED` of a paused agent → `blocked`, `QUOTA_EXCEEDED` →
  `quota_exceeded`. `ERROR` after a `status.blocked` gate uses the gate reason.
- `event: error` is classified by code (`SESSION_BUSY` → `blocked` retryable, `IDEMPOTENCY_CONFLICT`/
  `VALIDATION_ERROR` → `config`, `CONTEXT_LIMIT`/`CONFLICT` → `blocked`, `NOT_FOUND` → `not_found`,
  `SERVICE_UNAVAILABLE` → `server` not retryable, rate/quota/trial/subscription codes), with
  `timed_out` taking precedence. The `agent` kind is removed.
- Error kinds aligned with the Node.js client: `trial_expired`, `no_subscription`, `quota_exceeded`,
  `blocked`, `aborted` and `unknown` added; 401/403/404/408/429/5xx and `PLAN_LIMIT_REACHED` mapped
  the same way; commercial 403/429 are never retried. `ZihinError` gains `retryable`, `endpoint`,
  `attempts` and `timeout_clock`; messages redact API keys, Bearer tokens and stack traces.
- `base_url` must be a Zihin domain unless `allow_custom_base_url=True`; `http://` requires
  `allow_http=True`; private, loopback and link-local addresses are refused (IPv6 by range).
- Redirects are rejected instead of followed.
- API key format checked as `zhn_(live|test|dev)_[A-Za-z0-9]{20,}`.
- Retry policy: GET retries network errors, 408, 429 and 5xx; POST retries only 429/503 (with
  `Retry-After`); with `client_message_id`, the stream POST also retries network errors/5xx before
  the headers arrive. A stream cut mid-way raises `network` with `retryable=False`. Exponential
  backoff 1 s → 10 s with jitter.
- Deadlines cover the whole operation (per-operation defaults: stream 200 s, dispatch 260 s, short
  35 s). `timeout` defaults to `None` and overrides every operation when set.
- A stream that ends without `done` and without `response` returns the tokens with
  `content_source="tokens"` instead of raising.
- Unrecognized stream events have `type="unknown"`.

### Added

- Structured input in `invoke_agent`/`stream_agent`: `consumer_key`, `consumer_name`, `context`,
  `bindings`, `session_from`, `capabilities`, `client_message_id`, `attachments`, `metadata`,
  `timeout_ms` (sent as `options.timeout_ms`) and `include_raw`, validated locally with the server's
  limits. Capability check against `accepted_fields`/`ignored_fields`: a requested field the server
  did not apply raises `config` and closes the stream.
- `AgentResult`: `content_source`, `outcome`, `finish_reason`, `truncated`, `execution_id` (from
  `metadata`, then `done`, `response`, `metrics`), `client_message_id`, `idempotent_replay`,
  `user_message_id`, `assistant_message_id`, `accepted_fields`, `ignored_fields`, `gated`, `blocks`,
  `suggestions`, `sources`, `iterations`, `execution_time_ms`, `raw`.
- `IDEMPOTENT_REPLAY_OTHER_SESSION` guard (case-insensitive session comparison).
- Normalized `StreamEvent` fields for `metadata`, `status`, `phase`, `tool`, `action_proposed`,
  `action_forwarded`, `done`, `error`, `response` and `metrics`.
- Webhook triggers: `create_trigger`, `find_or_create_trigger`, `list_triggers`, `get_trigger`,
  `update_trigger`, `delete_trigger`, `dispatch_agent` (sync reads `outcome` before `success`) and
  `get_execution`.
- Exported helpers: `error_from_stream_event`, `error_from_done_event`, `error_from_outcome`,
  `classify_stream_error_kind`, `is_commercial_block`.
- `Agent.tools_count`; SSE size limits (`max_stream_event_bytes`, `max_stream_total_bytes`);
  `on_retry` hook; `Literal` types for kinds and outcomes.

## 0.1.0 (2026-10-05)

First release: `Zihin` client with `list_agents`, `invoke_agent` and `stream_agent`, a single
`ZihinError` with a `kind` to branch on, and an offline test suite.
