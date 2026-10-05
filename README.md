# zihin

Official Python client for [Zihin.ai](https://zihin.ai), a platform for building and running hosted
AI agents. Use it to invoke an agent from your backend and get its answer, buffered or as a stream.

This is an early release (0.1.x). It covers the direct invocation path. Structured context,
attachments, async triggers and tasks, which the Node.js client
[`@zihin/agent-client`](https://www.npmjs.com/package/@zihin/agent-client) already has, are not
implemented here yet.

## Install

```bash
pip install zihin
```

Requires Python 3.9 or later.

## Usage

Create an API key in the Zihin console. Keys start with `zhn_live_`, `zhn_test_` or `zhn_dev_`.
The client reads `ZIHIN_API_KEY` from the environment when no key is passed.

```python
from zihin import Zihin

with Zihin() as zihin:
    for agent in zihin.list_agents():
        print(agent.id, agent.name)

    result = zihin.invoke_agent("AGENT_ID", "What is the status of order 1234?")
    print(result.content)

    # Continue the same conversation
    follow_up = zihin.invoke_agent("AGENT_ID", "And when does it ship?", session_id=result.session_id)
```

### Streaming

```python
for event in zihin.stream_agent("AGENT_ID", "Hello"):
    if event.type == "token":
        print(event.content, end="", flush=True)
    elif event.type == "tool":
        print("\n[tool]", event.data)
```

`token` events are the model's working text across iterations. The answer the agent delivers is the
`response` event, which is what `invoke_agent` returns. Stopping the iteration early closes the
connection and cancels the turn on the server.

### Errors

Every failure raises `ZihinError`, with a `kind` you can branch on:

```python
from zihin import Zihin, ZihinError

try:
    Zihin().invoke_agent("AGENT_ID", "Hi")
except ZihinError as err:
    print(err.kind, err.status, err.code, err.message)
```

| `kind` | Meaning |
|---|---|
| `config` | Missing or malformed API key, or missing arguments |
| `auth` | The key was rejected, or its role does not allow the action |
| `not_found` | The agent does not exist or is not accessible with this key |
| `rate_limit` | Too many requests |
| `timeout` | The request or the agent turn timed out |
| `agent` | The agent turn ended with an error reported by the server |
| `server` | The Zihin server failed (5xx) |
| `network` | The connection failed |
| `protocol` | The response did not match the API contract |

`request_id`, `execution_id` and `session_id` are set on the error when the server provides them;
include them when contacting support.

## Configuration

```python
Zihin(
    api_key="zhn_live_...",          # default: ZIHIN_API_KEY
    base_url="https://llm.zihin.ai", # default
    timeout=35.0,                     # seconds, for short requests
    stream_timeout=200.0,             # seconds, for an agent turn
)
```

The stream timeout is above the server's own turn deadline on purpose, so the client receives the
server's structured outcome instead of cutting the connection first.

An agent turn costs LLM tokens on your Zihin workspace.

## Development

```bash
pip install -e ".[dev]"
pytest
```

The tests are offline: every request is answered by a mock transport.

## License

MIT
