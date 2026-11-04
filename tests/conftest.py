"""Shared helpers: every request is answered by an httpx MockTransport; no real sleeps."""

import json

import httpx
import pytest

import zihin._http as zhttp
from zihin import Zihin

KEY = "zhn_test_" + "x" * 24
UUID = "0b8f5e9c-3f1a-4c55-9d27-6e2a1b7c4d10"


def sse(*events):
    out = []
    for name, data in events:
        out.append(f"event: {name}\ndata: {json.dumps(data)}\n\n")
    return "".join(out).encode()


def make_client(handler, **kwargs):
    return Zihin(KEY, http_client=httpx.Client(transport=httpx.MockTransport(handler)), **kwargs)


class Recorder:
    """Handler wrapper that records requests and answers from a list of responders."""

    def __init__(self, *responders):
        self.responders = list(responders)
        self.requests = []

    def __call__(self, request):
        self.requests.append(request)
        responder = self.responders[min(len(self.requests), len(self.responders)) - 1]
        if isinstance(responder, BaseException):
            raise responder
        if callable(responder):
            return responder(request)
        return responder

    @property
    def bodies(self):
        return [json.loads(r.content) if r.content else None for r in self.requests]


class CutStream(httpx.SyncByteStream):
    """Delivers some bytes, then the connection drops."""

    def __init__(self, first: bytes):
        self.first = first

    def __iter__(self):
        yield self.first
        raise httpx.ReadError("connection reset by peer")


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr(zhttp, "_sleep", slept.append)
    return slept
