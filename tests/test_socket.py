"""Real sockets on localhost (allow_http=True): total deadline and closing the stream."""

import socket
import threading
import time

import pytest

from zihin import Zihin, ZihinError

from conftest import KEY

SSE_HEADERS = (
    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n"
)


def chunk(data: bytes) -> bytes:
    return b"%x\r\n%s\r\n" % (len(data), data)


class Server:
    """One-shot HTTP server: reads a request, then runs ``script(conn)`` in its own thread."""

    def __init__(self, script):
        self.script = script
        self.closed_at = None
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(5)
        self.port = self.sock.getsockname()[1]
        self.stop = threading.Event()
        threading.Thread(target=self._serve, daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def _serve(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        data = b""
        while b"\r\n\r\n" not in data:
            part = conn.recv(65536)
            if not part:
                return
            data += part
        head, _, rest = data.partition(b"\r\n\r\n")
        length = 0
        for line in head.split(b"\r\n"):
            if line.lower().startswith(b"content-length:"):
                length = int(line.split(b":")[1])
        while len(rest) < length:
            rest += conn.recv(65536)
        try:
            self.script(self, conn)
        except OSError:
            pass
        finally:
            conn.close()

    def wait_for_client_close(self, conn, timeout):
        conn.settimeout(timeout)
        try:
            while conn.recv(1024):
                pass
            self.closed_at = time.monotonic()
        except (socket.timeout, OSError):
            pass

    def close(self):
        self.stop.set()
        self.sock.close()


@pytest.fixture
def server():
    created = []

    def make(script):
        s = Server(script)
        created.append(s)
        return s

    yield make
    for s in created:
        s.close()


def client(srv, **kwargs):
    return Zihin(KEY, base_url=srv.url, allow_http=True, **kwargs)


T = 2.0


def test_stream_deadline_holds_when_a_ping_arrives_just_before_it_and_then_silence(server):
    def script(srv, conn):
        conn.sendall(SSE_HEADERS)
        time.sleep(0.9 * T)
        conn.sendall(chunk(b": ping\n\n"))
        time.sleep(10)

    srv = server(script)
    started = time.monotonic()
    with pytest.raises(ZihinError) as exc:
        client(srv, stream_timeout=T).invoke_agent("a", "Hi")
    elapsed = time.monotonic() - started
    assert exc.value.kind == "timeout"
    assert elapsed < T + 0.5


def test_stream_deadline_holds_against_trickled_headers(server):
    def script(srv, conn):
        conn.sendall(b"HTTP/1.1 200 OK\r\n")
        for _ in range(100):
            conn.sendall(b"X")
            time.sleep(0.3)

    srv = server(script)
    started = time.monotonic()
    with pytest.raises(ZihinError) as exc:
        client(srv, stream_timeout=T).invoke_agent("a", "Hi")
    assert exc.value.kind == "timeout"
    assert time.monotonic() - started <= T + 0.5


def test_buffered_deadline_holds_against_trickled_headers(server):
    def script(srv, conn):
        conn.sendall(b"HTTP/1.1 200 OK\r\n")
        for _ in range(100):
            conn.sendall(b"X")
            time.sleep(0.3)

    srv = server(script)
    started = time.monotonic()
    with pytest.raises(ZihinError) as exc:
        client(srv, timeout=T, max_retries=0).list_agents()
    assert exc.value.kind == "timeout"
    assert time.monotonic() - started <= T + 0.5


def test_buffered_deadline_holds_against_a_trickled_body(server):
    def script(srv, conn):
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n")
        for _ in range(100):
            conn.sendall(b" ")
            time.sleep(0.3)

    srv = server(script)
    started = time.monotonic()
    with pytest.raises(ZihinError) as exc:
        client(srv, timeout=T, max_retries=0).list_agents()
    assert exc.value.kind == "timeout"
    assert time.monotonic() - started <= T + 0.5


def test_closing_the_stream_closes_the_connection(server):
    def script(srv, conn):
        conn.sendall(SSE_HEADERS + chunk(b'event: token\ndata: {"content": "a"}\n\n'))
        srv.wait_for_client_close(conn, timeout=5)

    srv = server(script)
    events = client(srv).stream_agent("a", "Hi")
    assert next(events).type == "token"
    closed = time.monotonic()
    events.close()
    deadline = time.monotonic() + 1.0
    while srv.closed_at is None and time.monotonic() < deadline:
        time.sleep(0.02)
    assert srv.closed_at is not None and srv.closed_at - closed <= 1.0


def test_with_block_closes_the_connection(server):
    def script(srv, conn):
        conn.sendall(SSE_HEADERS + chunk(b'event: token\ndata: {"content": "a"}\n\n'))
        srv.wait_for_client_close(conn, timeout=5)

    srv = server(script)
    with client(srv).stream_agent("a", "Hi") as events:
        for _ in events:
            break
    deadline = time.monotonic() + 1.0
    while srv.closed_at is None and time.monotonic() < deadline:
        time.sleep(0.02)
    assert srv.closed_at is not None
