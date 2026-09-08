"""Regression coverage for Railway edge → native Hermes dashboard auth."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import httpx
from starlette.requests import Request

import server


async def _receive_once(body: bytes):
    sent = False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    return receive


class _RecordingClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def request(self, method, url, *, headers, content):
        self.calls.append({"method": method, "url": url, "headers": headers, "content": content})
        return httpx.Response(200, content=b'{"ok":true}', headers={"content-type": "application/json"})


class DashboardProxyHeadersTests(unittest.TestCase):
    def test_edge_proxy_injects_native_session_token_for_dashboard_api(self):
        headers = server._dashboard_proxy_headers(
            {
                "host": "hermes-agent-production.example",
                "cookie": "hermes_auth=edge-session",
                "x-hermes-session-token": "untrusted-client-value",
                "accept": "application/json",
            },
            native_session_token="native-loopback-token",
        )

        self.assertNotIn("host", headers)
        self.assertEqual(headers["cookie"], "hermes_auth=edge-session")
        self.assertEqual(headers["accept"], "application/json")
        self.assertEqual(headers[server._SESSION_TOKEN_HEADER], "native-loopback-token")
        self.assertNotIn("x-hermes-session-token", headers)


class DashboardSessionTokenTests(unittest.IsolatedAsyncioTestCase):
    async def test_session_token_scrape_authenticates_with_held_dashboard_cookie(self):
        class Client:
            def __init__(self):
                self.headers = None

            async def get(self, _url, *, headers, timeout):
                self.headers = headers
                return httpx.Response(
                    200,
                    text='<script>window.__HERMES_SESSION_TOKEN__="native-token"</script>',
                )

        client = Client()
        with patch.object(server, "get_http_client", return_value=client):
            token = await server._get_hermes_session_token(
                {"__Host-hermes_session": "held-session"}
            )

        self.assertEqual(token, "native-token")
        self.assertEqual(client.headers["cookie"], "__Host-hermes_session=held-session")


class DashboardProxyTests(unittest.IsolatedAsyncioTestCase):
    async def test_proxy_uses_scraped_native_token_not_client_token(self):
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/plugins/kanban",
                "query_string": b"limit=1",
                "headers": [
                    (b"host", b"edge.example"),
                    (b"accept", b"application/json"),
                    (b"x-hermes-session-token", b"client-supplied-token"),
                ],
                "client": ("127.0.0.1", 12345),
                "server": ("edge.example", 443),
                "scheme": "https",
            },
            await _receive_once(b'{"read_only":true}'),
        )
        client = _RecordingClient()
        with (
            patch.object(server, "get_http_client", return_value=client),
            patch.object(server.hermes_session, "snapshot", return_value=(1, {"__Host-hermes_session": "held-session"})),
            patch.object(server, "_get_hermes_session_token", return_value="loopback-native-token"),
        ):
            response = await server._proxy_to_dashboard(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(client.calls), 1)
        call = client.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:9119/api/plugins/kanban?limit=1")
        self.assertEqual(call["content"], b'{"read_only":true}')
        self.assertEqual(call["headers"][server._SESSION_TOKEN_HEADER], "loopback-native-token")
        self.assertNotIn("x-hermes-session-token", call["headers"])

    async def test_proxy_strips_native_dashboard_set_cookie_from_response(self):
        class CookieClient(_RecordingClient):
            async def request(self, method, url, *, headers, content):
                self.calls.append({"method": method, "url": url, "headers": headers, "content": content})
                return httpx.Response(
                    200,
                    content=b'{"ok":true}',
                    headers={
                        "content-type": "application/json",
                        "set-cookie": "__Host-hermes_session=server-held; Secure; HttpOnly",
                    },
                )

        request = Request(
            {
                "type": "http", "method": "GET", "path": "/api/status", "query_string": b"",
                "headers": [(b"host", b"edge.example")], "client": ("127.0.0.1", 12345),
                "server": ("edge.example", 443), "scheme": "https",
            },
            await _receive_once(b""),
        )
        client = CookieClient()
        with (
            patch.object(server, "get_http_client", return_value=client),
            patch.object(server.hermes_session, "snapshot", return_value=(1, {"__Host-hermes_session": "held-session"})),
            patch.object(server, "_get_hermes_session_token", return_value=""),
        ):
            response = await server._proxy_to_dashboard(request)

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("set-cookie", response.headers)

    async def test_proxy_allows_gated_dashboard_session_without_spa_token(self):
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/api/status",
                "query_string": b"",
                "headers": [(b"host", b"edge.example")],
                "client": ("127.0.0.1", 12345),
                "server": ("edge.example", 443),
                "scheme": "https",
            },
            await _receive_once(b""),
        )
        client = _RecordingClient()
        with (
            patch.object(server, "get_http_client", return_value=client),
            patch.object(server.hermes_session, "snapshot", return_value=(1, {"__Host-hermes_session": "held-session"})),
            patch.object(server, "_get_hermes_session_token", return_value=""),
        ):
            response = await server._proxy_to_dashboard(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(client.calls), 1)
        call = client.calls[0]
        self.assertEqual(call["headers"]["cookie"], "__Host-hermes_session=held-session")
        self.assertNotIn(server._SESSION_TOKEN_HEADER, call["headers"])


if __name__ == "__main__":
    unittest.main()
