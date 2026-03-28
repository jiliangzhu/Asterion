from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from asterion_core.blockchain.rpc_http import build_polygon_rpc_request_kwargs, load_polygon_rpc_headers


class PolygonRpcHeadersTest(unittest.TestCase):
    def test_load_polygon_rpc_headers_supports_api_key_shortcut(self) -> None:
        with patch.dict(os.environ, {"ASTERION_POLYGON_RPC_API_KEY": "secret-key"}, clear=False):
            headers = load_polygon_rpc_headers()
        self.assertEqual(headers, {"x-api-key": "secret-key"})

    def test_build_polygon_rpc_request_kwargs_includes_headers_when_configured(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ASTERION_POLYGON_RPC_HEADER_NAME": "x-api-key",
                "ASTERION_POLYGON_RPC_HEADER_VALUE": "secret-key",
            },
            clear=False,
        ):
            kwargs = build_polygon_rpc_request_kwargs(timeout_seconds=7.5)
        self.assertEqual(kwargs["timeout"], 7.5)
        self.assertEqual(kwargs["headers"], {"x-api-key": "secret-key"})
