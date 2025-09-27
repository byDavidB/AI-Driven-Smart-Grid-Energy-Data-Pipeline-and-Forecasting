from unittest.mock import patch, MagicMock

from streamlit import runtime  # type: ignore  # noqa: F401  (ensures streamlit caches are initialized if needed)

import streamlit as st  # noqa: F401

import streamlit.app as app_module  # type: ignore


def test_fetch_json_success():
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "ok"}
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response) as mock_get:
        data = app_module.fetch_json("http://example.com/health")
        assert data["status"] == "ok"
        mock_get.assert_called_once()


def test_fetch_json_error():
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("boom")

    with patch("requests.get", return_value=mock_response):
        try:
            app_module.fetch_json("http://example.com/health")
        except Exception as exc:  # noqa: BLE001
            assert "boom" in str(exc)
        else:  # pragma: no cover
            assert False, "Expected exception not raised"
