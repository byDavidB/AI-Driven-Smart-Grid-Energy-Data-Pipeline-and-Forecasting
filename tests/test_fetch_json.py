from importlib import util as importlib_util
from pathlib import Path
from unittest.mock import patch, MagicMock

from streamlit import runtime  # type: ignore  # noqa: F401  (ensures streamlit caches are initialized if needed)

import streamlit as st  # noqa: F401

# Load the dashboard module directly from the repo to avoid clashing with the third-party streamlit package
_streamlit_app_path = Path(__file__).resolve().parents[1] / "streamlit" / "app.py"
_spec = importlib_util.spec_from_file_location("streamlit_dashboard_app", _streamlit_app_path)
app_module = importlib_util.module_from_spec(_spec)
assert _spec and _spec.loader  # narrow type checkers
_spec.loader.exec_module(app_module)


def test_fetch_json_success():
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "ok"}
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response) as mock_get:
        data = app_module.fetch_json("http://example.com/health")
        assert data["status"] == "ok"
        mock_get.assert_called_once()


def test_fetch_json_error():
    # Clear cache so error path isn't short-circuited by previous call
    app_module.fetch_json.clear()

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("boom")

    with patch("requests.get", return_value=mock_response):
        try:
            app_module.fetch_json("http://example.com/health")
        except Exception as exc:  # noqa: BLE001
            assert "boom" in str(exc)
        else:  # pragma: no cover
            assert False, "Expected exception not raised"
