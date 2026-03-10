"""Tests for explorer route helper functions."""
from __future__ import annotations

import json

import pytest

from api.routes.explorer import _data_signature, _flatten_param_value, _merge_meta


# ---------------------------------------------------------------------------
# _flatten_param_value
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "v, expected",
    [
        (42, 42),
        ("hello", "hello"),
        (None, None),
        ({"value": 1.5, "orig": "1,5"}, 1.5),
        ({"orig": "raw"}, "raw"),           # no 'value' key → fallback to 'orig'
        ({"other": "x"}, {"other": "x"}),   # no 'value' or 'orig' → return dict
    ],
)
def test_flatten_param_value(v, expected):
    assert _flatten_param_value(v) == expected


# ---------------------------------------------------------------------------
# _data_signature
# ---------------------------------------------------------------------------

def test_data_signature_deterministic():
    d = {"b": 2, "a": 1}
    sig = _data_signature(d)
    assert sig == json.dumps({"a": 1, "b": 2}, sort_keys=True, ensure_ascii=True, default=str)


def test_data_signature_flattens_nested():
    d = {"x": {"value": 99, "orig": "xx"}}
    sig = _data_signature(d)
    parsed = json.loads(sig)
    assert parsed["x"] == 99


def test_data_signature_same_data_same_sig():
    d1 = {"a": 1, "b": {"value": 2}}
    d2 = {"b": {"value": 2}, "a": 1}
    assert _data_signature(d1) == _data_signature(d2)


# ---------------------------------------------------------------------------
# _merge_meta
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "existing, incoming, expected",
    [
        (None, "Alice", "Alice"),
        ("Alice", None, "Alice"),
        ("Alice", "Alice", "Alice"),            # dedup
        ("Alice", "Bob", "Alice | Bob"),
        ("", "Bob", "Bob"),
        (None, None, ""),
    ],
)
def test_merge_meta(existing, incoming, expected):
    assert _merge_meta(existing, incoming) == expected
