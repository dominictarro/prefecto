"""
Tests for the logging module.
"""

from __future__ import annotations

import logging

from prefecto.logging import get_prefect_or_default_logger


def test_get_prefect_or_default_logger():
    """Tests `get_prefect_or_default_logger`."""
    assert get_prefect_or_default_logger().__class__ == logging.RootLogger
    assert logging.getLogger("not root").__class__ == logging.Logger
    assert (
        get_prefect_or_default_logger(logging.Logger("not root")).__class__
        == logging.Logger
    )
