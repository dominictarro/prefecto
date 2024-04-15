"""
Prefect logging utilities.
"""

from __future__ import annotations

import logging

from prefect.logging import get_run_logger


def get_prefect_or_default_logger(
    __default: logging.Logger | str | None = None,
) -> logging.Logger:
    """Gets the Prefect logger if the global context is set. Returns the `__default` or
    root logger if not.
    """
    # Type check the default logger
    if not isinstance(__default, (logging.Logger, str, type(None))):
        raise TypeError(
            f"Expected `__default` to be a `logging.Logger`, `str`, or `None`, "
            f"got `{type(__default).__name__}`."
        )
    try:
        return get_run_logger()
    except RuntimeError:
        if isinstance(__default, str):
            return logging.getLogger(__default)
        return __default or logging.getLogger()
