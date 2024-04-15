"""
Unit test configuration file.
"""

from __future__ import annotations

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture
def harness():
    """Return a `prefect_test_harness`."""
    try:
        prefect_test_harness()
    finally:
        pass
