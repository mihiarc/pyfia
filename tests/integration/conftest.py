"""
Configuration for integration tests.

Integration tests verify that components work together correctly
with real database connections. They are slower than unit tests
but test realistic scenarios.
"""

from pathlib import Path

import pytest


def pytest_collection_modifyitems(items):
    """Mark tests in this directory as integration tests."""
    integration_dir = Path(__file__).parent
    for item in items:
        if integration_dir in item.path.parents:
            item.add_marker(pytest.mark.integration)
            item.add_marker(pytest.mark.db)
