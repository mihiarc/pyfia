"""
Configuration for integration tests.

Integration tests verify that components work together correctly
with real database connections. They are slower than unit tests
but test realistic scenarios.
"""

import pytest


def pytest_collection_modifyitems(items):
    """Mark all tests in this directory as integration tests."""
    for item in items:
        item.add_marker(pytest.mark.integration)
        item.add_marker(pytest.mark.db)
