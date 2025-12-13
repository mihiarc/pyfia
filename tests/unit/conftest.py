"""
Configuration for unit tests.

Unit tests are fast, isolated tests that don't require database connections
or external services. They use mock objects and synthetic data.
"""

import pytest


def pytest_collection_modifyitems(items):
    """Mark all tests in this directory as unit tests."""
    for item in items:
        item.add_marker(pytest.mark.unit)
