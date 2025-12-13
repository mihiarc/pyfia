"""
Configuration for end-to-end tests.

E2E tests verify complete workflows from download to analysis.
They are the slowest tests but provide the highest confidence.
"""

import pytest


def pytest_collection_modifyitems(items):
    """Mark all tests in this directory as e2e tests."""
    for item in items:
        item.add_marker(pytest.mark.e2e)
        item.add_marker(pytest.mark.slow)
