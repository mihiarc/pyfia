"""
Configuration for end-to-end tests.

E2E tests verify complete workflows from download to analysis.
They are the slowest tests but provide the highest confidence.
"""

from pathlib import Path

import pytest


def pytest_collection_modifyitems(items):
    """Mark tests in this directory as e2e tests."""
    e2e_dir = Path(__file__).parent
    for item in items:
        if e2e_dir in item.path.parents:
            item.add_marker(pytest.mark.e2e)
            item.add_marker(pytest.mark.slow)
