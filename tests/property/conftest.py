"""
Configuration for property-based tests.

Property-based tests use Hypothesis to generate random test cases
and verify invariants that should hold for all inputs.
"""

from pathlib import Path

import pytest


def pytest_collection_modifyitems(items):
    """Mark tests in this directory as property tests."""
    property_dir = Path(__file__).parent
    for item in items:
        if property_dir in item.path.parents:
            item.add_marker(pytest.mark.property)
