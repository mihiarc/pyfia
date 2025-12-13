"""
Configuration for property-based tests.

Property-based tests use Hypothesis to generate random test cases
and verify invariants that should hold for all inputs.
"""

import pytest


def pytest_collection_modifyitems(items):
    """Mark all tests in this directory as property tests."""
    for item in items:
        item.add_marker(pytest.mark.property)
