"""
Configuration for unit tests.

Unit tests are fast, isolated tests that don't require database connections
or external services. They use mock objects and synthetic data.

Some files in this directory contain integration-style tests that use
real databases. These are marked with `db` and `slow` markers so they
are skipped by default.
"""

from pathlib import Path

import pytest

# Test classes that require a real database connection
_DB_TEST_CLASSES = {
    "TestCarbonFluxIntegration",
    "TestEstimatorIntegration",
    "TestAreaChangeIntegration",
    "TestAreaChangeEdgeCases",
    "TestJoinSpeciesNames",
    "TestJoinForestTypeNames",
    "TestJoinStateNames",
    "TestJoinMultipleReferences",
    "TestClipByPolygonValidation",
    "TestClipByPolygonIntegration",
    "TestSpatialExtensionLoading",
    "TestIntersectPolygons",
}

# Entire test files that require a real database
_DB_TEST_FILES = {
    "test_panel.py",
}


def pytest_collection_modifyitems(items):
    """Mark tests in this directory as unit tests.

    Tests that use database connections are additionally marked as
    db + slow so they are excluded from default runs.
    """
    unit_dir = Path(__file__).parent
    for item in items:
        if unit_dir not in item.path.parents:
            continue
        item.add_marker(pytest.mark.unit)

        # Mark DB-dependent tests so they're skipped by default
        filename = item.path.name
        class_name = item.cls.__name__ if item.cls else ""

        if filename in _DB_TEST_FILES or class_name in _DB_TEST_CLASSES:
            item.add_marker(pytest.mark.db)
            item.add_marker(pytest.mark.slow)
