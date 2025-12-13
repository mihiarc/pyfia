"""
Modular test fixtures for pyFIA.

This package provides organized, reusable test fixtures:
- data.py: Sample FIA data structures (trees, conditions, plots, strata)
- mocks.py: Mock objects for unit testing
- grm.py: Growth-Removal-Mortality specific fixtures

Note: These fixtures are loaded via pytest_plugins in conftest.py.
Do not import them here to avoid PytestAssertRewriteWarning.
"""
