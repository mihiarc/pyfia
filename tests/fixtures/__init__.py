"""
Modular test fixtures for pyFIA.

This package provides organized, reusable test fixtures:
- data.py: Sample FIA data structures (trees, conditions, plots, strata)
- mocks.py: Mock objects for unit testing
- grm.py: Growth-Removal-Mortality specific fixtures
"""

from .data import *
from .mocks import *
from .grm import *
