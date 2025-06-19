"""
pyFIA - Python implementation of rFIA for Forest Inventory Analysis

A Python package for working with USDA Forest Inventory and Analysis (FIA) data.
"""

__version__ = "0.1.0"
__author__ = "Chris Mihiar"

# Import main functions for easy access
from .data_management import clipFIA, readFIA, getFIA
from .tree_metrics import tpa
from .biomass_carbon import biomass, carbon
from .optimized_sqlite_reader import read_fia_sqlite_optimized
from .evalid_utils import find_evalid, filter_by_evalid

# Define what's available when using "from pyfia import *"
__all__ = [
    'clipFIA', 'readFIA', 'getFIA',
    'tpa', 'biomass', 'carbon',
    'read_fia_sqlite_optimized',
    'find_evalid', 'filter_by_evalid'
]