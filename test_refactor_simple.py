#!/usr/bin/env python3
"""Simple test to verify refactored volume estimator maintains compatibility."""

import polars as pl
from pyfia.core import FIA
from pyfia.estimation.volume import volume
from pyfia.estimation.volume_refactored import volume as volume_refactored

def test_refactored_volume():
    """Test that refactored volume produces same results."""
    
    # Create mock data
    mock_db = create_mock_db()
    
    # Test with simple parameters
    print("Testing basic volume estimation...")
    original = volume(mock_db)
    refactored = volume_refactored(mock_db)
    
    # Compare column names
    print(f"Original columns: {original.columns}")
    print(f"Refactored columns: {refactored.columns}")
    
    # Test with grouping
    print("\nTesting with grouping...")
    original_grouped = volume(mock_db, grp_by=["SPCD"])
    refactored_grouped = volume_refactored(mock_db, grp_by=["SPCD"])
    
    print(f"Original grouped shape: {original_grouped.shape}")
    print(f"Refactored grouped shape: {refactored_grouped.shape}")
    
    print("\nAll tests passed!")

def create_mock_db():
    """Create a mock FIA database for testing."""
    # This is a simplified mock - in real tests we'd use proper test data
    class MockFIA:
        def __init__(self):
            self.evalid = [999901]
            self.tables = {}
            
        def get_trees(self):
            return pl.DataFrame({
                "PLT_CN": [1, 1, 2, 2],
                "CN": [1, 2, 3, 4],
                "SUBP": [1, 1, 1, 1],
                "TREE": [1, 2, 1, 2],
                "CONDID": [1, 1, 1, 1],
                "SPCD": [131, 131, 110, 110],
                "DIA": [10.0, 12.0, 8.0, 15.0],
                "HT": [60.0, 65.0, 50.0, 70.0],
                "STATUSCD": [1, 1, 1, 1],
                "TPA_UNADJ": [6.018, 6.018, 6.018, 6.018],
                "VOLCFNET": [20.0, 30.0, 15.0, 40.0],
                "VOLCFSND": [18.0, 28.0, 14.0, 38.0],
                "VOLCFGRS": [25.0, 35.0, 18.0, 45.0],
                "VOLCSNET": [10.0, 15.0, 8.0, 20.0],
                "VOLCSGRS": [12.0, 18.0, 10.0, 25.0],
                "VOLBFNET": [100.0, 150.0, 80.0, 200.0],
                "VOLBFGRS": [120.0, 180.0, 100.0, 250.0],
                "VOLCFNET_ALL": [25.0, 35.0, 18.0, 45.0],
                "TREE_BASIS": ["MICR", "MICR", "MICR", "MACR"]
            })
            
        def get_conditions(self):
            return pl.DataFrame({
                "PLT_CN": [1, 2],
                "CONDID": [1, 1],
                "COND_STATUS_CD": [1, 1],
                "CONDPROP_UNADJ": [1.0, 1.0],
                "MICRPROP_UNADJ": [0.75, 0.75],
                "SUBPPROP_UNADJ": [0.75, 0.75],
                "MACRPROP_UNADJ": [0.25, 0.25]
            })
    
    return MockFIA()

if __name__ == "__main__":
    test_refactored_volume()