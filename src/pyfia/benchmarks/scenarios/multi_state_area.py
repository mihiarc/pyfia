"""
Multi-state area estimation benchmark scenario.

This scenario represents area estimation across multiple states,
testing performance with larger datasets and complex filtering.
"""

from typing import Dict, List, Optional, Union
import polars as pl
from pathlib import Path

from ...core import FIA
from ...estimation import area


class MultiStateAreaScenario:
    """
    Benchmark scenario for multi-state area estimation.
    
    This scenario tests:
    - Area calculation across multiple states
    - Memory efficiency with larger datasets
    - Performance of land type filtering
    - Temporal analysis capabilities
    """
    
    def __init__(self, db_path: Union[str, Path], 
                 state_codes: List[int] = None):
        """
        Initialize scenario.
        
        Parameters
        ----------
        db_path : Union[str, Path]
            Path to FIA database
        state_codes : List[int]
            List of state FIPS codes (default: NC, SC, VA)
        """
        self.db_path = Path(db_path)
        self.state_codes = state_codes or [37, 45, 51]  # NC, SC, VA
        self.name = f"multi_state_area_{len(self.state_codes)}states"
        
    def setup(self, lazy_enabled: bool = True,
              cache_enabled: bool = True,
              most_recent: bool = True) -> FIA:
        """
        Setup database connection with configuration.
        
        Parameters
        ----------
        lazy_enabled : bool
            Enable lazy evaluation
        cache_enabled : bool
            Enable caching
        most_recent : bool
            Use only most recent evaluations
            
        Returns
        -------
        FIA
            Configured database connection
        """
        db = FIA(self.db_path)
        db.clip_by_state(self.state_codes)
        
        if most_recent:
            db.clip_most_recent("CURR")
        
        # Configure lazy evaluation if supported
        if hasattr(db, 'enable_lazy_evaluation'):
            if lazy_enabled:
                db.enable_lazy_evaluation()
            else:
                db.disable_lazy_evaluation()
        
        # Configure caching if supported
        if hasattr(db, 'enable_caching') and cache_enabled:
            db.enable_caching()
        
        return db
    
    def run_total_area(self, db: FIA) -> pl.DataFrame:
        """Run total forest area estimation."""
        results = area(db)
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_area_by_land_type(self, db: FIA) -> pl.DataFrame:
        """Run area estimation by land type."""
        results = area(
            db,
            land_type="all",  # Include all land types
            by_land_type=True
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_timber_area(self, db: FIA) -> pl.DataFrame:
        """Run timber land area estimation."""
        results = area(
            db,
            land_type="timber",
            by_forest_type=True
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_area_by_ownership(self, db: FIA) -> pl.DataFrame:
        """Run area estimation by ownership group."""
        results = area(
            db,
            land_type="forest",
            by_ownership_group=True
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_area_by_state(self, db: FIA) -> pl.DataFrame:
        """Run area estimation grouped by state."""
        results = area(
            db,
            land_type="forest",
            by_state=True,
            by_forest_type=True
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_protected_area(self, db: FIA) -> pl.DataFrame:
        """Run area estimation for protected lands."""
        results = area(
            db,
            area_domain="RESERVCD == 1",  # Reserved lands
            by_forest_type=True
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_disturbed_area(self, db: FIA) -> pl.DataFrame:
        """Run area estimation for recently disturbed lands."""
        results = area(
            db,
            area_domain="DSTRBCD1 > 0 AND DSTRBYR1 >= 2015",
            by_disturbance=True
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_temporal_area(self, db: FIA) -> pl.DataFrame:
        """Run area estimation over time."""
        # Setup for temporal analysis
        db_temporal = FIA(self.db_path)
        db_temporal.clip_by_state(self.state_codes)
        # Don't clip to most recent for temporal analysis
        
        results = area(
            db_temporal,
            land_type="forest",
            by_year=True,
            temporal_method="TI"  # Temporally indifferent
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_all_scenarios(self, lazy_enabled: bool = True,
                         cache_enabled: bool = True) -> Dict[str, pl.DataFrame]:
        """
        Run all area scenarios.
        
        Parameters
        ----------
        lazy_enabled : bool
            Enable lazy evaluation
        cache_enabled : bool
            Enable caching
            
        Returns
        -------
        Dict[str, pl.DataFrame]
            Results for each scenario
        """
        db = self.setup(lazy_enabled, cache_enabled)
        
        scenarios = {
            "total_area": self.run_total_area,
            "by_land_type": self.run_area_by_land_type,
            "timber_area": self.run_timber_area,
            "by_ownership": self.run_area_by_ownership,
            "by_state": self.run_area_by_state,
            "protected_area": self.run_protected_area,
            "disturbed_area": self.run_disturbed_area,
        }
        
        results = {}
        for name, scenario_func in scenarios.items():
            try:
                results[name] = scenario_func(db)
            except Exception as e:
                print(f"Error in scenario {name}: {e}")
                results[name] = pl.DataFrame()
        
        # Run temporal analysis separately
        try:
            results["temporal_area"] = self.run_temporal_area(db)
        except Exception as e:
            print(f"Error in temporal scenario: {e}")
            results["temporal_area"] = pl.DataFrame()
        
        return results
    
    def validate_results(self, eager_results: Dict[str, pl.DataFrame],
                        lazy_results: Dict[str, pl.DataFrame]) -> Dict[str, bool]:
        """
        Validate that lazy results match eager results.
        
        Parameters
        ----------
        eager_results : Dict[str, pl.DataFrame]
            Results from eager execution
        lazy_results : Dict[str, pl.DataFrame]
            Results from lazy execution
            
        Returns
        -------
        Dict[str, bool]
            Validation status for each scenario
        """
        validation = {}
        
        for scenario_name in eager_results:
            if scenario_name not in lazy_results:
                validation[scenario_name] = False
                continue
            
            eager_df = eager_results[scenario_name]
            lazy_df = lazy_results[scenario_name]
            
            # Skip empty results
            if eager_df.is_empty() or lazy_df.is_empty():
                validation[scenario_name] = eager_df.is_empty() == lazy_df.is_empty()
                continue
            
            # Sort for comparison
            sort_cols = [col for col in eager_df.columns if col != "VARIANCE"]
            if sort_cols:
                eager_df = eager_df.sort(sort_cols)
                lazy_df = lazy_df.sort(sort_cols)
            
            # Compare shapes
            if eager_df.shape != lazy_df.shape:
                validation[scenario_name] = False
                continue
            
            # Compare area estimates
            try:
                area_cols = [col for col in eager_df.columns 
                           if "AREA" in col or "ESTIMATE" in col]
                
                all_match = True
                for col in area_cols:
                    if col in eager_df.columns and col in lazy_df.columns:
                        eager_vals = eager_df[col].to_numpy()
                        lazy_vals = lazy_df[col].to_numpy()
                        
                        # Use relative tolerance for comparison
                        import numpy as np
                        if not np.allclose(eager_vals, lazy_vals, rtol=1e-5, atol=1e-8, equal_nan=True):
                            all_match = False
                            break
                
                validation[scenario_name] = all_match
                
            except Exception as e:
                print(f"Validation error for {scenario_name}: {e}")
                validation[scenario_name] = False
        
        return validation