"""
Single state volume estimation benchmark scenario.

This scenario represents a common use case: calculating timber volume
for a single state with various grouping options.
"""

from typing import Dict, List, Optional, Union
import polars as pl
from pathlib import Path

from ...core import FIA
from ...estimation import volume
from ...estimation.lazy_evaluation import CollectionStrategy


class SingleStateVolumeScenario:
    """
    Benchmark scenario for single state volume estimation.
    
    This scenario tests:
    - Basic volume calculation performance
    - Memory efficiency for moderate datasets
    - Impact of different grouping options
    - Cache effectiveness for repeated queries
    """
    
    def __init__(self, db_path: Union[str, Path], state_code: int = 37):
        """
        Initialize scenario.
        
        Parameters
        ----------
        db_path : Union[str, Path]
            Path to FIA database
        state_code : int
            State FIPS code (default: 37 for North Carolina)
        """
        self.db_path = Path(db_path)
        self.state_code = state_code
        self.name = f"single_state_volume_{state_code}"
        
    def setup(self, lazy_enabled: bool = True, 
              cache_enabled: bool = True) -> FIA:
        """
        Setup database connection with configuration.
        
        Parameters
        ----------
        lazy_enabled : bool
            Enable lazy evaluation
        cache_enabled : bool
            Enable caching
            
        Returns
        -------
        FIA
            Configured database connection
        """
        db = FIA(self.db_path)
        db.clip_by_state(self.state_code)
        db.clip_most_recent("VOL")
        
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
    
    def run_basic_volume(self, db: FIA) -> pl.DataFrame:
        """Run basic volume estimation."""
        results = volume(
            db,
            tree_domain="STATUSCD == 1",
            vol_type="net"
        )
        
        # Force collection if lazy
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_volume_by_species(self, db: FIA) -> pl.DataFrame:
        """Run volume estimation grouped by species."""
        results = volume(
            db,
            tree_domain="STATUSCD == 1",
            by_species=True,
            vol_type="net"
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_volume_by_size_class(self, db: FIA) -> pl.DataFrame:
        """Run volume estimation grouped by size class."""
        results = volume(
            db,
            tree_domain="STATUSCD == 1 AND DIA >= 5.0",
            by_size_class=True,
            vol_type="net"
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_volume_full_grouping(self, db: FIA) -> pl.DataFrame:
        """Run volume estimation with all grouping options."""
        results = volume(
            db,
            tree_domain="STATUSCD == 1",
            by_species=True,
            by_size_class=True,
            by_forest_type=True,
            by_stand_age=True,
            vol_type="net"
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_volume_filtered(self, db: FIA, min_dia: float = 10.0) -> pl.DataFrame:
        """Run volume estimation with diameter filter."""
        results = volume(
            db,
            tree_domain=f"STATUSCD == 1 AND DIA >= {min_dia}",
            area_domain="COND_STATUS_CD == 1",
            by_species=True,
            vol_type="net"
        )
        
        if isinstance(results, pl.LazyFrame):
            results = results.collect()
        
        return results
    
    def run_all_scenarios(self, lazy_enabled: bool = True,
                         cache_enabled: bool = True) -> Dict[str, pl.DataFrame]:
        """
        Run all volume scenarios.
        
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
            "basic": self.run_basic_volume,
            "by_species": self.run_volume_by_species,
            "by_size_class": self.run_volume_by_size_class,
            "full_grouping": self.run_volume_full_grouping,
            "filtered_10in": lambda db: self.run_volume_filtered(db, 10.0),
            "filtered_15in": lambda db: self.run_volume_filtered(db, 15.0),
        }
        
        results = {}
        for name, scenario_func in scenarios.items():
            results[name] = scenario_func(db)
        
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
            
            eager_df = eager_results[scenario_name].sort(eager_results[scenario_name].columns)
            lazy_df = lazy_results[scenario_name].sort(lazy_results[scenario_name].columns)
            
            # Compare shapes
            if eager_df.shape != lazy_df.shape:
                validation[scenario_name] = False
                continue
            
            # Compare values (allowing small floating point differences)
            try:
                # Select numeric columns
                numeric_cols = [col for col in eager_df.columns 
                              if eager_df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]]
                
                for col in numeric_cols:
                    eager_vals = eager_df[col].to_numpy()
                    lazy_vals = lazy_df[col].to_numpy()
                    
                    # Use relative tolerance for comparison
                    if not pl.Series(eager_vals).equals(pl.Series(lazy_vals), null_equal=True):
                        # Check with tolerance
                        import numpy as np
                        if not np.allclose(eager_vals, lazy_vals, rtol=1e-5, atol=1e-8, equal_nan=True):
                            validation[scenario_name] = False
                            break
                else:
                    validation[scenario_name] = True
                    
            except Exception as e:
                print(f"Validation error for {scenario_name}: {e}")
                validation[scenario_name] = False
        
        return validation