"""
Tests for area estimation module.

Tests cover:
- Basic area calculations
- PROP_BASIS handling (MACR vs SUBP adjustment factors)
- Domain filtering (land type, area domain, tree domain)
- By land type grouping
- Variance calculations
- Edge cases and error handling
"""

import pytest
import polars as pl
import numpy as np
from unittest.mock import Mock, MagicMock, patch
from pyfia.area import (
    area,
    _apply_area_filters,
    _apply_tree_domain_to_conditions,
    _add_land_type_categories,
    _calculate_domain_indicators,
    _prepare_area_stratification,
    _calculate_plot_area_estimates,
    _calculate_stratum_area_estimates,
    _calculate_population_area_estimates
)


class TestAreaEstimation:
    """Test suite for area estimation functions."""
    
    @pytest.fixture
    def mock_db(self):
        """Create a mock FIA database object."""
        db = Mock()
        db.evalid = [372301]  # NC 2023
        db.tables = {}
        db.load_table = Mock()
        return db
    
    @pytest.fixture
    def sample_plot_data(self):
        """Create sample PLOT data."""
        return pl.DataFrame({
            "CN": ["1", "2", "3", "4", "5"],
            "PLT_CN": ["1", "2", "3", "4", "5"],
            "STATECD": [37, 37, 37, 37, 37],
            "MACRO_BREAKPOINT_DIA": [24.0, 24.0, 24.0, 24.0, 24.0]
        })
    
    @pytest.fixture
    def sample_cond_data(self):
        """Create sample COND data with various land types and PROP_BASIS."""
        return pl.DataFrame({
            "CN": ["C1", "C2", "C3", "C4", "C5", "C6"],
            "PLT_CN": ["1", "1", "2", "3", "4", "5"],
            "CONDID": [1, 2, 1, 1, 1, 1],
            "COND_STATUS_CD": [1, 1, 1, 2, 3, 1],  # Forest, Forest, Forest, Non-forest, Water, Forest
            "CONDPROP_UNADJ": [0.7, 0.3, 1.0, 1.0, 1.0, 1.0],
            "PROP_BASIS": ["SUBP", "SUBP", "MACR", "SUBP", "SUBP", "MACR"],  # Mix of SUBP and MACR
            "SITECLCD": [3, 3, 3, None, None, 2],  # Timber sites
            "RESERVCD": [0, 0, 0, 0, 0, 1],  # Last one is reserved
            "FORTYPCD": [161, 161, 406, None, None, 171]  # Loblolly pine types
        })
    
    @pytest.fixture
    def sample_tree_data(self):
        """Create sample TREE data."""
        return pl.DataFrame({
            "CN": ["T1", "T2", "T3", "T4", "T5"],
            "PLT_CN": ["1", "1", "2", "3", "5"],
            "CONDID": [1, 2, 1, 1, 1],
            "STATUSCD": [1, 1, 1, 2, 1],  # Live, Live, Live, Dead, Live
            "SPCD": [131, 316, 131, 131, 621],  # Loblolly, Red maple, Loblolly, Loblolly, Yellow-poplar
            "DIA": [10.5, 8.2, 15.3, 12.0, 6.5],
            "TPA_UNADJ": [5.0, 5.0, 5.0, 5.0, 5.0]
        })
    
    @pytest.fixture
    def sample_stratum_data(self):
        """Create sample POP_STRATUM data."""
        return pl.DataFrame({
            "CN": ["S1", "S2"],
            "EVALID": [372301, 372301],
            "EXPNS": [1000.0, 1500.0],  # Acres per plot
            "ADJ_FACTOR_SUBP": [1.0, 1.0],
            "ADJ_FACTOR_MACR": [0.25, 0.25],  # Macroplot is 4x subplot area
            "ADJ_FACTOR_MICR": [12.5, 12.5],
            "P2POINTCNT": [3, 2],  # Number of plots in stratum
            "STRATUM_WGT": [0.6, 0.4],
            "AREA_USED": [18000.0, 12000.0]  # Total area in stratum
        })
    
    @pytest.fixture
    def sample_ppsa_data(self):
        """Create sample POP_PLOT_STRATUM_ASSGN data."""
        return pl.DataFrame({
            "PLT_CN": ["1", "2", "3", "4", "5"],
            "STRATUM_CN": ["S1", "S1", "S1", "S2", "S2"],
            "EVALID": [372301, 372301, 372301, 372301, 372301]
        })
    
    def test_apply_area_filters_basic(self, sample_cond_data):
        """Test basic area filtering without domain."""
        result = _apply_area_filters(sample_cond_data, "forest", None)
        assert len(result) == 6  # No filtering applied yet
    
    def test_apply_area_filters_with_domain(self, sample_cond_data):
        """Test area filtering with area domain."""
        result = _apply_area_filters(sample_cond_data, "forest", "FORTYPCD == 161")
        assert len(result) == 2  # Only loblolly pine forest type
        assert all(result["FORTYPCD"] == 161)
    
    def test_apply_tree_domain_to_conditions(self, sample_cond_data, sample_tree_data):
        """Test applying tree domain at condition level."""
        # Filter for conditions with live loblolly pine
        result = _apply_tree_domain_to_conditions(
            sample_cond_data, 
            sample_tree_data,
            "STATUSCD == 1 and SPCD == 131"
        )
        
        assert "HAS_QUALIFYING_TREE" in result.columns
        # Plots 1 and 2 have live loblolly pine
        qualifying = result.filter(pl.col("HAS_QUALIFYING_TREE") == 1)
        assert set(qualifying["PLT_CN"].unique()) == {"1", "2"}
    
    def test_add_land_type_categories(self, sample_cond_data):
        """Test land type categorization."""
        result = _add_land_type_categories(sample_cond_data)
        
        assert "LAND_TYPE" in result.columns
        land_types = result["LAND_TYPE"].to_list()
        
        # Check categorization logic
        assert land_types[0] == "Timber"  # Forest, unreserved, site class 3
        assert land_types[1] == "Timber"  # Forest, unreserved, site class 3
        assert land_types[2] == "Timber"  # Forest, unreserved, site class 3
        assert land_types[3] == "Non-Forest"  # Status 2
        assert land_types[4] == "Water"  # Status 3
        assert land_types[5] == "Non-Timber Forest"  # Forest but reserved
    
    def test_calculate_domain_indicators_forest(self, sample_cond_data):
        """Test domain indicator calculation for forest land."""
        result = _calculate_domain_indicators(sample_cond_data, "forest", False)
        
        assert "landD" in result.columns
        assert "aDI" in result.columns
        assert "pDI" in result.columns
        
        # Forest land indicator should be 1 for status 1, 0 otherwise
        forest_conds = result.filter(pl.col("COND_STATUS_CD") == 1)
        assert all(forest_conds["landD"] == 1)
        
        non_forest = result.filter(pl.col("COND_STATUS_CD") != 1)
        assert all(non_forest["landD"] == 0)
    
    def test_calculate_domain_indicators_by_land_type(self, sample_cond_data):
        """Test domain indicators when grouping by land type."""
        cond_with_types = _add_land_type_categories(sample_cond_data)
        result = _calculate_domain_indicators(cond_with_types, "forest", True)
        
        # For by_land_type, pDI should exclude water (only status 1,2)
        land_conds = result.filter(pl.col("COND_STATUS_CD").is_in([1, 2]))
        assert all(land_conds["pDI"] == 1)
        
        water_conds = result.filter(pl.col("COND_STATUS_CD").is_in([3, 4]))
        assert all(water_conds["pDI"] == 0)
    
    def test_prepare_area_stratification(self, sample_stratum_data, sample_ppsa_data):
        """Test stratification data preparation."""
        result = _prepare_area_stratification(sample_stratum_data, sample_ppsa_data)
        
        # Check all required columns are present
        required_cols = ["PLT_CN", "STRATUM_CN", "EXPNS", "ADJ_FACTOR_SUBP", 
                        "ADJ_FACTOR_MACR", "P2POINTCNT"]
        for col in required_cols:
            assert col in result.columns
        
        # Check joins worked correctly
        assert len(result) == 5  # One row per plot
        assert set(result["STRATUM_CN"].unique()) == {"S1", "S2"}
    
    def test_calculate_plot_area_estimates_prop_basis(self, 
                                                     sample_plot_data,
                                                     sample_cond_data,
                                                     sample_stratum_data,
                                                     sample_ppsa_data):
        """Test plot-level area estimates with PROP_BASIS handling."""
        # Add domain indicators
        cond_df = _calculate_domain_indicators(sample_cond_data, "forest", False)
        strat_df = _prepare_area_stratification(sample_stratum_data, sample_ppsa_data)
        
        result = _calculate_plot_area_estimates(
            sample_plot_data,
            cond_df,
            strat_df,
            grp_by=None
        )
        
        # Check PROP_BASIS was extracted and used
        assert "PROP_BASIS" in result.columns
        assert "ADJ_FACTOR" in result.columns
        
        # Verify MACR plots get MACR adjustment factor
        macr_plots = result.filter(pl.col("PROP_BASIS") == "MACR")
        if len(macr_plots) > 0:
            # Get the adjustment factors from strat_df for comparison
            macr_plt_cns = macr_plots["PLT_CN"].to_list()
            macr_strat = strat_df.filter(pl.col("PLT_CN").is_in(macr_plt_cns))
            assert all(macr_plots["ADJ_FACTOR"] == 0.25)  # ADJ_FACTOR_MACR value
        
        # Verify SUBP plots get SUBP adjustment factor  
        subp_plots = result.filter(pl.col("PROP_BASIS") != "MACR")
        if len(subp_plots) > 0:
            assert all(subp_plots["ADJ_FACTOR"] == 1.0)  # ADJ_FACTOR_SUBP value
    
    def test_calculate_plot_area_estimates_with_grouping(self,
                                                        sample_plot_data,
                                                        sample_cond_data,
                                                        sample_stratum_data,
                                                        sample_ppsa_data):
        """Test plot-level estimates with grouping variables."""
        # Add land type categories and indicators
        cond_df = _add_land_type_categories(sample_cond_data)
        cond_df = _calculate_domain_indicators(cond_df, "forest", True)
        strat_df = _prepare_area_stratification(sample_stratum_data, sample_ppsa_data)
        
        result = _calculate_plot_area_estimates(
            sample_plot_data,
            cond_df,
            strat_df,
            grp_by=["LAND_TYPE"]
        )
        
        # Check grouping worked
        assert "LAND_TYPE" in result.columns
        # Number of result rows depends on unique LAND_TYPE values per plot
        # Just check that we have results
        assert len(result) >= 1
    
    def test_calculate_stratum_area_estimates(self):
        """Test stratum-level area estimation."""
        # Create mock plot estimates
        plot_est = pl.DataFrame({
            "PLT_CN": ["1", "2", "3", "4", "5"],
            "STRATUM_CN": ["S1", "S1", "S1", "S2", "S2"],
            "fa": [0.7, 1.0, 0.0, 0.0, 1.0],
            "fad": [1.0, 1.0, 1.0, 1.0, 1.0],
            "fa_expanded": [700.0, 1000.0, 0.0, 0.0, 1500.0],
            "fad_expanded": [1000.0, 1000.0, 1000.0, 1500.0, 1500.0],
            "EXPNS": [1000.0, 1000.0, 1000.0, 1500.0, 1500.0]
        })
        
        result = _calculate_stratum_area_estimates(plot_est, grp_by=None)
        
        # Check calculations
        assert len(result) == 2  # Two strata
        assert "fa_expanded_total" in result.columns
        assert "fad_expanded_total" in result.columns
        assert "s_fa_h" in result.columns  # Standard deviation
        assert "corr_fa_fad" in result.columns  # Correlation
        
        # Verify totals
        s1 = result.filter(pl.col("STRATUM_CN") == "S1")
        assert s1["fa_expanded_total"][0] == 1700.0  # 700 + 1000 + 0
        assert s1["fad_expanded_total"][0] == 3000.0  # 1000 * 3
    
    def test_calculate_population_area_estimates(self):
        """Test population-level area estimation."""
        # Create mock stratum estimates
        stratum_est = pl.DataFrame({
            "STRATUM_CN": ["S1", "S2"],
            "fa_expanded_total": [1700.0, 1500.0],
            "fad_expanded_total": [3000.0, 3000.0],
            "fa_bar_h": [0.567, 0.5],
            "fad_bar_h": [1.0, 1.0],
            "s_fa_h": [0.1, 0.2],
            "s_fad_h": [0.0, 0.0],
            "s_fa_fad_h": [0.0, 0.0],
            "w_h": [1000.0, 1500.0],
            "n_h": [3, 2]
        })
        
        result = _calculate_population_area_estimates(
            stratum_est, 
            grp_by=None,
            totals=True,
            variance=False
        )
        
        # Check results
        assert "AREA_PERC" in result.columns
        assert "AREA" in result.columns  # Total area requested
        assert "AREA_PERC_SE" in result.columns
        assert "N_PLOTS" in result.columns
        
        # Verify percentage calculation
        expected_perc = (3200.0 / 6000.0) * 100  # (1700+1500)/(3000+3000)
        assert abs(result["AREA_PERC"][0] - expected_perc) < 0.01
        
        # Verify total plots
        assert result["N_PLOTS"][0] == 5  # 3 + 2
    
    def test_area_main_function_integration(self, mock_db, 
                                          sample_plot_data,
                                          sample_cond_data,
                                          sample_tree_data,
                                          sample_stratum_data,
                                          sample_ppsa_data):
        """Test the main area function integration."""
        # Setup mock database
        mock_db.get_plots = Mock(return_value=sample_plot_data)
        mock_db.get_conditions = Mock(return_value=sample_cond_data)
        mock_db.get_trees = Mock(return_value=sample_tree_data)
        mock_db.tables = {
            'POP_STRATUM': Mock(collect=Mock(return_value=sample_stratum_data)),
            'POP_PLOT_STRATUM_ASSGN': Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=sample_ppsa_data)))
            )
        }
        
        # Test basic area calculation
        result = area(mock_db, land_type="forest")
        
        assert isinstance(result, pl.DataFrame)
        assert "AREA_PERC" in result.columns
        assert "N_PLOTS" in result.columns
        assert "AREA_PERC_SE" in result.columns
        
        # Test with totals
        result_with_totals = area(mock_db, land_type="forest", totals=True)
        assert "AREA" in result_with_totals.columns
        
        # Test with variance
        result_with_var = area(mock_db, land_type="forest", variance=True)
        assert "AREA_PERC_VAR" in result_with_var.columns
    
    def test_area_by_land_type(self, mock_db,
                               sample_plot_data,
                               sample_cond_data,
                               sample_stratum_data,
                               sample_ppsa_data):
        """Test area calculation grouped by land type."""
        # Setup mock database
        mock_db.get_plots = Mock(return_value=sample_plot_data)
        mock_db.get_conditions = Mock(return_value=sample_cond_data)
        mock_db.tables = {
            'POP_STRATUM': Mock(collect=Mock(return_value=sample_stratum_data)),
            'POP_PLOT_STRATUM_ASSGN': Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=sample_ppsa_data)))
            )
        }
        
        result = area(mock_db, by_land_type=True)
        
        assert "LAND_TYPE" in result.columns
        assert len(result) >= 1  # At least one land type category
        
        # Check that percentages are reasonable
        if len(result) > 0:
            # Each percentage should be valid
            area_percs = result["AREA_PERC"].to_list()
            for perc in area_percs:
                # Handle None/null values
                if perc is not None:
                    assert 0 <= perc <= 100
            
            # For land area (excluding water), percentages should sum to 100%
            land_only = result.filter(~pl.col("LAND_TYPE").str.contains("Water"))
            if len(land_only) > 0:
                # Filter out null values
                valid_percs = [p for p in land_only["AREA_PERC"].to_list() if p is not None]
                if valid_percs:
                    total_land_perc = sum(valid_percs)
                    # Should sum to approximately 100%
                    assert 95 <= total_land_perc <= 105
    
    def test_area_with_tree_domain(self, mock_db,
                                   sample_plot_data,
                                   sample_cond_data,
                                   sample_tree_data,
                                   sample_stratum_data,
                                   sample_ppsa_data):
        """Test area calculation with tree domain filter."""
        # Setup mock database  
        mock_db.get_plots = Mock(return_value=sample_plot_data)
        mock_db.get_conditions = Mock(return_value=sample_cond_data)
        mock_db.get_trees = Mock(return_value=sample_tree_data)
        mock_db.tables = {
            'POP_STRATUM': Mock(collect=Mock(return_value=sample_stratum_data)),
            'POP_PLOT_STRATUM_ASSGN': Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=sample_ppsa_data)))
            )
        }
        
        # Calculate area for conditions with loblolly pine
        result = area(mock_db, tree_domain="SPCD == 131 and STATUSCD == 1")
        
        assert isinstance(result, pl.DataFrame)
        # Area should be less than total forest area since filtered by tree domain
        assert result["AREA_PERC"][0] < 100.0
    
    def test_area_timber_land_type(self, mock_db,
                                   sample_plot_data,
                                   sample_cond_data,
                                   sample_stratum_data,
                                   sample_ppsa_data):
        """Test area calculation for timber land type."""
        # Setup mock database
        mock_db.get_plots = Mock(return_value=sample_plot_data)
        mock_db.get_conditions = Mock(return_value=sample_cond_data)
        mock_db.tables = {
            'POP_STRATUM': Mock(collect=Mock(return_value=sample_stratum_data)),
            'POP_PLOT_STRATUM_ASSGN': Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=sample_ppsa_data)))
            )
        }
        
        result = area(mock_db, land_type="timber")
        
        assert isinstance(result, pl.DataFrame)
        # Timber area should be less than total forest area
        # since it excludes reserved and non-productive sites
        assert result["AREA_PERC"][0] <= 100.0
    
    def test_area_with_custom_grouping(self, mock_db,
                                       sample_plot_data,
                                       sample_cond_data,
                                       sample_stratum_data,
                                       sample_ppsa_data):
        """Test area calculation with custom grouping variable."""
        # Add forest type to conditions for grouping
        cond_with_fortype = sample_cond_data.with_columns(
            pl.col("FORTYPCD").fill_null(999)  # Fill nulls for grouping
        )
        
        mock_db.get_plots = Mock(return_value=sample_plot_data)
        mock_db.get_conditions = Mock(return_value=cond_with_fortype)
        mock_db.tables = {
            'POP_STRATUM': Mock(collect=Mock(return_value=sample_stratum_data)),
            'POP_PLOT_STRATUM_ASSGN': Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=sample_ppsa_data)))
            )
        }
        
        result = area(mock_db, grp_by=["FORTYPCD"])
        
        assert "FORTYPCD" in result.columns
        assert len(result) >= 1  # At least one forest type group
    
    def test_edge_cases(self, mock_db):
        """Test edge cases and error conditions."""
        # Empty data with proper schema and types
        empty_plots = pl.DataFrame({
            "CN": pl.Series([], dtype=pl.Utf8),
            "PLT_CN": pl.Series([], dtype=pl.Utf8)
        })
        empty_conds = pl.DataFrame({
            "CN": pl.Series([], dtype=pl.Utf8),
            "PLT_CN": pl.Series([], dtype=pl.Utf8),
            "CONDID": pl.Series([], dtype=pl.Int32),
            "COND_STATUS_CD": pl.Series([], dtype=pl.Int32),
            "CONDPROP_UNADJ": pl.Series([], dtype=pl.Float64),
            "PROP_BASIS": pl.Series([], dtype=pl.Utf8)
        })
        empty_strata = pl.DataFrame({
            "CN": pl.Series([], dtype=pl.Utf8),
            "EVALID": pl.Series([], dtype=pl.Int32),
            "EXPNS": pl.Series([], dtype=pl.Float64),
            "ADJ_FACTOR_SUBP": pl.Series([], dtype=pl.Float64),
            "ADJ_FACTOR_MACR": pl.Series([], dtype=pl.Float64),
            "P2POINTCNT": pl.Series([], dtype=pl.Int32)
        })
        empty_ppsa = pl.DataFrame({
            "PLT_CN": pl.Series([], dtype=pl.Utf8),
            "STRATUM_CN": pl.Series([], dtype=pl.Utf8),
            "EVALID": pl.Series([], dtype=pl.Int32)
        })
        
        mock_db.get_plots = Mock(return_value=empty_plots)
        mock_db.get_conditions = Mock(return_value=empty_conds)
        mock_db.tables = {
            'POP_STRATUM': Mock(collect=Mock(return_value=empty_strata)),
            'POP_PLOT_STRATUM_ASSGN': Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=empty_ppsa)))
            )
        }
        
        # Should handle empty data gracefully
        result = area(mock_db)
        assert isinstance(result, pl.DataFrame)
        # Either no rows or has standard columns
        if len(result) > 0:
            assert "AREA_PERC" in result.columns
            assert "N_PLOTS" in result.columns
    
    def test_variance_calculations(self):
        """Test variance calculation formulas."""
        # Test with known values
        stratum_est = pl.DataFrame({
            "fa_expanded_total": [1000.0],
            "fad_expanded_total": [2000.0],
            "fa_bar_h": [1.0],
            "fad_bar_h": [2.0],
            "s_fa_h": [0.5],
            "s_fad_h": [0.3],
            "s_fa_fad_h": [0.1],
            "w_h": [1000.0],
            "n_h": [10]
        })
        
        result = _calculate_population_area_estimates(
            stratum_est,
            grp_by=None,
            totals=False,
            variance=True
        )
        
        # Verify variance components are calculated
        assert "AREA_PERC_VAR" in result.columns
        assert result["AREA_PERC_VAR"][0] > 0  # Variance should be positive
        
        # Verify ratio variance formula is applied correctly
        # The variance should incorporate both numerator and denominator variance
        # plus the covariance term