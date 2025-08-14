"""
Tests for output formatting utilities.
"""

import polars as pl
import pytest

from pyfia.constants.constants import EstimatorType
from pyfia.estimation.formatters import OutputFormatter, format_estimation_output


class TestOutputFormatter:
    """Test OutputFormatter functionality."""
    
    def test_standardize_columns_area(self):
        """Test column standardization for area estimator."""
        formatter = OutputFormatter(EstimatorType.AREA)
        
        # Create test data with various naming patterns
        df = pl.DataFrame({
            "AREA_PERC": [25.5, 30.2],
            "AREA_PERC_VAR": [0.5, 0.6],
            "FA_TOTAL": [1000, 1200],
            "N_PLOTS": [100, 120],
        })
        
        result = formatter.standardize_columns(df)
        
        # Check that columns are preserved/renamed correctly
        assert "AREA_PERC" in result.columns
        assert "AREA_PERC_VAR" in result.columns
        assert "AREA" in result.columns  # FA_TOTAL renamed to AREA
        assert "FA_TOTAL" not in result.columns  # Original name removed
        
    def test_convert_variance_to_se(self):
        """Test variance to standard error conversion."""
        formatter = OutputFormatter(EstimatorType.AREA)
        
        df = pl.DataFrame({
            "AREA_PERC": [25.5, 30.2],
            "AREA_PERC_VAR": [4.0, 9.0],  # Variance values
            "AREA_VAR": [100.0, 144.0],
        })
        
        result = formatter.convert_variance_to_se(df)
        
        # Check SE columns were created
        assert "AREA_PERC_SE" in result.columns
        assert "AREA_SE" in result.columns
        
        # Check variance columns were removed
        assert "AREA_PERC_VAR" not in result.columns
        assert "AREA_VAR" not in result.columns
        
        # Check SE values are correct (sqrt of variance)
        assert result["AREA_PERC_SE"].to_list() == [2.0, 3.0]
        assert result["AREA_SE"].to_list() == [10.0, 12.0]
        
    def test_convert_se_to_variance(self):
        """Test standard error to variance conversion."""
        formatter = OutputFormatter(EstimatorType.BIOMASS)
        
        df = pl.DataFrame({
            "BIOMASS_ACRE": [100.0, 150.0],
            "BIOMASS_ACRE_SE": [5.0, 10.0],  # SE values
            "BIOMASS_SE": [20.0, 30.0],
        })
        
        result = formatter.convert_se_to_variance(df)
        
        # Check variance columns were created
        assert "BIOMASS_ACRE_VAR" in result.columns
        assert "BIOMASS_VAR" in result.columns
        
        # Check SE columns were removed
        assert "BIOMASS_ACRE_SE" not in result.columns
        assert "BIOMASS_SE" not in result.columns
        
        # Check variance values are correct (SE squared)
        assert result["BIOMASS_ACRE_VAR"].to_list() == [25.0, 100.0]
        assert result["BIOMASS_VAR"].to_list() == [400.0, 900.0]
        
    def test_add_metadata_columns(self):
        """Test metadata column addition."""
        formatter = OutputFormatter(EstimatorType.TPA)
        
        df = pl.DataFrame({
            "TPA": [100.0, 150.0],
            "BAA": [50.0, 75.0],
        })
        
        result = formatter.add_metadata_columns(
            df,
            year=2023,
            n_plots=pl.lit(250),
            additional_metadata={
                "EVAL_TYPE": "VOL",
                "STATE": 37
            }
        )
        
        # Check metadata columns were added
        assert "YEAR" in result.columns
        assert "N_PLOTS" in result.columns
        assert "N" in result.columns
        assert "EVAL_TYPE" in result.columns
        assert "STATE" in result.columns
        
        # Check values
        assert result["YEAR"][0] == 2023
        assert result["N_PLOTS"][0] == 250
        assert result["N"][0] == 2  # Number of rows
        assert result["EVAL_TYPE"][0] == "VOL"
        assert result["STATE"][0] == 37
        
    def test_format_grouped_results(self):
        """Test grouped result formatting."""
        formatter = OutputFormatter(EstimatorType.VOLUME)
        
        df = pl.DataFrame({
            "SPCD": [110, 131, 833],
            "VOLUME_ACRE": [1000.0, 1500.0, 800.0],
            "VOLUME_ACRE_SE": [50.0, 75.0, 40.0],
            "N_PLOTS": [100, 150, 80],
        })
        
        result = formatter.format_grouped_results(df, group_cols=["SPCD"])
        
        # Check that group columns come first
        assert list(result.columns)[0] == "SPCD"
        
    def test_format_output_complete(self):
        """Test complete output formatting."""
        formatter = OutputFormatter(EstimatorType.AREA)
        
        df = pl.DataFrame({
            "LAND_TYPE": ["Forest", "Timber", "Other"],
            "FA_TOTAL": [1000.0, 800.0, 200.0],
            "FAD_TOTAL": [2000.0, 2000.0, 2000.0],
            "AREA_PERC": [50.0, 40.0, 10.0],
            "AREA_PERC_VAR": [4.0, 3.0, 1.0],
            "nPlots": [100, 80, 20],
        })
        
        # Test with SE output (variance=False)
        result = formatter.format_output(
            df,
            variance=False,
            totals=True,
            group_cols=["LAND_TYPE"],
            year=2023
        )
        
        # Check columns are in correct order
        cols = list(result.columns)
        assert cols[0] == "LAND_TYPE"
        assert "YEAR" in cols
        assert "N" in cols
        assert "AREA_PERC" in cols
        assert "AREA_PERC_SE" in cols
        assert "AREA_PERC_VAR" not in cols
        
        # Test with variance output
        result_var = formatter.format_output(
            df,
            variance=True,
            totals=False,
            group_cols=["LAND_TYPE"],
            year=2023
        )
        
        # Check variance columns are present
        assert "AREA_PERC_VAR" in result_var.columns
        assert "AREA_PERC_SE" not in result_var.columns


class TestFormatEstimationOutput:
    """Test the convenience function."""
    
    def test_format_estimation_output(self):
        """Test the convenience function works correctly."""
        df = pl.DataFrame({
            "SPCD": [110, 131],
            "TPA": [100.0, 150.0],
            "TPA_VAR": [25.0, 36.0],
            "BAA": [50.0, 75.0],
            "BAA_VAR": [16.0, 25.0],
            "nPlots": [100, 120],
        })
        
        result = format_estimation_output(
            df,
            EstimatorType.TPA,
            variance=False,
            totals=False,
            group_cols=["SPCD"],
            year=2023
        )
        
        # Check output
        assert "SPCD" in result.columns
        assert "TPA" in result.columns
        assert "TPA_SE" in result.columns
        assert "TPA_VAR" not in result.columns
        assert "BAA" in result.columns
        assert "BAA_SE" in result.columns
        assert "YEAR" in result.columns
        
        # Check SE values are calculated correctly
        assert result["TPA_SE"].to_list() == [5.0, 6.0]
        assert result["BAA_SE"].to_list() == [4.0, 5.0]