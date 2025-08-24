"""
Basic functionality tests for lazy-enabled estimators.

This test suite verifies that the estimation functions with built-in lazy
evaluation work correctly and produce expected results.
"""

import pytest
import polars as pl
from pyfia import FIA
from pyfia.estimation import area, biomass, tpa, volume, growth, mortality


class TestLazyEstimators:
    """Test suite for lazy-enabled estimators."""
    
    def test_area_basic(self, sample_fia_instance):
        """Test basic area estimation functionality."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Basic area estimation
            result = area(db, land_type="forest")
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0
            assert "AREA_ESTIMATE" in result.columns
            
            # Test with grouping
            grouped = area(db, grp_by=["FORTYPCD"], totals=True)
            assert "FORTYPCD" in grouped.columns
            assert "AREA_ESTIMATE" in grouped.columns
            
            # Test with variance
            with_var = area(db, variance=True)
            assert "AREA_VARIANCE" in with_var.columns or "AREA_SE" in with_var.columns
    
    def test_biomass_basic(self, sample_fia_instance):
        """Test basic biomass estimation functionality."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Basic biomass estimation
            result = biomass(db, tree_type="live")
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0
            assert "BIOMASS_ESTIMATE" in result.columns or "BIOMASS_AG_ESTIMATE" in result.columns
            
            # Test different components
            ag_result = biomass(db, component="AG")
            assert isinstance(ag_result, pl.DataFrame)
            
            # Test by species
            by_species = biomass(db, by_species=True)
            if len(by_species) > 0:
                assert "SPCD" in by_species.columns
    
    def test_volume_basic(self, sample_fia_instance):
        """Test basic volume estimation functionality."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Basic volume estimation
            result = volume(db, tree_type="live")
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0
            assert any("VOLUME" in col for col in result.columns)
            
            # Test different volume types
            net_result = volume(db, vol_type="net")
            assert isinstance(net_result, pl.DataFrame)
    
    def test_tpa_basic(self, sample_fia_instance):
        """Test basic TPA estimation functionality."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Basic TPA estimation
            result = tpa(db, tree_type="live")
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0
            assert "TPA_ESTIMATE" in result.columns
            
            # Test with tree domain
            filtered = tpa(db, tree_domain="DIA >= 10.0")
            assert isinstance(filtered, pl.DataFrame)
    
    def test_mortality_basic(self, sample_fia_instance):
        """Test basic mortality estimation functionality."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Basic mortality estimation
            result = mortality(db)
            assert isinstance(result, pl.DataFrame)
            # Mortality might be empty in test data
            assert result is not None
            if len(result) > 0:
                assert any("MORTALITY" in col for col in result.columns)
    
    def test_growth_basic(self, sample_fia_instance):
        """Test basic growth estimation functionality."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Basic growth estimation
            result = growth(db)
            assert isinstance(result, pl.DataFrame)
            # Growth might be empty in test data
            assert result is not None
            if len(result) > 0:
                assert any("GROWTH" in col or "NET_GROWTH" in col for col in result.columns)
    
    def test_estimators_with_progress(self, sample_fia_instance):
        """Test that estimators work with progress tracking."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Test with progress enabled (default)
            result_with = area(db, show_progress=True)
            assert isinstance(result_with, pl.DataFrame)
            
            # Test with progress disabled
            result_without = area(db, show_progress=False)
            assert isinstance(result_without, pl.DataFrame)
            
            # Results should be identical
            assert result_with.shape == result_without.shape
    
    def test_estimators_with_domain_filters(self, sample_fia_instance):
        """Test estimators with various domain filters."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Area with area domain
            area_filtered = area(db, area_domain="COND_STATUS_CD == 1")
            assert isinstance(area_filtered, pl.DataFrame)
            
            # Biomass with tree domain
            bio_filtered = biomass(db, tree_domain="STATUSCD == 1 AND DIA >= 5.0")
            assert isinstance(bio_filtered, pl.DataFrame)
            
            # Volume with both domains
            vol_filtered = volume(
                db, 
                tree_domain="STATUSCD == 1",
                area_domain="COND_STATUS_CD == 1"
            )
            assert isinstance(vol_filtered, pl.DataFrame)