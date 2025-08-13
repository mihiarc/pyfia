"""
Tests for the MortalityConfig configuration class.

This module tests the Pydantic v2-based configuration for mortality
estimation, including validation, type safety, and integration with
the existing estimator framework.
"""

import pytest
from pyfia.estimation.config import MortalityConfig, EstimatorConfigV2, VALID_FIA_GROUPING_COLUMNS
from pyfia.estimation.base import EstimatorConfig


class TestMortalityConfig:
    """Test the MortalityConfig class."""
    
    def test_basic_creation(self):
        """Test basic configuration creation with defaults."""
        config = MortalityConfig()
        
        # Check defaults
        assert config.mortality_type == "tpa"
        assert config.tree_class == "all"
        assert config.land_type == "forest"
        assert config.tree_type == "live"
        assert config.variance == False
        assert config.totals == False
        assert config.include_components == False
        
    def test_mortality_type_validation(self):
        """Test mortality type validation."""
        # Valid types
        config1 = MortalityConfig(mortality_type="tpa")
        assert config1.mortality_type == "tpa"
        
        config2 = MortalityConfig(mortality_type="volume")
        assert config2.mortality_type == "volume"
        
        config3 = MortalityConfig(mortality_type="both")
        assert config3.mortality_type == "both"
        
        # Invalid type
        with pytest.raises(ValueError):
            MortalityConfig(mortality_type="invalid")
            
    def test_tree_class_validation(self):
        """Test tree class validation."""
        # Valid classes
        config1 = MortalityConfig(tree_class="all")
        assert config1.tree_class == "all"
        
        config2 = MortalityConfig(tree_class="timber")
        assert config2.tree_class == "timber"
        
        config3 = MortalityConfig(tree_class="growing_stock")
        assert config3.tree_class == "growing_stock"
        
        # Invalid class
        with pytest.raises(ValueError):
            MortalityConfig(tree_class="invalid")
            
    def test_mortality_specific_validation(self):
        """Test mortality-specific validation rules."""
        # Can't calculate volume mortality on live trees
        with pytest.raises(ValueError, match="Cannot calculate volume mortality"):
            MortalityConfig(
                mortality_type="volume",
                tree_type="live"
            )
            
        # Timber tree class requires timber land type
        with pytest.raises(ValueError, match="tree_class='timber' requires"):
            MortalityConfig(
                tree_class="timber",
                land_type="forest"
            )
            
        # Valid timber configuration
        config = MortalityConfig(
            tree_class="timber",
            land_type="timber"
        )
        assert config.tree_class == "timber"
        assert config.land_type == "timber"
        
    def test_grouping_columns(self):
        """Test grouping column generation."""
        # Basic grouping
        config1 = MortalityConfig(
            by_species=True,
            group_by_ownership=True
        )
        groups = config1.get_grouping_columns()
        assert "SPCD" in groups
        assert "OWNGRPCD" in groups
        
        # Complex grouping
        config2 = MortalityConfig(
            grp_by=["STATECD", "UNITCD"],
            by_species=True,
            group_by_species_group=True,
            group_by_ownership=True,
            group_by_agent=True,
            group_by_disturbance=True,
            by_size_class=True
        )
        groups = config2.get_grouping_columns()
        assert "STATECD" in groups
        assert "UNITCD" in groups
        assert "SPCD" in groups
        assert "SPGRPCD" in groups
        assert "OWNGRPCD" in groups
        assert "AGENTCD" in groups
        assert "DSTRBCD1" in groups
        assert "DSTRBCD2" in groups
        assert "DSTRBCD3" in groups
        assert "SIZE_CLASS" in groups
        
        # No duplicates
        assert len(groups) == len(set(groups))
        
    def test_output_columns(self):
        """Test output column generation."""
        # TPA only with standard error
        config1 = MortalityConfig(
            mortality_type="tpa",
            variance=False,
            totals=False
        )
        cols = config1.get_output_columns()
        assert "MORTALITY_TPA" in cols
        assert "MORTALITY_TPA_SE" in cols
        assert "MORTALITY_TPA_VAR" not in cols
        assert "MORTALITY_TPA_TOTAL" not in cols
        
        # Volume with variance and totals
        config2 = MortalityConfig(
            mortality_type="volume",
            variance=True,
            totals=True
        )
        cols = config2.get_output_columns()
        assert "MORTALITY_VOL" in cols
        assert "MORTALITY_VOL_VAR" in cols
        assert "MORTALITY_VOL_SE" not in cols
        assert "MORTALITY_VOL_TOTAL" in cols
        
        # Both types with components
        config3 = MortalityConfig(
            mortality_type="both",
            variance=True,
            totals=True,
            include_components=True
        )
        cols = config3.get_output_columns()
        assert "MORTALITY_TPA" in cols
        assert "MORTALITY_VOL" in cols
        assert "MORTALITY_BA" in cols
        assert "MORTALITY_TPA_VAR" in cols
        assert "MORTALITY_VOL_VAR" in cols
        assert "MORTALITY_BA_VAR" in cols
        assert "MORTALITY_TPA_TOTAL" in cols
        assert "MORTALITY_VOL_TOTAL" in cols
        assert "MORTALITY_BA_TOTAL" in cols
        
    def test_domain_validation(self):
        """Test domain expression validation."""
        # Valid expressions
        config1 = MortalityConfig(
            tree_domain="DIA >= 10.0 AND STATUSCD == 2",
            area_domain="FORTYPCD IN (121, 122, 123)"
        )
        assert config1.tree_domain == "DIA >= 10.0 AND STATUSCD == 2"
        
        # SQL injection attempts should be caught
        with pytest.raises(ValueError, match="forbidden keyword"):
            MortalityConfig(tree_domain="DIA >= 10; DROP TABLE TREE;")
            
        with pytest.raises(ValueError, match="forbidden keyword"):
            MortalityConfig(area_domain="CONDID = 1; DELETE FROM COND;")
            
    def test_grp_by_validation(self):
        """Test grouping column validation."""
        # Valid columns
        config = MortalityConfig(grp_by=["STATECD", "UNITCD", "SPCD"])
        assert config.grp_by == ["STATECD", "UNITCD", "SPCD"]
        
        # Unknown columns are allowed (might be custom)
        config2 = MortalityConfig(grp_by=["CUSTOM_COL"])
        assert config2.grp_by == ["CUSTOM_COL"]
        
    def test_backwards_compatibility(self):
        """Test conversion to legacy EstimatorConfig."""
        config = MortalityConfig(
            grp_by=["STATECD"],
            by_species=True,
            mortality_type="volume",
            group_by_ownership=True,
            tree_domain="DIA >= 10",
            variance=True,
            totals=True,
            tree_class="timber",
            land_type="timber"
        )
        
        legacy = config.to_estimator_config()
        
        # Check it's the right type
        assert isinstance(legacy, EstimatorConfig)
        
        # Check base parameters
        assert legacy.grp_by == ["STATECD"]
        assert legacy.by_species == True
        assert legacy.tree_domain == "DIA >= 10"
        assert legacy.variance == True
        assert legacy.totals == True
        assert legacy.land_type == "timber"
        
        # Check mortality-specific params in extra_params
        assert legacy.extra_params["mortality_type"] == "volume"
        assert legacy.extra_params["group_by_ownership"] == True
        assert legacy.extra_params["tree_class"] == "timber"
        
    def test_model_config(self):
        """Test Pydantic model configuration."""
        # Extra fields should be forbidden
        with pytest.raises(ValueError):
            MortalityConfig(unknown_field="value")
            
        # Assignment validation should work
        config = MortalityConfig()
        config.mortality_type = "volume"
        assert config.mortality_type == "volume"
        
        with pytest.raises(ValueError):
            config.mortality_type = "invalid"
            
    def test_variance_method(self):
        """Test variance method options."""
        config1 = MortalityConfig(variance_method="standard")
        assert config1.variance_method == "standard"
        
        config2 = MortalityConfig(variance_method="ratio")
        assert config2.variance_method == "ratio"
        
        config3 = MortalityConfig(variance_method="hybrid")
        assert config3.variance_method == "hybrid"
        
        with pytest.raises(ValueError):
            MortalityConfig(variance_method="invalid")
            
    def test_to_dict(self):
        """Test dictionary conversion."""
        config = MortalityConfig(
            mortality_type="tpa",
            by_species=True,
            extra_params={"custom": "value"}
        )
        
        d = config.to_dict()
        assert isinstance(d, dict)
        assert d["mortality_type"] == "tpa"
        assert d["by_species"] == True
        assert d["custom"] == "value"
        
    def test_lambda_validation(self):
        """Test lambda parameter validation."""
        # Valid range
        config1 = MortalityConfig(lambda_=0.5)
        assert config1.lambda_ == 0.5
        
        config2 = MortalityConfig(lambda_=0.0)
        assert config2.lambda_ == 0.0
        
        config3 = MortalityConfig(lambda_=1.0)
        assert config3.lambda_ == 1.0
        
        # Out of range
        with pytest.raises(ValueError):
            MortalityConfig(lambda_=-0.1)
            
        with pytest.raises(ValueError):
            MortalityConfig(lambda_=1.1)
            
    def test_whitespace_stripping(self):
        """Test that string fields have whitespace stripped."""
        config = MortalityConfig(
            tree_domain="  DIA >= 10.0  ",
            area_domain="  FORTYPCD = 121  "
        )
        assert config.tree_domain == "DIA >= 10.0"
        assert config.area_domain == "FORTYPCD = 121"