"""
Tests for the configuration adapter module.

This module tests the adapter functions and classes that enable
seamless integration between legacy and unified configurations.
"""

import pytest
import warnings
from typing import Optional

from pyfia.estimation.base import EstimatorConfig
from pyfia.estimation.config import EstimatorConfigV2, MortalityConfig
from pyfia.estimation.unified_config import (
    UnifiedEstimatorConfig,
    UnifiedVolumeConfig,
    LazyEvaluationConfig,
    LazyEvaluationMode,
)
from pyfia.estimation.config_adapter import (
    adapt_config,
    supports_unified_config,
    ConfigurationMigrator,
    EstimatorConfigAdapter,
)


class TestAdaptConfig:
    """Test the adapt_config function."""
    
    def test_adapt_legacy_to_unified(self):
        """Test adapting legacy config to unified."""
        legacy = EstimatorConfig(
            by_species=True,
            land_type="forest",
            method="TI"
        )
        
        unified = adapt_config(legacy, target_type=UnifiedEstimatorConfig)
        
        assert isinstance(unified, UnifiedEstimatorConfig)
        assert unified.by_species is True
        # Check enum values
        from pyfia.estimation.unified_config import LandType, EstimationMethod
        assert unified.land_type == LandType.FOREST
        assert unified.method == EstimationMethod.TI
    
    def test_adapt_unified_to_legacy(self):
        """Test adapting unified config to legacy."""
        unified = UnifiedEstimatorConfig(
            by_species=True,
            land_type="timber",
            variance=True
        )
        
        legacy = adapt_config(unified, target_type=EstimatorConfig)
        
        assert isinstance(legacy, EstimatorConfig)
        assert legacy.by_species is True
        assert legacy.land_type == "timber"
        assert legacy.variance is True
    
    def test_adapt_v2_to_unified(self):
        """Test adapting V2 config to unified."""
        v2 = EstimatorConfigV2(
            by_species=True,
            by_size_class=True,
            method="EMA",
            lambda_=0.7
        )
        
        unified = adapt_config(v2, target_type=UnifiedEstimatorConfig)
        
        assert isinstance(unified, UnifiedEstimatorConfig)
        assert unified.by_species is True
        assert unified.by_size_class is True
        from pyfia.estimation.unified_config import EstimationMethod
        assert unified.method == EstimationMethod.EMA
        assert unified.lambda_ == 0.7
    
    def test_adapt_dict_to_config(self):
        """Test adapting dictionary to configuration."""
        config_dict = {
            "by_species": True,
            "land_type": "forest",
            "totals": True
        }
        
        # To unified
        unified = adapt_config(config_dict, target_type=UnifiedEstimatorConfig)
        assert isinstance(unified, UnifiedEstimatorConfig)
        assert unified.by_species is True
        
        # To legacy
        legacy = adapt_config(config_dict, target_type=EstimatorConfig)
        assert isinstance(legacy, EstimatorConfig)
        assert legacy.totals is True
    
    def test_adapt_with_module(self):
        """Test adapting with module specification."""
        config_dict = {"by_species": True}
        
        # Should create volume-specific config
        volume_config = adapt_config(config_dict, module="volume")
        assert isinstance(volume_config, UnifiedVolumeConfig)
        
        # Legacy config with module
        legacy = EstimatorConfig(by_species=True)
        volume_config = adapt_config(legacy, module="volume")
        assert isinstance(volume_config, UnifiedVolumeConfig)
    
    def test_adapt_identity(self):
        """Test that adapting to same type returns same object."""
        legacy = EstimatorConfig(by_species=True)
        result = adapt_config(legacy, target_type=EstimatorConfig)
        assert result is legacy
        
        v2 = EstimatorConfigV2(by_species=True)
        result = adapt_config(v2, target_type=EstimatorConfigV2)
        assert result is v2


class TestSupportsUnifiedConfigDecorator:
    """Test the supports_unified_config decorator."""
    
    def test_decorator_with_legacy_input(self):
        """Test decorator with legacy config input."""
        @supports_unified_config("volume")
        def test_func(db, config: EstimatorConfig):
            return config
        
        legacy = EstimatorConfig(by_species=True)
        result = test_func("db", legacy)
        
        assert isinstance(result, EstimatorConfig)
        assert result.by_species is True
    
    def test_decorator_with_unified_input(self):
        """Test decorator with unified config input."""
        @supports_unified_config()
        def test_func(db, config: EstimatorConfig):
            return config
        
        unified = UnifiedEstimatorConfig(by_species=True, variance=True)
        result = test_func("db", unified)
        
        # Should be converted to legacy based on type hint
        assert isinstance(result, EstimatorConfig)
        assert result.by_species is True
        assert result.variance is True
    
    def test_decorator_with_kwargs(self):
        """Test decorator with config in kwargs."""
        @supports_unified_config("biomass")
        def test_func(db, config=None):
            return config
        
        unified = UnifiedEstimatorConfig(land_type="timber")
        result = test_func("db", config=unified)
        
        assert isinstance(result, EstimatorConfig)
        assert result.land_type == "timber"
    
    def test_decorator_preserves_function_metadata(self):
        """Test that decorator preserves function metadata."""
        @supports_unified_config()
        def test_func(db, config):
            """Test function docstring."""
            return config
        
        assert test_func.__name__ == "test_func"
        assert test_func.__doc__ == "Test function docstring."


class TestConfigurationMigrator:
    """Test the ConfigurationMigrator class."""
    
    def test_migrate_list_to_unified(self):
        """Test migrating a list of configs to unified."""
        configs = [
            EstimatorConfig(by_species=True),
            EstimatorConfigV2(land_type="timber"),
            {"method": "EMA", "lambda_": 0.5}
        ]
        
        unified_configs = ConfigurationMigrator.migrate_to_unified(configs)
        
        assert len(unified_configs) == 3
        assert all(isinstance(c, UnifiedEstimatorConfig) for c in unified_configs)
        assert unified_configs[0].by_species is True
        from pyfia.estimation.unified_config import LandType, EstimationMethod
        assert unified_configs[1].land_type == LandType.TIMBER
        assert unified_configs[2].method == EstimationMethod.EMA
    
    def test_migrate_with_validation(self):
        """Test migration with validation."""
        configs = [
            EstimatorConfig(by_species=True),
            {"tree_type": "invalid"}  # Invalid value
        ]
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            unified_configs = ConfigurationMigrator.migrate_to_unified(
                configs, validate=True
            )
            
            # Should have warning for invalid config
            assert len(w) > 0
            assert "Failed to migrate" in str(w[0].message)
    
    def test_validate_migration_success(self):
        """Test successful migration validation."""
        original = EstimatorConfig(
            by_species=True,
            land_type="forest",
            method="TI"
        )
        
        migrated = UnifiedEstimatorConfig.from_legacy(original)
        
        is_valid = ConfigurationMigrator.validate_migration(original, migrated)
        assert is_valid is True
    
    def test_validate_migration_failure(self):
        """Test failed migration validation."""
        original = EstimatorConfig(by_species=True, land_type="forest")
        
        # Create migrated with different values
        migrated = UnifiedEstimatorConfig(by_species=False, land_type="timber")
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            is_valid = ConfigurationMigrator.validate_migration(original, migrated)
            
            assert is_valid is False
            assert len(w) > 0
            assert "validation failed" in str(w[0].message).lower()
    
    def test_validate_migration_strict(self):
        """Test strict migration validation."""
        original = EstimatorConfig(by_species=True)
        migrated = UnifiedEstimatorConfig(by_species=False)
        
        with pytest.raises(ValueError, match="validation failed"):
            ConfigurationMigrator.validate_migration(
                original, migrated, strict=True
            )


class TestEstimatorConfigAdapter:
    """Test the EstimatorConfigAdapter class."""
    
    def test_adapter_with_legacy(self):
        """Test adapter with legacy config."""
        legacy = EstimatorConfig(
            by_species=True,
            land_type="forest",
            extra_params={"vol_type": "net"}
        )
        
        adapter = EstimatorConfigAdapter(legacy, module="volume")
        
        # Access as unified
        assert adapter.unified.by_species is True
        from pyfia.estimation.unified_config import LandType
        assert adapter.unified.land_type == LandType.FOREST
        
        # Access as legacy
        assert adapter.legacy.by_species is True
        
        # Get parameters
        assert adapter.get("by_species") is True
        assert adapter.get("vol_type") == "net"
        assert adapter.get("nonexistent", "default") == "default"
    
    def test_adapter_with_unified(self):
        """Test adapter with unified config."""
        unified = UnifiedEstimatorConfig(
            by_species=True,
            variance=True,
            lazy_config=LazyEvaluationConfig(
                mode=LazyEvaluationMode.ENABLED
            )
        )
        
        adapter = EstimatorConfigAdapter(unified)
        
        # Access different formats
        assert adapter.unified is unified
        assert adapter.legacy.by_species is True
        assert adapter.v2.variance is True
        
        # Access lazy config params
        assert adapter.get("mode") == LazyEvaluationMode.ENABLED
    
    def test_adapter_attribute_delegation(self):
        """Test attribute delegation in adapter."""
        config = UnifiedEstimatorConfig(
            by_species=True,
            totals=True
        )
        
        adapter = EstimatorConfigAdapter(config)
        
        # Direct attribute access
        assert adapter.by_species is True
        assert adapter.totals is True
        assert adapter.land_type.value == "forest"  # Default value
    
    def test_adapter_to_type(self):
        """Test converting adapter to specific type."""
        unified = UnifiedEstimatorConfig(by_species=True)
        adapter = EstimatorConfigAdapter(unified)
        
        # Convert to different types
        legacy = adapter.to_type(EstimatorConfig)
        assert isinstance(legacy, EstimatorConfig)
        assert legacy.by_species is True
        
        v2 = adapter.to_type(EstimatorConfigV2)
        assert isinstance(v2, EstimatorConfigV2)
        assert v2.by_species is True
    
    def test_adapter_validation(self):
        """Test configuration validation through adapter."""
        # Valid config
        valid_config = UnifiedEstimatorConfig(
            tree_type="dead"
        )
        adapter = EstimatorConfigAdapter(valid_config, module="mortality")
        assert adapter.validate() is True
        
        # Invalid config for mortality
        invalid_config = UnifiedEstimatorConfig(
            tree_type="live"  # Invalid for mortality
        )
        adapter = EstimatorConfigAdapter(invalid_config, module="mortality")
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            is_valid = adapter.validate()
            
            assert is_valid is False
            assert len(w) > 0
            assert "validation failed" in str(w[0].message).lower()
    
    def test_adapter_with_module_config(self):
        """Test adapter with module-specific config."""
        from pyfia.estimation.unified_config import (
            UnifiedVolumeConfig,
            VolumeSpecificConfig
        )
        
        volume_config = UnifiedVolumeConfig(
            by_species=True,
            module_config=VolumeSpecificConfig(
                volume_equation="regional",
                include_rotten=True
            )
        )
        
        adapter = EstimatorConfigAdapter(volume_config, module="volume")
        
        # Access module-specific params
        assert adapter.get("volume_equation") == "regional"
        assert adapter.get("include_rotten") is True
        
        # Access base params
        assert adapter.get("by_species") is True


class TestIntegrationScenarios:
    """Test real-world integration scenarios."""
    
    def test_legacy_function_with_unified_config(self):
        """Test using unified config with legacy function."""
        def legacy_estimator(db, config: EstimatorConfig):
            """Legacy estimator expecting EstimatorConfig."""
            assert isinstance(config, EstimatorConfig)
            return {
                "by_species": config.by_species,
                "extra": config.extra_params.get("test_param")
            }
        
        # Create unified config
        unified = UnifiedEstimatorConfig(
            by_species=True,
            extra_params={"test_param": "value"}
        )
        
        # Use adapter to convert
        adapted = adapt_config(unified, target_type=EstimatorConfig)
        result = legacy_estimator("db", adapted)
        
        assert result["by_species"] is True
        assert result["extra"] == "value"
    
    def test_unified_function_with_legacy_config(self):
        """Test using legacy config with unified function."""
        def unified_estimator(db, config: UnifiedEstimatorConfig):
            """Modern estimator expecting UnifiedEstimatorConfig."""
            assert isinstance(config, UnifiedEstimatorConfig)
            return {
                "by_species": config.by_species,
                "lazy_mode": config.lazy_config.mode
            }
        
        # Create legacy config
        legacy = EstimatorConfig(
            by_species=True,
            extra_params={"lazy_enabled": True}
        )
        
        # Use adapter to convert
        adapted = adapt_config(legacy, target_type=UnifiedEstimatorConfig)
        result = unified_estimator("db", adapted)
        
        assert result["by_species"] is True
        assert result["lazy_mode"] == LazyEvaluationMode.ENABLED
    
    def test_mixed_config_list_processing(self):
        """Test processing a mixed list of configurations."""
        configs = [
            EstimatorConfig(by_species=True),
            UnifiedEstimatorConfig(land_type="timber"),
            EstimatorConfigV2(method="EMA"),
            {"variance": True}
        ]
        
        # Process all configs through adapter
        adapters = [EstimatorConfigAdapter(c) for c in configs]
        
        # All should be accessible uniformly
        assert adapters[0].get("by_species") is True
        # For UnifiedEstimatorConfig, land_type is already an enum
        land_type = adapters[1].get("land_type")
        assert (land_type.value if hasattr(land_type, 'value') else land_type) == "timber"
        method = adapters[2].get("method")
        assert (method.value if hasattr(method, 'value') else method) == "EMA"
        assert adapters[3].get("variance") is True
        
        # All can be converted to any format
        legacy_configs = [a.legacy for a in adapters]
        assert all(isinstance(c, EstimatorConfig) for c in legacy_configs)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])