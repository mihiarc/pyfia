"""
Configuration adapter for seamless integration with existing estimators.

This module provides adapter functions and decorators to enable existing
estimators to work with both legacy and unified configurations without
requiring immediate refactoring of all code.
"""

from functools import wraps
from typing import Any, Callable, Optional, Type, TypeVar, Union
import warnings

from .base import EstimatorConfig
from .config import EstimatorConfigV2, MortalityConfig
from .unified_config import (
    UnifiedEstimatorConfig,
    ConfigFactory,
    UnifiedVolumeConfig,
    UnifiedBiomassConfig,
    UnifiedGrowthConfig,
    UnifiedAreaConfig,
    UnifiedMortalityConfig,
)


T = TypeVar("T")


def adapt_config(
    config: Any,
    module: Optional[str] = None,
    target_type: Optional[Type] = None
) -> Union[EstimatorConfig, EstimatorConfigV2, UnifiedEstimatorConfig]:
    """
    Adapt any configuration to the appropriate type.
    
    This function provides intelligent configuration adaptation based on:
    1. The input configuration type
    2. The target module (if specified)
    3. The desired target type (if specified)
    
    Parameters
    ----------
    config : Any
        Configuration object (unified, legacy, v2, or dict)
    module : Optional[str]
        Module name for specialized configs (e.g., "volume", "biomass")
    target_type : Optional[Type]
        Desired output configuration type
    
    Returns
    -------
    Union[EstimatorConfig, EstimatorConfigV2, UnifiedEstimatorConfig]
        Adapted configuration in the appropriate format
    
    Examples
    --------
    >>> # Convert legacy to unified
    >>> legacy = EstimatorConfig(by_species=True)
    >>> unified = adapt_config(legacy, target_type=UnifiedEstimatorConfig)
    
    >>> # Convert unified to legacy
    >>> unified = UnifiedEstimatorConfig(by_species=True)
    >>> legacy = adapt_config(unified, target_type=EstimatorConfig)
    
    >>> # Auto-detect for module
    >>> config = adapt_config({"by_species": True}, module="volume")
    >>> isinstance(config, UnifiedVolumeConfig)  # True
    """
    # If target type is specified, convert to that type
    if target_type is not None:
        if target_type == EstimatorConfig:
            # Convert to legacy
            if isinstance(config, EstimatorConfig):
                return config
            elif isinstance(config, UnifiedEstimatorConfig):
                return config.to_legacy()
            elif isinstance(config, EstimatorConfigV2):
                # V2 to legacy via unified
                unified = UnifiedEstimatorConfig.from_v2(config)
                return unified.to_legacy()
            elif isinstance(config, dict):
                # Dict to legacy
                unified = UnifiedEstimatorConfig(**config)
                return unified.to_legacy()
            else:
                raise TypeError(f"Cannot convert {type(config)} to EstimatorConfig")
        
        elif target_type == EstimatorConfigV2:
            # Convert to V2
            if isinstance(config, EstimatorConfigV2):
                return config
            elif isinstance(config, UnifiedEstimatorConfig):
                return config.to_v2()
            elif isinstance(config, EstimatorConfig):
                # Legacy to V2 via unified
                unified = UnifiedEstimatorConfig.from_legacy(config)
                return unified.to_v2()
            elif isinstance(config, dict):
                return EstimatorConfigV2(**config)
            else:
                raise TypeError(f"Cannot convert {type(config)} to EstimatorConfigV2")
        
        elif issubclass(target_type, UnifiedEstimatorConfig):
            # Convert to unified
            return ConfigFactory.ensure_unified(config, module)
    
    # If no target type specified, use module to determine
    if module:
        return ConfigFactory.create_config(module, config if isinstance(config, EstimatorConfig) else None, 
                                          **(config if isinstance(config, dict) else {}))
    
    # Default: ensure it's unified
    return ConfigFactory.ensure_unified(config)


def supports_unified_config(default_module: Optional[str] = None):
    """
    Decorator to add unified configuration support to estimator functions.
    
    This decorator allows existing estimator functions to accept both
    legacy and unified configurations transparently.
    
    Parameters
    ----------
    default_module : Optional[str]
        Default module name for configuration adaptation
    
    Examples
    --------
    >>> @supports_unified_config("volume")
    ... def volume_estimator(db, config):
    ...     # config is automatically adapted
    ...     pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Find config in args or kwargs
            config = None
            config_idx = None
            
            # Check positional args (usually second argument)
            if len(args) > 1:
                for i, arg in enumerate(args[1:], 1):
                    if isinstance(arg, (EstimatorConfig, EstimatorConfigV2, UnifiedEstimatorConfig, dict)):
                        config = arg
                        config_idx = i
                        break
            
            # Check kwargs
            if config is None and "config" in kwargs:
                config = kwargs["config"]
                config_idx = "config"
            
            # If config found, adapt it
            if config is not None:
                # Determine target type based on function signature
                import inspect
                sig = inspect.signature(func)
                
                # Get the config parameter type hint if available
                target_type = EstimatorConfig  # Default to legacy
                
                if "config" in sig.parameters:
                    param = sig.parameters["config"]
                    if param.annotation != inspect.Parameter.empty:
                        # Use type hint to determine target
                        if hasattr(param.annotation, "__origin__"):
                            # Handle Optional, Union, etc.
                            if hasattr(param.annotation, "__args__"):
                                for arg_type in param.annotation.__args__:
                                    if arg_type != type(None):
                                        target_type = arg_type
                                        break
                        else:
                            target_type = param.annotation
                
                # Adapt the configuration
                adapted_config = adapt_config(config, module=default_module, target_type=target_type)
                
                # Replace in args or kwargs
                if isinstance(config_idx, int):
                    args = list(args)
                    args[config_idx] = adapted_config
                    args = tuple(args)
                else:
                    kwargs[config_idx] = adapted_config
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


class ConfigurationMigrator:
    """
    Helper class for migrating configurations between formats.
    
    This class provides utilities for batch migration of configurations
    and validation of migration results.
    """
    
    @staticmethod
    def migrate_to_unified(
        configs: list,
        module: Optional[str] = None,
        validate: bool = True
    ) -> list:
        """
        Migrate a list of configurations to unified format.
        
        Parameters
        ----------
        configs : list
            List of configuration objects
        module : Optional[str]
            Module name for specialized configs
        validate : bool
            Whether to validate migrated configs
        
        Returns
        -------
        list
            List of unified configurations
        """
        unified_configs = []
        
        for i, config in enumerate(configs):
            try:
                unified = ConfigFactory.ensure_unified(config, module)
                
                if validate:
                    # Validate the configuration
                    if module:
                        unified.validate_for_module(module)
                
                unified_configs.append(unified)
                
            except Exception as e:
                warnings.warn(
                    f"Failed to migrate config at index {i}: {e}",
                    UserWarning
                )
                # Add original config as fallback
                unified_configs.append(config)
        
        return unified_configs
    
    @staticmethod
    def validate_migration(
        original: Any,
        migrated: UnifiedEstimatorConfig,
        strict: bool = False
    ) -> bool:
        """
        Validate that migration preserved all settings.
        
        Parameters
        ----------
        original : Any
            Original configuration
        migrated : UnifiedEstimatorConfig
            Migrated configuration
        strict : bool
            Whether to raise on validation failure
        
        Returns
        -------
        bool
            True if migration is valid
        
        Raises
        ------
        ValueError
            If strict=True and validation fails
        """
        # Convert original to dict for comparison
        if isinstance(original, EstimatorConfig):
            original_dict = original.to_dict()
        elif isinstance(original, (EstimatorConfigV2, MortalityConfig)):
            original_dict = original.model_dump()
        elif isinstance(original, dict):
            original_dict = original
        else:
            original_dict = {}
        
        # Convert migrated to dict
        migrated_dict = migrated.to_dict()
        
        # Check key parameters
        key_params = [
            "by_species", "by_size_class", "land_type", "tree_type",
            "method", "lambda_", "totals", "variance", "by_plot"
        ]
        
        mismatches = []
        for param in key_params:
            if param in original_dict:
                orig_val = original_dict[param]
                migr_val = migrated_dict.get(param)
                
                # Handle enum conversions
                if hasattr(orig_val, "value"):
                    orig_val = orig_val.value
                if hasattr(migr_val, "value"):
                    migr_val = migr_val.value
                
                if orig_val != migr_val:
                    mismatches.append(f"{param}: {orig_val} != {migr_val}")
        
        if mismatches:
            msg = f"Migration validation failed: {', '.join(mismatches)}"
            if strict:
                raise ValueError(msg)
            else:
                warnings.warn(msg, UserWarning)
                return False
        
        return True


class EstimatorConfigAdapter:
    """
    Adapter class that provides a unified interface for all configuration types.
    
    This adapter wraps any configuration type and provides a consistent
    interface for accessing parameters, regardless of the underlying type.
    """
    
    def __init__(self, config: Any, module: Optional[str] = None):
        """
        Initialize the configuration adapter.
        
        Parameters
        ----------
        config : Any
            Configuration object
        module : Optional[str]
            Module name for specialized behavior
        """
        self._original_config = config
        self._module = module
        
        # Ensure we have a unified config internally
        self._unified = ConfigFactory.ensure_unified(config, module)
    
    @property
    def unified(self) -> UnifiedEstimatorConfig:
        """Get the unified configuration."""
        return self._unified
    
    @property
    def legacy(self) -> EstimatorConfig:
        """Get as legacy configuration."""
        return self._unified.to_legacy()
    
    @property
    def v2(self) -> EstimatorConfigV2:
        """Get as V2 configuration."""
        return self._unified.to_v2()
    
    def get(self, param: str, default: Any = None) -> Any:
        """
        Get a configuration parameter.
        
        Parameters
        ----------
        param : str
            Parameter name
        default : Any
            Default value if parameter not found
        
        Returns
        -------
        Any
            Parameter value
        """
        # Try unified config first
        if hasattr(self._unified, param):
            return getattr(self._unified, param)
        
        # Check module config
        if self._unified.module_config and hasattr(self._unified.module_config, param):
            return getattr(self._unified.module_config, param)
        
        # Check extra params
        if param in self._unified.extra_params:
            return self._unified.extra_params[param]
        
        # Check lazy config
        if hasattr(self._unified.lazy_config, param):
            return getattr(self._unified.lazy_config, param)
        
        return default
    
    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to unified config."""
        return getattr(self._unified, name)
    
    def to_type(self, target_type: Type[T]) -> T:
        """
        Convert to specific configuration type.
        
        Parameters
        ----------
        target_type : Type[T]
            Target configuration type
        
        Returns
        -------
        T
            Configuration in target type
        """
        return adapt_config(self._unified, self._module, target_type)
    
    def validate(self) -> bool:
        """
        Validate the configuration.
        
        Returns
        -------
        bool
            True if configuration is valid
        """
        try:
            if self._module:
                self._unified.validate_for_module(self._module)
            return True
        except ValueError as e:
            warnings.warn(f"Configuration validation failed: {e}", UserWarning)
            return False


# === Example Usage Functions ===

def example_volume_estimator_with_adapter(db, config: Any):
    """
    Example of using the adapter in an estimator function.
    
    This function accepts any configuration type and uses the adapter
    to work with it transparently.
    """
    # Create adapter
    adapter = EstimatorConfigAdapter(config, module="volume")
    
    # Access parameters uniformly
    by_species = adapter.get("by_species", False)
    vol_type = adapter.get("vol_type", "net")
    lazy_enabled = adapter.get("lazy_enabled", True)
    
    # Get as specific type if needed
    if lazy_enabled:
        # Use unified config for lazy operations
        unified_config = adapter.unified
        # ... use unified config
    else:
        # Use legacy for backward compatibility
        legacy_config = adapter.legacy
        # ... use legacy config
    
    # Validate before proceeding
    if not adapter.validate():
        raise ValueError("Invalid configuration for volume estimation")
    
    # Continue with estimation...
    return {"by_species": by_species, "vol_type": vol_type}


@supports_unified_config("volume")
def example_decorated_estimator(db, config: EstimatorConfig):
    """
    Example of decorated estimator that accepts any config type.
    
    The decorator automatically converts the config to EstimatorConfig
    (as specified in the type hint) before the function receives it.
    """
    # config is guaranteed to be EstimatorConfig here
    assert isinstance(config, EstimatorConfig)
    
    # Use config normally
    return {
        "by_species": config.by_species,
        "land_type": config.land_type
    }


# Export public API
__all__ = [
    "adapt_config",
    "supports_unified_config",
    "ConfigurationMigrator",
    "EstimatorConfigAdapter",
]