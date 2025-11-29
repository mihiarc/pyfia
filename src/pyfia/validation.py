"""Simple input validation for pyFIA public API functions."""

from typing import Any, List, Optional, Union

# Valid values for common parameters
VALID_LAND_TYPES = {"forest", "timber", "all"}
VALID_TREE_TYPES = {"live", "dead", "gs", "all"}
VALID_VOL_TYPES = {"net", "gross", "sound", "sawlog"}
VALID_BIOMASS_COMPONENTS = {"total", "ag", "bg", "bole", "branch", "foliage", "root"}
VALID_TEMPORAL_METHODS = {"TI", "ANNUAL", "SMA", "LMA", "EMA"}


def validate_land_type(land_type: str) -> str:
    """Validate land_type parameter."""
    if land_type not in VALID_LAND_TYPES:
        raise ValueError(
            f"Invalid land_type '{land_type}'. "
            f"Must be one of: {', '.join(sorted(VALID_LAND_TYPES))}"
        )
    return land_type


def validate_tree_type(tree_type: str) -> str:
    """Validate tree_type parameter."""
    if tree_type not in VALID_TREE_TYPES:
        raise ValueError(
            f"Invalid tree_type '{tree_type}'. "
            f"Must be one of: {', '.join(sorted(VALID_TREE_TYPES))}"
        )
    return tree_type


def validate_vol_type(vol_type: str) -> str:
    """Validate vol_type parameter."""
    if vol_type not in VALID_VOL_TYPES:
        raise ValueError(
            f"Invalid vol_type '{vol_type}'. "
            f"Must be one of: {', '.join(sorted(VALID_VOL_TYPES))}"
        )
    return vol_type


def validate_biomass_component(component: str) -> str:
    """Validate biomass component parameter."""
    if component not in VALID_BIOMASS_COMPONENTS:
        raise ValueError(
            f"Invalid biomass component '{component}'. "
            f"Must be one of: {', '.join(sorted(VALID_BIOMASS_COMPONENTS))}"
        )
    return component


def validate_temporal_method(method: str) -> str:
    """Validate temporal method parameter."""
    if method not in VALID_TEMPORAL_METHODS:
        raise ValueError(
            f"Invalid temporal method '{method}'. "
            f"Must be one of: {', '.join(sorted(VALID_TEMPORAL_METHODS))}"
        )
    return method


def validate_domain_expression(
    domain: Optional[str], domain_type: str
) -> Optional[str]:
    """Basic validation of domain expression syntax."""
    if domain is None:
        return None

    if not isinstance(domain, str):
        raise TypeError(f"{domain_type} must be a string, got {type(domain).__name__}")

    # Basic sanity checks
    if domain.strip() == "":
        raise ValueError(f"{domain_type} cannot be an empty string")

    # Check for common SQL injection patterns with word boundaries
    # Using word boundaries to avoid false positives (e.g., "UPDATED_DATE" is OK)
    import re

    dangerous_patterns = [
        r"\bDROP\b",
        r"\bDELETE\b",
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bALTER\b",
        r"\bCREATE\b",
        r"\bEXEC\b",
        r"\bEXECUTE\b",
        r"\bTRUNCATE\b",
    ]
    domain_upper = domain.upper()
    for pattern in dangerous_patterns:
        if re.search(pattern, domain_upper):
            # Extract the keyword for the error message
            keyword = pattern.replace(r"\b", "")
            raise ValueError(
                f"{domain_type} contains potentially dangerous SQL keyword: {keyword}. "
                f"If this is a legitimate column name, please contact support."
            )

    return domain


def validate_grp_by(
    grp_by: Optional[Union[str, List[str]]],
) -> Optional[Union[str, List[str]]]:
    """Validate grp_by parameter."""
    if grp_by is None:
        return None

    # Convert single string to list for validation
    if isinstance(grp_by, str):
        columns = [grp_by]
    elif isinstance(grp_by, list):
        columns = grp_by
    else:
        raise TypeError(
            f"grp_by must be a string or list of strings, got {type(grp_by).__name__}"
        )

    # Check each column is a non-empty string
    for col in columns:
        if not isinstance(col, str):
            raise TypeError(f"grp_by columns must be strings, got {type(col).__name__}")
        if col.strip() == "":
            raise ValueError("grp_by columns cannot be empty strings")

    return grp_by


def validate_positive_number(value: Any, param_name: str) -> Union[int, float]:
    """Validate that a value is a positive number."""
    if not isinstance(value, (int, float)):
        raise TypeError(f"{param_name} must be a number, got {type(value).__name__}")
    if value <= 0:
        raise ValueError(f"{param_name} must be positive, got {value}")
    return value


def validate_boolean(value: Any, param_name: str) -> bool:
    """Validate that a value is a boolean."""
    if not isinstance(value, bool):
        raise TypeError(f"{param_name} must be a boolean, got {type(value).__name__}")
    return value


def validate_mortality_measure(measure: str) -> str:
    """Validate mortality measure parameter."""
    valid_measures = {"tpa", "basal_area", "volume", "biomass", "carbon"}
    if measure not in valid_measures:
        raise ValueError(
            f"Invalid measure '{measure}'. "
            f"Must be one of: {', '.join(sorted(valid_measures))}"
        )
    return measure
