"""Simple input validation for pyFIA public API functions."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

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


def validate_domain_expression(domain: str | None, domain_type: str) -> str | None:
    """Validate domain expression syntax, including SQL parsing.

    Performs two levels of validation:
    1. Security checks (SQL injection patterns)
    2. Syntax validation via DomainExpressionParser (catches typos like "DIA >== 5")
    """
    if domain is None:
        return None

    if not isinstance(domain, str):
        raise TypeError(f"{domain_type} must be a string, got {type(domain).__name__}")

    # Basic sanity checks
    if domain.strip() == "":
        raise ValueError(f"{domain_type} cannot be an empty string")

    # Check for common SQL injection patterns with word boundaries
    # Using word boundaries to avoid false positives (e.g., "UPDATED_DATE" is OK)
    dangerous_keyword_patterns = [
        r"\bDROP\b",
        r"\bDELETE\b",
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bALTER\b",
        r"\bCREATE\b",
        r"\bEXEC\b",
        r"\bEXECUTE\b",
        r"\bTRUNCATE\b",
        r"\bUNION\b",
        r"\bINTO\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
    ]
    domain_upper = domain.upper()
    for pattern in dangerous_keyword_patterns:
        if re.search(pattern, domain_upper):
            # Extract the keyword for the error message
            keyword = pattern.replace(r"\b", "")
            raise ValueError(
                f"{domain_type} contains potentially dangerous SQL keyword: {keyword}. "
                f"If this is a legitimate column name, please contact support."
            )

    # Check for SQL injection syntax patterns (not column names)
    dangerous_syntax_patterns = [
        (r";", "semicolon (statement separator)"),
        (r"--", "SQL comment"),
        (r"/\*", "SQL block comment"),
        (r"\\x[0-9a-fA-F]+", "hex escape sequence"),
    ]
    for pattern, description in dangerous_syntax_patterns:
        if re.search(pattern, domain):
            raise ValueError(
                f"{domain_type} contains potentially dangerous SQL syntax: {description}. "
                f"These characters are not allowed in filter expressions."
            )

    # Validate SQL syntax by attempting to parse the expression
    # This catches typos like "DIA >== 5" or "STATUSCD === 1" at call time
    # instead of failing deep in the query pipeline
    from pyfia.filtering.parser import DomainExpressionParser

    is_valid, error_message = DomainExpressionParser.validate_expression(
        domain, domain_type
    )
    if not is_valid:
        raise ValueError(
            f"Invalid {domain_type} expression: '{domain}'. {error_message}"
        )

    return domain


def validate_grp_by(
    grp_by: str | list[str] | None,
) -> str | list[str] | None:
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


def validate_positive_number(value: Any, param_name: str) -> int | float:
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


# =============================================================================
# SQL Security Validation Functions
# =============================================================================

# Characters that are dangerous in SQL string literals
# These could be used for SQL injection attacks
SQL_DANGEROUS_CHARS = re.compile(r"['\";\\]")


def sanitize_sql_path(path: str | Path) -> str:
    """
    Sanitize a file path for safe use in SQL queries.

    This function validates that a file path is safe to interpolate into
    SQL queries (e.g., in ST_Read() or read_csv_auto() functions). It
    prevents SQL injection by rejecting paths containing dangerous characters.

    Parameters
    ----------
    path : str or Path
        The file path to sanitize.

    Returns
    -------
    str
        The sanitized path string, safe for use in SQL.

    Raises
    ------
    ValueError
        If the path contains dangerous characters that could enable SQL injection.

    Examples
    --------
    >>> sanitize_sql_path("/data/counties.shp")
    '/data/counties.shp'
    >>> sanitize_sql_path("data'; DROP TABLE PLOT; --")
    ValueError: Path contains characters that are not allowed in SQL queries
    """
    path_str = str(path)

    # Check for dangerous characters that could enable SQL injection
    if SQL_DANGEROUS_CHARS.search(path_str):
        raise ValueError(
            f"Path contains characters that are not allowed in SQL queries: "
            f"single quotes, double quotes, semicolons, or backslashes. "
            f"Path: {path_str}"
        )

    # Check for SQL comment sequences
    if "--" in path_str or "/*" in path_str:
        raise ValueError(
            f"Path contains SQL comment sequences (-- or /*) which are not allowed. "
            f"Path: {path_str}"
        )

    return path_str


# Pattern for valid SQL identifiers: alphanumeric and underscore only
SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_sql_identifier(
    identifier: str, identifier_type: str = "identifier"
) -> str:
    """
    Validate that a string is a safe SQL identifier (table name, column name).

    SQL identifiers must start with a letter or underscore and contain only
    alphanumeric characters and underscores. This prevents SQL injection
    through malicious table or column names.

    Parameters
    ----------
    identifier : str
        The identifier to validate.
    identifier_type : str, default "identifier"
        Description of the identifier type for error messages
        (e.g., "table name", "column name").

    Returns
    -------
    str
        The validated identifier (unchanged if valid).

    Raises
    ------
    ValueError
        If the identifier contains invalid characters.

    Examples
    --------
    >>> validate_sql_identifier("PLOT", "table name")
    'PLOT'
    >>> validate_sql_identifier("TREE_GRM_COMPONENT", "table name")
    'TREE_GRM_COMPONENT'
    >>> validate_sql_identifier("table; DROP TABLE--", "table name")
    ValueError: Invalid table name
    """
    if not identifier:
        raise ValueError(f"Empty {identifier_type} is not allowed")

    if not SQL_IDENTIFIER_PATTERN.match(identifier):
        raise ValueError(
            f"Invalid {identifier_type}: '{identifier}'. "
            f"Must contain only letters, numbers, and underscores, "
            f"and must start with a letter or underscore."
        )

    return identifier


def quote_sql_identifier(identifier: str) -> str:
    """
    Quote a SQL identifier for safe use in queries.

    This function validates the identifier and returns it wrapped in
    double quotes for use in SQL queries. Double-quoted identifiers
    are treated as literal names by most SQL databases.

    Parameters
    ----------
    identifier : str
        The identifier to quote.

    Returns
    -------
    str
        The quoted identifier (e.g., '"PLOT"').

    Raises
    ------
    ValueError
        If the identifier contains invalid characters.

    Examples
    --------
    >>> quote_sql_identifier("PLOT")
    '"PLOT"'
    """
    validated = validate_sql_identifier(identifier)
    return f'"{validated}"'
