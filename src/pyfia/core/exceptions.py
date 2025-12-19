"""
Custom exceptions for pyFIA core operations.

This module defines exception classes for handling various error conditions
that can occur during FIA data analysis operations.

Exception Hierarchy
-------------------
PyFIAError (base)
├── DatabaseError
│   ├── TableNotFoundError
│   └── ConnectionError
├── EstimationError
│   ├── InsufficientDataError
│   └── StratificationError
├── FilterError
│   ├── InvalidDomainError
│   └── InvalidEVALIDError
└── ConfigurationError

Usage Guidelines
----------------
- Raise exceptions for errors that callers should handle
- Use warnings only for non-critical informational messages
- Always provide clear, actionable error messages
- Include relevant context (table names, column names, etc.)
"""

import logging

logger = logging.getLogger(__name__)


class PyFIAError(Exception):
    """
    Base exception for all pyFIA-related errors.

    All custom exceptions in pyFIA inherit from this class, making it
    easy to catch all pyFIA-specific errors with a single except clause.

    Parameters
    ----------
    message : str
        Human-readable error message describing what went wrong.
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


# === Database Errors ===


class DatabaseError(PyFIAError):
    """
    Base exception for database-related errors.

    Raised when there are issues with database connections, queries,
    or data retrieval operations.
    """

    pass


class TableNotFoundError(DatabaseError):
    """
    Raised when a required FIA table is not found in the database.

    Parameters
    ----------
    table : str
        The table name that was not found.
    available_tables : list of str, optional
        List of tables that are available, for reference.
    """

    def __init__(self, table: str, available_tables: list[str] | None = None):
        self.table = table
        self.available_tables = available_tables
        message = f"Table '{table}' not found in database"
        if available_tables:
            similar = [t for t in available_tables if table.upper() in t.upper()]
            if similar:
                message += f". Did you mean: {', '.join(similar[:3])}?"
        super().__init__(message)


class ConnectionError(DatabaseError):
    """
    Raised when database connection fails.

    Parameters
    ----------
    path : str
        Path to the database file.
    reason : str, optional
        Reason for the connection failure.
    """

    def __init__(self, path: str, reason: str | None = None):
        self.path = path
        self.reason = reason
        message = f"Failed to connect to database at '{path}'"
        if reason:
            message += f": {reason}"
        super().__init__(message)


# === Estimation Errors ===


class EstimationError(PyFIAError):
    """
    Base exception for estimation-related errors.

    Raised when statistical estimation cannot be performed due to
    data issues, missing columns, or invalid parameters.
    """

    pass


class InsufficientDataError(EstimationError):
    """
    Raised when there is insufficient data for estimation.

    Parameters
    ----------
    message : str
        Description of what data is missing or insufficient.
    n_records : int, optional
        Number of records found (if applicable).
    min_required : int, optional
        Minimum number of records required.
    """

    def __init__(
        self,
        message: str,
        n_records: int | None = None,
        min_required: int | None = None,
    ):
        self.n_records = n_records
        self.min_required = min_required
        if n_records is not None and min_required is not None:
            message += f" (found {n_records}, need at least {min_required})"
        super().__init__(message)


class StratificationError(EstimationError):
    """
    Raised when stratification data is missing or invalid.

    Parameters
    ----------
    message : str
        Description of the stratification issue.
    """

    pass


class MissingColumnError(EstimationError):
    """
    Raised when required columns are missing from the data.

    Parameters
    ----------
    columns : list of str
        List of missing column names.
    table : str, optional
        Table where the columns were expected.
    """

    def __init__(self, columns: list[str], table: str | None = None):
        self.columns = columns
        self.table = table
        cols_str = ", ".join(columns)
        message = f"Required columns missing: {cols_str}"
        if table:
            message += f" (in table '{table}')"
        super().__init__(message)


# === Filter Errors ===


class FilterError(PyFIAError):
    """
    Base exception for filtering-related errors.

    Raised when domain expressions, EVALID filters, or other
    filtering operations fail.
    """

    pass


class InvalidDomainError(FilterError):
    """
    Raised when a domain expression is invalid or cannot be parsed.

    Parameters
    ----------
    expression : str
        The invalid domain expression.
    domain_type : str
        Type of domain (tree, area, etc.).
    reason : str, optional
        Reason why the expression is invalid.
    """

    def __init__(self, expression: str, domain_type: str, reason: str | None = None):
        self.expression = expression
        self.domain_type = domain_type
        self.reason = reason
        message = f"Invalid {domain_type} domain expression: '{expression}'"
        if reason:
            message += f" - {reason}"
        super().__init__(message)


class InvalidEVALIDError(FilterError):
    """
    Raised when an EVALID is invalid or not found.

    Parameters
    ----------
    evalid : int or list of int
        The invalid EVALID(s).
    reason : str, optional
        Reason why the EVALID is invalid.
    """

    def __init__(self, evalid: int | list[int], reason: str | None = None):
        self.evalid = evalid
        self.reason = reason
        if isinstance(evalid, list):
            evalid_str = ", ".join(str(e) for e in evalid)
        else:
            evalid_str = str(evalid)
        message = f"Invalid EVALID: {evalid_str}"
        if reason:
            message += f" - {reason}"
        super().__init__(message)


class NoEVALIDError(FilterError):
    """
    Raised when no EVALID filter is set but one is required.

    This is raised when estimation requires an EVALID filter
    but none has been specified and auto-detection failed.

    Parameters
    ----------
    operation : str, optional
        The operation that requires an EVALID.
    suggestion : str, optional
        Suggested action to resolve the issue.
    """

    def __init__(self, operation: str | None = None, suggestion: str | None = None):
        self.operation = operation
        self.suggestion = suggestion
        message = "No EVALID filter specified"
        if operation:
            message += f" for {operation}"
        if suggestion:
            message += f". {suggestion}"
        else:
            message += ". Use clip_by_evalid() or clip_most_recent() to set one."
        super().__init__(message)


# === Configuration Errors ===


class ConfigurationError(PyFIAError):
    """
    Raised when pyFIA configuration is invalid.

    Parameters
    ----------
    message : str
        Description of the configuration issue.
    parameter : str, optional
        The parameter that has an invalid value.
    """

    def __init__(self, message: str, parameter: str | None = None):
        self.parameter = parameter
        if parameter:
            message = f"Invalid configuration for '{parameter}': {message}"
        super().__init__(message)


# === Spatial Errors ===


class SpatialError(PyFIAError):
    """
    Base exception for spatial operation errors.

    Raised when spatial operations (polygon clipping, spatial joins) fail
    due to invalid geometry, missing files, or extension issues.
    """

    pass


class SpatialFileError(SpatialError):
    """
    Raised when a spatial file cannot be read or is invalid.

    Parameters
    ----------
    path : str
        Path to the spatial file.
    reason : str, optional
        Reason why the file could not be read.
    supported_formats : list of str, optional
        List of supported file formats.
    """

    def __init__(
        self,
        path: str,
        reason: str | None = None,
        supported_formats: list[str] | None = None,
    ):
        self.path = path
        self.reason = reason
        self.supported_formats = supported_formats
        message = f"Cannot read spatial file: '{path}'"
        if reason:
            message += f" - {reason}"
        if supported_formats:
            message += f". Supported formats: {', '.join(supported_formats)}"
        super().__init__(message)


class SpatialExtensionError(SpatialError):
    """
    Raised when the DuckDB spatial extension cannot be loaded.

    Parameters
    ----------
    reason : str, optional
        Reason why the extension could not be loaded.
    """

    def __init__(self, reason: str | None = None):
        self.reason = reason
        message = "Failed to load DuckDB spatial extension"
        if reason:
            message += f": {reason}"
        message += ". Try running: duckdb -c 'INSTALL spatial;'"
        super().__init__(message)


class NoSpatialFilterError(SpatialError):
    """
    Raised when no plots match the spatial filter.

    Parameters
    ----------
    polygon_path : str
        Path to the polygon file used for filtering.
    n_polygons : int, optional
        Number of polygons in the file.
    """

    def __init__(self, polygon_path: str, n_polygons: int | None = None):
        self.polygon_path = polygon_path
        self.n_polygons = n_polygons
        message = f"No plots found within polygon(s) from '{polygon_path}'"
        if n_polygons is not None:
            message += f" ({n_polygons} polygon(s) checked)"
        message += ". Check that the polygon CRS matches plot coordinates (EPSG:4326)"
        super().__init__(message)
