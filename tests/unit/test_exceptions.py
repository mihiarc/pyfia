"""Tests for pyFIA custom exception classes."""

import pytest

from pyfia.core.exceptions import (
    ConfigurationError,
    ConnectionError,
    DatabaseError,
    EstimationError,
    FilterError,
    InsufficientDataError,
    InvalidDomainError,
    InvalidEVALIDError,
    MissingColumnError,
    NoEVALIDError,
    PyFIAError,
    StratificationError,
    TableNotFoundError,
)


class TestExceptionHierarchy:
    """Test that exception hierarchy is correct."""

    def test_base_exception(self):
        """Test PyFIAError is the base for all exceptions."""
        exc = PyFIAError("test message")
        assert isinstance(exc, Exception)
        assert exc.message == "test message"
        assert str(exc) == "test message"

    def test_database_error_hierarchy(self):
        """Test DatabaseError inherits from PyFIAError."""
        exc = DatabaseError("db error")
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, DatabaseError)

    def test_table_not_found_hierarchy(self):
        """Test TableNotFoundError inherits from DatabaseError."""
        exc = TableNotFoundError("TREE")
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, DatabaseError)
        assert isinstance(exc, TableNotFoundError)

    def test_connection_error_hierarchy(self):
        """Test ConnectionError inherits from DatabaseError."""
        exc = ConnectionError("/path/to/db")
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, DatabaseError)
        assert isinstance(exc, ConnectionError)

    def test_estimation_error_hierarchy(self):
        """Test EstimationError inherits from PyFIAError."""
        exc = EstimationError("estimation failed")
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, EstimationError)

    def test_insufficient_data_hierarchy(self):
        """Test InsufficientDataError inherits from EstimationError."""
        exc = InsufficientDataError("not enough data")
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, EstimationError)
        assert isinstance(exc, InsufficientDataError)

    def test_stratification_error_hierarchy(self):
        """Test StratificationError inherits from EstimationError."""
        exc = StratificationError("bad strata")
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, EstimationError)
        assert isinstance(exc, StratificationError)

    def test_missing_column_hierarchy(self):
        """Test MissingColumnError inherits from EstimationError."""
        exc = MissingColumnError(["COL1"])
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, EstimationError)
        assert isinstance(exc, MissingColumnError)

    def test_filter_error_hierarchy(self):
        """Test FilterError inherits from PyFIAError."""
        exc = FilterError("filter failed")
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, FilterError)

    def test_invalid_domain_hierarchy(self):
        """Test InvalidDomainError inherits from FilterError."""
        exc = InvalidDomainError("bad expr", "tree")
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, FilterError)
        assert isinstance(exc, InvalidDomainError)

    def test_invalid_evalid_hierarchy(self):
        """Test InvalidEVALIDError inherits from FilterError."""
        exc = InvalidEVALIDError(123456)
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, FilterError)
        assert isinstance(exc, InvalidEVALIDError)

    def test_no_evalid_hierarchy(self):
        """Test NoEVALIDError inherits from FilterError."""
        exc = NoEVALIDError()
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, FilterError)
        assert isinstance(exc, NoEVALIDError)

    def test_configuration_error_hierarchy(self):
        """Test ConfigurationError inherits from PyFIAError."""
        exc = ConfigurationError("bad config")
        assert isinstance(exc, PyFIAError)
        assert isinstance(exc, ConfigurationError)


class TestTableNotFoundError:
    """Test TableNotFoundError message formatting."""

    def test_basic_message(self):
        """Test basic error message."""
        exc = TableNotFoundError("TREE")
        assert exc.table == "TREE"
        assert exc.available_tables is None
        assert "Table 'TREE' not found in database" in str(exc)

    def test_with_available_tables_no_similar(self):
        """Test with available tables but no similar matches."""
        exc = TableNotFoundError("TREE", ["PLOT", "COND", "SURVEY"])
        assert exc.table == "TREE"
        assert exc.available_tables == ["PLOT", "COND", "SURVEY"]
        assert "Did you mean" not in str(exc)

    def test_with_available_tables_with_similar(self):
        """Test with available tables that have similar matches."""
        exc = TableNotFoundError("TREE", ["TREE_GRM", "TREE_REGIONAL", "PLOT"])
        assert "Did you mean" in str(exc)
        assert "TREE_GRM" in str(exc)

    def test_similar_match_case_insensitive(self):
        """Test that similar match is case insensitive."""
        exc = TableNotFoundError("tree", ["TREE_GRM", "TREE_REGIONAL", "PLOT"])
        assert "Did you mean" in str(exc)


class TestConnectionError:
    """Test ConnectionError message formatting."""

    def test_basic_message(self):
        """Test basic error message."""
        exc = ConnectionError("/path/to/db.duckdb")
        assert exc.path == "/path/to/db.duckdb"
        assert exc.reason is None
        assert "Failed to connect to database at '/path/to/db.duckdb'" in str(exc)

    def test_with_reason(self):
        """Test error message with reason."""
        exc = ConnectionError("/path/to/db.duckdb", "file not found")
        assert exc.reason == "file not found"
        assert "file not found" in str(exc)


class TestInsufficientDataError:
    """Test InsufficientDataError message formatting."""

    def test_basic_message(self):
        """Test basic error message."""
        exc = InsufficientDataError("Not enough plots")
        assert "Not enough plots" in str(exc)
        assert exc.n_records is None
        assert exc.min_required is None

    def test_with_counts(self):
        """Test error message with record counts."""
        exc = InsufficientDataError("Not enough plots", n_records=5, min_required=10)
        assert exc.n_records == 5
        assert exc.min_required == 10
        assert "found 5" in str(exc)
        assert "need at least 10" in str(exc)

    def test_partial_counts(self):
        """Test error message with only n_records."""
        exc = InsufficientDataError("Not enough plots", n_records=5)
        assert exc.n_records == 5
        assert "found 5" not in str(exc)  # Only shows if both are provided


class TestMissingColumnError:
    """Test MissingColumnError message formatting."""

    def test_single_column(self):
        """Test error message with single column."""
        exc = MissingColumnError(["DIA"])
        assert exc.columns == ["DIA"]
        assert exc.table is None
        assert "DIA" in str(exc)

    def test_multiple_columns(self):
        """Test error message with multiple columns."""
        exc = MissingColumnError(["DIA", "HT", "STATUSCD"])
        assert exc.columns == ["DIA", "HT", "STATUSCD"]
        assert "DIA" in str(exc)
        assert "HT" in str(exc)
        assert "STATUSCD" in str(exc)

    def test_with_table(self):
        """Test error message with table name."""
        exc = MissingColumnError(["DIA"], table="TREE")
        assert exc.table == "TREE"
        assert "in table 'TREE'" in str(exc)


class TestInvalidDomainError:
    """Test InvalidDomainError message formatting."""

    def test_basic_message(self):
        """Test basic error message."""
        exc = InvalidDomainError("DIA >>= 10", "tree")
        assert exc.expression == "DIA >>= 10"
        assert exc.domain_type == "tree"
        assert exc.reason is None
        assert "Invalid tree domain expression: 'DIA >>= 10'" in str(exc)

    def test_with_reason(self):
        """Test error message with reason."""
        exc = InvalidDomainError("DIA >>= 10", "tree", "invalid operator")
        assert exc.reason == "invalid operator"
        assert "invalid operator" in str(exc)


class TestInvalidEVALIDError:
    """Test InvalidEVALIDError message formatting."""

    def test_single_evalid(self):
        """Test error message with single EVALID."""
        exc = InvalidEVALIDError(123456)
        assert exc.evalid == 123456
        assert exc.reason is None
        assert "Invalid EVALID: 123456" in str(exc)

    def test_multiple_evalids(self):
        """Test error message with multiple EVALIDs."""
        exc = InvalidEVALIDError([123456, 789012])
        assert exc.evalid == [123456, 789012]
        assert "123456" in str(exc)
        assert "789012" in str(exc)

    def test_with_reason(self):
        """Test error message with reason."""
        exc = InvalidEVALIDError(123456, "not found in database")
        assert exc.reason == "not found in database"
        assert "not found in database" in str(exc)


class TestNoEVALIDError:
    """Test NoEVALIDError message formatting."""

    def test_basic_message(self):
        """Test basic error message."""
        exc = NoEVALIDError()
        assert exc.operation is None
        assert exc.suggestion is None
        assert "No EVALID filter specified" in str(exc)
        assert "clip_by_evalid()" in str(exc)

    def test_with_operation(self):
        """Test error message with operation."""
        exc = NoEVALIDError(operation="volume estimation")
        assert exc.operation == "volume estimation"
        assert "for volume estimation" in str(exc)

    def test_with_suggestion(self):
        """Test error message with custom suggestion."""
        exc = NoEVALIDError(suggestion="Call find_evalid() first")
        assert exc.suggestion == "Call find_evalid() first"
        assert "Call find_evalid() first" in str(exc)
        assert "clip_by_evalid()" not in str(exc)


class TestConfigurationError:
    """Test ConfigurationError message formatting."""

    def test_basic_message(self):
        """Test basic error message."""
        exc = ConfigurationError("value out of range")
        assert exc.parameter is None
        assert "value out of range" in str(exc)

    def test_with_parameter(self):
        """Test error message with parameter name."""
        exc = ConfigurationError("must be positive", parameter="n_cores")
        assert exc.parameter == "n_cores"
        assert "Invalid configuration for 'n_cores'" in str(exc)
        assert "must be positive" in str(exc)


class TestExceptionCatching:
    """Test that exceptions can be caught by parent classes."""

    def test_catch_all_pyfia_errors(self):
        """Test catching all pyFIA errors with base class."""
        errors = [
            PyFIAError("test"),
            DatabaseError("test"),
            TableNotFoundError("TREE"),
            ConnectionError("/path"),
            EstimationError("test"),
            InsufficientDataError("test"),
            StratificationError("test"),
            MissingColumnError(["COL"]),
            FilterError("test"),
            InvalidDomainError("expr", "tree"),
            InvalidEVALIDError(123),
            NoEVALIDError(),
            ConfigurationError("test"),
        ]

        for error in errors:
            try:
                raise error
            except PyFIAError as e:
                assert True  # Successfully caught
            except Exception:
                pytest.fail(f"{type(error).__name__} was not caught by PyFIAError")

    def test_catch_database_errors(self):
        """Test catching database errors with DatabaseError."""
        errors = [
            DatabaseError("test"),
            TableNotFoundError("TREE"),
            ConnectionError("/path"),
        ]

        for error in errors:
            try:
                raise error
            except DatabaseError:
                assert True
            except Exception:
                pytest.fail(f"{type(error).__name__} was not caught by DatabaseError")

    def test_catch_estimation_errors(self):
        """Test catching estimation errors with EstimationError."""
        errors = [
            EstimationError("test"),
            InsufficientDataError("test"),
            StratificationError("test"),
            MissingColumnError(["COL"]),
        ]

        for error in errors:
            try:
                raise error
            except EstimationError:
                assert True
            except Exception:
                pytest.fail(f"{type(error).__name__} was not caught by EstimationError")

    def test_catch_filter_errors(self):
        """Test catching filter errors with FilterError."""
        errors = [
            FilterError("test"),
            InvalidDomainError("expr", "tree"),
            InvalidEVALIDError(123),
            NoEVALIDError(),
        ]

        for error in errors:
            try:
                raise error
            except FilterError:
                assert True
            except Exception:
                pytest.fail(f"{type(error).__name__} was not caught by FilterError")
