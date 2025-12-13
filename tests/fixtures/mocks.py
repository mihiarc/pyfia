"""
Mock fixtures for unit testing pyFIA.

Provides mock objects that simulate FIA database behavior
without requiring actual database connections.
"""

from unittest.mock import Mock

import pytest


@pytest.fixture
def mock_fia_database():
    """Create a mock FIA database object for testing."""
    db = Mock()
    db.evalid = [372301]
    db.statecd = [37]
    db.tables = {}
    db.load_table = Mock()
    db.get_evaluation_info = Mock()
    db._reader = Mock()
    db._reader.conn = None
    return db


@pytest.fixture
def mock_database_query_interface():
    """Create a mock database query interface for testing."""
    mock_interface = Mock()
    mock_interface.execute_query = Mock()
    mock_interface.get_table_columns = Mock()
    mock_interface.table_exists = Mock(return_value=True)
    return mock_interface




@pytest.fixture
def mock_evalidator_client():
    """Create a mock EVALIDator client for testing without network calls."""
    client = Mock()
    client.get_estimate = Mock(return_value={
        "estimate": 24000000.0,
        "se": 120000.0,
        "se_percent": 0.5,
    })
    client.get_forest_area = Mock(return_value={
        "estimate": 24500000.0,
        "se": 125000.0,
    })
    client.get_volume = Mock(return_value={
        "estimate": 50000000000.0,
        "se": 500000000.0,
    })
    return client


@pytest.fixture
def mock_data_reader():
    """Create a mock FIADataReader for testing."""
    reader = Mock()
    reader.engine = "duckdb"
    reader.get_table_schema = Mock(return_value={
        "CN": "VARCHAR",
        "PLT_CN": "VARCHAR",
        "DIA": "DOUBLE",
    })
    reader.read_table = Mock()
    reader.table_exists = Mock(return_value=True)
    return reader


@pytest.fixture
def mock_backend():
    """Create a mock database backend for testing."""
    backend = Mock()
    backend.connect = Mock()
    backend.disconnect = Mock()
    backend.execute = Mock()
    backend.get_schema = Mock(return_value={})
    backend.table_exists = Mock(return_value=True)
    return backend
