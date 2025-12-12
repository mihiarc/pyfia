"""
Custom exceptions for FIA data download operations.

This module defines exception classes for handling various error conditions
that can occur during FIA data downloads from the DataMart.
"""


class DownloadError(Exception):
    """
    Base exception for all download-related errors.

    Parameters
    ----------
    message : str
        Human-readable error message.
    url : str, optional
        URL that caused the error.
    """

    def __init__(self, message: str, url: str | None = None):
        self.message = message
        self.url = url
        super().__init__(self.message)


class StateNotFoundError(DownloadError):
    """
    Raised when an invalid state code is provided.

    Parameters
    ----------
    state : str
        The invalid state code.
    valid_states : list of str, optional
        List of valid state codes for reference.
    """

    def __init__(self, state: str, valid_states: list[str] | None = None):
        self.state = state
        self.valid_states = valid_states
        message = f"Invalid state code: '{state}'"
        if valid_states:
            message += f". Valid codes: {', '.join(sorted(valid_states)[:10])}..."
        super().__init__(message)


class TableNotFoundError(DownloadError):
    """
    Raised when a requested table is not available for download.

    Parameters
    ----------
    table : str
        The table name that was not found.
    state : str, optional
        The state for which the table was requested.
    """

    def __init__(self, table: str, state: str | None = None):
        self.table = table
        self.state = state
        message = f"Table '{table}' not found"
        if state:
            message += f" for state '{state}'"
        super().__init__(message)


class NetworkError(DownloadError):
    """
    Raised when a network-related download failure occurs.

    Parameters
    ----------
    message : str
        Description of the network error.
    url : str, optional
        URL that caused the error.
    status_code : int, optional
        HTTP status code if available.
    """

    def __init__(
        self, message: str, url: str | None = None, status_code: int | None = None
    ):
        self.status_code = status_code
        super().__init__(message, url)


class ChecksumError(DownloadError):
    """
    Raised when a downloaded file fails checksum verification.

    Parameters
    ----------
    file_path : str
        Path to the file that failed verification.
    expected : str
        Expected checksum value.
    actual : str
        Actual checksum value.
    """

    def __init__(self, file_path: str, expected: str, actual: str):
        self.file_path = file_path
        self.expected = expected
        self.actual = actual
        message = (
            f"Checksum mismatch for {file_path}: expected {expected}, got {actual}"
        )
        super().__init__(message)


class InsufficientSpaceError(DownloadError):
    """
    Raised when there is insufficient disk space for download.

    Parameters
    ----------
    required_bytes : int
        Required disk space in bytes.
    available_bytes : int
        Available disk space in bytes.
    path : str
        Path where download was attempted.
    """

    def __init__(self, required_bytes: int, available_bytes: int, path: str):
        self.required_bytes = required_bytes
        self.available_bytes = available_bytes
        self.path = path
        required_mb = required_bytes / (1024 * 1024)
        available_mb = available_bytes / (1024 * 1024)
        message = (
            f"Insufficient disk space at {path}: "
            f"need {required_mb:.1f} MB, have {available_mb:.1f} MB"
        )
        super().__init__(message)
