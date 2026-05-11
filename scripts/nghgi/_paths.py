"""Shared path resolution for NGHGI reproduction scripts."""

from __future__ import annotations

import os
from pathlib import Path


def resolve_db_dir(cli_value: str | os.PathLike | None) -> Path:
    """Resolve the state-DB directory from CLI, env, or default.

    Precedence:
      1. ``cli_value`` (``--db-dir`` argument), if not None
      2. ``PYFIA_FIADB_DIR`` environment variable, if set
      3. Default: ``./data/fiadb`` relative to the current working directory

    Raises ``FileNotFoundError`` with a clear message if the resolved path
    does not exist.
    """
    if cli_value is not None:
        path = Path(cli_value).expanduser().resolve()
        source = "--db-dir"
    elif os.environ.get("PYFIA_FIADB_DIR"):
        path = Path(os.environ["PYFIA_FIADB_DIR"]).expanduser().resolve()
        source = "$PYFIA_FIADB_DIR"
    else:
        path = (Path.cwd() / "data" / "fiadb").resolve()
        source = "default (./data/fiadb)"

    if not path.is_dir():
        raise FileNotFoundError(
            f"FIA state-DB directory not found: {path} (resolved from {source}). "
            "Pass --db-dir <path> or set PYFIA_FIADB_DIR to override."
        )
    return path
