"""Version is sourced from package metadata, not hardcoded (#92)."""

from importlib.metadata import version

import pyfia


def test_version_is_nonempty_string():
    assert isinstance(pyfia.__version__, str)
    assert pyfia.__version__


def test_version_matches_installed_metadata():
    # __version__ must track the installed distribution version (pyproject),
    # never a separately-maintained string that can drift.
    assert pyfia.__version__ == version("pyfia")
