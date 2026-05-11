"""NGHGI report reproduction utilities (not part of the pyfia public API).

These modules live under ``scripts/`` rather than ``pyfia/`` because they
exist to reproduce EPA Chapter 6 / Annex 3.13 forest carbon tables, not
to provide new estimation capability. The pyfia carbon estimators
themselves stay in ``pyfia.carbon``.

Stages
------
- ``stage_a``                — reproduce EPA Table 6-10 forest carbon stocks
- ``stage_b``                — state-level flux vs EPA Annex 3.13 Table A-208
- ``multi_year``             — multi-year stock comparison 2019-2023
- ``dead_wood_diagnostic``   — isolate the NSVB-vs-FIADB standing-dead gap
- ``_compile``               — pool aggregation helper used by the above

All scripts read state DuckDB files from a directory configured via
``--db-dir`` or the ``PYFIA_FIADB_DIR`` environment variable.
"""
