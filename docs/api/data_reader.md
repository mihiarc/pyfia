# Data Reader

Low-level data reading utilities for FIA databases.

## Overview

The `FIADataReader` provides optimized reading capabilities with backend abstraction for DuckDB and SQLite databases.

```python
from pyfia import FIADataReader

reader = FIADataReader("georgia.duckdb")
schema = reader.get_table_schema("TREE")
data = reader.read_table("TREE", columns=["CN", "DIA", "SPCD"])
```

## Class Reference

::: pyfia.FIADataReader
    options:
      members:
        - __init__
        - get_table_schema
        - read_table
        - read_plot_data
        - read_tree_data
        - read_cond_data
        - read_pop_tables
        - read_evalid_data
      show_root_heading: true
      show_source: true
