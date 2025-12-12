# FIA Database

The `FIA` class is the main entry point for working with FIA data.

## Overview

```python
import pyfia

# Basic usage
db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Use estimation functions
result = pyfia.volume(db, grp_by="SPCD")
```

## Class Reference

::: pyfia.FIA
    options:
      members:
        - __init__
        - load_table
        - find_evalid
        - clip_by_evalid
        - clip_by_state
        - clip_most_recent
        - get_plots
        - get_trees
        - get_conditions
        - prepare_estimation_data
      show_root_heading: true
      show_source: true
