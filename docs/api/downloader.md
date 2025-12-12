# Data Download

PyFIA provides functions to download FIA data directly from the USDA Forest Service FIA DataMart.

## Overview

```python
from pyfia import download

# Download single state
db_path = download("GA")

# Download multiple states (merged)
db_path = download(["GA", "FL", "SC"])

# Download specific tables
db_path = download("GA", tables=["PLOT", "TREE", "COND"])
```

## Main Function

::: pyfia.download
    options:
      show_root_heading: true
      show_source: true

## DataMart Client

::: pyfia.downloader.DataMartClient
    options:
      members:
        - __init__
        - download_table
        - download_tables
        - check_url_exists
      show_root_heading: true
      show_source: true

## Download Cache

::: pyfia.downloader.DownloadCache
    options:
      members:
        - __init__
        - get_cached
        - add_to_cache
        - clear_cache
        - get_cache_info
        - list_cached
      show_root_heading: true
      show_source: true

## Cache Management Functions

::: pyfia.downloader.clear_cache
    options:
      show_root_heading: true
      show_source: true

::: pyfia.downloader.cache_info
    options:
      show_root_heading: true
      show_source: true

## Table Definitions

::: pyfia.downloader.COMMON_TABLES
    options:
      show_root_heading: true

::: pyfia.downloader.ALL_TABLES
    options:
      show_root_heading: true

::: pyfia.downloader.VALID_STATE_CODES
    options:
      show_root_heading: true

## Exceptions

::: pyfia.downloader.DownloadError
    options:
      show_root_heading: true
      show_source: true

::: pyfia.downloader.StateNotFoundError
    options:
      show_root_heading: true
      show_source: true

::: pyfia.downloader.TableNotFoundError
    options:
      show_root_heading: true
      show_source: true

::: pyfia.downloader.NetworkError
    options:
      show_root_heading: true
      show_source: true
