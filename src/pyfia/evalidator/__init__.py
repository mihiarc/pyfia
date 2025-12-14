"""
EVALIDator API client for validating pyFIA estimates against official USFS values.

This package provides programmatic access to the USFS EVALIDator API, enabling
automated comparison of pyFIA estimates with official FIA population estimates.

The EVALIDator API is documented at:
https://apps.fs.usda.gov/fiadb-api/

Modules
-------
estimate_types
    EVALIDator estimate type codes (snum parameter values)
client
    EVALIDator API client for retrieving official estimates
validation
    Functions for comparing pyFIA estimates with EVALIDator

Example
-------
>>> from pyfia.evalidator import EVALIDatorClient, compare_estimates
>>>
>>> # Get official estimate from EVALIDator
>>> client = EVALIDatorClient()
>>> official = client.get_forest_area(state_code=37, year=2023)
>>>
>>> # Compare with pyFIA estimate
>>> from pyfia import FIA, area
>>> with FIA("path/to/db.duckdb") as db:
...     db.clip_by_state(37)
...     db.clip_most_recent(eval_type="EXPALL")
...     pyfia_result = area(db, land_type="forest")
>>>
>>> # Validate
>>> comparison = compare_estimates(pyfia_result, official)
>>> print(f"Difference: {comparison['pct_diff']:.2f}%")

References
----------
- FIADB-API Documentation: https://apps.fs.usda.gov/fiadb-api/
- Arbor Analytics Guide: https://arbor-analytics.com/post/2023-10-25-using-r-and-python-to-get-forest-resource-data-through-the-evalidator-api/
"""
