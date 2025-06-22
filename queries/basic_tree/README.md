# Basic Tree Queries

This section contains fundamental tree enumeration and counting queries that form the foundation of forest inventory analysis.

## Queries in this Section

### 1. [Oregon Total Live Trees](./oregon_total_live_trees.sql)
**EVALID: 412101 (Oregon 2021)**  
**Result: 10,481,113,490 live trees (357.8 trees/acre)**

A comprehensive example of EVALIDator-style tree counting that demonstrates:
- Proper adjustment factors (MICR/SUBP/MACR) based on tree diameter
- Population expansion factors (EXPNS) for statistical estimates
- Exact Oracle EVALIDator query structure
- Forest area calculation: 29,292,380 acres

## Key Concepts Demonstrated

- **Diameter-based Adjustment Factors**: Trees are counted differently based on their size
- **Population Expansion**: Converting plot-level data to population estimates
- **Status Filtering**: Focusing on live trees in forest conditions
- **EVALID Usage**: Proper statistical grouping for estimates

## EVALIDator Methodology

These queries follow Oracle EVALIDator methodology exactly:
- Uses `TPA_UNADJ` (trees per acre, unadjusted) as the base metric
- Applies diameter-specific adjustment factors
- Multiplies by expansion factors for population estimates
- Filters appropriately for live trees and forest conditions

## Usage Notes

- Always verify EVALID values match your analysis needs
- Understand that different EVALIDs represent different time periods and methodologies
- Results represent statistical estimates, not exact counts
- Plot counts provide context for sample size and reliability 