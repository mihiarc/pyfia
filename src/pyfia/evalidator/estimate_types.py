"""
EVALIDator estimate type codes (snum parameter values).

This module provides the snum codes used by the EVALIDator API to identify
different types of FIA estimates.

Reference: https://apps.fs.usda.gov/fiadb-api/fullreport/parameters/snum
"""


class EstimateType:
    """EVALIDator estimate type codes (snum parameter values)."""

    # Area estimates
    AREA_FOREST = 2  # Area of forest land, in acres
    AREA_TIMBERLAND = 3  # Area of timberland, in acres
    AREA_SAMPLED = 79  # Area of sampled land and water, in acres

    # Area change estimates (remeasured plots only)
    # These require plots measured at two points in time
    AREA_CHANGE_SAMPLED = 126  # Area change of sampled land and water
    AREA_CHANGE_FOREST_REMEASURED = 127  # Forest land area change (remeasured conditions)
    AREA_CHANGE_FOREST_EITHER = 128  # Forest land area change (either measurement)
    AREA_CHANGE_TIMBERLAND_REMEASURED = 129  # Timberland area change (remeasured)
    AREA_CHANGE_TIMBERLAND_EITHER = 130  # Timberland area change (either measurement)

    # Annual area change estimates (annualized rates)
    AREA_CHANGE_ANNUAL_SAMPLED = 135  # Average annual area change of sampled land
    AREA_CHANGE_ANNUAL_FOREST_BOTH = 136  # Annual forest land area change (both measurements)
    AREA_CHANGE_ANNUAL_FOREST_EITHER = 137  # Annual forest land area change (either)
    AREA_CHANGE_ANNUAL_TIMBERLAND_BOTH = 138  # Annual timberland area change (both)
    AREA_CHANGE_ANNUAL_TIMBERLAND_EITHER = 139  # Annual timberland area change (either)

    # Tree counts - Live trees (all species, all tree classes)
    TREE_COUNT_1INCH_FOREST = 4  # Live trees >=1" d.b.h. on forest land
    TREE_COUNT_5INCH_FOREST = (
        5  # Growing-stock trees >=5" d.b.h. on forest land (TREECLCD=2)
    )
    TREE_COUNT_1INCH_TIMBER = 7  # Live trees >=1" d.b.h. on timberland
    TREE_COUNT_5INCH_TIMBER = (
        8  # Growing-stock trees >=5" d.b.h. on timberland (TREECLCD=2)
    )

    # Legacy aliases (for backwards compatibility)
    TREE_COUNT_1INCH = 4  # Alias for TREE_COUNT_1INCH_FOREST
    TREE_COUNT_5INCH = 5  # Alias for TREE_COUNT_5INCH_FOREST (corrected: was 7)

    # Basal area
    BASAL_AREA_1INCH = 1004  # Basal area of live trees >=1" d.b.h. (sq ft)
    BASAL_AREA_5INCH = 1007  # Basal area of live trees >=5" d.b.h. (sq ft)

    # Volume (net merchantable bole)
    VOLUME_NET_GROWINGSTOCK = 15  # Net volume growing-stock trees (cu ft)
    VOLUME_NET_ALLSPECIES = 18  # Net volume all species (cu ft)

    # Sawlog volume (board feet)
    VOLUME_SAWLOG_DOYLE = 19  # Sawlog volume - Doyle rule
    VOLUME_SAWLOG_INTERNATIONAL = 20  # Sawlog volume - International 1/4" rule
    VOLUME_SAWLOG_SCRIBNER = 21  # Sawlog volume - Scribner rule

    # Biomass (dry short tons)
    BIOMASS_AG_LIVE = 10  # Aboveground biomass live trees
    BIOMASS_AG_LIVE_5INCH = 13  # Aboveground biomass live trees >=5" d.b.h.
    BIOMASS_BG_LIVE = 59  # Belowground biomass live trees
    BIOMASS_BG_LIVE_5INCH = 73  # Belowground biomass live trees >=5" d.b.h.

    # Carbon (metric tonnes)
    CARBON_AG_LIVE = 53000  # Aboveground carbon in live trees
    CARBON_TOTAL_LIVE = 55000  # Above + belowground carbon in live trees
    CARBON_POOL_AG = 98  # Aboveground live tree carbon pool
    CARBON_POOL_BG = 99  # Belowground live tree carbon pool
    CARBON_POOL_DEADWOOD = 100  # Dead wood carbon pool
    CARBON_POOL_LITTER = 101  # Litter carbon pool
    CARBON_POOL_SOIL = 102  # Soil organic carbon pool
    CARBON_POOL_TOTAL = 103  # Total forest ecosystem carbon

    # Growth (annual net growth)
    GROWTH_NET_VOLUME = 202  # Annual net growth volume (cu ft)
    GROWTH_NET_BIOMASS = 311  # Annual net growth biomass

    # Mortality (growing-stock trees on forest land)
    MORTALITY_VOLUME = 214  # Annual mortality volume (cu ft) - growing-stock, forest
    MORTALITY_BIOMASS = 336  # Annual mortality biomass (AG) - growing-stock, forest

    # Removals
    REMOVALS_VOLUME = 226  # Annual removals volume (cu ft)
    REMOVALS_BIOMASS = 369  # Annual removals biomass
