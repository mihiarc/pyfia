# Biomass and Carbon Queries

This section contains queries for calculating forest biomass and carbon storage using species-specific equations and properties.

## Queries in this Section

### 1. [Colorado Above-Ground Dry Biomass by Species Group](./colorado_biomass_by_species_group.md)
**EVALID: 82101 (Colorado 2021)**
**Result: 1.096 billion tons total above-ground dry biomass, 10 species groups**

A comprehensive biomass calculation that demonstrates:
- Species-specific wood and bark properties
- Moisture content adjustments for accurate dry weight
- Specific gravity calculations for wood and bark
- Bark volume percentage corrections
- Simplified approach that produces identical results to Oracle EVALIDator

**Top Species Groups by Biomass:**
- **18**: Engelmann and other spruces - 288,927,955 tons (26.4%)
- **44**: Cottonwood and aspen (West) - 247,391,052 tons (22.6%)
- **12**: True fir - 122,095,237 tons (11.1%)
- **23**: Woodland softwoods - 114,570,440 tons (10.5%)

### 2. [North Carolina Merchantable Bole Biomass](./north_carolina_merchantable_biomass.md)
**EVALID: 372301 (North Carolina 2023)**
**Result: 1.56 billion green short tons merchantable bole biomass**

Calculates merchantable bole bark and wood biomass for timber species ≥5" DBH:
- Green weight (includes moisture content) calculations
- Merchantable bole components (DRYBIO_BOLE + DRYBIO_BOLE_BARK)
- Results by diameter class and species
- Complex moisture content conversions for wood and bark
- EVALIDator-compatible formatting with species details

**Key Findings:**
- Peak biomass in 11.0-12.9" diameter class (202.1 million tons)
- Loblolly pine dominates across all size classes
- 995 unique diameter class × species combinations
- Demonstrates managed forest structure with concentration in pole-sized trees

## Key Concepts Demonstrated

- **Species-Specific Calculations**: Each species has unique wood and bark properties
- **Moisture Content Adjustments**: Separate adjustments for wood and bark moisture
- **Specific Gravity Applications**: Converting volume to weight using species properties
- **Bark Volume Corrections**: Accounting for bark proportion in total tree volume
- **Unit Conversions**: Converting from pounds to tons (/2000)
- **Query Simplification**: Readable approach maintaining statistical accuracy

## EVALIDator Methodology

These queries follow Oracle EVALIDator biomass methodology:
- Uses `DRYBIO_AG` (above-ground dry biomass) as base measurement
- Applies complex species-specific conversion factors
- Includes moisture content adjustments (MC_PCT_GREEN_WOOD, MC_PCT_GREEN_BARK)
- Uses specific gravity values (WOOD_SPGR_GREENVOL_DRYWT, BARK_SPGR_GREENVOL_DRYWT)
- Handles bark volume percentages (BARK_VOL_PCT) for accurate wood/bark ratios
- Applies diameter-based adjustment factors with nested CASE logic

## Advanced Features

- **Special Species Mapping**: Eastern redcedar (SPCD 122) mapped to different species groups by state
- **Default Values**: Uses 1.76 specific gravity when species data unavailable
- **Complex Calculations**: Multi-step biomass equations matching Oracle formulas exactly
- **Verification**: Simplified version tested to produce identical results to complex Oracle translation

## Usage Notes

- Biomass calculations are computationally intensive due to species-specific formulas
- Results represent dry weight biomass (moisture removed)
- Carbon content typically estimated as ~50% of dry biomass
- Query demonstrates balance between accuracy and maintainability