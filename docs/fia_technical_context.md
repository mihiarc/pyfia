# FIA Technical Context for API Documentation Enhancement

## Overview
This document compiles authoritative FIA technical information gathered from official USDA Forest Service sources to enhance pyFIA's API documentation.

## Key References

### Primary Methodological Reference
**Bechtold, W.A. and Patterson, P.L. (Editors). 2005.** The enhanced forest inventory and analysis program - national sampling design and estimation procedures. General Technical Report SRS-80. U.S. Department of Agriculture, Forest Service, Southern Research Station, Asheville, NC. 85 p.

- Foundational document for FIA's enhanced program methodology
- Describes transition to annual inventory system
- Details three-phase sampling design
- Presents core estimators for National Information Management System (NIMS)
- Available at: https://www.srs.fs.usda.gov/pubs/gtr/gtr_srs080.pdf

### Additional Key References
- **Woudenberg, S.W. et al. 2010.** The Forest Inventory and Analysis Database: Database description and users manual version 4.0 for Phase 2. Gen. Tech. Rep. RMRS-GTR-245. Fort Collins, CO: U.S. Department of Agriculture, Forest Service, Rocky Mountain Research Station.
- **Westfall, J.A. et al. 2023.** National scale volume and biomass (NSVB) estimators. Adopted by FIA program September 2023.

## FIA Sampling Design

### Three-Phase Design
1. **Phase 1**: Remote sensing for stratification and identification of forested land
2. **Phase 2**: Field plots - one per 6,000 acres of forest land
   - Forest type, site attributes, tree species, tree size, tree condition
   - Spatially explicit tree compositional data
3. **Phase 3**: Subset of Phase 2 plots with expanded measurements
   - Tree crown conditions, lichen composition, understory vegetation
   - Down woody debris, soil attributes

### Plot Configuration
FIA uses a nested plot design with specific radii:

#### Plot Components and Sizes
- **Microplot**:
  - Radius: 2.073 m (6.8 ft)
  - Trees measured: DBH < 12.446 cm (4.9 in)
  - Area: 13.5 m² (145 ft²)

- **Subplot**:
  - Radius: 7.315 m (24.0 ft)
  - Trees measured: DBH ≥ 12.446 cm (4.9 in)
  - Area: 168.3 m² (1,809 ft²)

- **Macroplot**:
  - Radius: 17.953 m (58.9 ft)
  - Trees measured: Large trees based on regional breakpoint diameter
  - Area: 1,012 m² (10,890 ft²)

#### Total Sample Area
- Each plot represents approximately 2.47 acres
- Data extrapolated to represent ~6,000 acres

## Evaluation System (EVALID)

### EVALID Structure
Format: **SSYYTT** (6-digit code)
- **SS**: State FIPS code (e.g., 48 for Texas)
- **YY**: Evaluation year (last 2 digits, e.g., 23 for 2023)
- **TT**: Evaluation type code

### Evaluation Types (EVAL_TYP)
- **EXPALL** (typically TT=00): All area estimation plots - most comprehensive
- **EXPVOL** (typically TT=01): Volume/biomass plots with tree measurements
- **EXPGROW**: Growth estimation plots
- **EXPMORT**: Mortality estimation plots
- **EXPREMV**: Removal estimation plots
- **EXPCHNG** (typically TT=03): Change estimation plots
- **EXPDWM** (typically TT=07): Down woody materials plots
- **EXPINV** (typically TT=09): Inventory plots

### Critical Rules
1. Never mix EVALIDs - use only ONE per estimation
2. EXPALL required for unbiased area estimates
3. Evaluation year represents complete statistical estimate, not individual plot measurements

## Volume Calculations

### Volume Variables in FIA Database

#### VOLCFNET (Net Cubic-Foot Volume)
- Net volume after deducting defects and cull
- Used for growing stock calculations
- Formula: TPACURR × VOLCFNET × EXPVOL

#### VOLCFGRS (Gross Cubic-Foot Volume)
- Total stem volume (1-ft stump to 4-in top)
- Trees ≥ 5.0 inches DBH
- No deductions for defects

#### VOLCFSND (Sound Cubic-Foot Volume)
- Volume excluding rotten or missing cull
- Used for merchantable bole biomass when available

#### VOLBFNET (Net Board-Foot Volume)
- Sawlog volume in board feet
- Different size thresholds:
  - Softwoods: ≥9" DBH, 1-ft stump to 6" top
  - Hardwoods: ≥11" DBH, 1-ft stump to 8" top

### Growing Stock Definition
- Live trees ≥5" DBH
- Tree class code (TREECLCD) = 2
- No serious defects affecting merchantability
- Volume: 1-ft stump to 4-inch top diameter

## Expansion Factors and Adjustments

### Tree-Level Expansion
**TPA_UNADJ (Trees Per Acre Unadjusted)**
- Base expansion factor before plot-level adjustments
- Varies by plot component:
  - Microplot: 74.965282 trees/acre
  - Subplot: 6.018046 trees/acre
  - Macroplot: Varies by region

### Plot-Level Adjustments
**ADJ_FACTOR_MICR, ADJ_FACTOR_SUBP, ADJ_FACTOR_MACR**
- Adjustment factors from POP_STRATUM table
- Applied based on tree diameter:
  - DIA < 5.0": ADJ_FACTOR_MICR
  - 5.0" ≤ DIA < MACRO_BREAKPOINT_DIA: ADJ_FACTOR_SUBP
  - DIA ≥ MACRO_BREAKPOINT_DIA: ADJ_FACTOR_MACR

### Stratum-Level Expansion
**EXPNS (Expansion Factor)**
- From POP_STRATUM table
- Expands plot to population level
- Accounts for stratification design

### Complete Expansion Formula
```
Per-acre value = Σ(Tree measurement × TPA_UNADJ × ADJ_FACTOR × EXPNS × CONDPROP_UNADJ)
```

## Biomass and Carbon

### Biomass Variables (DRYBIO_*)
- **DRYBIO_AG**: Aboveground biomass (pounds)
- **DRYBIO_BG**: Belowground biomass (coarse roots)
- **DRYBIO_BOLE**: Main stem wood and bark
- **DRYBIO_STUMP**: Stump biomass
- **DRYBIO_SAPLING**: Sapling biomass
- **DRYBIO_TOP**: Top and branches above merchantable height

### Carbon Calculation
- Standard factor: 47% of dry biomass (IPCC guidelines)
- Formula: Carbon = Biomass × 0.47

### Unit Conversions
- Pounds to tons: Divide by 2,000
- Cubic feet to cords: Divide by 79

## Land Classification

### Forest Land (COND_STATUS_CD = 1)
- At least 10% stocked with forest trees
- At least 1 acre in size
- At least 120 feet wide

### Timberland
- Forest land that is:
  - Not reserved (RESERVCD = 0)
  - Capable of producing ≥20 ft³/acre/year (SITECLCD < 7)
  - Available for timber harvest

## Growth-Removal-Mortality (GRM) Tables

### Key GRM Tables
- **TREE_GRM_COMPONENT**: Component-level data for each tree
- **TREE_GRM_MIDPT**: Tree measurements at remeasurement midpoint

### Component Types
- **SURVIVOR**: Trees alive at both measurements
- **MORTALITY1, MORTALITY2**: Trees that died between measurements
- **CUT**: Trees removed through harvest
- **INGROWTH**: New trees growing into measurable size

### GRM Column Naming Pattern
```
SUBP_{METRIC}_{TREE_TYPE}_{LAND_TYPE}
Example: SUBP_TPAMORT_UNADJ_GS_FOREST
```
- Metrics: TPAMORT_UNADJ, TPAREMV_UNADJ, TPAGROW_UNADJ
- Tree types: GS (growing stock), AL (all live)
- Land types: FOREST, TIMBER

## Annual vs. Periodic Inventory

### Rotating Panel Design
- Each state divided into panels (typically 5-7)
- One panel measured annually
- Complete cycle every 5-7 years (10 years in western states)

### Evaluation vs. Inventory Years
- **INVYR**: Year individual plot was measured
- **EVALID Year**: Reference year for complete statistical estimate
- Example: EVALID 482300 represents Texas 2023 conditions, though includes plots from 2019-2023

## Common Grouping Variables

### Ownership (OWNGRPCD)
- 10: National Forest
- 20: Other Federal
- 30: State and Local Government
- 40: Private

### Stand Size Class (STDSZCD)
- 1: Large diameter
- 2: Medium diameter
- 3: Small diameter
- 4: Seedling/sapling
- 5: Nonstocked

### Site Productivity Class (SITECLCD)
- 1: 225+ cu ft/ac/yr
- 2: 165-224 cu ft/ac/yr
- 3: 120-164 cu ft/ac/yr
- 4: 85-119 cu ft/ac/yr
- 5: 50-84 cu ft/ac/yr
- 6: 20-49 cu ft/ac/yr
- 7: 0-19 cu ft/ac/yr

### Tree Status (STATUSCD)
- 1: Live tree
- 2: Dead tree
- 3: Removed (cut or dead)

## Statistical Considerations

### Design-Based Estimation
- Post-stratified estimation for improved precision
- Finite population correction factors
- Ratio-of-means estimators for per-acre values

### Variance Calculation
- Follows Bechtold & Patterson (2005) methodology
- Accounts for stratification
- Includes finite population corrections

### Temporal Methods
- **TI**: Temporally Indifferent (all available data)
- **Annual**: Single year estimates
- **SMA**: Simple Moving Average
- **LMA**: Linear Moving Average
- **EMA**: Exponential Moving Average

## Data Quality Notes

### Pre-1999 Periodic Inventories
- Variable plot designs by region/state
- Some attributes may not be populated
- Different calculation methods possible

### NULL Value Handling
- Common in certain fields (e.g., PHYSCLCD ~18% NULL)
- Handle safely in grouping operations
- May appear as separate group in results

## Implementation Recommendations for pyFIA

### Documentation Enhancements
1. Add Bechtold & Patterson (2005) as primary reference
2. Include plot configuration details in Notes sections
3. Document expansion factor hierarchy
4. Explain EVALID vs INVYR distinction
5. Add volume type definitions
6. Include standard grouping variable descriptions

### Code Improvements
1. Validate EVALID selection to prevent overcounting
2. Implement proper diameter-based adjustment factor selection
3. Add warnings for pre-1999 data limitations
4. Include unit conversion helpers
5. Document GRM table requirements

### User Guidance
1. Emphasize single EVALID rule
2. Explain land type classifications
3. Provide grouping variable recommendations
4. Include expansion factor examples
5. Document regional variations