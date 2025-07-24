# FIA Database Tables and Structure

## Overview
The Forest Inventory and Analysis (FIA) database contains numerous tables that store forest inventory data collected across the United States. The database is organized around plots, conditions, trees, and population estimation tables.

## Core Data Tables

### PLOT Table
**Purpose**: Central table containing plot-level information and location data
**Key columns**:
- `CN` (VARCHAR(34)): Control Number - unique identifier for the plot record
- `PREV_PLT_CN`: Link to previous plot measurement
- `INVYR`: Inventory year
- `STATECD`, `UNITCD`, `COUNTYCD`, `PLOT`: Location identifiers
- `PLOT_STATUS_CD`: Plot status (1=Sampled, 2=Nonsampled)
- `LAT`, `LON`: Geographic coordinates
- `MEASYEAR`, `MEASMON`, `MEASDAY`: Measurement date
- `DESIGNCD`: Sample design code
- `REMPER`: Remeasurement period

### TREE Table  
**Purpose**: Contains individual tree measurements and attributes
**Key columns**:
- `CN`: Unique tree record identifier
- `PLT_CN`: Link to PLOT table
- `PREV_TRE_CN`: Link to previous tree measurement
- `INVYR`: Inventory year
- `SUBP`: Subplot number
- `TREE`: Tree number on subplot
- `CONDID`: Condition class number
- `STATUSCD`: Tree status (1=Live, 2=Dead, 3=Removed)
- `SPCD`: Species code
- `DIA`: Diameter at breast height (inches)
- `HT`: Total height (feet)
- `ACTUALHT`: Actual measured height
- `TPA_UNADJ`: Unadjusted trees per acre factor
- `VOLCFNET`: Net cubic foot volume
- `VOLCSNET`: Net cubic foot volume in sawlog portion
- `DRYBIO_AG`: Aboveground biomass (pounds)

### COND Table
**Purpose**: Describes forest conditions on plots
**Key columns**:
- `CN`: Unique condition record identifier  
- `PLT_CN`: Link to PLOT table
- `CONDID`: Condition number
- `COND_STATUS_CD`: Condition status (1=Forest, 2=Nonforest)
- `OWNGRPCD`: Ownership group code
- `FORTYPCD`: Forest type code
- `STDAGE`: Stand age
- `CONDPROP_UNADJ`: Unadjusted proportion of plot in condition

### SEEDLING Table
**Purpose**: Seedling counts by species and condition
**Key columns**:
- `CN`: Unique seedling record identifier
- `PLT_CN`: Link to PLOT table
- `CONDID`: Condition number
- `SPCD`: Species code  
- `TREECOUNT`: Number of seedlings counted
- `TREECOUNT_CALC`: Calculated seedlings per acre

## Population Estimation Tables

### POP_EVAL Table
**Purpose**: Defines evaluation groups for population estimates
**Key columns**:
- `CN`: Unique evaluation identifier
- `EVALID`: 6-digit evaluation code (state + year + type)
- `EVAL_DESCR`: Evaluation description
- `STATECD`: State code
- `START_INVYR`, `END_INVYR`: Temporal boundaries
- `GROWTH_ACCT`: Growth accounting capability flag
- `ESTN_METHOD`: Estimation method

### POP_EVAL_TYP Table
**Purpose**: Links evaluations to estimation types
**Key columns**:
- `EVAL_CN`: Link to POP_EVAL
- `EVAL_TYP`: Type of evaluation (VOL, GRM, CHNG, DWM, INVASIVE)

### POP_STRATUM Table  
**Purpose**: Stratification information and expansion factors
**Key columns**:
- `CN`: Unique stratum identifier
- `ESTN_UNIT_CN`: Link to estimation unit
- `EVALID`: Evaluation identifier
- `STRATUMCD`: Stratum code
- `P1POINTCNT`, `P2POINTCNT`: Phase 1 and 2 point counts
- `EXPNS`: Expansion factor
- `ADJ_FACTOR_*`: Various adjustment factors

### POP_PLOT_STRATUM_ASSGN Table
**Purpose**: Links plots to strata for population estimation
**Key columns**:
- `STRATUM_CN`: Link to POP_STRATUM
- `PLT_CN`: Link to PLOT table
- `EVALID`: Evaluation identifier

### POP_ESTN_UNIT Table
**Purpose**: Defines estimation units (geographic areas for estimation)
**Key columns**:
- `CN`: Unique estimation unit identifier
- `EVAL_CN`: Link to POP_EVAL
- `ESTN_UNIT`: Estimation unit code
- `AREA_USED`: Area in acres

## Supporting Tables

### SUBPLOT Table
**Purpose**: Subplot-level information
**Key columns**:
- `CN`: Unique subplot identifier
- `PLT_CN`: Link to PLOT table
- `SUBP`: Subplot number
- `SUBP_STATUS_CD`: Subplot status

### SUBP_COND Table
**Purpose**: Links subplots to conditions
**Key columns**:
- `PLT_CN`: Link to PLOT table
- `SUBP`: Subplot number
- `CONDID`: Condition number
- `SUBPCOND_PROP`: Proportion of subplot in condition

### SURVEY Table
**Purpose**: Survey information and inventory cycles
**Key columns**:
- `CN`: Unique survey identifier
- `INVYR`: Inventory year
- `CYCLE`, `SUBCYCLE`: Inventory cycle information

### REF_SPECIES Table
**Purpose**: Species reference information
**Key columns**:
- `SPCD`: Species code
- `COMMON_NAME`: Common species name
- `GENUS`, `SPECIES`: Scientific classification
- `SCIENTIFIC_NAME`: Full scientific name

## Additional Tables

### COND_DWM_CALC Table
**Purpose**: Down woody material calculations by condition

### INVASIVE_SUBPLOT_SPP Table
**Purpose**: Invasive species presence by subplot

### P2VEG_SUBP_STRUCTURE Table
**Purpose**: Phase 2 vegetation structure data

### TREE_GRM_* Tables
**Purpose**: Growth, removal, and mortality components
- `TREE_GRM_BEGIN`: Beginning tree attributes
- `TREE_GRM_MIDPT`: Midpoint attributes for growth calculations
- `TREE_GRM_COMPONENT`: Growth/removal/mortality classification

### PLOTGEOM Table
**Purpose**: Plot geometry for spatial operations

### SUBP_COND_CHNG_MTRX Table
**Purpose**: Condition change matrix between measurements

## Key Relationships

1. **Plot-Tree**: PLOT.CN → TREE.PLT_CN (one-to-many)
2. **Plot-Condition**: PLOT.CN → COND.PLT_CN (one-to-many)
3. **Tree-Condition**: Trees linked to conditions via CONDID
4. **Plot-Evaluation**: Through POP_PLOT_STRATUM_ASSGN
5. **Evaluation-Stratum**: Through POP_STRATUM
6. **Previous Measurements**: PREV_PLT_CN and PREV_TRE_CN fields

## Important Notes

- CN (Control Number) fields are VARCHAR(34), not integers
- EVALID system is critical for proper population estimation
- Many tables have both adjusted and unadjusted values
- Temporal relationships maintained through INVYR and PREV_* fields
- Expansion factors in POP_STRATUM are essential for population estimates