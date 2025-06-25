# Section 9.7: Population Stratum Table
**Oracle Table Name:** POP_STRATUM
**Extracted Pages:** 487-494 (Chapter pages 9-21 to 9-28)
**Source:** FIA Database Handbook v9.3
**Chapter:** 9 - Database Tables - Population

---

## 9.7 Population Stratum Table (Oracle table name: POP\_STRATUM)

| Subsection   | Column name (attribute)   | Descriptive name                                                   | Oracle data type   |
|--------------|---------------------------|--------------------------------------------------------------------|--------------------|
| 9.7.1        | CN                        | Sequence number                                                    | VARCHAR2(34)       |
| 9.7.2        | ESTN_UNIT_CN              | Estimation unit sequence number                                    | VARCHAR2(34)       |
| 9.7.3        | RSCD                      | Region or station code                                             | NUMBER(2)          |
| 9.7.4        | EVALID                    | Evaluation identifier                                              | NUMBER(6)          |
| 9.7.5        | ESTN_UNIT                 | Estimation unit                                                    | NUMBER(6)          |
| 9.7.6        | STRATUMCD                 | Stratum code                                                       | NUMBER(6)          |
| 9.7.7        | STRATUM_DESCR             | Stratum description                                                | VARCHAR2(255)      |
| 9.7.8        | STATECD                   | State code                                                         | NUMBER(4)          |
| 9.7.9        | P1POINTCNT                | Phase 1 point count                                                | NUMBER(12)         |
| 9.7.10       | P2POINTCNT                | Phase 2 point count                                                | NUMBER(12)         |
| 9.7.11       | EXPNS                     | Expansion factor                                                   | NUMBER             |
| 9.7.12       | ADJ_FACTOR_MACR           | Adjustment factor for the macroplot                                | NUMBER             |
| 9.7.13       | ADJ_FACTOR_SUBP           | Adjustment factor for the subplot                                  | NUMBER             |
| 9.7.14       | ADJ_FACTOR_MICR           | Adjustment factor for the microplot                                | NUMBER             |
| 9.7.15       | ADJ_FACTOR_CWD            | Adjustment factor for coarse woody debris                          | NUMBER             |
| 9.7.16       | ADJ_FACTOR_FWD_SM         | Adjustment factor for small fine woody debris                      | NUMBER             |
| 9.7.17       | ADJ_FACTOR_FWD_LG         | Adjustment factor for large fine woody debris                      | NUMBER             |
| 9.7.18       | ADJ_FACTOR_DUFF           | Adjustment factor for the duff and litter layer                    | NUMBER             |
| 9.7.19       | CREATED_BY                | Created by                                                         | VARCHAR2(30)       |
| 9.7.20       | CREATED_DATE              | Created date                                                       | DATE               |
| 9.7.21       | CREATED_IN_INSTANCE       | Created in instance                                                | VARCHAR2(6)        |
| 9.7.22       | MODIFIED_BY               | Modified by                                                        | VARCHAR2(30)       |
| 9.7.23       | MODIFIED_DATE             | Modified date                                                      | DATE               |
| 9.7.24       | MODIFIED_IN_INSTANCE      | Modified in instance                                               | VARCHAR2(6)        |
| 9.7.25       | ADJ_FACTOR_PILE           | Adjustment factor for piles                                        | NUMBER             |
| 9.7.26       | ADJ_FACTOR_REGEN_MICR     | Adjustment factor for tree regeneration indicator on the microplot | NUMBER             |
| 9.7.27       | ADJ_FACTOR_INV_SUBP       | Adjustment factor for invasive species on the subplot              | NUMBER             |
| 9.7.28       | ADJ_FACTOR_P2VEG_SUBP     | Adjustment factor for Phase 2 vegetation profile on the subplot    | NUMBER             |

| Subsection   | Column name (attribute)       | Descriptive name                                             | Oracle data type   |
|--------------|-------------------------------|--------------------------------------------------------------|--------------------|
| 9.7.29       | ADJ_FACTOR_GRNDLYR_MIC ROQUAD | Adjustment factor for ground cover layer on the microquadrat | NUMBER             |
| 9.7.30       | ADJ_FACTOR_SOIL               | Adjustment factor for soils                                  | NUMBER             |

| Key Type   | Column(s) order                    | Tables to link               | Abbreviated notation   |
|------------|------------------------------------|------------------------------|------------------------|
| Primary    | CN                                 | N/A                          | PSM_PK                 |
| Unique     | RSCD, EVALID, ESTN_UNIT, STRATUMCD | N/A                          | PSM_UK                 |
| Foreign    | ESTN_UNIT_CN                       | POP_STRATUM to POP_ESTN_UNIT | PSM_PEU_FK             |

## 9.7.1 CN

Sequence number. A unique sequence number used to identify a population stratum record.

## 9.7.2 ESTN\_UNIT\_CN

Estimation unit sequence number. Foreign key linking the stratum record to the estimation unit record.

## 9.7.3 RSCD

Region or Station code. See SURVEY.RSCD description for definition.

## 9.7.4 EVALID

Evaluation identifier. See POP\_EVAL.EVALID description for definition.

## 9.7.5 ESTN\_UNIT

Estimation unit. A number assigned to the specific geographic area that is stratified. Estimation units are often determined by a combination of geographical boundaries, sampling intensity and ownership.

## 9.7.6 STRATUMCD

Stratum code. A code uniquely identifying a stratum within an estimation unit.

## 9.7.7 STRATUM\_DESCR

Stratum description. A brief description or phrase used to identify a stratum. A stratum is a non-overlapping subdivision of the population. Each plot is assigned to one and only one subdivision or stratum; the relative sizes of strata are used to compute strata weights (Bechtold and Patterson 2005). Strata are usually based on land use (e.g., forest or nonforest) but may also be based on other criteria (e.g., ownership, crown cover).

## 9.7.8 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B. For evaluations that do not conform to the boundaries of a single State the value of STATECD should be set to 99.

## 9.7.9 P1POINTCNT

Phase 1 point count. The number of basic units (pixels or points) in the stratum.

## 9.7.10 P2POINTCNT

Phase 2 point count. The number of field plots that are within the stratum.

## 9.7.11 EXPNS

Expansion factor. The area, in acres, that a stratum represents divided by the number of sampled plots in that stratum:

EXPNS = (POP\_ESTN\_UNIT.AREA\_USED*P1POINTCNT / POP\_ESTN\_UNIT.P1PNTCNT\_EU) / P2POINTCNT.

This attribute can be used to obtain estimates of population area when summed across all the plots in the population of interest.

Refer to The Forest Inventory and Analysis Database: Population Estimation User Guide for detailed examples.

## 9.7.12 ADJ\_FACTOR\_MACR

Adjustment factor for the macroplot. A value that adjusts the population estimates to account for partially nonsampled plots due to hazardous conditions or denied access. It is used with condition proportion (COND.CONDPROP\_UNADJ) and area expansion (EXPNS) to provide area estimates, when COND.PROP\_BASIS = 'MACR' (indicating macroplot installed). ADJ\_FACTOR\_MACR is also used with EXPNS and trees per acre unadjusted (e.g., TREE.TPA\_UNADJ) to provide tree estimates for sampled land. If a macroplot was not installed, this attribute is left blank (null). Refer to The Forest Inventory and Analysis Database: Population Estimation User Guide for detailed examples.

## 9.7.13 ADJ\_FACTOR\_SUBP

Adjustment factor for the subplot. A value that adjusts the population estimates to account for partially nonsampled plots due to hazardous conditions or denied access. It is used with condition proportion (COND.CONDPROP\_UNADJ) and area expansion (EXPNS) to provide area estimates, when COND.PROP\_BASIS = 'SUBP' (indicating subplots installed). ADJ\_FACTOR\_SUBP is also used with EXPNS and trees per acre unadjusted (e.g., TREE.TPA\_UNADJ) to provide tree estimates for sampled land. Refer to The Forest Inventory and Analysis Database: Population Estimation User Guide for detailed examples.

## 9.7.14 ADJ\_FACTOR\_MICR

Adjustment factor for the microplot. A value that adjusts population estimates to account for partially nonsampled plots due to hazardous conditions or denied access. It is used with area expansion (EXPNS) and seedlings per acre unadjusted (SEEDLING.TPA\_UNADJ) or saplings per acre unadjusted (TREE.TPA\_UNADJ where TREE.DIA &lt;5.0) to provide tree estimates for sampled land. Refer to The Forest Inventory and Analysis Database: Population Estimation User Guide for detailed examples.

## 9.7.15 ADJ\_FACTOR\_CWD

Adjustment factor for coarse woody debris. A value that adjusts the population estimates to account for partially nonsampled transects due to hazardous conditions or denied access. This attribute is used in the process that populates adjusted values in COND\_DWM\_CALC (i.e., plot-level estimate, condition, and adjustment for estimation).

## 9.7.16 ADJ\_FACTOR\_FWD\_SM

Adjustment factor for small fine woody debris. A value that adjusts the population estimates to account for partially nonsampled transects due to hazardous conditions or denied access. This attribute is used in the process that populates adjusted values in COND\_DWM\_CALC (i.e., plot-level estimate, condition, and adjustment for estimation).

## 9.7.17 ADJ\_FACTOR\_FWD\_LG

Adjustment factor for large fine woody debris. A value that adjusts the population estimates to account for partially nonsampled transects due to hazardous conditions or denied access. This attribute is used in the process that populates adjusted values in COND\_DWM\_CALC (i.e., plot-level estimate, condition, and adjustment for estimation).

## 9.7.18 ADJ\_FACTOR\_DUFF

Adjustment factor for the duff and litter layer. A value that adjusts the population estimates to account for partially nonsampled points due to hazardous conditions or denied access. This attribute is used in the process that populates adjusted values in COND\_DWM\_CALC (i.e., plot-level estimate, condition, and adjustment for estimation).

## 9.7.19 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 9.7.20 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 9.7.21 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 9.7.22 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 9.7.23 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 9.7.24 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 9.7.25 ADJ\_FACTOR\_PILE

Adjustment factor for piles. A value that adjusts the population estimates to account for partially nonsampled transects or plots due to hazardous conditions or denied access.

## 9.7.26 ADJ\_FACTOR\_REGEN\_MICR

Adjustment factor for tree regeneration indicator on the microplot. A value that adjusts the population estimates to account for partially nonsampled plots due to hazardous conditions or denied access. This is the ratio of the total area of the microplot footprint to the area of the microplot footprint that was actually sampled. This value is for plots that include an optional tree regeneration indicator sample protocol. Only populated by certain FIA work units (SURVEY.RSCD = 23, 24).

## 9.7.27 ADJ\_FACTOR\_INV\_SUBP

Adjustment factor for invasive species on the subplot. A value that adjusts the population estimates to account for partially nonsampled plots due to hazardous conditions or denied access. This is the ratio of the total area of the subplot footprint to the area of the subplot footprint that was actually sampled. This value is for plots that include an optional invasive species sample protocol.

## 9.7.28 ADJ\_FACTOR\_P2VEG\_SUBP

Adjustment factor for Phase 2 vegetation profile on the subplot. A value that adjusts the population estimates to account for partially nonsampled plots due to hazardous conditions or denied access. This is the ratio of the total area of the subplot footprint to the area of the subplot footprint that was actually sampled. This value is for plots that include an optional P2 (Phase 2) vegetation profile sample protocol.

## 9.7.29 ADJ\_FACTOR\_GRNDLYR\_MICROQUAD

Adjustment factor for ground cover layer on the microquadrat. A value that adjusts the population estimates to account for partially nonsampled plots due to hazardous conditions or denied access. This is the ratio of the total area of the microquadrat footprint to the area of the microquadrat footprint that was actually sampled. This value is for plots that include an optional ground cover layer sample protocol. Only populated by certain FIA work units (SURVEY.RSCD = 27).

## 9.7.30 ADJ\_FACTOR\_SOIL

Adjustment factor for soils. A value that adjusts the population estimates to account for partially nonsampled plots due to hazardous conditions or denied access. This is the ratio of the total soil points of the footprint to the number of soil points that were actually sampled. This value is for plots that include a soil sample.

Population Stratum Table

Chapter 9 (revision: 04.2024)

Section revision: 12.2024

## Chapter 10: Database Tables - Plot Geometry; Plot Snapshot

## Chapter Contents:

|   Section | Database table      | Oracle table name   |
|-----------|---------------------|---------------------|
|      10.1 | Plot Geometry Table | PLOTGEOM            |
|      10.2 | Plot Snapshot Table | PLOTSNAP            |

## Definitions for database tables:

For further detail and examples, refer to the Overview (chapter 1).

## Keys Presented with the Tables

| Key type   | Definition                                                                                                                                           |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| Primary    | A single column in a table whose values uniquely identify each row in an Oracle table.                                                               |
| Unique     | Multiple columns in a table whose values uniquely identify each row in an Oracle table. There can be one and only one row for each unique key value. |
| Natural    | A type of unique key made from existing attributes in the table. It is stored as an index in this database.                                          |
| Foreign    | A column in a table that is used as a link to a matching column in another Oracle table.                                                             |

## Oracle Data Types

| Oracle data type   | Definition                                                                                                                                                                           |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DATE               | A data type that stores the date.                                                                                                                                                    |
| NUMBER             | A data type that contains only numbers, positive or negative, with a floating-decimal point.                                                                                         |
| NUMBER(SIZE, D)    | A data type that contains only numbers up to a specified maximum size. The maximum size ( and optional fixed-decimal point ) is specified by the value(s) listed in the parentheses. |
| VARCHAR2(SIZE)     | A data type that contains alphanumeric data (numbers and/or characters) up to a specified maximum size.                                                                              |