# Section 3.3: Tree Growth, Removal, and Mortality Component Table
**Oracle Table Name:** TREE_GRM_COMPONENT
**Extracted Pages:** 237-258 (Chapter pages 3-79 to 3-100)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.3 Tree Growth, Removal, and Mortality Component Table

## (Oracle table name: TREE\_GRM\_COMPONENT)

| Subsection   | Column name (attribute)       | Descriptive name                                                                                                             | Oracle data type   |
|--------------|-------------------------------|------------------------------------------------------------------------------------------------------------------------------|--------------------|
| 3.3.1        | TRE_CN                        | Tree sequence number                                                                                                         | VARCHAR2(34)       |
| 3.3.2        | PREV_TRE_CN                   | Previous tree sequence number                                                                                                | VARCHAR2(34)       |
| 3.3.3        | PLT_CN                        | Plot sequence number                                                                                                         | VARCHAR2(34)       |
| 3.3.4        | STATECD                       | State code                                                                                                                   | NUMBER             |
| 3.3.5        | DIA_BEGIN                     | Beginning diameter                                                                                                           | NUMBER(5,2)        |
| 3.3.6        | DIA_MIDPT                     | Midpoint diameter                                                                                                            | NUMBER(5,2)        |
| 3.3.7        | DIA_END                       | Ending diameter                                                                                                              | NUMBER(5,2)        |
| 3.3.8        | ANN_DIA_GROWTH                | Computed annual diameter growth                                                                                              | NUMBER(5,2)        |
| 3.3.9        | ANN_HT_GROWTH                 | Computed annual height growth                                                                                                | NUMBER(5,2)        |
| 3.3.10       | SUBPTYP_BEGIN                 | Beginning plot type code                                                                                                     | NUMBER(1)          |
| 3.3.11       | SUBPTYP_MIDPT                 | Midpoint plot type code                                                                                                      | NUMBER(1)          |
| 3.3.12       | SUBPTYP_END                   | Ending plot type code                                                                                                        | NUMBER(1)          |
| 3.3.13       | MICR_COMPONENT_AL_FOREST      | Trees with DIA  1.0 inch - growth component for the all live estimation type on forest land                                 | VARCHAR2(15)       |
| 3.3.14       | MICR_SUBPTYP_GRM_AL_FOREST    | Trees with DIA  1.0 inch - plot type for GRM for the all live estimation type on forest land                                | NUMBER(1)          |
| 3.3.15       | MICR_TPAGROW_UNADJ_AL_FORE ST | Trees with DIA  1.0 inch - unadjusted trees per acre for growth for the all live estimation type on forest land             | NUMBER(11,6)       |
| 3.3.16       | MICR_TPAREMV_UNADJ_AL_FORE ST | Trees with DIA  1.0 inch - unadjusted trees per acre per year for removals for the all live estimation type on forest land  | NUMBER(11,6)       |
| 3.3.17       | MICR_TPAMORT_UNADJ_AL_FORE ST | Trees with DIA  1.0 inch - unadjusted trees per acre per year for mortality for the all live estimation type on forest land | NUMBER(11,6)       |
| 3.3.18       | SUBP_COMPONENT_AL_FOREST      | Trees with DIA  5.0 inches - growth component for the all live estimation type on forest land                               | VARCHAR2(15)       |
| 3.3.19       | SUBP_SUBPTYP_GRM_AL_FOREST    | Trees with DIA  5.0 inches - plot type for GRM for the all live estimation type on forest land                              | NUMBER(1)          |

| Subsection   | Column name (attribute)       | Descriptive name                                                                                                                    | Oracle data type   |
|--------------|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|--------------------|
| 3.3.20       | SUBP_TPAGROW_UNADJ_AL_FORE ST | Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the all live estimation type on forest land                  | NUMBER(11,6)       |
| 3.3.21       | SUBP_TPAREMV_UNADJ_AL_FORE ST | Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the all live estimation type on forest land       | NUMBER(11,6)       |
| 3.3.22       | SUBP_TPAMORT_UNADJ_AL_FORE ST | Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the all live estimation type on forest land      | NUMBER(11,6)       |
| 3.3.23       | SUBP_COMPONENT_GS_FOREST      | Trees with DIA  5.0 inches - growth component for the growing-stock estimation type on forest land                                 | VARCHAR2(15)       |
| 3.3.24       | SUBP_SUBPTYP_GRM_GS_FOREST    | Trees with DIA  5.0 inches - plot type for GRM for the growing-stock estimation type on forest land                                | NUMBER(1)          |
| 3.3.25       | SUBP_TPAGROW_UNADJ_GS_FORE ST | Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the growing-stock estimation type on forest land             | NUMBER(11,6)       |
| 3.3.26       | SUBP_TPAREMV_UNADJ_GS_FORE ST | Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the growing-stock estimation type on forest land  | NUMBER(11,6)       |
| 3.3.27       | SUBP_TPAMORT_UNADJ_GS_FORE ST | Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the growing-stock estimation type on forest land | NUMBER(11,6)       |
| 3.3.28       | SUBP_COMPONENT_SL_FOREST      | Trees with DIA  5.0 inches - growth component for the sawtimber estimation type on forest land                                     | VARCHAR2(15)       |
| 3.3.29       | SUBP_SUBPTYP_GRM_SL_FOREST    | Trees with DIA  5.0 inches - plot type for GRM for the sawtimber estimation type on forest land                                    | NUMBER(1)          |
| 3.3.30       | SUBP_TPAGROW_UNADJ_SL_FORE ST | Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the sawtimber estimation type on forest land                 | NUMBER(11,6)       |
| 3.3.31       | SUBP_TPAREMV_UNADJ_SL_FORE ST | Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the sawtimber estimation type on forest land      | NUMBER(11,6)       |
| 3.3.32       | SUBP_TPAMORT_UNADJ_SL_FORE ST | Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the sawtimber estimation type on forest land     | NUMBER(11,6)       |

| Subsection   | Column name (attribute)       | Descriptive name                                                                                                              | Oracle data type   |
|--------------|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------|--------------------|
| 3.3.33       | MICR_COMPONENT_AL_TIMBER      | Trees with DIA  1.0 inch - growth component for the all live estimation type on timberland                                   | VARCHAR2(15)       |
| 3.3.34       | MICR_SUBPTYP_GRM_AL_TIMBER    | Trees with DIA  1.0 inch - plot type for GRM for the all live estimation type on timberland                                  | NUMBER(1)          |
| 3.3.35       | MICR_TPAGROW_UNADJ_AL_TIMB ER | Trees with DIA  1.0 inch - unadjusted trees per acre for growth for the all live estimation type on timberland               | NUMBER(11,6)       |
| 3.3.36       | MICR_TPAREMV_UNADJ_AL_TIMBE R | Trees with DIA  1.0 inch - unadjusted trees per acre per year for removals for the all live estimation type on timberland    | NUMBER(11,6)       |
| 3.3.37       | MICR_TPAMORT_UNADJ_AL_TIMB ER | Trees with DIA  1.0 inch - unadjusted trees per acre per year for mortality for the all live estimation type on timberland   | NUMBER(11,6)       |
| 3.3.38       | SUBP_COMPONENT_AL_TIMBER      | Trees with DIA  5.0 inches - growth component for the all live estimation type on timberland                                 | VARCHAR2(15)       |
| 3.3.39       | SUBP_SUBPTYP_GRM_AL_TIMBER    | Trees with DIA  5.0 inches - plot type for GRM for the all live estimation type on timberland                                | NUMBER(1)          |
| 3.3.40       | SUBP_TPAGROW_UNADJ_AL_TIMB ER | Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the all live estimation type on timberland             | NUMBER(11,6)       |
| 3.3.41       | SUBP_TPAREMV_UNADJ_AL_TIMBE R | Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the all live estimation type on timberland  | NUMBER(11,6)       |
| 3.3.42       | SUBP_TPAMORT_UNADJ_AL_TIMB ER | Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the all live estimation type on timberland | NUMBER(11,6)       |
| 3.3.43       | SUBP_COMPONENT_GS_TIMBER      | Trees with DIA  5.0 inches - growth component for the growing-stock estimation type on timberland                            | VARCHAR2(15)       |
| 3.3.44       | SUBP_SUBPTYP_GRM_GS_TIMBER    | Trees with DIA  5.0 inches - plot type for GRM for the growing-stock estimation type on timberland                           | NUMBER(1)          |
| 3.3.45       | SUBP_TPAGROW_UNADJ_GS_TIMB ER | Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the growing-stock estimation type on timberland        | NUMBER(11,6)       |

| Subsection   | Column name (attribute)       | Descriptive name                                                                                                                   | Oracle data type   |
|--------------|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------|--------------------|
| 3.3.46       | SUBP_TPAREMV_UNADJ_GS_TIMB ER | Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the growing-stock estimation type on timberland  | NUMBER(11,6)       |
| 3.3.47       | SUBP_TPAMORT_UNADJ_GS_TIMB ER | Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the growing-stock estimation type on timberland | NUMBER(11,6)       |
| 3.3.48       | SUBP_COMPONENT_SL_TIMBER      | Trees with DIA  5.0 inches - growth component for the sawtimber estimation type on timberland                                     | VARCHAR2(15)       |
| 3.3.49       | SUBP_SUBPTYP_GRM_SL_TIMBER    | Trees with DIA  5.0 inches - plot type for GRM for the sawtimber estimation type on timberland                                    | NUMBER(1)          |
| 3.3.50       | SUBP_TPAGROW_UNADJ_SL_TIMB ER | Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the sawtimber estimation type on timberland                 | NUMBER(11,6)       |
| 3.3.51       | SUBP_TPAREMV_UNADJ_SL_TIMBE R | Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the sawtimber estimation type on timberland      | NUMBER(11,6)       |
| 3.3.52       | SUBP_TPAMORT_UNADJ_SL_TIMB ER | Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the sawtimber estimation type on timberland     | NUMBER(11,6)       |
| 3.3.53       | GROWTSAL_FOREST               | Net annual sound cubic-foot total-stem wood growth of a live tree for the all live estimation type on forest land                  | NUMBER(13,6)       |
| 3.3.54       | GROWCFAL_FOREST               | Net annual sound cubic-foot stem wood growth of a live tree for the all live estimation type on forest land                        | NUMBER(13,6)       |
| 3.3.55       | GROWCFGS_FOREST               | Net annual merchantable cubic-foot stem wood growth of a growing-stock tree on forest land                                         | NUMBER(13,6)       |
| 3.3.56       | GROWBFSL_FOREST               | Net annual merchantable board-foot wood growth of a sawtimber tree on forest land                                                  | NUMBER(13,6)       |
| 3.3.57       | REMVTSAL_FOREST               | Sound cubic-foot total-stem wood volume of a live tree for removal purposes for the all live estimation type on forest land        | NUMBER(13,6)       |
| 3.3.58       | REMVCFAL_FOREST               | Sound cubic-foot stem wood volume of a live tree for removal purposes for the all live estimation type on forest land              | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name                                                                                                           | Oracle data type   |
|--------------|---------------------------|----------------------------------------------------------------------------------------------------------------------------|--------------------|
| 3.3.59       | REMVCFGS_FOREST           | Merchantable cubic-foot stem wood volume of a growing-stock tree for removal purposes on forest land                       | NUMBER(13,6)       |
| 3.3.60       | REMVBFSL_FOREST           | Merchantable board-foot wood volume of a sawtimber tree for removal purposes on forest land                                | NUMBER(13,6)       |
| 3.3.61       | MORTTSAL_FOREST           | Sound cubic-foot total-stem wood volume of a tree for mortality purposes for the all live estimation type on forest land   | NUMBER(13,6)       |
| 3.3.62       | MORTCFAL_FOREST           | Sound cubic-foot stem wood volume of a tree for mortality purposes for the all live estimation type on forest land         | NUMBER(13,6)       |
| 3.3.63       | MORTCFGS_FOREST           | Merchantable cubic-foot stem wood volume of a growing-stock tree for mortality purposes on forest land                     | NUMBER(13,6)       |
| 3.3.64       | MORTBFSL_FOREST           | Merchantable board-foot wood volume of a sawtimber tree for mortality purposes on forest land                              | NUMBER(13,6)       |
| 3.3.65       | GROWTSAL_TIMBER           | Net annual sound cubic-foot total-stem wood growth of a live tree for the all live estimation type on timberland           | NUMBER(13,6)       |
| 3.3.66       | GROWCFAL_TIMBER           | Net annual sound cubic-foot stem wood growth of a live tree for the all live estimation type on timberland                 | NUMBER(13,6)       |
| 3.3.67       | GROWCFGS_TIMBER           | Net annual merchantable cubic-foot stem wood growth of a growing-stock tree on timberland                                  | NUMBER(13,6)       |
| 3.3.68       | GROWBFSL_TIMBER           | Net annual merchantable board-foot wood growth of a sawtimber tree on timberland                                           | NUMBER(13,6)       |
| 3.3.69       | REMVTSAL_TIMBER           | Sound cubic-foot total-stem wood volume of a live tree for removal purposes for the all live estimation type on timberland | NUMBER(13,6)       |
| 3.3.70       | REMVCFAL_TIMBER           | Sound cubic-foot stem wood volume of a live tree for removal purposes for the all live estimation type on timberland       | NUMBER(13,6)       |
| 3.3.71       | REMVCFGS_TIMBER           | Merchantable cubic-foot stem wood volume of a growing-stock tree for removal purposes on timberland                        | NUMBER(13,6)       |
| 3.3.72       | REMVBFSL_TIMBER           | Merchantable board-foot wood volume of a sawtimber tree for removal purposes on timberland                                 | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name                                                                                                        | Oracle data type   |
|--------------|---------------------------|-------------------------------------------------------------------------------------------------------------------------|--------------------|
| 3.3.73       | MORTTSAL_TIMBER           | Sound cubic-foot total-stem wood volume of a tree for mortality purposes for the all live estimation type on timberland | NUMBER(13,6)       |
| 3.3.74       | MORTCFAL_TIMBER           | Sound cubic-foot stem wood volume of a tree for mortality purposes for the all live estimation type on timberland       | NUMBER(13,6)       |
| 3.3.75       | MORTCFGS_TIMBER           | Merchantable cubic-foot stem wood volume of a growing-stock tree for mortality purposes on timberland                   | NUMBER(13,6)       |
| 3.3.76       | MORTBFSL_TIMBER           | Merchantable board-foot wood volume of a sawtimber tree for mortality purposes on timberland                            | NUMBER(13,6)       |
| 3.3.77       | CREATED_BY                | Created by                                                                                                              | VARCHAR2(30)       |
| 3.3.78       | CREATED_DATE              | Created date                                                                                                            | DATE               |
| 3.3.79       | CREATED_IN_INSTANCE       | Created in instance                                                                                                     | VARCHAR2(6)        |
| 3.3.80       | MODIFIED_BY               | Modified by                                                                                                             | VARCHAR2(30)       |
| 3.3.81       | MODIFIED_DATE             | Modified date                                                                                                           | DATE               |
| 3.3.82       | MODIFIED_IN_INSTANCE      | Modified in instance                                                                                                    | VARCHAR2(6)        |

| Key Type   | Column(s) order   | Tables to link             | Abbreviated notation   |
|------------|-------------------|----------------------------|------------------------|
| Primary    | TRE_CN            | N/A                        | TRE_GRM_CMP_PK         |
| Foreign    | TRE_CN            | TREE_GRM_COMPONENT to TREE | TRE_GRM_CMP_FK         |

This table stores information used to compute net growth, removals, and mortality (GRM) estimates for remeasurement trees. Remeasurement is from the time 1 (T1, most recent past measurement) date to the time 2 (T2, current) date. This table provides the same information as the TREE\_GRM\_ESTN table, but the data have been reformatted such that each remeasurement tree is represented by a single record in this table as opposed to multiple records in the TREE\_GRM\_ESTN table. This is an experimental restructuring of the data intended to help FIA develop new methods of presenting data and supporting estimation through download files as well as estimation tools like EVALIDator. Details about the land basis (forest land or timberland), component of change (e.g., survivor tree), and estimation type (all live, growing stock, or sawtimber) are incorporated into the columns in various combinations.

For example, the column SUBP\_COMPONENT\_AL\_FOREST identifies the change component for the all live estimation type on forest land. The same information could be queried from rows in the TREE\_GRM\_ESTN table by including the following in the WHERE clause of a SQL statement:

AND LAND\_BASIS = 'FORESTLAND'

AND ESTN\_TYPE = 'AL'

Queries of rows by attribute estimates and accompanying units (e.g., TREE\_GRM\_ESTN.ESTIMATE = 'VOLUME' and TREE\_GRM\_ESTN.ESTN\_UNITS = 'CF') are not applicable to this table. The attribute estimates and units are identified by columns in the TREE\_GRM\_COMPONENT, TREE\_GRM\_BEGIN, and TREE\_GRM\_MIDPT tables. For example, TREE\_GRM\_COMPONENT.GROWCFAL\_FOREST stores the net annual sound cubic-foot growth of a live tree on forest land (all live estimation type). The begin and mid-point diameters as well as the begin and mid-point estimates that were part of the TREE\_GRM\_ESTN table structure are now stored in independent tables (TREE\_GRM\_BEGIN and TREE\_GRM\_MIDPT). The standard net growth, removals, and mortality estimates for volume only are included in the TREE\_GRM\_COMPONENT table. Information on the individual growth components (e.g., growth on ingrowth: G\_I) are not included. The TREE\_GRM\_BEGIN, TREE\_GRM\_MIDPT, and TREE tables currently support estimates of volume as well as biomass.

## 3.3.1 TRE\_CN

Tree sequence number. Foreign key linking the GRM tree component record to the tree record.

## 3.3.2 PREV\_TRE\_CN

Previous tree sequence number. Foreign key linking the GRM tree component record to the time 1 tree record if one exists. It can be blank (null) in some cases. For example, an ingrowth tree would not have a time 1 (T1) record.

## 3.3.3 PLT\_CN

Plot sequence number. Foreign key linking the GRM tree component record to the plot record.

## 3.3.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 3.3.5 DIA\_BEGIN

Beginning diameter. Diameter at the beginning of the measurement interval. This is the value actually used in the calculation of GRM estimates and may not match the value on the T1 tree record in all cases. For example, in cases where the point of diameter measurement is moved between T1 and T2, the T1 diameter can be estimated by a model.

## 3.3.6 DIA\_MIDPT

Midpoint diameter. Diameter at the midpoint of the measurement interval.

## 3.3.7 DIA\_END

Ending diameter. Diameter at the end of the remeasurement period.

## 3.3.8 ANN\_DIA\_GROWTH

Computed annual diameter growth. The annual diameter growth for the tree expressed as inches per year.

## 3.3.9 ANN\_HT\_GROWTH

Computed annual height growth. The annual height growth for a tree expressed as feet per year.

## 3.3.10 SUBPTYP\_BEGIN

Beginning plot type code. A code indicating the plot type at the beginning of the remeasurement period. This value is assigned based on the size of the tree at the beginning of the remeasurement period.

## Codes: SUBTYP\_BEGIN

|   Code | Description                     |
|--------|---------------------------------|
|      0 | No plot type. Tree not present. |
|      1 | Subplot.                        |
|      2 | Microplot.                      |
|      3 | Macroplot.                      |

## 3.3.11 SUBPTYP\_MIDPT

Midpoint plot type code. A code indicating the plot type at the midpoint of the remeasurement period. This value is assigned based on the size of the tree at the midpoint of the remeasurement period. See SUBPTYP\_BEGIN description for codes.

## 3.3.12 SUBPTYP\_END

Ending plot type code. A code indicating the plot type at the end of the remeasurement period. This value is assigned based on the size of the tree at the end of the remeasurement period. See SUBPTYP\_BEGIN description for codes.

## 3.3.13 MICR\_COMPONENT\_AL\_FOREST

Trees with DIA  1.0 inch - growth component for the all live estimation type on forest land. Growth component (trees with DIA  1.0 inch) on forest land for the all live estimation type.

Note: The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## Codes: MICR\_COMPONENT\_AL\_FOREST

| Code     | Description                                                                                                                                                                                                   |
|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CUT0     | Tree was killed due to harvesting activity by T2 ((TREE.STATUSCD = 3) or (TREE. STATUSCD = 2 and TREE.AGENTCD = 80)). Applicable only in periodic-to-periodic, periodic-to-annual, and modeled GRM estimates. |
| CUT 1    | Tree was previously in estimate at T1 and was killed due to harvesting activity by T2. The tree must be in the same land basis (forest land or timberland) at time T1 and T2.                                 |
| CUT2     | Tree grew across minimum threshold diameter for the estimate since T1 and was killed due to harvesting activity by T2. The tree must be in the same land basis (forest land or timberland) at time T1 and T2. |
| INGROWTH | Tree grew across minimum threshold diameter for the estimate since T1. For example, a sapling grows across the 5-inch diameter threshold becoming ingrowth on the subplot.                                    |

| Code           | Description                                                                                                                                                                                                                                                                              |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MORTALITY0     | Tree died of natural causes by T2 (TREE.AGENTCD <> 80). Applicable only in periodic-to-periodic, periodic-to-annual, and modeled GRM estimates.                                                                                                                                          |
| MORTALITY1     | Tree was previously in estimate at T1 and died of natural causes by T2 (TREE.AGENTCD <> 80).                                                                                                                                                                                             |
| MORTALITY2     | Tree grew across minimum threshold diameter for the estimate since T1 and died of natural causes by T2 (TREE.AGENTCD <> 80).                                                                                                                                                             |
| NOT USED       | Tree was either live or dead at T1 and has no status at T2.                                                                                                                                                                                                                              |
| SURVIVOR       | Tree has remained live and in the estimate from T1 through T2.                                                                                                                                                                                                                           |
| UNKNOWN        | Tree lacks information required to classify component usually due to procedural changes.                                                                                                                                                                                                 |
| REVERSION1     | Tree grew across minimum threshold diameter for the estimate by the midpoint of the measurement interval and the condition reverted to the land basis by T2.                                                                                                                             |
| REVERSION2     | Tree grew across minimum threshold diameter for the estimate after the midpoint of the measurement interval and the condition reverted to the land basis by T2.                                                                                                                          |
| DIVERSION0     | Tree was removed from the estimate by something other than harvesting activity by T2 (not (TREE.STATUSCD = 3) and not (TREE.STATUSCD = 2 and TREE.AGENTCD = 80)). Applicable only in periodic-to-periodic, periodic-to-annual, and modeled GRM estimates.                                |
| DIVERSION1     | Tree was previously in estimate at T1 and the condition diverted from the land basis by T2. This component assignment is not dependent upon tree status (TREE.STATUSCD). For example, the tree can be live, dead and still present, or dead and removed.                                 |
| DIVERSION2     | Tree grew across minimum threshold diameter for the estimate since T1 and the condition diverted from the land basis by T2. This component assignment is not dependent upon tree status (TREE.STATUSCD). For example, the tree can be live, dead and still present, or dead and removed. |
| CULLINCR       | Not used at this time.                                                                                                                                                                                                                                                                   |
| CULLDECR       | Not used at this time.                                                                                                                                                                                                                                                                   |
| N/A - A2A      | Component of change is not defined or does not exist. Applicable only in annual-to-annual GRM estimates.                                                                                                                                                                                 |
| N/A - A2A SOON | Component of change is not defined or does not exist. Applicable only in annual-to-annual GRM estimates.                                                                                                                                                                                 |
| N/A - MODELED  | Component of change is not defined or does not exist. Applicable only in annual-to-annual GRM estimates.                                                                                                                                                                                 |
| N/A - P2A      | Component of change is not defined or does not exist. Applicable only in periodic-to-annual GRM estimates.                                                                                                                                                                               |
| N/A - P2P      | Component of change is not defined or does not exist. Applicable only in periodic-to-periodic GRM estimates.                                                                                                                                                                             |
| N/A - PERIODIC | Component of change is not defined or does not exist. Applicable only in periodic-to-periodic GRM estimates.                                                                                                                                                                             |

## 3.3.14 MICR\_SUBPTYP\_GRM\_AL\_FOREST

Trees with DIA  1.0 inch - plot type for GRM for the all live estimation type on forest land. The plot type for growth, removals, and mortality (GRM) (trees with DIA  1.0 inch) on forest land for the all live estimation type. This plot type is used during estimation to

locate the appropriate stratum adjustment factor. See SUBPTYP\_BEGIN description for codes.

Note: The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.15 MICR\_TPAGROW\_UNADJ\_AL\_FOREST

Trees with DIA  1.0 inch - unadjusted trees per acre for growth for the all live estimation type on forest land. Unadjusted trees per acre for growth (trees with DIA  1.0 inch) on forest land for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.16 MICR\_TPAREMV\_UNADJ\_AL\_FOREST

Trees with DIA  1.0 inch - unadjusted trees per acre per year for removals for the all live estimation type on forest land. Unadjusted trees per acre for removals (trees with DIA  1.0 inch) on forest land for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.17 MICR\_TPAMORT\_UNADJ\_AL\_FOREST

Trees with DIA  1.0 inch - unadjusted trees per acre per year for mortality for the all live estimation type on forest land. Unadjusted trees per acre per year for mortality (trees with DIA  1.0 inch) on forest land for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.18 SUBP\_COMPONENT\_AL\_FOREST

Trees with DIA  5.0 inches - growth component for the all live estimation type on forest land. Growth component (trees with DIA  5.0 inches) on forest land for the all live estimation type. See MICR\_COMPONENT\_AL\_FOREST description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.19 SUBP\_SUBPTYP\_GRM\_AL\_FOREST

Trees with DIA  5.0 inches - plot type for GRM for the all live estimation type on forest land. The plot type for growth, removals, and mortality (GRM) (trees with DIA  5.0 inches) on forest land for the all live estimation type. This plot type is used during estimation to locate the appropriate stratum adjustment factor. See SUBPTYP\_BEGIN description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.20 SUBP\_TPAGROW\_UNADJ\_AL\_FOREST

Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the all live estimation type on forest land. Unadjusted trees per acre for growth (trees with DIA  5.0 inches) on forest land for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.21 SUBP\_TPAREMV\_UNADJ\_AL\_FOREST

Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the all live estimation type on forest land. Unadjusted trees per acre per year for removals (trees with DIA  5.0 inches) on forest land for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.22 SUBP\_TPAMORT\_UNADJ\_AL\_FOREST

Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the all live estimation type on forest land. Unadjusted trees per acre per year for mortality (trees with DIA  5.0 inches) on forest land for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.23 SUBP\_COMPONENT\_GS\_FOREST

Trees with DIA  5.0 inches - growth component for the growing-stock estimation type on forest land. Growth component (trees with DIA  5 inches) on forest land for the growing-stock estimation type. See MICR\_COMPONENT\_AL\_FOREST description for codes.

Note:

The SUBP prefix on the column name does not relate to the plot size.

## 3.3.24 SUBP\_SUBPTYP\_GRM\_GS\_FOREST

Trees with DIA  5.0 inches - plot type for GRM for the growing-stock estimation type on forest land. The plot type for growth, removals, and mortality (GRM) (trees with DIA  5.0 inches) on forest land for the growing-stock estimation type. This plot type is used during estimation to locate the appropriate stratum adjustment factor. See SUBPTYP\_BEGIN description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.25 SUBP\_TPAGROW\_UNADJ\_GS\_FOREST

Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the growing-stock estimation type on forest land. Unadjusted trees per acre for growth (trees with DIA  5.0 inches) on forest land for the growing-stock estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.26 SUBP\_TPAREMV\_UNADJ\_GS\_FOREST

Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the growing-stock estimation type on forest land. Unadjusted trees per acre per year for removals (trees with DIA  5.0 inches) on forest land for the growing-stock estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.27 SUBP\_TPAMORT\_UNADJ\_GS\_FOREST

Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the growing-stock estimation type on forest land. Unadjusted trees per acre per year for mortality (trees with DIA  5.0 inches) on forest land for the growing-stock estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.28 SUBP\_COMPONENT\_SL\_FOREST

Trees with DIA  5.0 inches - growth component for the sawtimber estimation type on forest land. Growth component (trees with DIA  5.0 inches) on forest land for the sawtimber estimation type. See MICR\_COMPONENT\_AL\_FOREST description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.29 SUBP\_SUBPTYP\_GRM\_SL\_FOREST

Trees with DIA  5.0 inches - plot type for GRM for the sawtimber estimation type on forest land. The plot type for growth, removals, and mortality (GRM) (trees with DIA  5.0 inches) on forest land for the sawtimber estimation type. This plot type is used during estimation to locate the appropriate stratum adjustment factor. See SUBPTYP\_BEGIN description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.30 SUBP\_TPAGROW\_UNADJ\_SL\_FOREST

Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the sawtimber estimation type on forest land. Unadjusted trees per acre for growth (trees with DIA  5.0 inches) on forest land for the sawtimber estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.31 SUBP\_TPAREMV\_UNADJ\_SL\_FOREST

Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the sawtimber estimation type on forest land. Unadjusted trees per acre per year for removals (trees with DIA  5.0 inches) on forest land for the sawtimber estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.32 SUBP\_TPAMORT\_UNADJ\_SL\_FOREST

Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the sawtimber estimation type on forest land. Unadjusted trees per acre per year for mortality (trees with DIA  5.0 inches) on forest land for the sawtimber estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.33 MICR\_COMPONENT\_AL\_TIMBER

Trees with DIA  1.0 inch - growth component for the all live estimation type on timberland. Growth component (trees with DIA  1.0 inch) on timberland for the all live estimation type. See MICR\_COMPONENT\_AL\_FOREST description for codes.

Note: The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.34 MICR\_SUBPTYP\_GRM\_AL\_TIMBER

Trees with DIA  1.0 inch - plot type for GRM for the all live estimation type on timberland. The plot type for growth, removals, and mortality (GRM) (trees with DIA  1.0 inch) on timberland for the all live estimation type. This plot type is used during estimation to locate the appropriate stratum adjustment factor. See SUBPTYP\_BEGIN description for codes.

Note: The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.35 MICR\_TPAGROW\_UNADJ\_AL\_TIMBER

Trees with DIA  1.0 inch - unadjusted trees per acre for growth for the all live estimation type on timberland. Unadjusted trees per acre for growth (trees with DIA  1.0 inch) on timberland for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.36 MICR\_TPAREMV\_UNADJ\_AL\_TIMBER

Trees with DIA  1.0 inch - unadjusted trees per acre per year for removals for the all live estimation type on timberland. Unadjusted trees per acre per year for removals (trees with DIA  1.0 inch) on timberland for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.37 MICR\_TPAMORT\_UNADJ\_AL\_TIMBER

Trees with DIA  1.0 inch - unadjusted trees per acre per year for mortality for the all live estimation type on timberland. Unadjusted trees per acre per year for mortality (trees with DIA  1.0 inch) on timberland for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The MICR prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.38 SUBP\_COMPONENT\_AL\_TIMBER

Trees with DIA  5.0 inches - growth component for the all live estimation type on timberland. Growth component (trees with DIA  5.0 inches) on timberland for the all live estimation type. See MICR\_COMPONENT\_AL\_FOREST description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.39 SUBP\_SUBPTYP\_GRM\_AL\_TIMBER

Trees with DIA  5.0 inches - plot type for GRM for the all live estimation type on timberland. The plot type for growth, removals, and mortality (GRM) (trees with DIA  5.0 inches) on timberland for the all live estimation type. This plot type is used during estimation to locate the appropriate stratum adjustment factor. See SUBPTYP\_BEGIN description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.40 SUBP\_TPAGROW\_UNADJ\_AL\_TIMBER

Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the all live estimation type on timberland. Unadjusted trees per acre for growth (trees with DIA  5.0 inches) on timberland for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.41 SUBP\_TPAREMV\_UNADJ\_AL\_TIMBER

Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the all live estimation type on timberland. Unadjusted trees per acre per year for removals (trees with DIA  5.0 inches) on timberland for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.42 SUBP\_TPAMORT\_UNADJ\_AL\_TIMBER

Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the all live estimation type on timberland. Unadjusted trees per acre per year for mortality (trees with DIA  5.0) on timberland for the all live estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.43 SUBP\_COMPONENT\_GS\_TIMBER

Trees with DIA  5.0 inches - growth component for the growing-stock estimation type on timberland. Growth component (trees with DIA  5.0 inches) on timberland for the growing-stock estimation type.

Note: The SUBP prefix on the column name does not relate to the plot size. See MICR\_COMPONENT\_AL\_FOREST description for codes.

## 3.3.44 SUBP\_SUBPTYP\_GRM\_GS\_TIMBER

Trees with DIA  5.0 inches - plot type for GRM for the growing-stock estimation type on timberland. The plot type for growth, removals, and mortality (GRM) (trees with DIA  5.0 inches) on timberland for the growing-stock estimation type. This plot type is used during estimation to locate the appropriate stratum adjustment factor. See SUBPTYP\_BEGIN description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.45 SUBP\_TPAGROW\_UNADJ\_GS\_TIMBER

Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the growing-stock estimation type on timberland. Unadjusted trees per acre for growth (trees with DIA  5.0 inches) on timberland for the growing-stock estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.46 SUBP\_TPAREMV\_UNADJ\_GS\_TIMBER

Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the growing-stock estimation type on timberland. Unadjusted trees per acre per year for removals (trees with DIA  5.0 inches) on timberland for the growing-stock estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.47 SUBP\_TPAMORT\_UNADJ\_GS\_TIMBER

Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the growing-stock estimation type on timberland. Unadjusted trees per acre per year for mortality (trees with DIA  5.0 inches) on timberland for the growing-stock estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.48 SUBP\_COMPONENT\_SL\_TIMBER

Trees with DIA  5.0 inches - growth component for the sawtimber estimation type on timberland. Growth component (trees with DIA  5.0 inches) on timberland for the sawtimber estimation type. See MICR\_COMPONENT\_AL\_FOREST description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.49 SUBP\_SUBPTYP\_GRM\_SL\_TIMBER

Trees with DIA  5.0 inches - plot type for GRM for the sawtimber estimation type on timberland. The plot type for growth, removals, and mortality (GRM) (trees with DIA  5.0 inches) on timberland for the sawtimber estimation type. This plot type is used during estimation to locate the appropriate stratum adjustment factor. See SUBPTYP\_BEGIN description for codes.

Note: The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.50 SUBP\_TPAGROW\_UNADJ\_SL\_TIMBER

Trees with DIA  5.0 inches - unadjusted trees per acre for growth for the sawtimber estimation type on timberland. Unadjusted trees per acre for growth (trees with DIA  5.0 inches) on timberland for the sawtimber estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.51 SUBP\_TPAREMV\_UNADJ\_SL\_TIMBER

Trees with DIA  5.0 inches - unadjusted trees per acre per year for removals for the sawtimber estimation type on timberland. Unadjusted trees per acre per year for removals (trees with DIA  5.0 inches) on timberland for the sawtimber estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.52 SUBP\_TPAMORT\_UNADJ\_SL\_TIMBER

Trees with DIA  5.0 inches - unadjusted trees per acre per year for mortality for the sawtimber estimation type on timberland. Unadjusted trees per acre per year for mortality (trees with DIA  5.0 inches) on timberland for the sawtimber estimation type. This value must be adjusted using the stratum adjustment factors stored in the POP\_STRATUM table.

Note: This column is populated with a constant value based on the plot size for those plots using a fixed-radius design. It is populated using a value inversely related to the tree size for plots using a variable-radius design. The SUBP prefix on the column name does

not relate to the plot size, but rather to the threshold at which a given estimate can be made.

## 3.3.53 GROWTSAL\_FOREST

Net annual sound cubic-foot total-stem growth of a live tree for the all live estimation type on forest land. The net change in sound cubic-foot total-stem wood volume (TREE.VOLTSSND) per year of the tree (for trees on remeasured plots, (V2 - V1)/(T2 T1)) where 1 and 2 denote the past and current measurement, respectively; V is volume; T indicates date of measurement; and T2 - T1 = PLOT.REMPER). Because this value is net growth, it may be a negative number. Negative growth values are usually due to mortality (V2 = 0) but can also occur on live trees that have a net loss in volume because of damage, rot, broken top, or other causes. To expand to a per acre value, multiply by SUBP\_TPAGROW\_UNADJ\_AL\_FOREST.

## 3.3.54 GROWCFAL\_FOREST

Net annual sound cubic-foot stem wood growth of a live tree for the all live estimation type on forest land. The net change in sound cubic-foot stem wood volume (TREE.VOLCFSND) per year of the tree (for trees on remeasured plots, (V2 - V1)/(T2 T1)) where 1 and 2 denote the past and current measurement, respectively; V is volume; T indicates date of measurement; and T2 - T1 = PLOT.REMPER). Because this value is net growth, it may be a negative number. Negative growth values are usually due to mortality (V2 = 0) but can also occur on live trees that have a net loss in volume because of damage, rot, broken top, or other causes. To expand to a per acre value, multiply by SUBP\_TPAGROW\_UNADJ\_AL\_FOREST.

## 3.3.55 GROWCFGS\_FOREST

Net annual merchantable cubic-foot stem wood growth of a growing-stock tree on forest land. The net change in merchantable cubic-foot stem wood volume (TREE.VOLCFNET) per year of the tree (for trees on remeasured plots, (V2 - V1)/(T2 T1)). Because this value is net growth, it may be a negative number. Negative growth values are usually due to mortality (V2 = 0) but can also occur on live trees that have a net loss in volume because of damage, rot, broken top, or other causes. To expand to a per acre value, multiply by SUBP\_TPAGROW\_UNADJ\_GS\_FOREST.

## 3.3.56 GROWBFSL\_FOREST

Net annual merchantable board-foot wood growth of a sawtimber tree on forest land. The net change in merchantable board-foot (TREE.VOLBFNET, International ¼-inch Rule) wood volume per year of the tree (for trees on remeasured plots, (V2 - V1)/(T2 - T1)). Because this value is net growth, it may be a negative number. Negative growth values are usually due to mortality (V2 = 0) but can also occur on live trees that have a net loss in volume because of damage, rot, broken top, or other causes. To expand to a per acre value, multiply by SUBP\_TPAGROW\_UNADJ\_SL\_FOREST.

## 3.3.57 REMVTSAL\_FOREST

Sound cubic-foot total-stem wood volume of a live tree for removal purposes for the all live estimation type on forest land. The sound cubic-foot total-stem wood volume (TREE.VOLTSSND) of the tree at the time of removal. To obtain estimates of annual per acre removals, multiply by SUBP\_TPAREMV\_UNADJ\_AL\_FOREST.

## 3.3.58 REMVCFAL\_FOREST

Sound cubic-foot stem wood volume of a live tree for removal purposes for the all live estimation type on forest land. The sound cubic-foot stem wood volume (TREE.VOLCFSND) of the tree at the time of removal. To obtain estimates of annual per acre removals, multiply by SUBP\_TPAREMV\_UNADJ\_AL\_FOREST.

## 3.3.59 REMVCFGS\_FOREST

Merchantable cubic-foot stem wood volume of a growing-stock tree for removal purposes on forest land. The merchantable cubic-foot stem woodvolume (TREE.VOLCFNET) of the tree at the time of removal. To obtain estimates of annual per acre removals, multiply by SUBP\_TPAREMV\_UNADJ\_GS\_FOREST.

## 3.3.60 REMVBFSL\_FOREST

Merchantable board-foot wood volume of a sawtimber tree for removal purposes on forest land. The merchantable board-foot (TREE.VOLBFNET, International ¼-inch Rule) wood volume of the tree at the time of removal. To obtain estimates of annual per acre removals, multiply by SUBP\_TPAREMV\_UNADJ\_SL\_FOREST.

## 3.3.61 MORTTSAL\_FOREST

Sound cubic-foot total-stem wood volume of a tree for mortality purposes for the all live estimation type on forest land. The sound cubic-foot total-stem wood volume (TREE.VOLTSSND) of the tree at the time of mortality. To obtain estimates of annual per acre mortality, multiply by SUBP\_TPAMORT\_UNADJ\_AL\_FOREST.

## 3.3.62 MORTCFAL\_FOREST

Sound cubic-foot stem wood volume of a tree for mortality purposes for the all live estimation type on forest land. The sound cubic-foot stem wood volume (TREE.VOLCFSND) of the tree at the time of mortality. To obtain estimates of annual per acre mortality, multiply by SUBP\_TPAMORT\_UNADJ\_AL\_FOREST.

## 3.3.63 MORTCFGS\_FOREST

Merchantable cubic-foot stem wood volume of a growing-stock tree for mortality purposes on forest land. The merchantable cubic-foot stem wood volume (TREE.VOLCFNET) of the tree at the time of mortality. To obtain estimates of annual per acre mortality, multiply by SUBP\_TPAMORT\_UNADJ\_GS\_FOREST.

## 3.3.64 MORTBFSL\_FOREST

Merchantable board-foot wood volume of a sawtimber tree for mortality purposes on forest land. The merchantable board-foot (TREE.VOLBFNET, International ¼-inch Rule) wood volume of the tree at the time of mortality. To obtain estimates of annual per acre mortality, multiply by SUBP\_TPAMORT\_UNADJ\_SL\_FOREST.

## 3.3.65 GROWTSAL\_TIMBER

Net annual sound cubic-foot total-stem wood growth of a live tree for the all live estimation type on timberland. The net change in sound cubic-foot total-stem wood volume (TREE.VOLTSSND) per year of the tree (for trees on remeasured plots, (V2 V1)/(T2 - T1)). Because this value is net growth, it may be a negative number. Negative growth values are usually due to mortality (V2 = 0) but can also occur on live trees that have a net loss in volume because of damage, rot, broken top, or other causes. To expand to a per acre value, multiply by SUBP\_TPAGROW\_UNADJ\_AL\_TIMBER.

## 3.3.66 GROWCFAL\_TIMBER

Net annual sound cubic-foot stem wood growth of a live tree for the all live estimation type on timberland. The net change in sound cubic-foot stem wood volume (TREE.VOLCFSND) per year of the tree (for trees on remeasured plots, (V2 - V1)/(T2 T1)). Because this value is net growth, it may be a negative number. Negative growth values are usually due to mortality (V2 = 0) but can also occur on live trees that have a net loss in volume because of damage, rot, broken top, or other causes. To expand to a per acre value, multiply by SUBP\_TPAGROW\_UNADJ\_AL\_TIMBER.

## 3.3.67 GROWCFGS\_TIMBER

Net annual merchantable cubic-foot stem wood growth of a growing-stock tree on timberland. The net change in merchantable cubic-foot stem wood volume (TREE.VOLCFNET per year of the tree (for trees on remeasured plots, (V2 - V1)/(T2 T1)). Because this value is net growth, it may be a negative number. Negative growth values are usually due to mortality (V2 = 0) but can also occur on live trees that have a net loss in volume because of damage, rot, broken top, or other causes. To expand to a per acre value, multiply by SUBP\_TPAGROW\_UNADJ\_GS\_TIMBER.

## 3.3.68 GROWBFSL\_TIMBER

Net annual merchantable board-foot wood growth of a sawtimber tree on timberland. The net change in merchantable board-foot (TREE.VOLBFNET, International ¼-inch Rule) wood volume per year of the tree (for trees on remeasured plots, (V2 - V1)/(T2 - T1)). Because this value is net growth, it may be a negative number. Negative growth values are usually due to mortality (V2 = 0) but can also occur on live trees that have a net loss in volume because of damage, rot, broken top, or other causes. To expand to a per acre value, multiply by SUBP\_TPAGROW\_UNADJ\_SL\_TIMBER.

## 3.3.69 REMVTSAL\_TIMBER

Sound cubic-foot total-stem wood volume of a live tree for removal purposes for the all live estimation type on timberland. The sound cubic-foot total-stem wood volume (TREE.VOLTSSND) of the tree at the time of the removal. To obtain estimates of annual per acre removals, multiply by SUBP\_TPAREMV\_UNADJ\_AL\_TIMBER.

## 3.3.70 REMVCFAL\_TIMBER

Sound cubic-foot stem wood volume of a live tree for removal purposes for the all live estimation type on timberland. The sound cubic-foot stem wood volume (TREE.VOLCFSND) of the tree at the time of the removal. To obtain estimates of annual per acre removals, multiply by SUBP\_TPAREMV\_UNADJ\_AL\_TIMBER.

## 3.3.71 REMVCFGS\_TIMBER

Merchantable cubic-foot stem wood volume of a growing-stock tree for removal purposes on timberland. The merchantable cubic-foot stem wood volume (TREE.VOLCFNET of the tree at the time of removal. To obtain estimates of annual per acre removals, multiply by SUBP\_TPAREMV\_UNADJ\_GS\_TIMBER.

## 3.3.72 REMVBFSL\_TIMBER

Merchantable board-foot wood volume of a sawtimber tree for removal purposes on timberland. The merchantable board-foot (TREE.VOLBFNET, International ¼-inch Rule) wood volume of the tree at the time of removal. To obtain estimates of annual per acre removals, multiply by SUBP\_TPAREMV\_UNADJ\_SL\_TIMBER.

## 3.3.73 MORTTSAL\_TIMBER

Sound cubic-foot total-stem wood volume of a tree for mortality purposes for the all live estimation type on timberland. The sound cubic-foot total-stem wood volume (TREE.VOLTSSND) of the tree at the time of mortality. To obtain estimates of annual per acre mortality, multiply by SUBP\_TPAMORT\_UNADJ\_AL\_TIMBER.

## 3.3.74 MORTCFAL\_TIMBER

Sound cubic-foot stem wood volume of a tree for mortality purposes for the all live estimation type on timberland. The sound cubic-foot stem wood volume (TREE.VOLCFSND) of the tree at the time of mortality. To obtain estimates of annual per acre mortality, multiply by SUBP\_TPAMORT\_UNADJ\_AL\_TIMBER.

## 3.3.75 MORTCFGS\_TIMBER

Merchantable cubic-foot stem wood volume of a growing-stock tree for mortality purposes on timberland. The merchantable cubic-foot stem wood volume (TREE.VOLCFNET) of the tree at the time of mortality. To obtain estimates of annual per acre mortality, multiply by SUBP\_TPAMORT\_UNADJ\_GS\_TIMBER.

## 3.3.76 MORTBFSL\_TIMBER

Merchantable board-foot wood volume of a sawtimber tree for mortality purposes on timberland. The merchantable board-foot (TREE.VOLBFNET, International ¼-inch Rule) volume of the tree at the time of mortality. To obtain estimates of annual per acre mortality, multiply by SUBP\_TPAMORT\_UNADJ\_SL\_TIMBER.

## 3.3.77 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.3.78 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.3.79 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.3.80 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.3.81 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.3.82 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

Tree Growth, Removal, and Mortality Component Table

Chapter 3 (revision: 12.2024)