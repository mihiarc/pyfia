# Section 3.7: Tree Growth, Removal, and Mortality Estimation Table
**Oracle Table Name:** TREE_GRM_ESTN
**Extracted Pages:** 289-298 (Chapter pages 3-131 to 3-140)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.7 Tree Growth, Removal, and Mortality Estimation Table

## (Oracle table name: TREE\_GRM\_ESTN)

| Subsection   | Column name (attribute)   | Descriptive name                             | Oracle data type   |
|--------------|---------------------------|----------------------------------------------|--------------------|
| 3.7.1        | CN                        | Sequence number                              | VARCHAR2(34)       |
| 3.7.2        | STATECD                   | State code                                   | NUMBER             |
| 3.7.3        | INVYR                     | Inventory year                               | NUMBER(4)          |
| 3.7.4        | PLT_CN                    | Plot sequence number                         | VARCHAR2(34)       |
| 3.7.5        | TRE_CN                    | Tree sequence number                         | VARCHAR2(34)       |
| 3.7.6        | LAND_BASIS                | Land basis for estimate                      | VARCHAR2(10)       |
| 3.7.7        | ESTIMATE                  | Base attribute that is being estimated       | VARCHAR2(20)       |
| 3.7.8        | ESTN_TYPE                 | Estimation type of the tree                  | VARCHAR2(10)       |
| 3.7.9        | ESTN_UNITS                | Estimation unit of measurement               | VARCHAR2(3)        |
| 3.7.10       | COMPONENT                 | Growth component type                        | VARCHAR2(15)       |
| 3.7.11       | SUBPTYP_GRM               | Subplot type used for GRM estimation         | NUMBER(1)          |
| 3.7.12       | REMPER                    | Remeasurement period                         | NUMBER(3,1)        |
| 3.7.13       | TPAGROW_UNADJ             | Growth trees per acre unadjusted             | NUMBER(11,6)       |
| 3.7.14       | TPAREMV_UNADJ             | Removal trees per acre per year unadjusted   | NUMBER(11,6)       |
| 3.7.15       | TPAMORT_UNADJ             | Mortality trees per acre per year unadjusted | NUMBER(11,6)       |
| 3.7.16       | ANN_NET_GROWTH            | Average annual net growth estimate           | NUMBER(13,6)       |
| 3.7.17       | REMOVALS                  | Removal estimate                             | NUMBER(13,6)       |
| 3.7.18       | MORTALITY                 | Mortality estimate                           | NUMBER(13,6)       |
| 3.7.19       | EST_BEGIN                 | Beginning estimate                           | NUMBER(13,6)       |
| 3.7.20       | EST_BEGIN_RECALC          | Recalculated beginning estimate              | VARCHAR2(1)        |
| 3.7.21       | EST_END                   | Ending estimate                              | NUMBER(13,6)       |
| 3.7.22       | EST_MIDPT                 | Midpoint estimate                            | NUMBER(13,6)       |
| 3.7.23       | EST_THRESHOLD             | Threshold estimate                           | NUMBER(13,6)       |
| 3.7.24       | DIA_BEGIN                 | Beginning diameter                           | NUMBER(5,2)        |
| 3.7.25       | DIA_BEGIN_RECALC          | Recalculated diameter                        | VARCHAR2(1)        |
| 3.7.26       | DIA_END                   | Ending diameter                              | NUMBER(5,2)        |
| 3.7.27       | DIA_MIDPT                 | Midpoint diameter                            | NUMBER(5,2)        |
| 3.7.28       | DIA_THRESHOLD             | Threshold diameter                           | NUMBER(5,2)        |
| 3.7.29       | G_S                       | Survivor growth                              | NUMBER(13,6)       |
| 3.7.30       | I                         | Ingrowth                                     | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name      | Oracle data type   |
|--------------|---------------------------|-----------------------|--------------------|
| 3.7.31       | G_I                       | Growth on ingrowth    | NUMBER(13,6)       |
| 3.7.32       | M                         | Mortality             | NUMBER(13,6)       |
| 3.7.33       | G_M                       | Mortality growth      | NUMBER(13,6)       |
| 3.7.34       | C                         | Cut                   | NUMBER(13,6)       |
| 3.7.35       | G_C                       | Cut growth            | NUMBER(13,6)       |
| 3.7.36       | R                         | Reversion             | NUMBER(13,6)       |
| 3.7.37       | G_R                       | Reversion growth      | NUMBER(13,6)       |
| 3.7.38       | D                         | Diversion             | NUMBER(13,6)       |
| 3.7.39       | G_D                       | Diversion growth      | NUMBER(13,6)       |
| 3.7.40       | CD                        | Cull decrement        | NUMBER(13,6)       |
| 3.7.41       | G_CD                      | Cull decrement growth | NUMBER(13,6)       |
| 3.7.42       | CI                        | Cull increment        | NUMBER(13,6)       |
| 3.7.43       | G_CI                      | Cull increment growth | NUMBER(13,6)       |
| 3.7.44       | CREATED_BY                | Created by            | VARCHAR2(30)       |
| 3.7.45       | CREATED_DATE              | Created date          | DATE               |
| 3.7.46       | CREATED_IN_INSTANCE       | Created in instance   | VARCHAR2(6)        |
| 3.7.47       | MODIFIED_BY               | Modified by           | VARCHAR2(30)       |
| 3.7.48       | MODIFIED_DATE             | Modified date         | DATE               |
| 3.7.49       | MODIFIED_IN_INSTANCE      | Modified in instance  | VARCHAR2(6)        |

| Key Type   | Column(s) order                                     | Tables to link        | Abbreviated notation   |
|------------|-----------------------------------------------------|-----------------------|------------------------|
| Primary    | CN                                                  | N/A                   | TGE_PK                 |
| Unique     | TRE_CN, LAND_BASIS, ESTIMATE, ESTN_TYPE, ESTN_UNITS | N/A                   | TGE_UK                 |
| Foreign    | PLT_CN                                              | TREE_GRM_ESTN to PLOT | TGE_PLT_FK             |
| Foreign    | TRE_CN                                              | TREE_GRM_ESTN to TREE | TGE_TRE_FK             |

This table stores information used to compute net growth, removal, and mortality (GRM) estimates on remeasurement tree records. This includes the detailed land basis, component, estimation type, estimation units, as well as the begin, end, and mid-point diameters and the begin, end, and mid-point estimates. In addition, the standard net growth, removal, and mortality estimates are included, as well as estimates for each individual growth component. Users should note that this table usually includes multiple records for each remeasurement tree. For volume estimates, there are generally three records storing estimates for each estimation type (all live, growing stock, sawlog) for each land basis (forest land or timberland). However, if the estimation type is not applicable to the tree (e.g., the tree is not growing-stock form or is not sawlog size), then there could be only one record for each land basis (all live). Currently, this table only stores GRM estimates for volume.

## 3.7.1 CN

Sequence number. A unique sequence number used to identify a tree GRM estimation record.

## 3.7.2 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 3.7.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 3.7.4 PLT\_CN

Plot sequence number. Foreign key linking the GRM tree estimation record to the plot record.

## 3.7.5 TRE\_CN

Tree sequence number. Foreign key linking the GRM tree estimation record to the tree record.

## 3.7.6 LAND\_BASIS

Land basis for estimate. An attribute that categorizes estimates by the land-based domain of interest.

Note: Starting with PLOT.MANUAL  6.0, code descriptions have been modified to match FIA's new definition for accessible forest land and nonforest land. The current wording of "at least 10 percent canopy cover" replaces older wording of "at least 10 percent stocked" as the qualifying criterion in classification. This criterion applies to any tally tree species, including woodland tree species.

## Codes: LAND\_BASIS

| Code       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| FORESTLAND | Land that has at least 10 percent canopy cover by live tally trees of any size or has had at least 10 percent canopy cover of live tally species in the past, based on the presence of stumps, snags, or other evidence. To qualify, the area must be at least 1.0 acre in size and 120.0 feet wide. Forest land includes transition zones, such as areas between forest and nonforest lands that meet the minimal tree canopy cover and forest areas adjacent to urban and built-up lands. Roadside, streamside, and shelterbelt strips of trees must have a width of at least 120 feet and continuous length of at least 363 feet to qualify as forest land. Unimproved roads and trails, streams, and clearings in forest areas are classified as forest if they are less than 120 feet wide or less than an acre in size. Tree-covered areas in agricultural production settings, such as fruit orchards, or tree-covered areas in urban settings, such as city parks, are not considered forest land. |
| TIMBERLAND | Forest land that is producing or capable of producing 20 cubic feet per acre or more per year of wood at culmination of mean annual increment (MAI). Timberland excludes reserved forest lands.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

## 3.7.7 ESTIMATE

Base attribute that is being estimated. A descriptor for the base attribute that is being estimated.

## 3.7.8 ESTN\_TYPE

Estimation type of the tree. A code indicating whether the estimation record is for all live, growing-stock, or sawlog trees.

## Codes: ESTN\_TYPE

| Code   | Description    |
|--------|----------------|
| AL     | All live.      |
| GS     | Growing stock. |
| SL     | Sawlog.        |

## 3.7.9 ESTN\_UNITS

Estimation unit of measurement. A code indicating the unit of measurement for the estimation record.

## Codes: ESTN\_UNITS

| Code   | Description   |
|--------|---------------|
| BF     | Board feet.   |
| CF     | Cubic feet.   |

## 3.7.10 COMPONENT

Growth component type. A code indicating the type of change that occurred on the tree between the previous and the current field observations.

## Codes: COMPONENT

| Code       | Description                                                                                                                                                                                                   |
|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CUT0       | Tree was killed due to harvesting activity by T2 ((TREE.STATUSCD = 3) or (TREE. STATUSCD = 2 and TREE.AGENTCD = 80)). Applicable only in periodic-to-periodic, periodic-to-annual, and modeled GRM estimates. |
| CUT1       | Tree was previously in estimate at T1 and was killed due to harvesting activity by T2. The tree must be in the same land basis (forest land or timberland) at time T1 and T2.                                 |
| CUT2       | Tree grew across minimum threshold diameter for the estimate since T1 and was killed due to harvesting activity by T2. The tree must be in the same land basis (forest land or timberland) at time T1 and T2. |
| INGROWTH   | Tree grew across minimum threshold diameter for the estimate since T1. For example, a sapling grows across the 5-inch diameter threshold becoming ingrowth on the subplot.                                    |
| MORTALITY0 | Tree died of natural causes by T2 (TREE.AGENTCD <> 80). Applicable only in periodic-to-periodic, periodic-to-annual, and modeled GRM estimates.                                                               |
| MORTALITY1 | Tree was previously in estimate (T1) and died of natural causes by T2 (TREE.AGENTCD <> 80).                                                                                                                   |
| MORTALITY2 | Tree grew across minimum threshold diameter for the estimate since T1 and died of natural causes by T2 (TREE.AGENTCD <> 80).                                                                                  |

| Code           | Description                                                                                                                                                                                                                                                                              |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| NOT USED       | Tree was either live or dead at T1 and has no status at T2.                                                                                                                                                                                                                              |
| SURVIVOR       | Tree has remained live and in the estimate from T1 through T2.                                                                                                                                                                                                                           |
| UNKNOWN        | Tree lacks information required to classify component usually due to procedural changes.                                                                                                                                                                                                 |
| REVERSION1     | Tree grew across minimum threshold diameter for the estimate by the midpoint of the measurement interval and the condition reverted to the land basis by T2.                                                                                                                             |
| REVERSION2     | Tree grew across minimum threshold diameter for the estimate after the midpoint of the measurement interval and the condition reverted to the land basis by T2.                                                                                                                          |
| DIVERSION0     | Tree was removed from the estimate by something other than harvesting activity by T2 (not (TREE.STATUSCD= 3) and not (TREE.STATUSCD = 2 and TREE.AGENTCD = 80)). Applicable only in periodic-to-periodic, periodic-to-annual, and modeled GRM estimates.                                 |
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

## 3.7.11 SUBPTYP\_GRM

Subplot type used for GRM estimation. A code indicating what plot type is used for assigning the tree per acre value, and which population adjustment factor is used for GRM estimates.

Codes: SUBTYP\_GRM

|   Code | Description   |
|--------|---------------|
|      1 | Subplot.      |
|      2 | Microplot.    |
|      3 | Macroplot.    |

## 3.7.12 REMPER

Remeasurement period. The number of years between measurements for remeasured plots to the nearest 0.1 year. This attribute is blank (null) for new plots or remeasured plots that are not used for growth, removals, or mortality estimates.

## 3.7.13 TPAGROW\_UNADJ

Growth trees per acre unadjusted. The number of growth trees per acre that the sample tree theoretically represents based on the sample design. For fixed-radius plots taken with the mapped plot design (PLOT.DESIGNCD = 1), TPAGROW\_UNADJ is set to a constant derived from the plot size. Variable-radius plots were often used in earlier inventories, so the value in TPAGROW\_UNADJ decreases as the tree diameter increases. This attribute will be blank (null) if the tree does not contribute to growth estimates. Based on the procedures described in Bechtold and Patterson (2005), this attribute must be adjusted using factors stored in the POP\_STRATUM table to derive population estimates. Examples of estimating population totals are shown in The Forest Inventory and Analysis Database: Population Estimation User Guide.

## 3.7.14 TPAREMV\_UNADJ

Removal trees per acre per year unadjusted. The number of removal trees per acre per year that the sample tree theoretically represents based on the sample design. For fixed-radius plots taken with the mapped plot design (PLOT.DESIGNCD = 1), TPAREMV\_UNADJ is set to a constant derived from the plot size divided by PLOT.REMPER. Variable-radius plots were often used in earlier inventories, so the value in TPAREMV\_UNADJ decreases as the tree diameter increases. This attribute will be blank (null) if the tree does not contribute to removals estimates. Based on the procedures described in Bechtold and Patterson (2005), this attribute must be adjusted using factors stored in the POP\_STRATUM table to derive population estimates. Examples of estimating population totals are shown in The Forest Inventory and Analysis Database: Population Estimation User Guide.

## 3.7.15 TPAMORT\_UNADJ

Mortality trees per acre per year unadjusted. The number of mortality trees per acre per year that the sample tree theoretically represents based on the sample design. For fixed-radius plots taken with the mapped plot design (PLOT.DESIGNCD = 1), TPAMORT\_UNADJ is set to a constant derived from the plot size divided by PLOT.REMPER Variable-radius plots were often used in earlier inventories, so the value in TPAMORT\_UNADJ decreases as the tree diameter increases. This attribute will be blank (null) if the tree does not contribute to mortality estimates. Based on the procedures described in Bechtold and Patterson (2005), this attribute must be adjusted using factors stored in the POP\_STRATUM table to derive population estimates. Examples of estimating population totals are shown in The Forest Inventory and Analysis Database: Population Estimation User Guide.

## 3.7.16 ANN\_NET\_GROWTH

Average annual net growth estimate. The net change in the estimate per year of this tree. Because this value is net growth, it may be a negative number. Negative values are usually due to mortality but can also occur on live trees that have a net loss because of damage, rot, broken top, or other causes. To expand to a per acre value, multiply by TPAGROW\_UNADJ.

## 3.7.17 REMOVALS

Removal estimate. The trees that were cut, utilized or not, and trees removed from the land basis (diversion) between time 1 and time 2. The estimate is calculated for the mid-point of the measurement interval.

## 3.7.18 MORTALITY

Mortality estimate. The trees that died between time 1 and time 2. The estimate is calculated for the mid-point of the measurement interval.

## 3.7.19 EST\_BEGIN

Beginning estimate. Estimate derived from original field observations at time 1, modeled time 1 values for missing trees (TREE.RECONILECD 3 or 4), or recomputed time 1 variables.

## 3.7.20 EST\_BEGIN\_RECALC

Recalculated beginning estimate. A code indicating when EST\_BEGIN is different (i.e., recalculated) from the time 1 estimate for the purpose of calculating growth. EST\_BEGIN is recalculated when any of the follow occur:

- · TREE.DIACHECK = 2 at time 2
- · TREE.SPCD observed at time 1 &lt;&gt; TREE.SPCD observed at time 2
- · TREE.STATUSCD = 2 and TREE.STANDING\_DEAD\_CD = 1 at time 1 but TREE.STATUSCD = 1 at time 2
- · TREE.TREECLCD = 3 or 4 at time 1 but TREE.TREECLCD = 2 at time 2

## Codes: EST\_BEGIN\_RECALC

| Code   | Description                                                                                          |
|--------|------------------------------------------------------------------------------------------------------|
| Y      | EST_BEGIN is recalculated.                                                                           |
| N      | EST_BEGIN is from time 1 field observations or derived from modeled time 1 values for missing trees. |

## 3.7.21 EST\_END

Ending estimate. Estimate at time 2.

## 3.7.22 EST\_MIDPT

Midpoint estimate. Estimate at midpoint of measurement interval. Only calculated for removal and mortality trees.

## 3.7.23 EST\_THRESHOLD

Threshold estimate. Estimate at threshold size.

## 3.7.24 DIA\_BEGIN

Beginning diameter. Diameter from original field observations at time 1, modeled time 1 diameter for missing trees (TREE.RECONCILECD 3 or 4), or recomputed time 1 diameter based on time 2 observations (see DIA\_BEGIN\_RECALC).

## 3.7.25 DIA\_BEGIN\_RECALC

Recalculated diameter. A code indicating when DIA\_BEGIN is different (i.e., recalculated) from the time 1 diameter for the purpose of calculating growth. DIA\_BEGIN is recalculated when TREE.DIACHECK = 2 and time 2.

## Codes: DIA\_BEGIN\_RECALC

| Code   | Description                                                                                        |
|--------|----------------------------------------------------------------------------------------------------|
| Y      | DIA_BEGIN is recalculated.                                                                         |
| N      | DIA_BEGIN is from time 1 field diameter or derived from modeled time 1 diameter for missing trees. |

## 3.7.26 DIA\_END

Ending diameter. Diameter at time 2.

## 3.7.27 DIA\_MIDPT

Midpoint diameter. Diameter at midpoint of measurement interval.

## 3.7.28 DIA\_THRESHOLD

Threshold diameter. Diameter at threshold size.

## 3.7.29 G\_S

Survivor growth. The growth on trees tallied at time 1 that survive until time 2.

## 3.7.30 I

Ingrowth. The estimate of trees at the time that they grow across the diameter threshold between time 1 and time 2. This term also includes trees that subsequently die (i.e., ingrowth mortality), are cut (i.e., ingrowth cut), or diverted to nonforest (i.e., ingrowth diversion); as well as trees that achieve the threshold after an area reverts to a forest land use (i.e., reversion ingrowth).

## 3.7.31 G\_I

Growth on ingrowth. The growth of trees between the time they grow across the diameter threshold and time 2.

## 3.7.32 M

Mortality. The estimate of trees that die from natural causes between time 1 and time 2. The estimate is based on tree size at the midpoint of the measurement interval (includes mortality growth).

## 3.7.33 G\_M

Mortality growth. The growth of trees that died from natural causes between time 1 and the midpoint of the measurement interval. This term also includes the subsequent growth on ingrowth trees that achieve the diameter threshold prior to mortality.

## 3.7.34 C

Cut. The estimate of trees cut between time 1 and time 2. The estimate is based on tree size at the midpoint of the measurement interval (includes cut growth). Trees felled or killed in conjunction with a harvest or silvicultural operation (whether they are utilized or

not) are included, but trees on land diverted from forest to nonforest (diversions) are excluded.

## 3.7.35 G\_C

Cut growth. The growth of cut trees between time 1 and the midpoint of the measurement interval. This term also includes the growth on ingrowth trees that achieve the diameter threshold prior to being cut.

## 3.7.36 R

Reversion. The estimate of trees on land that reverts from a nonforest land use to a forest land use or land that reverts from any source to timberland between time 1 and time 2. The estimate is based on tree size at the midpoint of the measurement interval.

## 3.7.37 G\_R

Reversion growth. The growth of reversion trees from the midpoint of the measurement interval to time 2. This term also includes the growth on ingrowth trees that achieve the diameter threshold after reversion.

## 3.7.38 D

Diversion. The estimate of trees on forest land diverted to nonforest, or timberland diverted to reserved forest land and other unproductive forest land, whether the tree is utilized or not, between time 1 and time 2. The estimate is based on tree size at the midpoint of the measurement interval (includes diversion growth).

## 3.7.39 G\_D

Diversion growth. The growth of diversion trees from time 1 to the midpoint of the measurement interval. This term also includes the growth on ingrowth trees that achieve the diameter threshold prior to diversion.

## 3.7.40 CD

Cull decrement. (core optional) The net gain in the growing-stock component due to reclassification of cull trees to growing-stock trees between two surveys (i.e., the estimate of trees that were given a cull code at time 1, but reclassified with a growing-stock code at time 2). The estimate is based on tree size at the midpoint of the measurement interval.

## 3.7.41 G\_CD

Cull decrement growth. (core optional) The growth from the midpoint of the measurement interval to time 2 on trees that were cull at time 1, but growing-stock at time 2.

## 3.7.42 CI

Cull increment. (core optional) The net reduction in the growing-stock component due to reclassification of growing-stock trees to cull trees between two surveys (i.e., the estimate of trees that were given a growing-stock code at time 1, but reclassified with a cull code at time 2). The estimate is based on tree size at the midpoint of the measurement interval (includes cull increment growth).

## 3.7.43 G\_CI

Cull increment growth. (core optional) The growth to the midpoint of the measurement interval between time 1 and 2 of trees that were given a growing-stock code at time 1, but reclassified with a cull code at time 2.

## 3.7.44 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.7.45 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.7.46 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.7.47 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.7.48 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.7.49 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.