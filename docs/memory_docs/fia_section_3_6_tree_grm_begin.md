# Section 3.6: Tree Growth, Removal, and Mortality Begin Table
**Oracle Table Name:** TREE_GRM_BEGIN
**Extracted Pages:** 279-288 (Chapter pages 3-121 to 3-130)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.6 Tree Growth, Removal, and Mortality Begin Table

## (Oracle table name: TREE\_GRM\_BEGIN)

| Subsection   | Column name (attribute)   | Descriptive name                              | Oracle data type   |
|--------------|---------------------------|-----------------------------------------------|--------------------|
| 3.6.1        | TRE_CN                    | Tree sequence number                          | VARCHAR2(34)       |
| 3.6.2        | PREV_TRE_CN               | Previous tree sequence number                 | VARCHAR2(34)       |
| 3.6.3        | PLT_CN                    | Plot sequence number                          | VARCHAR2(34)       |
| 3.6.4        | STATECD                   | State code                                    | NUMBER(2)          |
| 3.6.5        | SUBPTYP                   | Plot type code                                | NUMBER(1)          |
| 3.6.6        | SPCD                      | Species code                                  | NUMBER(4)          |
| 3.6.7        | STATUSCD                  | Status code                                   | NUMBER(2)          |
| 3.6.8        | DIA                       | Diameter at T1                                | NUMBER(5,2)        |
| 3.6.9        | HT                        | Total height                                  | NUMBER(3)          |
| 3.6.10       | ACTUALHT                  | Actual height                                 | NUMBER(3)          |
| 3.6.11       | CR                        | Compacted crown ratio                         | NUMBER(3)          |
| 3.6.12       | STANDING_DEAD_CD          | Standing dead code                            | NUMBER(2)          |
| 3.6.13       | DIAHTCD                   | Diameter height code                          | NUMBER(1)          |
| 3.6.14       | CULL                      | Rotten and missing cull                       | NUMBER(3)          |
| 3.6.15       | ROUGHCULL                 | Rough cull                                    | NUMBER(3)          |
| 3.6.16       | CULLFORM                  | Form Cull                                     | NUMBER(3)          |
| 3.6.17       | CULLMSTOP                 | Missing top cull                              | NUMBER(3)          |
| 3.6.18       | DECAYCD                   | Decay class code                              | NUMBER(2)          |
| 3.6.19       | TREECLCD                  | Tree class code                               | NUMBER(2)          |
| 3.6.20       | HTDMP                     | Height to diameter measurement point          | NUMBER(3,1)        |
| 3.6.21       | WDLDSTEM                  | Woodland tree species stem count              | NUMBER(3)          |
| 3.6.22       | STDORGCD                  | Stand origin code                             | NUMBER(2)          |
| 3.6.23       | SITREE                    | Calculated site index                         | NUMBER(3)          |
| 3.6.24       | BALIVE                    | Basal area per acre of live trees             | NUMBER(9,4)        |
| 3.6.25       | VOLTSGRS                  | Gross cubic-foot total-stem wood volume at T1 | NUMBER(13,6)       |
| 3.6.26       | VOLTSGRS_BARK             | Gross cubic-foot total-stem bark volume at T1 | NUMBER(13,6)       |
| 3.6.27       | VOLTSSND                  | Sound cubic-foot total-stem wood volume at T1 | NUMBER(13,6)       |
| 3.6.28       | VOLTSSND_BARK             | Sound cubic-foot total-stem bark volume at T1 | NUMBER(13,6)       |
| 3.6.29       | VOLCFGRS_STUMP            | Gross cubic-foot stump wood volume at T1      | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name                                                             | Oracle data type   |
|--------------|---------------------------|------------------------------------------------------------------------------|--------------------|
| 3.6.30       | VOLCFGRS_STUMP_BARK       | Gross cubic-foot stump bark volume at T1                                     | NUMBER(13,6)       |
| 3.6.31       | VOLCFSND_STUMP            | Sound cubic-foot stump wood volume at T1                                     | NUMBER(13,6)       |
| 3.6.32       | VOLCFSND_STUMP_BARK       | Sound cubic-foot stump bark volume at T1                                     | NUMBER(13,6)       |
| 3.6.33       | VOLCFGRS                  | Gross cubic-foot stem wood volume at T1                                      | NUMBER(13,6)       |
| 3.6.34       | VOLCFGRS_BARK             | Gross cubic-foot stem bark volume at T1                                      | NUMBER(13,6)       |
| 3.6.35       | VOLCFGRS_TOP              | Gross cubic-foot stem-top wood volume at T1                                  | NUMBER(13,6)       |
| 3.6.36       | VOLCFGRS_TOP_BARK         | Gross cubic-foot stem-top bark volume at T1                                  | NUMBER(13,6)       |
| 3.6.37       | VOLCFSND                  | Sound cubic-foot stem wood volume at T1                                      | NUMBER(13,6)       |
| 3.6.38       | VOLCFSND_BARK             | Sound cubic-foot stem bark volume at T1                                      | NUMBER(13,6)       |
| 3.6.39       | VOLCFSND_TOP              | Sound cubic-foot stem-top wood volume at T1                                  | NUMBER(13,6)       |
| 3.6.40       | VOLCFSND_TOP_BARK         | Sound cubic-foot stem-top bark volume at T1                                  | NUMBER(13,6)       |
| 3.6.41       | VOLCFNET                  | Net cubic-foot stem wood volume at T1                                        | NUMBER(13,6)       |
| 3.6.42       | VOLCFNET_BARK             | Net cubic-foot stem bark volume at T1                                        | NUMBER(13,6)       |
| 3.6.43       | VOLCSGRS                  | Gross cubic-foot wood volume in the sawlog portion of a sawtimber tree at T1 | NUMBER(13,6)       |
| 3.6.44       | VOLCSGRS_BARK             | Gross cubic-foot bark volume in the sawlog portion of a sawtimber tree at T1 | NUMBER(13,6)       |
| 3.6.45       | VOLCSSND                  | Sound cubic-foot wood volume in the sawlog portion of a sawtimber tree at T1 | NUMBER(13,6)       |
| 3.6.46       | VOLCSSND_BARK             | Sound cubic-foot bark volume in the sawlog portion of a sawtimber tree at T1 | NUMBER(13,6)       |
| 3.6.47       | VOLCSNET                  | Net cubic-foot wood volume in the sawlog portion of a sawtimber tree at T1   | NUMBER(13,6)       |
| 3.6.48       | VOLCSNET_BARK             | Net cubic-foot bark volume in the sawlog portion of a sawtimber tree at T1   | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name                                                                             | Oracle data type   |
|--------------|---------------------------|----------------------------------------------------------------------------------------------|--------------------|
| 3.6.49       | VOLBFGRS                  | Gross board-foot wood volume in the sawlog portion of a sawtimber tree at T1                 | NUMBER(13,6)       |
| 3.6.50       | VOLBFNET                  | Net board-foot wood volume in the sawlog portion of a sawtimber tree at T1                   | NUMBER(13,6)       |
| 3.6.51       | VOLBSGRS                  | Gross board-foot wood volume in the sawlog portion of a sawtimber tree at T1 (Scribner Rule) | NUMBER(13,6)       |
| 3.6.52       | VOLBSNET                  | Net board-foot wood volume in the sawlog portion of a sawtimber tree at T1 (Scribner Rule)   | NUMBER(13,6)       |
| 3.6.53       | DRYBIO_STEM               | Dry biomass of wood in the total stem at T1                                                  | NUMBER(13,6)       |
| 3.6.54       | DRYBIO_STEM_BARK          | Dry biomass of bark in the total stem at T1                                                  | NUMBER(13,6)       |
| 3.6.55       | DRYBIO_STUMP              | Dry biomass of wood in the stump at T1                                                       | NUMBER(13,6)       |
| 3.6.56       | DRYBIO_STUMP_BARK         | Dry biomass of bark in the stump at T1                                                       | NUMBER(13,6)       |
| 3.6.57       | DRYBIO_BOLE               | Dry biomass of wood in the merchantable bole at T1                                           | NUMBER(13,6)       |
| 3.6.58       | DRYBIO_BOLE_BARK          | Dry biomass of bark in the merchantable bole at T1                                           | NUMBER(13,6)       |
| 3.6.59       | DRYBIO_BRANCH             | Dry biomass of branches at T1                                                                | NUMBER(13,6)       |
| 3.6.60       | DRYBIO_FOLIAGE            | Dry biomass of foliage at T1                                                                 | NUMBER(13,6)       |
| 3.6.61       | DRYBIO_AG                 | Aboveground dry biomass of wood and bark at T1                                               | NUMBER(13,6)       |
| 3.6.62       | DRYBIO_BG                 | Belowground dry biomass at T1                                                                | NUMBER(13,6)       |
| 3.6.63       | CARBON_AG                 | Aboveground carbon of wood and bark at T1                                                    | NUMBER(13,6)       |
| 3.6.64       | CARBON_BG                 | Belowground carbon at T1                                                                     | NUMBER(13,6)       |
| 3.6.65       | DRYBIO_SAWLOG             | Dry biomass of wood in the sawlog portion of a sawtimber tree at T1                          | NUMBER(13,6)       |
| 3.6.66       | DRYBIO_SAWLOG_BARK        | Dry biomass of bark in the sawlog portion of a sawtimber tree at T1                          | NUMBER(13,6)       |
| 3.6.67       | CREATED_BY                | Created by                                                                                   | VARCHAR2(30)       |
| 3.6.68       | CREATED_DATE              | Created date                                                                                 | DATE               |
| 3.6.69       | CREATED_IN_INSTANCE       | Created in instance                                                                          | VARCHAR2(6)        |
| 3.6.70       | MODIFIED_BY               | Modified by                                                                                  | VARCHAR2(30)       |
| 3.6.71       | MODIFIED_DATE             | Modified date                                                                                | DATE               |
| 3.6.72       | MODIFIED_IN_INSTANCE      | Modified in instance                                                                         | VARCHAR2(6)        |

| Key Type   | Column(s) order   | Tables to link         | Abbreviated notation   |
|------------|-------------------|------------------------|------------------------|
| Primary    | TRE_CN            | N/A                    | TRE_GRM_BGN_PK         |
| Foreign    | TRE_CN            | TREE_GRM_BEGIN to TREE | TRE_GRM_BGN_FK         |

This table stores information about a remeasurement tree at the beginning of the remeasurement period (also called time 1, T1, most recent past measurement) and is used to compute growth, removal, and mortality (GRM) estimates on remeasured trees. All trees contributing to change in annual-to-annual inventories have records in this table. T1 information may not be available for periodic-to-periodic and periodic-to-annual inventories. In some cases, the T1 values have been recalculated during the GRM process for various reasons including movement of the diameter measurement point or disagreement in the species identification between the T2 (time 2, current) and T1 field crews. In the case where the species identification changed, the T1 information is recalculated using the T2 species. This table includes a single record per tree. The current structure of the table supports estimates of volume, biomass, and carbon.

## 3.6.1 TRE\_CN

Tree sequence number. Foreign key linking the tree GRM begin record to the T2 tree record.

## 3.6.2 PREV\_TRE\_CN

Previous tree sequence number. Foreign key linking the tree GRM begin record to T1 tree record.

## 3.6.3 PLT\_CN

Plot sequence number. Foreign key linking the tree GRM begin record to the plot record.

## 3.6.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 3.6.5 SUBPTYP

Plot type code. A code indicating the plot type used for the tree estimates in the TREE\_GRM\_BEGIN table.

## Codes: SUBPTYP

|   Code | Description                                                                                                                                                                           |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | Undetermined. Assigned in cases where there is no T1 tree record, and the modeled tree grows across either the microplot-to-subplot threshold, or the subplot-to-macroplot threshold. |
|      1 | Subplot.                                                                                                                                                                              |
|      2 | Microplot.                                                                                                                                                                            |
|      3 | Macroplot.                                                                                                                                                                            |

## 3.6.6 SPCD

Species code. The FIA tree species code at T2, which may be different from the species code at T1. Refer to appendix F for codes.

## 3.6.7 STATUSCD

Status code. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.STATUSCD description for definition.

## 3.6.8 DIA

Diameter at T1. The estimated T1 diameter used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.DIA description for definition.

## 3.6.9 HT

Total height. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.HT description for definition.

## 3.6.10 ACTUALHT

Actual height. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.ACTUALHT description for definition.

## 3.6.11 CR

Compacted crown ratio. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.CR description for definition.

## 3.6.12 STANDING\_DEAD\_CD

Standing dead code. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.STANDING\_DEAD\_CD description for definition.

## 3.6.13 DIAHTCD

Diameter height code. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.DIAHTCD description for definition.

## 3.6.14 CULL

Rotten and missing cull. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.CULL description for definition.

## 3.6.15 ROUGHCULL

Rough cull. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.ROUGHCULL description for definition.

## 3.6.16 CULLFORM

Form cull. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.CULLFORM description for definition.

## 3.6.17 CULLMSTOP

Missing top cull. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.CULLMSTOP description for definition.

## 3.6.18 DECAYCD

Decay class code. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.DECAYCD description for definition.

## 3.6.19 TREECLCD

Tree class code. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.TREECLCD description for definition.

## 3.6.20 HTDMP

Height to diameter measurement point. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.HTDMP description for definition.

## 3.6.21 WDLDSTEM

Woodland tree species stem count. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.WDLDSTEM description for definition.

## 3.6.22 STDORGCD

Stand origin code. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See COND.STDORGCD description for definition.

## 3.6.23 SITREE

Calculated site index. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See TREE.SITREE description for definition.

## 3.6.24 BALIVE

Basal area per acre of live trees. The value used for the tree estimates in the TREE\_GRM\_BEGIN table. See COND.BALIVE description for definition.

## 3.6.25 VOLTSGRS

Gross cubic-foot total-stem wood volume at T1 . See TREE.VOLTSGRS description for definition.

## 3.6.26 VOLTSGRS\_BARK

Gross cubic-foot total-stem bark volume at T1 . See TREE.VOLTSGRS\_BARK description for definition.

## 3.6.27 VOLTSSND

Sound cubic-foot total-stem wood volume at T1 . See TREE.VOLTSSND description for definition.

## 3.6.28 VOLTSSND\_BARK

Sound cubic-foot total-stem bark volume at T1 . See TREE.VOLTSSND\_BARK description for definition.

## 3.6.29 VOLCFGRS\_STUMP

Gross cubic-foot stump wood volume at T1 . See TREE.VOLCFGRS\_STUMP description for definition.

## 3.6.30 VOLCFGRS\_STUMP\_BARK

Gross cubic-foot stump bark volume at T1 . See TREE.VOLCFGRS\_STUMP\_BARK description for definition.

## 3.6.31 VOLCFSND\_STUMP

Sound cubic-foot stump wood volume at T1 . See TREE.VOLCFSND\_STUMP description for definition.

## 3.6.32 VOLCFSND\_STUMP\_BARK

Sound cubic-foot stump bark volume at T1 . See TREE.VOLCFSND\_STUMP\_BARK description for definition.

## 3.6.33 VOLCFGRS

Gross cubic-foot stem wood volume at T1 . See TREE.VOLCFGRS description for definition.

## 3.6.34 VOLCFGRS\_BARK

Gross cubic-foot stem bark volume at T1 . See TREE.VOLCFGRS\_BARK description for definition.

## 3.6.35 VOLCFGRS\_TOP

Gross cubic-foot stem-top wood volume at T1. See TREE.VOLCFGRS\_TOP description for definition.

## 3.6.36 VOLCFGRS\_TOP\_BARK

Gross cubic-foot stem-top bark volume at T1 . See TREE.VOLCFGRS\_TOP\_BARK description for definition.

## 3.6.37 VOLCFSND

Sound cubic-foot stem wood volume at T1. See the TREE.VOLCFSND description for definition.

## 3.6.38 VOLCFSND\_BARK

Sound cubic-foot stem bark volume at T1. See TREE.VOLCFSND\_BARK description for definition.

## 3.6.39 VOLCFSND\_TOP

Sound cubic-foot stem-top wood volume at T1. See TREE.VOLCFSND\_TOP description for definition.

## 3.6.40 VOLCFSND\_TOP\_BARK

Sound cubic-foot stem-top bark volume at T1. See TREE.VOLCFSND\_TOP\_BARK description for definition.

## 3.6.41 VOLCFNET

Net cubic-foot stem wood volume at T1. See the TREE.VOLCFNET description for definition.

## 3.6.42 VOLCFNET\_BARK

Net cubic-foot stem bark volume at T1 . See TREE.VOLCFNET\_BARK description for definition.

## 3.6.43 VOLCSGRS

Gross cubic-foot wood volume in the sawlog portion of a sawtimber tree at T1 . See TREE.VOLCSGRS description for definition.

## 3.6.44 VOLCSGRS\_BARK

Gross cubic-foot bark volume in the sawlog portion of a sawtimber tree at T1. See TREE.VOLCSGRS\_BARK description for definition.

## 3.6.45 VOLCSSND

Sound cubic-foot wood volume in the sawlog portion of a sawtimber tree at T1. See TREE.VOLCSSND description for definition.

## 3.6.46 VOLCSSND\_BARK

Sound cubic-foot bark volume in the sawlog portion of a sawtimber tree at T1. See TREE.VOLCSSND\_BARK description for definition.

## 3.6.47 VOLCSNET

Net cubic-foot wood volume in the sawlog portion of a sawtimber tree at T1. See the TREE.VOLCSNET description for definition.

## 3.6.48 VOLCSNET\_BARK

Net cubic-foot bark volume in the sawlog portion of a sawtimber tree at T1 . See TREE.VOLCSNET\_BARK description for definition.

## 3.6.49 VOLBFGRS

Gross board-foot wood volume in the sawlog portion of a sawtimber tree at T1 . See TREE.VOLBFGRS description for definition.

## 3.6.50 VOLBFNET

Net board-foot wood volume in the sawlog portion of a sawtimber tree at T1. See the TREE.VOLBFNET description for definition.

## 3.6.51 VOLBSGRS

Gross board-foot wood volume in the sawlog portion of a sawtimber tree at T1 (Scribner Rule). See TREE.VOLBSGRS description for definition.

## 3.6.52 VOLBSNET

Net board-foot wood volume in the sawlog portion of a sawtimber tree at T1 (Scribner Rule). See TREE.VOLBSNET description for the definition.

## 3.6.53 DRYBIO\_STEM

Dry biomass of wood in the total stem at T1. See TREE.DRYBIO\_STEM description for definition.

## 3.6.54 DRYBIO\_STEM\_BARK

Dry biomass of bark in the total stem at T1. See TREE.DRYBIO\_STEM\_BARK description for definition.

## 3.6.55 DRYBIO\_STUMP

Dry biomass of wood in the tree stump at T1. See the TREE.DRYBIO\_STUMP description for definition.

## 3.6.56 DRYBIO\_STUMP\_BARK

Dry biomass of bark in the stump at T1. See TREE.DRYBIO\_STUMP\_BARK description for definition.

## 3.6.57 DRYBIO\_BOLE

Dry biomass of wood in the merchantable bole at T1. See the TREE.DRYBIO\_BOLE description for definition.

## 3.6.58 DRYBIO\_BOLE\_BARK

Dry biomass of bark in the merchantable bole at T1. See TREE.DRYBIO\_BOLE\_BARK description for definition.

## 3.6.59 DRYBIO\_BRANCH

Dry biomass of branches at T1. See TREE.DRYBIO\_BRANCH description for definition.

## 3.6.60 DRYBIO\_FOLIAGE

Dry biomass of foliage at T1. See TREE.DRYBIO\_FOLIAGE description for definition.

## 3.6.61 DRYBIO\_AG

Aboveground dry biomass of wood and bark at T1. See the TREE.DRYBIO\_AG description for definition.

## 3.6.62 DRYBIO\_BG

Belowground dry biomass at T1. See the TREE.DRYBIO\_BG description for definition.

## 3.6.63 CARBON\_AG

Aboveground carbon of wood and bark at T1. See TREE.CARBON\_AG description for definition.

## 3.6.64 CARBON\_BG

Belowground carbon at T1. See TREE.CARBON\_BG description for definition.

## 3.6.65 DRYBIO\_SAWLOG

Dry biomass of wood in the sawlog portion of a sawtimber tree at T1. See the TREE.DRYBIO\_SAWLOG description for definition.

## 3.6.66 DRYBIO\_SAWLOG\_BARK

Dry biomass of bark in the sawlog portion of a sawtimber tree at T1. See TREE.DRYBIO\_SAWLOG\_BARK description for definition.

## 3.6.67 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.6.68 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.6.69 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.6.70 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.6.71 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.6.72 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.