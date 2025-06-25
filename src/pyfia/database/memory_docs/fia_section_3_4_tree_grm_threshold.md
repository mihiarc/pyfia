# Section 3.4: Tree Growth, Removal, and Mortality Threshold Table
**Oracle Table Name:** TREE_GRM_THRESHOLD
**Extracted Pages:** 259-268 (Chapter pages 3-101 to 3-110)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.4 Tree Growth, Removal, and Mortality Threshold Table

## (Oracle table name: TREE\_GRM\_THRESHOLD)

| Subsection   | Column name (attribute)   | Descriptive name                                         | Oracle data type   |
|--------------|---------------------------|----------------------------------------------------------|--------------------|
| 3.4.1        | TRE_CN                    | Tree sequence number                                     | VARCHAR2(34)       |
| 3.4.2        | PREV_TRE_CN               | Previous tree sequence number                            | VARCHAR2(34)       |
| 3.4.3        | PLT_CN                    | Plot sequence number                                     | VARCHAR2(34)       |
| 3.4.4        | STATECD                   | State code                                               | NUMBER(2)          |
| 3.4.5        | THRESHOLD_TYPE            | Threshold type                                           | VARCHAR2(10)       |
| 3.4.6        | SUBPTYP                   | Plot type code                                           | NUMBER(1)          |
| 3.4.7        | SPCD                      | Species code                                             | NUMBER(4)          |
| 3.4.8        | STATUSCD                  | Status code                                              | NUMBER(2)          |
| 3.4.9        | DIA                       | Threshold diameter                                       | NUMBER(5,2)        |
| 3.4.10       | HT                        | Total height                                             | NUMBER(3)          |
| 3.4.11       | ACTUALHT                  | Actual height                                            | NUMBER(3)          |
| 3.4.12       | CR                        | Compacted crown ratio                                    | NUMBER(3)          |
| 3.4.13       | STANDING_DEAD_CD          | Standing dead code                                       | NUMBER(2)          |
| 3.4.14       | DIAHTCD                   | Diameter height code                                     | NUMBER(1)          |
| 3.4.15       | CULL                      | Rotten and missing cull                                  | NUMBER(3)          |
| 3.4.16       | ROUGHCULL                 | Rough cull                                               | NUMBER(3)          |
| 3.4.17       | CULLFORM                  | Form Cull                                                | NUMBER(3)          |
| 3.4.18       | CULLMSTOP                 | Missing top cull                                         | NUMBER(3)          |
| 3.4.19       | DECAYCD                   | Decay class code                                         | NUMBER(2)          |
| 3.4.20       | TREECLCD                  | Tree class code                                          | NUMBER(2)          |
| 3.4.21       | HTDMP                     | Height to diameter measurement point                     | NUMBER(3,1)        |
| 3.4.22       | WDLDSTEM                  | Woodland tree species stem count                         | NUMBER(3)          |
| 3.4.23       | STDORGCD                  | Stand origin code                                        | NUMBER(2)          |
| 3.4.24       | SITREE                    | Calculated site index                                    | NUMBER(3)          |
| 3.4.25       | BALIVE                    | Basal area per acre of live trees                        | NUMBER(9,4)        |
| 3.4.26       | VOLTSGRS                  | Gross cubic-foot total-stem wood volume at the threshold | NUMBER(13,6)       |
| 3.4.27       | VOLTSGRS_BARK             | Gross cubic-foot total-stem bark volume at the threshold | NUMBER(13,6)       |
| 3.4.28       | VOLTSSND                  | Sound cubic-foot total-stem wood volume at the threshold | NUMBER(13,6)       |
| 3.4.29       | VOLTSSND_BARK             | Sound cubic-foot total-stem bark volume at the threshold | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name                                                                        | Oracle data type   |
|--------------|---------------------------|-----------------------------------------------------------------------------------------|--------------------|
| 3.4.30       | VOLCFGRS_STUMP            | Gross cubic-foot stump wood volume at the threshold                                     | NUMBER(13,6)       |
| 3.4.31       | VOLCFGRS_STUMP_BARK       | Gross cubic-foot stump bark volume at the threshold                                     | NUMBER(13,6)       |
| 3.4.32       | VOLCFSND_STUMP            | Sound cubic-foot stump wood volume at the threshold                                     | NUMBER(13,6)       |
| 3.4.33       | VOLCFSND_STUMP_BARK       | Sound cubic-foot stump bark volume at the threshold                                     | NUMBER(13,6)       |
| 3.4.34       | VOLCFGRS                  | Gross cubic-foot stem wood volume at the threshold                                      | NUMBER(13,6)       |
| 3.4.35       | VOLCFGRS_BARK             | Gross cubic-foot stem bark volume at the threshold                                      | NUMBER(13,6)       |
| 3.4.36       | VOLCFGRS_TOP              | Gross cubic-foot stem-top wood volume at the threshold                                  | NUMBER(13,6)       |
| 3.4.37       | VOLCFGRS_TOP_BARK         | Gross cubic-foot stem-top bark volume at the threshold                                  | NUMBER(13,6)       |
| 3.4.38       | VOLCFSND                  | Sound cubic-foot stem wood volume at the threshold                                      | NUMBER(13,6)       |
| 3.4.39       | VOLCFSND_BARK             | Sound cubic-foot stem bark volume at the threshold                                      | NUMBER(13,6)       |
| 3.4.40       | VOLCFSND_TOP              | Sound cubic-foot stem-top wood volume at the threshold                                  | NUMBER(13,6)       |
| 3.4.41       | VOLCFSND_TOP_BARK         | Sound cubic-foot stem-top bark volume at the threshold                                  | NUMBER(13,6)       |
| 3.4.42       | VOLCFNET                  | Net cubic-foot stem wood volume at the threshold                                        | NUMBER(13,6)       |
| 3.4.43       | VOLCFNET_BARK             | Net cubic-foot stem bark volume at the threshold                                        | NUMBER(13,6)       |
| 3.4.44       | VOLCSGRS                  | Gross cubic-foot wood volume in the sawlog portion of a sawtimber tree at the threshold | NUMBER(13,6)       |
| 3.4.45       | VOLCSGRS_BARK             | Gross cubic-foot bark volume in the sawlog portion of a sawtimber tree at the threshold | NUMBER(13,6)       |
| 3.4.46       | VOLCSSND                  | Sound cubic-foot wood volume in the sawlog portion of a sawtimber tree at the threshold | NUMBER(13,6)       |
| 3.4.47       | VOLCSSND_BARK             | Sound cubic-foot bark volume in the sawlog portion of a sawtimber tree at the threshold | NUMBER(13,6)       |
| 3.4.48       | VOLCSNET                  | Net cubic-foot wood volume in the sawlog portion of a sawtimber tree at the threshold   | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name                                                                                        | Oracle data type   |
|--------------|---------------------------|---------------------------------------------------------------------------------------------------------|--------------------|
| 3.4.49       | VOLCSNET_BARK             | Net cubic-foot bark volume in the sawlog portion of a sawtimber tree at the threshold                   | NUMBER(13,6)       |
| 3.4.50       | VOLBFGRS                  | Gross board-foot wood volume in the sawlog portion of a sawtimber tree at the threshold                 | NUMBER(13,6)       |
| 3.4.51       | VOLBFNET                  | Net board-foot wood volume in the sawlog portion of a sawtimber tree at the threshold                   | NUMBER(13,6)       |
| 3.4.52       | VOLBSGRS                  | Gross board-foot wood volume in the sawlog portion of a sawtimber tree at the threshold (Scribner Rule) | NUMBER(13,6)       |
| 3.4.53       | VOLBSNET                  | Net board-foot wood volume in the sawlog portion of a sawtimber tree at the threshold (Scribner Rule)   | NUMBER(13,6)       |
| 3.4.54       | DRYBIO_STEM               | Dry biomass of wood in the total stem at the threshold                                                  | NUMBER(13,6)       |
| 3.4.55       | DRYBIO_STEM_BARK          | Dry biomass of bark in the total stem at the threshold                                                  | NUMBER(13,6)       |
| 3.4.56       | DRYBIO_STUMP              | Dry biomass of wood in the stump at the threshold                                                       | NUMBER(13, 6)      |
| 3.4.57       | DRYBIO_STUMP_BARK         | Dry biomass of bark in the stump at the threshold                                                       | NUMBER(13,6)       |
| 3.4.58       | DRYBIO_BOLE               | Dry biomass of wood in the merchantable bole at the threshold                                           | NUMBER(13,6)       |
| 3.4.59       | DRYBIO_BOLE_BARK          | Dry biomass of bark in the merchantable bole at the threshold                                           | NUMBER(13,6)       |
| 3.4.60       | DRYBIO_BRANCH             | Dry biomass of branches at the threshold                                                                | NUMBER(13,6)       |
| 3.4.61       | DRYBIO_FOLIAGE            | Dry biomass of foliage at the threshold                                                                 | NUMBER(13,6)       |
| 3.4.62       | DRYBIO_AG                 | Aboveground dry biomass of wood and bark at the threshold                                               | NUMBER(13,6)       |
| 3.4.63       | DRYBIO_BG                 | Belowground dry biomass at the threshold                                                                | NUMBER(13,6)       |
| 3.4.64       | CARBON_AG                 | Aboveground carbon of wood and bark at the threshold                                                    | NUMBER(13,6)       |
| 3.4.65       | CARBON_BG                 | Belowground carbon at the threshold                                                                     | NUMBER(13,6)       |
| 3.4.66       | DRYBIO_SAWLOG             | Dry biomass of wood in the sawlog portion of a sawtimber tree at the threshold                          | NUMBER(13,6)       |
| 3.4.67       | DRYBIO_SAWLOG_BARK        | Dry biomass of bark in the sawlog portion of a sawtimber tree at the threshold                          | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name     | Oracle data type   |
|--------------|---------------------------|----------------------|--------------------|
| 3.4.68       | CREATED_BY                | Created by           | VARCHAR2(30)       |
| 3.4.69       | CREATED_DATE              | Created date         | DATE               |
| 3.4.70       | CREATED_IN_INSTANCE       | Created in instance  | VARCHAR2(6)        |
| 3.4.71       | MODIFIED_BY               | Modified by          | VARCHAR2(30)       |
| 3.4.72       | MODIFIED_DATE             | Modified date        | DATE               |
| 3.4.73       | MODIFIED_IN_INSTANCE      | Modified in instance | VARCHAR2(6)        |

| Key Type   | Column(s) order        | Tables to link             | Abbreviated notation   |
|------------|------------------------|----------------------------|------------------------|
| Primary    | TRE_CN, THRESHOLD_TYPE | N/A                        | TRE_GRM_THRESHS_PK     |
| Foreign    | TRE_CN                 | TREE_GRM_THRESHOLD to TREE | TRE_GRM_THRESH_FK      |

This table stores information about ingrowth trees at specific tree threshold sizes. An ingrowth tree was not present at T1, but grew across a minimum quality and/or size threshold between inventories. This table does not include a record for every remeasurement tree, only ingrowth trees that require threshold values. Threshold estimates are computed for trees that grow across one or more thresholds during the remeasurement period (see THRESHOLD\_TYPE. The information in this table is used to compute growth, removal, and mortality (GRM) estimates on ingrowth trees. The current structure of the table supports estimates of volume as well as biomass.

## 3.4.1 TRE\_CN

Tree sequence number. Foreign key linking the tree GRM threshold record to the T2 tree record.

## 3.4.2 PREV\_TRE\_CN

Previous tree sequence number. Foreign key linking the tree GRM threshold record to the T1 tree record, if one exists. It can be blank (null) in some cases.

## 3.4.3 PLT\_CN

Plot sequence number. Foreign key linking the tree GRM threshold record to the plot record.

## 3.4.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 3.4.5 THRESHOLD\_TYPE

Threshold type. A code indicating the threshold type. Threshold types correspond with the tree-size thresholds recognized by FIA.

## Codes: THRESHOLD\_TYPE

| Code      | Description                                                                        |
|-----------|------------------------------------------------------------------------------------|
| Microplot | Tree at the 1-inch size threshold.                                                 |
| Subplot   | Tree at the 5-inch size threshold.                                                 |
| Sawlog    | Tree at the sawtimber threshold (9 inches for softwoods, 11 inches for hardwoods). |

## 3.4.6 SUBPTYP

Plot type code. A code indicating the plot type used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE\_GRM\_MIDPT.SUBPTYP description for codes.

## 3.4.7 SPCD

Species code. The FIA tree species code at T2, which may be different from the species code at T1. Refer to appendix F for codes.

## 3.4.8 STATUSCD

Status code. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.STATUSCD description for definition.

## 3.4.9 DIA

Threshold diameter. The threshold diameter, in inches, used for the tree estimates in the TREE\_GRM\_THRESHOLD table.DIAHTCD See TREE.DIA for details about how DIA is measured.

## 3.4.10 HT

Total height. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.HT description for definition.

## 3.4.11 ACTUALHT

Actual height. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.ACTUALHT description for definition.

## 3.4.12 CR

Compacted crown ratio. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.CR description for definition.

## 3.4.13 STANDING\_DEAD\_CD

Standing dead code. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.STANDING\_DEAD\_CD description for definition.

## 3.4.14 DIAHTCD

Diameter height code. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.DIAHTCD description for definition.

## 3.4.15 CULL

Rotten and missing cull. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.CULL description for definition.

## 3.4.16 ROUGHCULL

Rough cull. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.ROUGHCULL description for definition.

## 3.4.17 CULLFORM

Form cull. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.CULLFORM description for definition.

## 3.4.18 CULLMSTOP

Missing top cull. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.CULLMSTOP description for definition.

## 3.4.19 DECAYCD

Decay class code. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.DECAYCD description for definition.

## 3.4.20 TREECLCD

Tree class code. The value used for the estimates in the TREE\_GRM\_THRESHOLD table. See the TREE.TREECLCD description for codes.

## 3.4.21 HTDMP

Height to diameter measurement point. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.HTDMP description for definition.

## 3.4.22 WDLDSTEM

Woodland tree species stem count. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.WDLDSTEM description for definition.

## 3.4.23 STDORGCD

Stand origin code. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See COND.STDORGCD description for definition.

## 3.4.24 SITREE

Calculated site index. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See TREE.SITREE description for definition.

## 3.4.25 BALIVE

Basal area per acre of live trees. The value used for the tree estimates in the TREE\_GRM\_THRESHOLD table. See COND.BALIVE description for definition.

## 3.4.26 VOLTSGRS

Gross cubic-foot total-stem wood volume at the threshold. See TREE.VOLTSGRS description for definition.

## 3.4.27 VOLTSGRS\_BARK

Gross cubic-foot total-stem bark volume at the threshold. See TREE.VOLTSGRS\_BARK description for definition.

## 3.4.28 VOLTSSND

Sound cubic-foot total-stem wood volume at the threshold. See TREE.VOLTSSND description for definition.

## 3.4.29 VOLTSSND\_BARK

Sound cubic-foot total-stem bark volume at the threshold. See TREE.VOLTSSND\_BARK description for definition.

## 3.4.30 VOLCFGRS\_STUMP

Gross cubic-foot stump wood volume at the threshold. See TREE.VOLCFGRS\_STUMP description for definition.

## 3.4.31 VOLCFGRS\_STUMP\_BARK

Gross cubic-foot stump bark volume at the threshold. See TREE.VOLCFGRS\_STUMP\_BARK description for definition.

## 3.4.32 VOLCFSND\_STUMP

Sound cubic-foot stump wood volume at the threshold. See TREE.VOLCFSND\_STUMP description for definition.

## 3.4.33 VOLCFSND\_STUMP\_BARK

Sound cubic-foot stump bark volume at the threshold. See TREE.VOLCFSND\_STUMP\_BARK description for definition.

## 3.4.34 VOLCFGRS

Gross cubic-foot stem wood volume at the threshold. See TREE.VOLCFGRS description for definition.

## 3.4.35 VOLCFGRS\_BARK

Gross cubic-foot stem bark volume at the threshold. See TREE.VOLCFGRS\_BARK description for definition.

## 3.4.36 VOLCFGRS\_TOP

Gross cubic-foot stem-top wood volume at the threshold. See TREE.VOLCFGRS\_TOP description for definition.

## 3.4.37 VOLCFGRS\_TOP\_BARK

Gross cubic-foot stem-top bark volume at the threshold. See TREE.VOLCFGRS\_TOP\_BARK description for definition.

## 3.4.38 VOLCFSND

Sound cubic-foot stem wood volume at the threshold. See the TREE.VOLCFSND description for definition.

## 3.4.39 VOLCFSND\_BARK

Sound cubic-foot stem bark volume at the threshold. See TREE.VOLCFSND\_BARK description for definition.

## 3.4.40 VOLCFSND\_TOP

Sound cubic-foot stem-top wood volume at the threshold. See TREE.VOLCFSND\_TOP description for definition.

## 3.4.41 VOLCFSND\_TOP\_BARK

Sound cubic-foot stem-top bark volume at the threshold. See

TREE.VOLCFSND\_TOP\_BARK description for definition.

## 3.4.42 VOLCFNET

Net cubic-foot stem wood volume at the threshold. See the TREE.VOLCFNET description for definition.

## 3.4.43 VOLCFNET\_BARK

Net cubic-foot stem bark volume at the threshold. See TREE.VOLCFNET\_BARK description for definition.

## 3.4.44 VOLCSGRS

Gross cubic-foot wood volume in the sawlog portion of a sawtimber tree at the threshold. See TREE.VOLCSGRS description for definition.

## 3.4.45 VOLCSGRS\_BARK

Gross cubic-foot bark volume in the sawlog portion of a sawtimber tree at the threshold. See TREE.VOLCSGRS\_BARK description for definition.

## 3.4.46 VOLCSSND

Sound cubic-foot wood volume in the sawlog portion of a sawtimber tree at the threshold. See TREE.VOLCSSND description for definition.

## 3.4.47 VOLCSSND\_BARK

Sound cubic-foot bark volume in the sawlog portion of a sawtimber tree at the threshold. See TREE.VOLCSSND\_BARK description for definition.

## 3.4.48 VOLCSNET

Net cubic-foot wood volume in the sawlog portion of a sawtimber tree at the threshold. See the TREE.VOLCSNET description for definition.

## 3.4.49 VOLCSNET\_BARK

Net cubic-foot bark volume in the sawlog portion of a sawtimber tree at the threshold. See TREE.VOLCSNET\_BARK description for definition.

## 3.4.50 VOLBFGRS

Gross board-foot wood volume in the sawlog portion of a sawtimber tree at the threshold. See TREE.VOLBFGRS description for definition.

## 3.4.51 VOLBFNET

Net board-foot wood volume in the sawlog portion of a sawtimber tree at the threshold. See the TREE.VOLBFNET description for definition.

## 3.4.52 VOLBSGRS

Gross board-foot wood volume in the sawlog portion of a sawtimber tree at the threshold (Scribner Rule). See TREE.VOLBSGRS description for definition.

## 3.4.53 VOLBSNET

Net board-foot wood volume in the sawlog portion of a sawtimber tree at the threshold (Scribner Rule). See TREE.VOLBSNET description for definition.

## 3.4.54 DRYBIO\_STEM

Dry biomass of wood in the total stem at the threshold. See TREE.DRYBIO\_STEM description for definition.

## 3.4.55 DRYBIO\_STEM\_BARK

Dry biomass of bark in the total stem at the threshold. See TREE.DRYBIO\_STEM\_BARK description for definition.

## 3.4.56 DRYBIO\_STUMP

Dry biomass of wood in the stump at the threshold. See the TREE.DRYBIO\_STUMP description for definition.

## 3.4.57 DRYBIO\_STUMP\_BARK

Dry biomass of bark in the stump at the threshold. See TREE.DRYBIO\_STUMP\_BARK description for definition.

## 3.4.58 DRYBIO\_BOLE

Dry biomass of wood in the merchantable bole at the threshold. See the TREE.DRYBIO\_BOLE description for definition.

## 3.4.59 DRYBIO\_BOLE\_BARK

Dry biomass of bark in the merchantable bole at the threshold. See TREE.DRYBIO\_BOLE\_BARK description for definition.

## 3.4.60 DRYBIO\_BRANCH

Dry biomass of branches at the threshold. See TREE.DRYBIO\_BRANCH description for definition.

## 3.4.61 DRYBIO\_FOLIAGE

Dry biomass of foliage at the threshold. See TREE.DRYBIO\_FOLIAGE description for definition.

## 3.4.62 DRYBIO\_AG

Aboveground dry biomass of wood and bark at the threshold. See the TREE.DRYBIO\_AG description for definition.

## 3.4.63 DRYBIO\_BG

Belowground dry biomass at the threshold. See the TREE.DRYBIO\_BG description for definition.

## 3.4.64 CARBON\_AG

Aboveground carbon of wood and bark at the threshold. See TREE.CARBON\_AG description for definition.

## 3.4.65 CARBON\_BG

Belowground carbon at the threshold. See TREE.CARBON\_BG description for definition.

## 3.4.66 DRYBIO\_SAWLOG

Dry biomass of wood in the sawlog portion of a sawtimber tree at the threshold. See TREE.DRYBIO\_SAWLOG description for definition.

## 3.4.67 DRYBIO\_SAWLOG\_BARK

Dry biomass of bark in the sawlog portion of a sawtimber tree at the threshold. See TREE.DRYBIO\_SAWLOG\_BARK description for definition.

## 3.4.68 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.4.69 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.4.70 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.4.71 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.4.72 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.4.73 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.