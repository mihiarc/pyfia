# Section 3.5: Tree Growth, Removal, and Mortality Midpoint Table
**Oracle Table Name:** TREE_GRM_MIDPT
**Extracted Pages:** 269-278 (Chapter pages 3-111 to 3-120)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.5 Tree Growth, Removal, and Mortality Midpoint Table

## (Oracle table name: TREE\_GRM\_MIDPT)

| Subsection   | Column name (attribute)   | Descriptive name                                        | Oracle data type   |
|--------------|---------------------------|---------------------------------------------------------|--------------------|
| 3.5.1        | TRE_CN                    | Tree sequence number                                    | VARCHAR2(34)       |
| 3.5.2        | PREV_TRE_CN               | Previous tree sequence number                           | VARCHAR2(34)       |
| 3.5.3        | PLT_CN                    | Plot sequence number                                    | VARCHAR2(34)       |
| 3.5.4        | STATECD                   | State code                                              | NUMBER(2)          |
| 3.5.5        | SUBPTYP                   | Plot type code                                          | NUMBER(1)          |
| 3.5.6        | SPCD                      | Species code                                            | NUMBER(4)          |
| 3.5.7        | STATUSCD                  | Status code                                             | NUMBER(2)          |
| 3.5.8        | DIA                       | Midpoint diameter                                       | NUMBER(5,2)        |
| 3.5.9        | HT                        | Total height                                            | NUMBER(3)          |
| 3.5.10       | ACTUALHT                  | Actual height                                           | NUMBER(3)          |
| 3.5.11       | CR                        | Compacted crown ratio                                   | NUMBER(3)          |
| 3.5.12       | STANDING_DEAD_CD          | Standing dead code                                      | NUMBER(2)          |
| 3.5.13       | DIAHTCD                   | Diameter height code                                    | NUMBER(1)          |
| 3.5.14       | CULL                      | Rotten and missing cull                                 | NUMBER(3)          |
| 3.5.15       | ROUGHCULL                 | Rough cull                                              | NUMBER(3)          |
| 3.5.16       | CULLFORM                  | Form cull                                               | NUMBER(3)          |
| 3.5.17       | CULLMSTOP                 | Missing top cull                                        | NUMBER(3)          |
| 3.5.18       | DECAYCD                   | Decay class code                                        | NUMBER(2)          |
| 3.5.19       | TREECLCD                  | Tree class code                                         | NUMBER(2)          |
| 3.5.20       | HTDMP                     | Height to diameter measurement point                    | NUMBER(3,1)        |
| 3.5.21       | WDLDSTEM                  | Woodland tree species stem count                        | NUMBER(3)          |
| 3.5.22       | STDORGCD                  | Stand origin code                                       | NUMBER(2)          |
| 3.5.23       | SITREE                    | Calculated site index                                   | NUMBER(3)          |
| 3.5.24       | BALIVE                    | Basal area per acre of live trees                       | NUMBER(9,4)        |
| 3.5.25       | VOLTSGRS                  | Gross cubic-foot total-stem wood volume at the midpoint | NUMBER(13,6)       |
| 3.5.26       | VOLTSGRS_BARK             | Gross cubic-foot total-stem bark volume at the midpoint | NUMBER(13,6)       |
| 3.5.27       | VOLTSSND                  | Sound cubic-foot total-stem wood volume at the midpoint | NUMBER(13,6)       |
| 3.5.28       | VOLTSSND_BARK             | Sound cubic-foot total-stem bark volume at the midpoint | NUMBER(13,6)       |
| 3.5.29       | VOLCFGRS_STUMP            | Gross cubic-foot stump wood volume at the midpoint      | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name                                                                       | Oracle data type   |
|--------------|---------------------------|----------------------------------------------------------------------------------------|--------------------|
| 3.5.30       | VOLCFGRS_STUMP_BARK       | Gross cubic-foot stump bark volume at the midpoint                                     | NUMBER(13,6)       |
| 3.5.31       | VOLCFSND_STUMP            | Sound cubic-foot stump wood volume at the midpoint                                     | NUMBER(13,6)       |
| 3.5.32       | VOLCFSND_STUMP_BARK       | Sound cubic-foot stump bark volume at the midpoint                                     | NUMBER(13,6)       |
| 3.5.33       | VOLCFGRS                  | Gross cubic-foot stem wood volume at the midpoint                                      | NUMBER(13,6)       |
| 3.5.34       | VOLCFGRS_BARK             | Gross cubic-foot stem bark volume at the midpoint                                      | NUMBER(13,6)       |
| 3.5.35       | VOLCFGRS_TOP              | Gross cubic-foot stem-top wood volume at the midpoint                                  | NUMBER(13,6)       |
| 3.5.36       | VOLCFGRS_TOP_BARK         | Gross cubic-foot stem-top bark volume at the midpoint                                  | NUMBER(13,6)       |
| 3.5.37       | VOLCFSND                  | Sound cubic-foot stem wood volume at the midpoint                                      | NUMBER(13,6)       |
| 3.5.38       | VOLCFSND_BARK             | Sound cubic-foot stem bark volume at the midpoint                                      | NUMBER(13,6)       |
| 3.5.39       | VOLCFSND_TOP              | Sound cubic-foot stem-top wood volume at the midpoint                                  | NUMBER(13,6)       |
| 3.5.40       | VOLCFSND_TOP_BARK         | Sound cubic-foot stem-top bark volume at the midpoint                                  | NUMBER(13,6)       |
| 3.5.41       | VOLCFNET                  | Net cubic-foot stem wood volume at the midpoint                                        | NUMBER(13, 6)      |
| 3.5.42       | VOLCFNET_BARK             | Net cubic-foot stem bark volume at the midpoint                                        | NUMBER(13,6)       |
| 3.5.43       | VOLCSGRS                  | Gross cubic-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint | NUMBER(13,6)       |
| 3.5.44       | VOLCSGRS_BARK             | Gross cubic-foot bark volume in the sawlog portion of a sawtimber tree at the midpoint | NUMBER(13,6)       |
| 3.5.45       | VOLCSSND                  | Sound cubic-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint | NUMBER(13,6)       |
| 3.5.46       | VOLCSSND_BARK             | Sound cubic-foot bark volume in the sawlog portion of a sawtimber tree at the midpoint | NUMBER(13,6)       |
| 3.5.47       | VOLCSNET                  | Net cubic-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint   | NUMBER(13,6)       |
| 3.5.48       | VOLCSNET_BARK             | Net cubic-foot bark volume in the sawlog portion of a sawtimber tree at the midpoint   | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name                                                                                       | Oracle data type   |
|--------------|---------------------------|--------------------------------------------------------------------------------------------------------|--------------------|
| 3.5.49       | VOLBFGRS                  | Gross board-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint                 | NUMBER(13,6)       |
| 3.5.50       | VOLBFNET                  | Net board-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint                   | NUMBER(13,6)       |
| 3.5.51       | VOLBSGRS                  | Gross board-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint (Scribner Rule) | NUMBER(13,6)       |
| 3.5.52       | VOLBSNET                  | Net board-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint (Scribner Rule)   | NUMBER(13, 6)      |
| 3.5.53       | DRYBIO_STEM               | Dry biomass of wood in the total stem at the midpoint                                                  | NUMBER(13,6)       |
| 3.5.54       | DRYBIO_STEM_BARK          | Dry biomass of bark in the total stem at the midpoint                                                  | NUMBER(13,6)       |
| 3.5.55       | DRYBIO_STUMP              | Dry biomass of wood in the stump at the midpoint                                                       | NUMBER(13,6)       |
| 3.5.56       | DRYBIO_STUMP_BARK         | Dry biomass of bark in the stump at the midpoint                                                       | NUMBER(13,6)       |
| 3.5.57       | DRYBIO_BOLE               | Dry biomass of wood in the merchantable bole at the midpoint                                           | NUMBER(13,6)       |
| 3.5.58       | DRYBIO_BOLE_BARK          | Dry biomass of bark in the merchantable bole at the midpoint                                           | NUMBER(13,6)       |
| 3.5.59       | DRYBIO_BRANCH             | Dry biomass of branches at the midpoint                                                                | NUMBER(13,6)       |
| 3.5.60       | DRYBIO_FOLIAGE            | Dry biomass of foliage at the midpoint                                                                 | NUMBER(13,6)       |
| 3.5.61       | DRYBIO_AG                 | Aboveground dry biomass of wood and bark at the midpoint                                               | NUMBER(13,6)       |
| 3.5.62       | DRYBIO_BG                 | Belowground dry biomass at the midpoint                                                                | NUMBER(13,6)       |
| 3.5.63       | CARBON_AG                 | Aboveground carbom of wood and bark at the midpoint                                                    | NUMBER(13,6)       |
| 3.5.64       | CARBON_BG                 | Belowground carbon at the midpoint                                                                     | NUMBER(13,6)       |
| 3.5.65       | DRYBIO_SAWLOG             | Dry biomass of wood in the sawlog portion of a sawtimber tree at the midpoint                          | NUMBER(13,6)       |
| 3.5.66       | DRYBIO_SAWLOG_BARK        | Dry biomass of bark in the sawlog portion of a sawtimber tree at the midpoint                          | NUMBER(13,6)       |
| 3.5.67       | CREATED_BY                | Created by                                                                                             | VARCHAR2(30)       |
| 3.5.68       | CREATED_DATE              | Created date                                                                                           | DATE               |
| 3.5.69       | CREATED_IN_INSTANCE       | Created in instance                                                                                    | VARCHAR2(6)        |

| Subsection   | Column name (attribute)   | Descriptive name     | Oracle data type   |
|--------------|---------------------------|----------------------|--------------------|
| 3.5.70       | MODIFIED_BY               | Modified by          | VARCHAR2(30)       |
| 3.5.71       | MODIFIED_DATE             | Modified date        | DATE               |
| 3.5.72       | MODIFIED_IN_INSTANCE      | Modified in instance | VARCHAR2(6)        |

| Key Type   | Column(s) order   | Tables to link         | Abbreviated notation   |
|------------|-------------------|------------------------|------------------------|
| Primary    | TRE_CN            | N/A                    | TRE_GRM_MIDPT_PK       |
| Foreign    | TRE_CN            | TREE_GRM_MIDPT to TREE | TRE_GRM_MIDPT_FK       |

This table stores information about a remeasurement tree at the midpoint of the remeasurement period and is used to compute growth, removal, and mortality (GRM) estimates on remeasured trees. In annual-to-annual inventories, the midpoint is the point in time exactly between the time 1 (T1, most recent past measurement) and time 2 (T2, current) measurement dates. All trees involved in annual-to-annual change have records in this table. Midpoint information may not be available for periodic-to-periodic and periodic-to-annual inventories; when the information is available, it may be from the estimated time of mortality or removal instead of the midpoint of the remeasurement period. This table includes a single record per tree. The current structure of the table supports estimates of volume, biomass, and carbon.

## 3.5.1 TRE\_CN

Tree sequence number. Foreign key linking the tree GRM midpoint record to the T2 tree record.

## 3.5.2 PREV\_TRE\_CN

Previous tree sequence number. Foreign key linking the GRM midpoint record to the T1 tree record, if one exists. It can be blank (null) in some cases. For example, an ingrowth tree would not have a T1 record.

## 3.5.3 PLT\_CN

Plot sequence number. Foreign key linking the tree GRM midpoint record to the plot record.

## 3.5.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 3.5.5 SUBPTYP

Plot type code. A code indicating the plot type used for the tree estimates in TREE\_GRM\_MIDPT table.

## Codes: SUBPTYP

|   Code | Description                                                                                                                                                                           |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | Undetermined. Assigned in cases where there is no T1 tree record, and the modeled tree grows across either the microplot-to-subplot threshold, or the subplot-to-macroplot threshold. |
|      1 | Subplot.                                                                                                                                                                              |
|      2 | Microplot.                                                                                                                                                                            |
|      3 | Macroplot.                                                                                                                                                                            |

## 3.5.6 SPCD

Species code. The FIA tree species code at T2, which may be different from the species code at T1. Refer to appendix F for codes.

## 3.5.7 STATUSCD

Status code. The value used for the tree estimates in the TREE\_GRM\_MIDPT table.See TREE.STATUSCD description for definition.

## 3.5.8 DIA

Midpoint diameter. The estimated midpoint diameter used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.DIA description for definition.

## 3.5.9 HT

Total height. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.HT description for definition.

## 3.5.10 ACTUALHT

Actual height. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.ACTUALHT description for definition.

## 3.5.11 CR

Compacted crown ratio. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.CR description for definition.

## 3.5.12 STANDING\_DEAD\_CD

Standing dead code. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.STANDING\_DEAD\_CD description for definition.

## 3.5.13 DIAHTCD

Diameter height code. The value for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.DIAHTCD description for definition.

## 3.5.14 CULL

Rotten and missing cull. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.CULL description for definition.

## 3.5.15 ROUGHCULL

Rough cull. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.ROUGHCULL description for definition.

## 3.5.16 CULLFORM

Form cull. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.CULLFORM description for definition.

## 3.5.17 CULLMSTOP

Missing top cull. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.CULLMSTOP description for definition.

## 3.5.18 DECAYCD

Decay class code. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.DECAYCD description for definition.

## 3.5.19 TREECLCD

Tree class code. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.TREECLCD description for definition.

## 3.5.20 HTDMP

Height to diameter measurement point. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.HTDMP description for definition.

## 3.5.21 WDLDSTEM

Woodland tree species stem count. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.WDLDSTEM description for definition.

## 3.5.22 STDORGCD

Stand origin code. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See COND.STDORGCD description for definition.

## 3.5.23 SITREE

Calculated site index. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See TREE.SITREE description for definition.

## 3.5.24 BALIVE

Basal area per acre of live trees. The value used for the tree estimates in the TREE\_GRM\_MIDPT table. See COND.BALIVE description for definition.

## 3.5.25 VOLTSGRS

Gross cubic-foot total-stem wood volume at the midpoint. See TREE.VOLTSGRS description for definition.

## 3.5.26 VOLTSGRS\_BARK

Gross cubic-foot total-stem bark volume at the midpoint. See TREE.VOLTSGRS\_BARK description for definition.

## 3.5.27 VOLTSSND

Sound cubic-foot total-stem wood volume at the midpoint. See TREE.VOLTSSND description for definition.

## 3.5.28 VOLTSSND\_BARK

Sound cubic-foot total-stem bark volume at the midpoint. See TREE.VOLTSSND\_BARK description for definition.

## 3.5.29 VOLCFGRS\_STUMP

Gross cubic-foot stump wood volume at the midpoint. See TREE.VOLCFGRS\_STUMP description for definition.

## 3.5.30 VOLCFGRS\_STUMP\_BARK

Gross cubic-foot stump bark volume at the midpoint. See TREE.VOLCFGRS\_STUMP\_BARK description for definition.

## 3.5.31 VOLCFSND\_STUMP

Sound cubic-foot stump wood volume at the midpoint. See TREE.VOLCFSND\_STUMP description for definition.

## 3.5.32 VOLCFSND\_STUMP\_BARK

Sound cubic-foot stump bark volume at the midpoint. See

TREE.VOLCFSND\_STUMP\_BARK description for definition.

## 3.5.33 VOLCFGRS

Gross cubic-foot stem wood volume at the midpoint. See TREE.VOLCFGRS description for definition.

## 3.5.34 VOLCFGRS\_BARK

Gross cubic-foot stem bark volume at the midpoint. See TREE.VOLCFGRS\_BARK description for definition.

## 3.5.35 VOLCFGRS\_TOP

Gross cubic-foot stem-top wood volume at the midpoint. See TREE.VOLCFGRS\_TOP description for definition.

## 3.5.36 VOLCFGRS\_TOP\_BARK

Gross cubic-foot stem-top bark volume at tbe midpoint. See

TREE.VOLCFGRS\_TOP\_BARK description for definition.

## 3.5.37 VOLCFSND

Sound cubic-foot stem wood volume at the midpoint. See TREE.VOLCFSND description for definition.

## 3.5.38 VOLCFSND\_BARK

Sound cubic-foot stem bark volume at the midpoint. See TREE.VOLCFSND\_BARK description for definition.

## 3.5.39 VOLCFSND\_TOP

Sound cubic-foot stem-top wood volume at the midpoint. See TREE.VOLCFSND\_TOP description for definition.

## 3.5.40 VOLCFSND\_TOP\_BARK

Sound cubic-foot stem-top bark volume at the midpoint. See

TREE.VOLCFSND\_TOP\_BARK description for definition.

## 3.5.41 VOLCFNET

Net cubic-foot stem wood volume at the midpoint. See TREE.VOLCFNET description for definition.

## 3.5.42 VOLCFNET\_BARK

Net cubic-foot stem bark volume at the midpoiont. See TREE.VOLCFNET\_BARK description for definition.

## 3.5.43 VOLCSGRS

Gross cubic-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint. See TREE.VOLCSGRS description for definition.

## 3.5.44 VOLCSGRS\_BARK

Gross cubic-foot bark volume in the sawlog portion of a sawtimber tree at the midpoint. See TREE.VOLCSGRS\_BARK description for definition.

## 3.5.45 VOLCSSND

Sound cubic-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint. See TREE.VOLCSSND description for definition.

## 3.5.46 VOLCSSND\_BARK

Sound cubic-foot bark volume in the sawlog portion of a sawtimber tree at the midpoint. See TREE.VOLCSSND\_BARK description for definition.

## 3.5.47 VOLCSNET

Net cubic-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint. See TREE.VOLCSNET description for definition.

## 3.5.48 VOLCSNET\_BARK

Net cubic-foot bark volume in the sawlog portion of a sawtimber tree at the midpoint. See TREE.VOLCSNET\_BARK description for definition.

## 3.5.49 VOLBFGRS

Gross board-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint. See TREE.VOLBFGRS description for definition.

## 3.5.50 VOLBFNET

Net board-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint. See TREE.VOLBFNET description for definition.

## 3.5.51 VOLBSGRS

Gross board-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint (Scribner Rule). See TREE.VOLBSGRS description for definition.

## 3.5.52 VOLBSNET

Net board-foot wood volume in the sawlog portion of a sawtimber tree at the midpoint (Scribner Rule). See TREE.VOLBSNET description for definition.

## 3.5.53 DRYBIO\_STEM

Dry biomass of wood in the total stem at the midpoint. See TREE.DRYBIO\_STEM description for definition.

## 3.5.54 DRYBIO\_STEM\_BARK

Dry biomass of bark in the total stem at the midpoint. See TREE.DRYBIO\_STEM\_BARK description for definition.

## 3.5.55 DRYBIO\_STUMP

Dry biomass wood in the stump at the midpoint. See TREE.DRYBIO\_STUMP description for definition.

## 3.5.56 DRYBIO\_STUMP\_BARK

Dry biomass of bark in the stump at the midpoint. See TREE.DRYBIO\_STUMP\_BARK description for definition.

## 3.5.57 DRYBIO\_BOLE

Dry biomass of wood in the merchantable bole at the midpoint. See TREE.DRYBIO\_BOLE description for definition.

## 3.5.58 DRYBIO\_BOLE\_BARK

Dry biomass of bark in the merchantable bole at the midpoint. See

TREE.DRYBIO\_BOLE\_BARK description for definition.

## 3.5.59 DRYBIO\_BRANCH

Dry biomass of branches at the midpoint. See TREE.DRYBIO\_BRANCH description for definition.

## 3.5.60 DRYBIO\_FOLIAGE

Dry biomass of foliage at the midpoint. See TREE.DRYBIO\_FOLIAGE description for definition.

## 3.5.61 DRYBIO\_AG

Aboveground dry biomass of wood and bark at the midpoint. See TREE.DRYBIO\_AG description for definition.

## 3.5.62 DRYBIO\_BG

Belowground dry biomass at the midpoint. See TREE.DRYBIO\_BG description for definition.

## 3.5.63 CARBON\_AG

Aboveground carbon of wood and bark at the midpoint. See TREE.CARBON\_AG description for definition.

## 3.5.64 CARBON\_BG

Belowground carbon at the midpoint. See TREE.CARBON\_BG description for definition.

## 3.5.65 DRYBIO\_SAWLOG

Dry biomass of wood in the sawlog portion of a sawtimber tree at the midpoint. See TREE.DRYBIO\_SAWLOG description for definition.

## 3.5.66 DRYBIO\_SAWLOG\_BARK

Dry biomass of bark in the sawlog portion of a sawtimber tree at the midpoint. See TREE.DRYBIO\_SAWLOG\_BARK description for definition.

## 3.5.67 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.5.68 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.5.69 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.5.70 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.5.71 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.5.72 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.