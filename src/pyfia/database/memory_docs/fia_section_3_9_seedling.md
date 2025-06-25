# Section 3.9: Seedling Table
**Oracle Table Name:** SEEDLING
**Extracted Pages:** 301-306 (Chapter pages 3-143 to 3-148)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.9 Seedling Table

## (Oracle table name: SEEDLING)

| Subsection   | Column name (attribute)         | Descriptive name                                                                  | Oracle data type   |
|--------------|---------------------------------|-----------------------------------------------------------------------------------|--------------------|
| 3.9.1        | CN                              | Sequence number                                                                   | VARCHAR2(34)       |
| 3.9.2        | PLT_CN                          | Plot sequence number                                                              | VARCHAR2(34)       |
| 3.9.3        | INVYR                           | Inventory year                                                                    | NUMBER(4)          |
| 3.9.4        | STATECD                         | State code                                                                        | NUMBER(4)          |
| 3.9.5        | UNITCD                          | Survey unit code                                                                  | NUMBER(2)          |
| 3.9.6        | COUNTYCD                        | County code                                                                       | NUMBER(3)          |
| 3.9.7        | PLOT                            | Plot number                                                                       | NUMBER(5)          |
| 3.9.8        | SUBP                            | Subplot number                                                                    | NUMBER(3)          |
| 3.9.9        | CONDID                          | Condition class number                                                            | NUMBER(1)          |
| 3.9.10       | SPCD                            | Species code                                                                      | NUMBER             |
| 3.9.11       | SPGRPCD                         | Species group code                                                                | NUMBER(2)          |
| 3.9.12       | STOCKING                        | Tree stocking                                                                     | NUMBER(7,4)        |
| 3.9.13       | TREECOUNT                       | Tree count for seedlings                                                          | NUMBER(3)          |
| 3.9.14       | TOTAGE                          | Total age                                                                         | NUMBER(3)          |
| 3.9.15       | CREATED_BY                      | Created by                                                                        | VARCHAR2(30)       |
| 3.9.16       | CREATED_DATE                    | Created date                                                                      | DATE               |
| 3.9.17       | CREATED_IN_INSTANCE             | Created in instance                                                               | VARCHAR2(6)        |
| 3.9.18       | MODIFIED_BY                     | Modified by                                                                       | VARCHAR2(30)       |
| 3.9.19       | MODIFIED_DATE                   | Modified date                                                                     | DATE               |
| 3.9.20       | MODIFIED_IN_INSTANCE            | Modified in instance                                                              | VARCHAR2(6)        |
| 3.9.21       | TREECOUNT_CALC                  | Tree count used in calculations                                                   | NUMBER             |
| 3.9.22       | TPA_UNADJ                       | Trees per acre unadjusted                                                         | NUMBER(11,6)       |
| 3.9.23       | CYCLE                           | Inventory cycle number                                                            | NUMBER(2)          |
| 3.9.24       | SUBCYCLE                        | Inventory subcycle number                                                         | NUMBER(2)          |
| 3.9.25       | DAMAGE_AGENT_CD1_SRS            | Damage agent code 1 (Caribbean Islands), Southern Research Station                | NUMBER(5)          |
| 3.9.26       | PCT_AFFECTED_DAMAGE_AGENT1 _SRS | Percent affected by damage agent 1 (Caribbean Islands), Southern Research Station | NUMBER(3)          |
| 3.9.27       | DAMAGE_AGENT_CD2_SRS            | Damage agent code 2 (Caribbean Islands), Southern Research Station                | NUMBER(5)          |
| 3.9.28       | PCT_AFFECTED_DAMAGE_AGENT2 _SRS | Percent affected by damage agent 2 (Caribbean Islands), Southern Research Station | NUMBER(3)          |
| 3.9.29       | DAMAGE_AGENT_CD3_SRS            | Damage agent code 3 (Caribbean Islands), Southern Research Station                | NUMBER(5)          |

| Subsection   | Column name (attribute)         | Descriptive name                                                                  | Oracle data type   |
|--------------|---------------------------------|-----------------------------------------------------------------------------------|--------------------|
| 3.9.30       | PCT_AFFECTED_DAMAGE_AGENT3 _SRS | Percent affected by damage agent 3 (Caribbean Islands), Southern Research Station | NUMBER(3)          |
| 3.9.31       | AGECD_RMRS                      | Seedling age code, Rocky Mountain Research Station                                | NUMBER(1)          |
| 3.9.32       | COUNTCHKCD_RMRS                 | Seedling count check code, Rocky Mountain Research Station                        | NUMBER(1)          |

| Key Type   | Column(s) order                                            | Tables to link   | Abbreviated notation   |
|------------|------------------------------------------------------------|------------------|------------------------|
| Primary    | CN                                                         | N/A              | SDL_PK                 |
| Unique     | PLT_CN, SUBP, CONDID, SPCD                                 | N/A              | SDL_UK                 |
| Natural    | STATECD, INVYR, UNITCD, COUNTYCD, PLOT, SUBP, CONDID, SPCD | N/A              | SDL_NAT_I              |
| Foreign    | PLT_CN                                                     | SEEDLING to PLOT | SDL_PLT_FK             |

Seedling data collection overview - When PLOT.MANUAL &lt;2.0, the national  core procedure was to record the actual seedling count up to six seedlings and then record 6+ if at least six seedlings were present. However, the following regions collected the actual seedling count when PLOT.MANUAL &lt;2.0: Rocky Mountain Research Station (RMRS) and North Central Research Station (NCRS). If PLOT.MANUAL &lt;2.0 and TREECOUNT is blank (null), then a value of 6 in TREECOUNT\_CALC represents 6 or more seedlings. In the past, seedlings were often tallied in FIA inventories only to the extent necessary to determine if some minimum number were present, which means that seedlings were often under-reported. Note: The SEEDLING record may not exist for some periodic inventories.

## 3.9.1 CN

Sequence number. A unique sequence number used to identify a seedling record.

## 3.9.2 PLT\_CN

Plot sequence number. Foreign key linking the seedling record to the plot record.

## 3.9.3 INVYR

Inventory year.

See SURVEY.INVYR description for definition.

## 3.9.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 3.9.5 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 3.9.6 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 3.9.7 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 3.9.8 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 3.9.9 CONDID

Condition class number. The unique identifying number assigned to a condition on which the seedling(s) is located, and is defined in the COND table. See COND.CONDID for details on the attributes which delineate a condition.

## 3.9.10 SPCD

Species code. An FIA species code identifying the tree species of the seedling count. Refer to appendix F for codes.

## 3.9.11 SPGRPCD

Species group code. A code assigned to each tree species in order to group them for reporting purposes. Codes and their associated names (see REF\_SPECIES\_GROUP.NAME) are shown in appendix E. Refer to appendix F for individual tree species and corresponding species group codes.

## 3.9.12 STOCKING

Tree stocking. The stocking value, in percent, assigned to each count of seedlings, by species. Stocking values are computed using several specific species equations that were developed from normal yield tables and stocking charts. The stocking of seedling count records is used to calculate COND.GSSTK, COND.GSSTKCD, COND.ALSTK, and COND.ALSTKCD on the condition record.

## 3.9.13 TREECOUNT

Tree count for seedlings. The number of live seedlings (DIA &lt;1.0 inch) present on the microplot by species and condition class. To qualify for counting, conifer seedlings must be at least 6 inches tall and hardwood seedlings must be at least 12 inches tall. When PLOT.MANUAL &lt;2.0, the national core procedure was to record the actual seedling count up to six seedlings and then record 6+ if at least six seedlings were present. However, the following regions collected the actual seedling count when PLOT.MANUAL &lt;2.0: Rocky Mountain Research Station (RMRS) and North Central Research Station (NCRS). If

PLOT.MANUAL &lt;2.0 and TREECOUNT is blank (null), then a value of 6 in TREECOUNT\_CALC represents 6 or more seedlings.

## 3.9.14 TOTAGE

Total age. The seedling's total age. Total age is collected for a subset of seedling count records, using one representative seedling for the species. The age is obtained by counting the terminal bud scars or the whorls of branches and may be used in the stand age calculation. Only populated by certain FIA work units (SURVEY.RSCD = 22) and is blank (null) when it is not collected.

## 3.9.15 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.9.16 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.9.17 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.9.18 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.9.19 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.9.20 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 3.9.21 TREECOUNT\_CALC

Tree count used in calculations. This attribute is set either to COUNTCD, which was dropped in FIADB version 2.1, or TREECOUNT. When PLOT.MANUAL &lt;2.0, the national core procedure was to record the actual seedling count up to six seedlings and then record 6+ if at least six seedlings were present. However, the following regions collected the actual seedling count when PLOT.MANUAL &lt;2.0: Rocky Mountain Research Station (RMRS) and North Central Research Station (NCRS). If PLOT.MANUAL &lt;2.0 and TREECOUNT is blank (null), then a value of 6 in TREECOUNT\_CALC represents 6 or more seedlings.

## 3.9.22 TPA\_UNADJ

Trees per acre unadjusted. The number of seedlings per acre that the seedling count theoretically represents based on the sample design. For fixed-radius plots taken with the mapped plot design (PLOT.DESIGNCD = 1), TPA\_UNADJ equals 74.965282 times the number of seedlings counted. For plots taken with other sample designs, this attribute may be blank (null). Based on the procedures described in Bechtold and Patterson (2005), this attribute can be adjusted using factors stored in the POP\_STRATUM table to derive population estimates. Examples of estimating population totals are shown in The Forest Inventory and Analysis Database: Population Estimation User Guide.

## 3.9.23 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 3.9.24 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 3.9.25 DAMAGE\_AGENT\_CD1\_SRS

Damage agent code 1 ( Caribbean Islands ), Southern Research Station. A code indicating the first damage agent observed when inspecting the tree from bottom to top (roots, bole, branches, foliage). Up to three damage agents can be recorded (DAMAGE\_AGENT\_CD1\_SRS, DAMAGE\_AGENT\_CD2\_SRS, DAMAGE\_AGENT\_CD3\_SRS). If more than one agent is observed, the most threatening one is listed first where agents threatening survival are listed first and agents threatening wood quality second. The codes used for damage agents come from the January 2012 Pest Trend Impact Plot System (PTIPS) list from the Forest Health Assessment and Applied Sciences Team (FHAAST) that has been modified to meet FIA's needs. See appendix H for the complete list of codes. Only populated by certain FIA work units (SURVEY.RSCD = 33) for the Caribbean Islands.

## 3.9.26 PCT\_AFFECTED\_DAMAGE\_AGENT1\_SRS

Percent affected by damage agent 1 ( Caribbean Islands ), Southern Research Station. The percent of seedlings on the microplot, by species and condition, which are affected by DAMAGE\_AGENT\_CD1\_SRS. Only populated by certain FIA work units (SURVEY.RSCD = 33) for the Caribbean Islands.

## 3.9.27 DAMAGE\_AGENT\_CD2\_SRS

Damage agent code 2 ( Caribbean Islands ), Southern Research Station. See DAMAGE\_AGENT\_CD1\_SRS.

## 3.9.28 PCT\_AFFECTED\_DAMAGE\_AGENT2\_SRS

Percent affected by damage agent 2 ( Caribbean Islands ), Southern Research Station. The percent of seedlings on the microplot, by species and condition, which are affected by DAMAGE\_AGENT\_CD2\_SRS.

## 3.9.29 DAMAGE\_AGENT\_CD3\_SRS

Damage agent code 3 ( Caribbean Islands ), Southern Research Station. See DAMAGE\_AGENT\_CD1\_SRS.

## 3.9.30 PCT\_AFFECTED\_DAMAGE\_AGENT3\_SRS

Percent affected by damage agent 3 ( Caribbean Islands ), Southern Research Station. The percent of seedlings on the microplot, by species and condition, which are affected by DAMAGE\_AGENT\_CD3\_SRS.

## 3.9.31 AGECD\_RMRS

Seedling age code, Rocky Mountain Research Station. A code used in the field indicating which seedling counts require total age information to be collected. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: AGECD\_RMRS

|   Code | Description                                             |
|--------|---------------------------------------------------------|
|      0 | Do not collect age information for this seedling count. |
|      1 | Collect total age information for this seedling count.  |

## 3.9.32 COUNTCHKCD\_RMRS

Seedling count check code, Rocky Mountain Research Station. A code indicating if the seedling count was estimated. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: COUNTCHKCD\_RMRS

|   Code | Description                   |
|--------|-------------------------------|
|      0 | Seedlings counted accurately. |
|      1 | Seedling count estimated.     |