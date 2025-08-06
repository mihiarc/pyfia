# Section 6.2: Subplot Regeneration Table
**Oracle Table Name:** SUBPLOT_REGEN
**Extracted Pages:** 421-424 (Chapter pages 6-7 to 6-10)
**Source:** FIA Database Handbook v9.3
**Chapter:** 6 - Database Tables - Northern Research Station (NRS) Tree Regeneration Indicator

---

## 6.2 Subplot Regeneration Table

## (Oracle table name: SUBPLOT\_REGEN)

| Subsection   | Column name (attribute)    | Descriptive name                    | Oracle data type   |
|--------------|----------------------------|-------------------------------------|--------------------|
| 6.2.1        | CN                         | Sequence number                     | VARCHAR2(34)       |
| 6.2.2        | PLT_CN                     | Plot sequence number                | VARCHAR2(34)       |
| 6.2.3        | SBP_CN                     | Subplot sequence number             | VARCHAR2(34)       |
| 6.2.4        | INVYR                      | Inventory year                      | NUMBER(4)          |
| 6.2.5        | STATECD                    | State code                          | NUMBER(4)          |
| 6.2.6        | UNITCD                     | Survey unit code                    | NUMBER(2)          |
| 6.2.7        | COUNTYCD                   | County code                         | NUMBER(3)          |
| 6.2.8        | PLOT                       | Plot number                         | NUMBER(5)          |
| 6.2.9        | SUBP                       | Subplot number                      | NUMBER(2)          |
| 6.2.10       | REGEN_SUBP_STATUS_CD       | Regeneration subplot status code    | NUMBER(1)          |
| 6.2.11       | REGEN_NONSAMPLE_REASN_CD   | Regeneration nonsampled reason code | NUMBER(2)          |
| 6.2.12       | SUBPLOT_SITE_LIMITATIONS   | Subplot site limitations            | NUMBER(1)          |
| 6.2.13       | MICROPLOT_SITE_LIMITATIONS | Microplot site limitations          | NUMBER(1)          |
| 6.2.14       | CREATED_BY                 | Created by                          | VARCHAR2(30)       |
| 6.2.15       | CREATED_DATE               | Created date                        | DATE               |
| 6.2.16       | CREATED_IN_INSTANCE        | Created in instance                 | VARCHAR2(6)        |
| 6.2.17       | MODIFIED_BY                | Modified by                         | VARCHAR2(30)       |
| 6.2.18       | MODIFIED_DATE              | Modified date                       | DATE               |
| 6.2.19       | MODIFIED_IN_INSTANCE       | Modified in instance                | VARCHAR2(6)        |
| 6.2.20       | CYCLE                      | Inventory cycle number              | NUMBER(2)          |
| 6.2.21       | SUBCYCLE                   | Inventory subcycle number           | NUMBER(2)          |
| 6.2.22       | REGEN_MICR_STATUS_CD       | Regeneration microplot status code  | NUMBER (1)         |

| Key Type   | Column(s) order                   | Tables to link           | Abbreviated notation   |
|------------|-----------------------------------|--------------------------|------------------------|
| Primary    | CN                                | N/A                      | SBPREGEN_PK            |
| Unique     | STATECD,COUNTYCD,PLOT,SUBP, INVYR | N/A                      | SBPREGEN_UK            |
| Foreign    | PLT_CN                            | SUBPLOT_REGEN to PLOT    | SBPREGEN_PLT_FK        |
| Foreign    | SBP_CN                            | SUBPLOT_REGEN to SUBPLOT | SBPREGEN_SBP_FK        |

## 6.2.1 CN

Sequence number. A unique sequence number used to identify a subplot regeneration record.

## 6.2.2 PLT\_CN

Plot sequence number. Foreign key linking the subplot regeneration record to the plot record for this location.

## 6.2.3 SBP\_CN

Subplot sequence number. Foreign key linking the subplot regeneration record to the subplot record for this location.

## 6.2.4 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 6.2.5 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 6.2.6 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 6.2.7 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 6.2.8 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 6.2.9 SUBP

Subplot number. The number assigned to the subplot where subplot regeneration data were collected.

## Codes: SUBP

|   Code | Description        |
|--------|--------------------|
|      1 | Center subplot.    |
|      2 | North subplot.     |
|      3 | Southeast subplot. |
|      4 | Southwest subplot. |

## 6.2.10 REGEN\_SUBP\_STATUS\_CD

Regeneration subplot status code. A code indicating whether or not the subplot was sampled for advanced regeneration. This attribute was retired starting with INVYR = 2015. Data for all years are now populated in REGEN\_MICR\_STATUS\_CD.

Note: For INVYR &lt;2015, the field guide referenced the sampling unit as the subplot even though seedlings are and have only been counted on the microplots.

## Codes: REGEN\_SUBP\_STATUS\_CD

|   Code | Description                                    |
|--------|------------------------------------------------|
|      1 | Subplot sampled for advanced regeneration.     |
|      2 | Subplot not sampled for advanced regeneration. |

## 6.2.11 REGEN\_NONSAMPLE\_REASN\_CD

Regeneration nonsampled reason code. A code indicating the reason a microplot was not sampled for advanced regeneration.

## Codes: REGEN\_NONSAMPLE\_REASN\_CD

|   Code | Description                                      |
|--------|--------------------------------------------------|
|     10 | Other (e.g., snow or water covering vegetation). |

## 6.2.12 SUBPLOT\_SITE\_LIMITATIONS

Subplot site limitations. A code indicating if the site has a limitation on at least 30 percent of the accessible forest area of the subplot that would inhibit or preclude the presence of regenerating seedlings. This attribute was retired starting with INVYR = 2015. Note: For INVYR &lt;2015, the field guide referenced the sampling unit as the subplot even though seedlings are and have only been counted on the microplots.

## Codes: SUBPLOT\_SITE\_LIMITATIONS

|   Code | Description                                        |
|--------|----------------------------------------------------|
|      1 | No site limitation.                                |
|      2 | Rocky surface with little or no soil.              |
|      3 | Water-saturated soils (during the growing season). |

## 6.2.13 MICROPLOT\_SITE\_LIMITATIONS

Microplot site limitations. A code indicating if the site has a limitation on at least 30 percent of the accessible forest area of the microplot that would inhibit or preclude the presence of regenerating seedlings.

## Codes: MICROPLOT\_SITE\_LIMITATIONS

|   Code | Description                                       |
|--------|---------------------------------------------------|
|      1 | No site limitation.                               |
|      2 | Rocky surface with little or no soil.             |
|      3 | Water-saturated soil (during the growing season). |
|      4 | Thick duff layer (in excess of 2 inches thick).   |

## 6.2.14 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 6.2.15 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 6.2.16 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 6.2.17 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 6.2.18 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 6.2.19 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 6.2.20 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 6.2.21 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 6.2.22 REGEN\_MICR\_STATUS\_CD

Regeneration microplot status code. A code indicating whether the microplot was sampled for advanced regeneration. Based on the procedures described in Bechtold and Patterson (2005), POP\_STRATUM.ADJ\_FACTOR\_REGEN\_MICR should be applied when making population estimates. This compensates for any nonsampled microplots or cases where the sampling status is ambiguous (codes 3 through 9).

## Codes: REGEN\_MICR\_STATUS\_CD

|   Code | Description                                                                                                                                                                                      |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Advance regeneration sampled - accessible forest land condition present on the microplot.                                                                                                        |
|      2 | Advance regeneration sampled - no accessible forest land condition present on the microplot.                                                                                                     |
|      3 | Advance regeneration nonsampled - accessible forest land condition present on the microplot, but advance regeneration variables cannot be assessed ( core SEEDLING.TREECOUNT is still measured). |
|      4 | Advance regeneration nonsampled - QA/QC did not measure subplot/microplot for tree/sapling/seedling data (PLOT.QA_STATUS = 2-5 only).                                                            |
|      5 | Nonsampled - subplot not sampled (SUBPLOT.SUBP_STATUS_CD = 3).                                                                                                                                   |
|      9 | Advance regeneration sample status is ambiguous - collected under earlier, more general definition; refer to REGEN_SUBP_STATUS_CD.                                                               |