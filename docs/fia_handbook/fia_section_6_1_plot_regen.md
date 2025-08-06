# Section 6.1: Plot Regeneration Table
**Oracle Table Name:** PLOT_REGEN
**Extracted Pages:** 417-420 (Chapter pages 6-3 to 6-6)
**Source:** FIA Database Handbook v9.3
**Chapter:** 6 - Database Tables - Northern Research Station (NRS) Tree Regeneration Indicator

---

## 6.1 Plot Regeneration Table

## (Oracle table name: PLOT\_REGEN)

| Subsection   | Column name (attribute)   | Descriptive name          | Oracle data type   |
|--------------|---------------------------|---------------------------|--------------------|
| 6.1.1        | CN                        | Sequence number           | VARCHAR2(34)       |
| 6.1.2        | PLT_CN                    | Plot sequence number      | VARCHAR2(34)       |
| 6.1.3        | INVYR                     | Inventory year            | NUMBER(4)          |
| 6.1.4        | STATECD                   | State code                | NUMBER(4)          |
| 6.1.5        | UNITCD                    | Survey unit code          | NUMBER(2)          |
| 6.1.6        | COUNTYCD                  | County code               | NUMBER(3)          |
| 6.1.7        | PLOT                      | Plot number               | NUMBER(5)          |
| 6.1.8        | BROWSE_IMPACT             | Browse impact             | NUMBER(1)          |
| 6.1.9        | CREATED_BY                | Created by                | VARCHAR2(30)       |
| 6.1.10       | CREATED_DATE              | Created date              | DATE               |
| 6.1.11       | CREATED_IN_INSTANCE       | Created in instance       | VARCHAR2(6)        |
| 6.1.12       | MODIFIED_BY               | Modified by               | VARCHAR2(30)       |
| 6.1.13       | MODIFIED_DATE             | Modified date             | DATE               |
| 6.1.14       | MODIFIED_IN_INSTANCE      | Modified in instance      | VARCHAR2(6)        |
| 6.1.15       | CYCLE                     | Inventory cycle number    | NUMBER(2)          |
| 6.1.16       | SUBCYCLE                  | Inventory subcycle number | NUMBER(2)          |

| Key Type   | Column(s) order                          | Tables to link   | Abbreviated notation   |
|------------|------------------------------------------|------------------|------------------------|
| Primary    | CN                                       | N/A              | PLTREGEN_PK            |
| Unique     | STATECD,COUNTYCD,PLOT,INVYR              | N/A              | PLTREGEN_UK1           |
| Unique     | STATECD, COUNTYCD, PLOT, CYCLE, SUBCYCLE | N/A              | PLTREGEN_UK2           |
| Foreign    | PLT_CN                                   | PLTREGEN to PLOT | PLTREGEN_PLT_FK        |

## 6.1.1 CN

Sequence number. A unique sequence number used to identify a plot regeneration record.

## 6.1.2 PLT\_CN

Plot sequence number. Foreign key linking the plot regeneration record to the plot record for this location.

## 6.1.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 6.1.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 6.1.5 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 6.1.6 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 6.1.7 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 6.1.8 BROWSE\_IMPACT

Browse impact. A code designating the amount of animal browse pressure exerted on the regeneration of the accessible forest area within the four subplots. Pressure may be due to browse by deer, elk, feral hogs, livestock, moose, and other wildlife.

## Codes: BROWSE\_IMPACT

|   Code | Description                                                                         |
|--------|-------------------------------------------------------------------------------------|
|      1 | Very low - plot is inside a well-maintained exclosure.                              |
|      2 | Low - no browsing observed, vigorous seedling(s) present (no exclosure present).    |
|      3 | Medium - browsing evidence observed but not common, seedlings common.               |
|      4 | High - browsing evidence common OR seedlings are rare.                              |
|      5 | Very high - browsing evidence omnipresent OR forest floor bare, severe browse line. |

## 6.1.9 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 6.1.10 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 6.1.11 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 6.1.12 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 6.1.13

MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 6.1.14 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 6.1.15 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 6.1.16 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

Plot Regeneration Table

Chapter 6 (revision: 01.2024)