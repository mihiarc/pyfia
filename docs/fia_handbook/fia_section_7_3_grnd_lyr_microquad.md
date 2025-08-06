# Section 7.3: Ground Layer Microquadrat Table
**Oracle Table Name:** GRND_LYR_MICROQUAD
**Extracted Pages:** 443-450 (Chapter pages 7-15 to 7-22)
**Source:** FIA Database Handbook v9.3
**Chapter:** 7 - Database Tables - Ground Cover, Pacific Northwest Research Station (PNWRS)

---

## 7.3 Ground Layer Microquadrat Table (Oracle table name: GRND\_LYR\_MICROQUAD)

| Subsection   | Column name (attribute)   | Descriptive name                           | Oracle data type   |
|--------------|---------------------------|--------------------------------------------|--------------------|
| 7.3.1        | CN                        | Sequence number                            | VARCHAR2(34)       |
| 7.3.2        | PLT_CN                    | Plot sequence number                       | VARCHAR2(34)       |
| 7.3.3        | STATECD                   | State code                                 | NUMBER(2)          |
| 7.3.4        | CYCLE                     | Inventory cycle number                     | NUMBER(2)          |
| 7.3.5        | SUBCYCLE                  | Inventory subcycle number                  | NUMBER(2)          |
| 7.3.6        | INVYR                     | Inventory year                             | NUMBER(4)          |
| 7.3.7        | INV_VST_NBR               | Inventory visit number                     | NUMBER(2)          |
| 7.3.8        | UNITCD                    | Survey unit code                           | NUMBER(2)          |
| 7.3.9        | COUNTYCD                  | County code                                | NUMBER(3)          |
| 7.3.10       | PLOT                      | Plot number                                | NUMBER(5)          |
| 7.3.11       | SUBP                      | Subplot number                             | NUMBER(1)          |
| 7.3.12       | TRANSECT                  | Transect (Interior Alaska)                 | NUMBER(3)          |
| 7.3.13       | MICROQUAD                 | Microquadrat number (Interior Alaska)      | NUMBER(2)          |
| 7.3.14       | CONDID                    | Condition class number                     | NUMBER(1)          |
| 7.3.15       | MICROQUAD_STATUS_CD       | Microquadrat status code (Interior Alaska) | NUMBER(1)          |
| 7.3.16       | SNOW_COVER_PCT            | Percent snow cover (Interior Alaska)       | NUMBER(3)          |
| 7.3.17       | TRAMPLING                 | Trampling code (Interior Alaska)           | NUMBER(1)          |
| 7.3.18       | MODIFIED_BY               | Modified by                                | VARCHAR2(30)       |
| 7.3.19       | MODIFIED_DATE             | Modified date                              | DATE               |
| 7.3.20       | MODIFIED_IN_INSTANCE      | Modified in instance                       | VARCHAR2(6)        |
| 7.3.21       | CREATED_BY                | Created by                                 | VARCHAR2(30)       |
| 7.3.22       | CREATED_DATE              | Created date                               | DATE               |
| 7.3.23       | CREATED_IN_INSTANCE       | Created in instance                        | VARCHAR2(6)        |

| Key Type   | Column(s) order                                                                  | Tables to link             | Abbreviated notation   |
|------------|----------------------------------------------------------------------------------|----------------------------|------------------------|
| Primary    | CN                                                                               | N/A                        | FGLMP_PK               |
| Unique     | PLT_CN, SUBP, TRANSECT, MICROQUAD                                                | N/A                        | FGLMP_UK               |
| Unique     | STATECD, COUNTYCD, PLOT, INVYR, INV_VST_NBR, SUBP, TRANSECT, MICROQUAD           | N/A                        | FGLMP_UK2              |
| Unique     | STATECD, CYCLE, SUBCYCLE, COUNTYCD, PLOT, SUBP, TRANSECT, MICROQUAD, INV_VST_NBR | N/A                        | FGLMP_UK3              |
| Foreign    | PLT_CN                                                                           | GRND_LYR_MICROQUAD to PLOT | FGLMP_PLT_FK           |

Currently, this table is populated only by the PNWRS FIA work unit (SURVEY.RSCD = 27).

## 7.3.1 CN

Sequence number. A unique sequence number used to identify a ground layer microquadrat record.

## 7.3.2 PLT\_CN

Plot sequence number. Foreign key linking the ground layer microquadrat record to the plot record.

## 7.3.3 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 7.3.4 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 7.3.5 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 7.3.6 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 7.3.7 INV\_VST\_NBR

Inventory visit number. Visit number within a cycle. A plot is usually visited once per cycle, but may be visited again for quality assurance visits or other measurements.

## 7.3.8 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 7.3.9 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 7.3.10 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 7.3.11 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4.

## Codes: SUBP

|   Code | Description        |
|--------|--------------------|
|      1 | Center subplot.    |
|      2 | North subplot.     |
|      3 | Southeast subplot. |
|      4 | Southwest subplot. |

## 7.3.12 TRANSECT

Transect (Interior Alaska). The transect azimuth, in degrees, to identify which transect is being sampled. Azimuth indicates direction from subplot center.

## Codes: TRANSECT

|   Code |   Subplot |
|--------|-----------|
|     90 |         1 |
|    270 |         1 |
|    360 |         2 |
|    180 |         2 |
|    135 |         3 |
|    315 |         3 |
|     45 |         4 |
|    225 |         4 |

## 7.3.13 MICROQUAD

Microquadrat number (Interior Alaska). A code indicating the number of the microquadrat. This code identifies the placement of the microquadrat, in feet (horizontal distance), on the transect.

## Codes: MICROQUAD

|   Code | Description                                               |
|--------|-----------------------------------------------------------|
|      5 | Microquadrat located at the 5-foot mark on the transect.  |
|     10 | Microquadrat located at the 10-foot mark on the transect. |
|     15 | Microquadrat located at the 15-foot mark on the transect. |
|     20 | Microquadrat located at the 20-foot mark on the transect. |

## 7.3.14 CONDID

Condition class number. Unique identifying number assigned to each condition on a plot. A condition is initially defined by condition class status. Differences in reserved status, owner group, forest type, stand-size class, regeneration status, and stand density further define condition for forest land. Mapped nonforest conditions are also assigned numbers. At the time of the plot establishment, the condition class at plot center (the center of subplot 1) is usually designated as condition class 1. Other condition classes are assigned numbers sequentially at the time each condition class is delineated. On a plot, each sampled condition class must have a unique number that can change at remeasurement to reflect new conditions on the plot.

## 7.3.15 MICROQUAD\_STATUS\_CD

Microquadrat status code. A code indicating how the microquadrat was sampled.

## Codes: MICROQUAD\_STATUS\_CD

|   Code | Description                                                                                                                                                                  |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Microquad sampled (  50% of the microquad is in an accessible forest condition), lichens or moss were found.                                                                |
|      2 | Microquad sampled (  50% of the microquad is in an accessible nonforest vegetated or noncensus water condition), lichens or moss were found.                                |
|      3 | Microquad sampled (  50% of the microquad is in an accessible forest condition), lichens and moss were not found or were 100% snow covered.                                 |
|      4 | Microquad sampled (  50% of the microquad is in an accessible nonforest vegetated or noncensus water condition), lichens and moss were not found or were 100% snow covered. |
|      5 | Microquad not sampled, access denied.                                                                                                                                        |
|      6 | Microquad not sampled, hazardous.                                                                                                                                            |
|      7 | Microquad not sampled, census water.                                                                                                                                         |
|      8 | Microquad not sampled, other reason - enter in microquad notes.                                                                                                              |

## 7.3.16 SNOW\_COVER\_PCT

Percent snow cover (Interior Alaska). The percent of the microquadrat area covered in snow.

## 7.3.17 TRAMPLING

Trampling code (Interior Alaska). A code indicating the level of damage to plants or disturbance of the ground layer by humans, livestock, or wildlife. This code is assigned to the microquadrat at the start of the ground layer measurements.

## Codes: TRAMPLING

|   Code | Description                                                                 |
|--------|-----------------------------------------------------------------------------|
|      1 | Low: 0-10% of microquad trampled; pristine to relatively undisturbed.       |
|      2 | Moderate: 10-50% of microquad trampled; trampling by animals or field crew. |
|      3 | Heavy: >50% of microquad trampled; hiking trail or heavily grazed.          |

## 7.3.18 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 7.3.19 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 7.3.20 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 7.3.21 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 7.3.22 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 7.3.23 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

Ground Layer Microquadrat Table

Chapter 7 (revision: 01.2024)

Section revision: 04.2024

## Chapter 8: Database Tables - Soils, Pacific Northwest Research Station (PNWRS)

## Chapter Contents:

|   Section | Database table                     | Oracle table name      |
|-----------|------------------------------------|------------------------|
|       8.1 | Subplot Soil Sample Location Table | SUBP_SOIL_SAMPLE_LOC   |
|       8.2 | Subplot Soil Sample Layer Table    | SUBP_SOIL_SAMPLE_LAYER |

The soils tables in this chapter are currently only populated by the PNWRS FIA work unit (SURVEY.rscd = 26, 27).

Refer to the "The Forest Inventory and Analysis Database: Database Description and User Guide for Phase 3 (version 6.0.1)" for documentation pertaining to other soils tables populated by FIA (available at web address:

https://research.fs.usda.gov/products/dataandtools/tools/fia-datamart).

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