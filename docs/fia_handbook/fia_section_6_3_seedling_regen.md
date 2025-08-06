# Section 6.3: Seedling Regeneration Table
**Oracle Table Name:** SEEDLING_REGEN
**Extracted Pages:** 425-430 (Chapter pages 6-11 to 6-16)
**Source:** FIA Database Handbook v9.3
**Chapter:** 6 - Database Tables - Northern Research Station (NRS) Tree Regeneration Indicator

---

## 6.3 Seedling Regeneration Table

## (Oracle table name: SEEDLING\_REGEN)

| Subsection   | Column name (attribute)   | Descriptive name                  | Oracle data type   |
|--------------|---------------------------|-----------------------------------|--------------------|
| 6.3.1        | CN                        | Sequence number                   | VARCHAR2(34)       |
| 6.3.2        | PLT_CN                    | Plot sequence number              | VARCHAR2(34)       |
| 6.3.3        | CND_CN                    | Condition sequence number         | VARCHAR2(34)       |
| 6.3.4        | SCD_CN                    | Subplot condition sequence number | VARCHAR2(34)       |
| 6.3.5        | INVYR                     | Inventory year                    | NUMBER(4)          |
| 6.3.6        | STATECD                   | State code                        | NUMBER(4)          |
| 6.3.7        | UNITCD                    | Survey unit code                  | NUMBER(2)          |
| 6.3.8        | COUNTYCD                  | County code                       | NUMBER(3)          |
| 6.3.9        | PLOT                      | Plot number                       | NUMBER(5)          |
| 6.3.10       | SUBP                      | Subplot number                    | NUMBER(1)          |
| 6.3.11       | CONDID                    | Condition class number            | NUMBER(1)          |
| 6.3.12       | SPCD                      | Species code                      | NUMBER             |
| 6.3.13       | SPGRPCD                   | Species group code                | NUMBER(2)          |
| 6.3.14       | SEEDLING_SOURCE_CD        | Seedling source code              | VARCHAR2(2)        |
| 6.3.15       | LENGTH_CLASS_CD           | Length class code                 | NUMBER(1)          |
| 6.3.16       | SEEDLINGCOUNT             | Count of qualifying seedlings     | NUMBER(3)          |
| 6.3.17       | CREATED_BY                | Created by                        | VARCHAR2(30)       |
| 6.3.18       | CREATED_DATE              | Created date                      | DATE               |
| 6.3.19       | CREATED_IN_INSTANCE       | Created in instance               | VARCHAR2(6)        |
| 6.3.20       | MODIFIED_BY               | Modified by                       | VARCHAR2(30)       |
| 6.3.21       | MODIFIED_DATE             | Modified date                     | DATE               |
| 6.3.22       | MODIFIED_IN_INSTANCE      | Modified in instance              | VARCHAR2(6)        |
| 6.3.23       | CYCLE                     | Inventory cycle number            | NUMBER(2)          |
| 6.3.24       | SUBCYCLE                  | Inventory subcycle number         | NUMBER(2)          |
| 6.3.25       | TPA_UNADJ                 | Trees per acre unadjusted         | NUMBER(11,6)       |

| Key Type   | Column(s) order                                                                        | Tables to link              | Abbreviated notation   |
|------------|----------------------------------------------------------------------------------------|-----------------------------|------------------------|
| Primary    | CN                                                                                     | N/A                         | SDLREGEN_PK            |
| Unique     | STATECD, COUNTYCD,PLOT, SUBP, INVYR, SPCD, CONDID, SEEDLING_SOURCE_CD, LENGTH_CLASS_CD | N/A                         | SDLREGEN_UK            |
| Foreign    | CND_CN                                                                                 | SEEDLING_REGEN to COND      | SDLREGEN_CND_FK        |
| Foreign    | PLT_CN                                                                                 | SEEDLING_REGEN to PLOT      | SDLREGEN_PLT_FK        |
| Foreign    | SCD_CN                                                                                 | SEEDLING_REGEN to SUBP_COND | SDLREGEN_SCD_FK        |

## 6.3.1 CN

Sequence number. A unique sequence number used to identify a seedling regeneration record.

## 6.3.2 PLT\_CN

Plot sequence number. Foreign key linking the seedling regeneration record to the plot record for this location.

## 6.3.3 CND\_CN

Condition sequence number. Foreign key linking the seedling regeneration record to the condition record for this location.

## 6.3.4 SCD\_CN

Subplot condition sequence number. Foreign key linking the seedling regeneration record to the subplot condition record for this location.

## 6.3.5 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 6.3.6 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 6.3.7 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 6.3.8 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 6.3.9 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 6.3.10 SUBP

Subplot number. The number assigned to the subplot where seedling regeneration data were collected.

## Codes: SUBP

|   Code | Description        |
|--------|--------------------|
|      1 | Center subplot.    |
|      2 | North subplot.     |
|      3 | Southeast subplot. |
|      4 | Southwest subplot. |

## 6.3.11 CONDID

Condition class number. The unique identifying number assigned to a condition on which the regeneration seedling is located, and is defined in the COND table. See COND.CONDID for details on the attributes which delineate a condition.

## 6.3.12 SPCD

Species code. An FIA tree species code. Refer to appendix F for codes.

## 6.3.13 SPGRPCD

Species group code. A code assigned to each tree species in order to group them for reporting purposes. Codes and their associated names (see REF\_SPECIES\_GROUP.NAME) are shown in appendix E. Refer to appendix F for individual tree species and corresponding species group codes.

## 6.3.14 SEEDLING\_SOURCE\_CD

Seedling source code. A code indicating the source of the seedlings.

## Codes: SEEDLING\_SOURCE\_CD

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Other seedling.                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|      2 | Stump sprout.                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|      3 | Competitive oak, hickory, or butternut seedling (Note: Research indicates that competitive seedlings are highly likely to become dominant or codominant stems in the next stand during forest succession. To be classified as competitive, stems must have a root collar diameter [d.r.c.] >0.75 inches or have a length of at least 3 feet. In situations with relatively high tally, it should only be necessary to check at least 10% of d.r.c.'s.) |

## 6.3.15 LENGTH\_CLASS\_CD

Length class code. A code indicating the length class of the seedlings.

## Codes: LENGTH\_CLASS\_CD

|   Code | Description                       |
|--------|-----------------------------------|
|      1 | 2 inches to less than 6 inches.   |
|      2 | 6 inches to less than 12 inches.  |
|      3 | 1 foot to less than 3 feet.       |
|      4 | 3 feet to less than 5 feet.       |
|      5 | 5 feet to less than 10 feet.      |
|      6 | Greater than or equal to 10 feet. |

## 6.3.16 SEEDLINGCOUNT

Count of qualifying seedlings. A count of the number of established live tally tree seedlings counted on the microplot by subplot, species, condition class number, seedling source, and length class.

## 6.3.17 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 6.3.18 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 6.3.19 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 6.3.20 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 6.3.21 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 6.3.22 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 6.3.23 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 6.3.24 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 6.3.25 TPA\_UNADJ

Trees per acre unadjusted. The number of trees per acre that the sample seedling count theoretically represents for the whole plot. Sum TPA\_UNADJ for all seedling regeneration table records by plot to derive the total number of seedings per acre represented by plot. This attribute must be adjusted using POP\_STRATUM.ADJ\_FACTOR\_REGEN\_MICR to derive population estimates. Examples of estimating population totals are shown in The Forest Inventory and Analysis Database: Population Estimation User Guide.

## Chapter 7: Database Tables - Ground Cover, Pacific Northwest Research Station (PNWRS)

## Chapter Contents:

|   Section | Database table                       | Oracle table name   |
|-----------|--------------------------------------|---------------------|
|       7.1 | Ground Cover Table                   | GRND_CVR            |
|       7.2 | Ground Layer Functional Groups Table | GRND_LYR_FNCTL_GRP  |
|       7.3 | Ground Layer Microquadrat Table      | GRND_LYR_MICROQUAD  |

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