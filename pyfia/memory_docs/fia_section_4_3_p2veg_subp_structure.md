# Section 4.3: Phase 2 Vegetation Subplot Structure Table
**Oracle Table Name:** P2VEG_SUBP_STRUCTURE
**Extracted Pages:** 331-340 (Chapter pages 4-13 to 4-22)
**Source:** FIA Database Handbook v9.3
**Chapter:** 4 - Database Tables - Invasive Species; Understory Vegetation

---

## 4.3 Phase 2 Vegetation Subplot Structure Table (Oracle table name: P2VEG\_SUBP\_STRUCTURE)

| Subsection   | Column name (attribute)   | Descriptive name                                      | Oracle data type   |
|--------------|---------------------------|-------------------------------------------------------|--------------------|
| 4.3.1        | CN                        | Sequence number                                       | VARCHAR2(34)       |
| 4.3.2        | PLT_CN                    | Plot sequence number                                  | VARCHAR2(34)       |
| 4.3.3        | STATECD                   | State code                                            | NUMBER(4)          |
| 4.3.4        | UNITCD                    | Survey unit code                                      | NUMBER(2)          |
| 4.3.5        | COUNTYCD                  | County code                                           | NUMBER(3)          |
| 4.3.6        | PLOT                      | Plot number                                           | NUMBER             |
| 4.3.7        | INVYR                     | Inventory year                                        | NUMBER(4)          |
| 4.3.8        | SUBP                      | Subplot number                                        | NUMBER             |
| 4.3.9        | CONDID                    | Condition class number                                | NUMBER(1)          |
| 4.3.10       | GROWTH_HABIT_CD           | Growth habit code (vegetation structure growth habit) | VARCHAR2(2)        |
| 4.3.11       | LAYER                     | Layer (layer distribution of growth habits)           | NUMBER(1)          |
| 4.3.12       | COVER_PCT                 | Cover percent (canopy cover percent)                  | NUMBER(3)          |
| 4.3.13       | CREATED_BY                | Created by                                            | VARCHAR2(30)       |
| 4.3.14       | CREATED_DATE              | Created date                                          | DATE               |
| 4.3.15       | CREATED_IN_INSTANCE       | Created in instance                                   | VARCHAR2(6)        |
| 4.3.16       | MODIFIED_BY               | Modified by                                           | VARCHAR2(30)       |
| 4.3.17       | MODIFIED_DATE             | Modified date                                         | DATE               |
| 4.3.18       | MODIFIED_IN_INSTANCE      | Modified in instance                                  | VARCHAR2(6)        |
| 4.3.19       | CYCLE                     | Inventory cycle number                                | NUMBER(2)          |
| 4.3.20       | SUBCYCLE                  | Inventory subcycle number                             | NUMBER(2)          |

| Key Type   | Column(s) order                                                                | Tables to link   | Abbreviated notation   |
|------------|--------------------------------------------------------------------------------|------------------|------------------------|
| Primary    | CN                                                                             | N/A              | P2VSS_PK               |
| Unique     | PLT_CN, SUBP, CONDID, GROWTH_HABIT_CD, LAYER                                   | N/A              | P2VSS_UK               |
| Unique     | STATECD, COUNTYCD, PLOT, INVYR, SUBP, CONDID, GROWTH_HABIT_CD, LAYER           | N/A              | P2VSS_UK2              |
| Unique     | STATECD, CYCLE, SUBCYCLE, COUNTYCD, PLOT, SUBP, CONDID, GROWTH_HABIT_CD, LAYER | N/A              | P2VSS_UK3              |

| Key Type   | Column(s) order      | Tables to link                    | Abbreviated notation   |
|------------|----------------------|-----------------------------------|------------------------|
| Foreign    | PLT_CN               | P2VEG_SUBP_STRUCTURE to PLOT      | P2VSS_PLT_FK           |
| Foreign    | PLT_CN, SUBP, CONDID | P2VEG_SUBP_STRUCTURE to SUBP_COND | P2VSS_SCD_FK           |

## 4.3.1 CN

Sequence number. A unique sequence number used to identify a Phase 2 (P2) vegetation subplot structure record.

## 4.3.2 PLT\_CN

Plot sequence number. Foreign key linking the Phase 2 (P2) vegetation subplot structure record to the plot record for this location.

## 4.3.3 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 4.3.4 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. Refer to appendix B for codes.

## 4.3.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 4.3.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 4.3.7 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 4.3.8 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4.

Codes: SUBP

|   Code | Description     |
|--------|-----------------|
|      1 | Center subplot. |
|      2 | North subplot.  |

|   Code | Description        |
|--------|--------------------|
|      3 | Southeast subplot. |
|      4 | Southwest subplot. |

## 4.3.9 CONDID

Condition class number. The unique identifying number assigned to a condition that exists on the subplot, and is defined in the COND table. See COND.CONDID for details on the attributes which delineate a condition.

## 4.3.10 GROWTH\_HABIT\_CD

Growth habit code (vegetation structure growth habit). Vegetation structure growth habit based on species and appearance of plants on the subplot condition. The tree species listed on the FIA Master Tree Species List (refer to Public Box folder available at web address: https://usfs-public.app.box.com/v/FIA-TreeSpeciesList), after taking into account the Island, Mainland, and Phase 3 / Phase 3 (P2/P3) Sub-lists, are recorded as a tally tree species growth habit (TT), even if the species grows as a shrub in some environments. Woody plants not listed on the Master Tree Species List or those on the exclusion list for the area the plot is located in may have a tree growth habit in some environments, and these are recorded as non-tally tree species (NT). If the growth habit is shrub in another environment, that species is recorded as a shrub (SH).

Note: P2VEG\_SUBP\_STRUCTURE.GROWTH\_HABIT\_CD is not to be confused with P2VEG\_SUBPLOT\_SPP.GROWTH\_HABIT\_CD. The codes are similar, but not exactly the same.

## Codes: GROWTH\_HABIT\_CD

| Code   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| TT     | Tally Tree Species: All core tree species and any core optional tree species selected by a particular FIA work unit. Only tree species on the FIA Master Tree Species List (or those listed as a hybrid, variety, or subspecies) are included, after taking into account the Island, Mainland, and P2/P3 Sub-lists. Any plant of that species is included, regardless of its shape and regardless of whether it was tallied on the subplot or microplot during tree tally. Seedlings (any length, no minimum), saplings, and mature plants are included. |
| NT     | Non-tally Tree Species: Tree species not on a particular FIA work unit's tree tally list that are woody plants with a single well-defined, dominant main stem, not supported by other vegetation or structures (not vines), and which are, or are expected to become, greater than 13 feet in height after taking into account the Island, Mainland , and P2/P3 BDSub-lists. Seedlings (any length, no minimum), saplings, and mature plants are included.                                                                                               |
| SH     | Shrubs/Subshrubs/Woody Vines: Woody, multiple-stemmed plants of any size, subshrubs (low-growing shrubs under 1.5 feet tall at maturity), and woody vines. Most cacti are included in this category.                                                                                                                                                                                                                                                                                                                                                     |
| FB     | Forbs: Herbaceous, broad-leaved plants; includes non-woody-vines, ferns (does not include mosses and cryptobiotic crusts).                                                                                                                                                                                                                                                                                                                                                                                                                               |

| Code   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| GR     | Graminoids: Grasses and grass-like plants (includes rushes and sedges).                                                                                                                                                                                                                                                                                                                                                                              |
| DS     | Dead pinyon species shrubs: Dead pinyon species shrubs and dead portions of live pinyon species shrubs for the following field-recorded forest types: Rocky Mountain juniper (COND.FLDTYPCD = 182), juniper woodland (COND.FLDTYPCD = 184), pinyon-juniper woodland (COND.FLDTYPCD = 185), and western juniper (COND.FLDTYPCD = 369). Refer to appendix D for forest type descriptions. Only populated by certain FIA work units (SURVEY.RSCD = 22). |

Codes: GROWTH\_HABIT\_CD (additional codes for PNWRS, SURVEY.RSCD = 26, 27)

| Code   | Description                                                                                                                                                                                                                                                                                        |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| AL     | All vegetation: Populated for PLOT.MANUAL <5.0.                                                                                                                                                                                                                                                    |
| MO     | Moss/bryophytes: Nonvascular, terrestrial green plant, including mosses, hornworts, and liverworts. Only populated for Pacific Islands.                                                                                                                                                            |
| SL     | Bare soil: Mineral material that, when viewed from above, is not over-topped by grass, forbs, shrubs, or seedlings. It is also not covered by duff, litter, cowpies, woody debris, moss or other material. Sand, stones, and bedrock are not considered bare soil. Populated for PLOT.MANUAL <5.0. |
| SS     | Newly sprouted shrub cover: Cover of newly sprouted shrubs after fire. Only populated for PNWRS Fire Effects and Recovery Study (FERS) plots. For more information, contact the PNWRS Analyst Contact (see table 1-1).                                                                             |
| ST     | Seedlings: Small trees <1 inch d.b.h.or d.r.c. Populated for PLOT.MANUAL <5.0.                                                                                                                                                                                                                     |

## 4.3.11 LAYER

Layer (layer distribution of growth habits). A code indicating the vertical layer distribution of growth habits. Canopy cover for growth forms is distributed between layers.

## Codes: LAYER

|   Code | Description                          |
|--------|--------------------------------------|
|      1 | 0 to 2.0 feet.                       |
|      2 | 2.1 to 6.0 feet.                     |
|      3 | 6.1 to 16.0 feet.                    |
|      4 | Greater than 16 feet.                |
|      5 | Aerial: Canopy cover for all layers. |

## 4.3.12 COVER\_PCT

Cover percent (canopy cover percent). The canopy cover percent for each combination of growth habit and layer. Canopy cover is based on a vertically projected polygon described by the outline of the foliage, ignoring any normal spaces occurring between the leaves of plants (Daubenmire 1959), and ignoring overlap among multiple layers of a species. For each species, cover can never exceed 100 percent.

Note: Cover is always recorded as a percent of the full subplot area, even if the condition that was assessed did not cover the full subplot.

## 4.3.13 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 4.3.14 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 4.3.15 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 4.3.16 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 4.3.17 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 4.3.18 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 4.3.19 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 4.3.20 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

Phase 2 Vegetation Subplot Structure Table

Chapter 4 (revision: 04.2024)

Section revision: 04.2024

## Chapter 5: Database Tables - Down Woody Material

## Chapter Contents:

|   Section | Database table                                  | Oracle table name       |
|-----------|-------------------------------------------------|-------------------------|
|       5.1 | Down Woody Material Visit Table                 | DWM_VISIT               |
|       5.2 | Down Woody Material Coarse Woody Debris Table   | DWM_COARSE_WOODY_DEBRIS |
|       5.3 | Down Woody Material Duff, Litter, Fuel Table    | DWM_DUFF_LITTER_FUEL    |
|       5.4 | Down Woody Material Fine Woody Debris Table     | DWM_FINE_WOODY_DEBRIS   |
|       5.5 | Down Woody Material Microplot Fuel Table        | DWM_MICROPLOT_FUEL      |
|       5.6 | Down Woody Material Residual Pile Table         | DWM_RESIDUAL_PILE       |
|       5.7 | Down Woody Material Transect Segment Table      | DWM_TRANSECT_SEGMENT    |
|       5.8 | Condition Down Woody Material Calculation Table | COND_DWM_CALC           |

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

## 5.1 Down Woody Material Visit Table (Oracle table name: DWM\_VISIT)

| Subsection   | Column name (attribute)   | Descriptive name                    | Oracle data type   |
|--------------|---------------------------|-------------------------------------|--------------------|
| 5.1.1        | CN                        | Sequence number                     | VARCHAR2(34)       |
| 5.1.2        | PLT_CN                    | Plot sequence number                | VARCHAR2(34)       |
| 5.1.3        | INVYR                     | Inventory year                      | NUMBER(4)          |
| 5.1.4        | STATECD                   | State code                          | NUMBER(4)          |
| 5.1.5        | COUNTYCD                  | County code                         | NUMBER(3)          |
| 5.1.6        | PLOT                      | Plot number                         | NUMBER(5)          |
| 5.1.7        | MEASDAY                   | Measurement day                     | NUMBER(2)          |
| 5.1.8        | MEASMON                   | Measurement month                   | NUMBER(2)          |
| 5.1.9        | MEASYEAR                  | Measurement year                    | NUMBER(4)          |
| 5.1.10       | QASTATCD                  | Quality assurance status code       | NUMBER(1)          |
| 5.1.11       | CRWTYPCD                  | Crew type code                      | NUMBER(1)          |
| 5.1.12       | SMPKNDCD                  | Sample kind code                    | NUMBER(2)          |
| 5.1.13       | CREATED_BY                | Created by                          | VARCHAR2(30)       |
| 5.1.14       | CREATED_DATE              | Created date                        | DATE               |
| 5.1.15       | CREATED_IN_INSTANCE       | Created in instance                 | VARCHAR2(6)        |
| 5.1.16       | MODIFIED_BY               | Modified by                         | VARCHAR2(30)       |
| 5.1.17       | MODIFIED_DATE             | Modified date                       | DATE               |
| 5.1.18       | MODIFIED_IN_INSTANCE      | Modified in instance                | VARCHAR2(6)        |
| 5.1.19       | CWD_SAMPLE_METHOD         | Coarse woody debris sample method   | VARCHAR2(6)        |
| 5.1.20       | FWD_SAMPLE_METHOD         | Fine woody debris sample method     | VARCHAR2(6)        |
| 5.1.21       | MICR_SAMPLE_METHOD        | Microplot sample method             | VARCHAR2(6)        |
| 5.1.22       | DLF_SAMPLE_METHOD         | Duff, litter, fuelbed sample method | VARCHAR2(6)        |
| 5.1.23       | PILE_SAMPLE_METHOD        | Pile sample method                  | VARCHAR2(6)        |
| 5.1.24       | DWM_SAMPLING_STATUS_CD    | DWM sampling status code            | NUMBER(1)          |
| 5.1.25       | DWM_NBR_SUBP              | DWM number of subplots              | NUMBER(1)          |
| 5.1.26       | DWM_NBR_SUBP_TRANSECT     | DWM number of transects on subplot  | NUMBER(1)          |
| 5.1.27       | DWM_SUBPLIST              | DWM subplot list                    | NUMBER(4)          |
| 5.1.28       | DWM_TRANSECT_LENGTH       | DWM transect length                 | NUMBER(4,1)        |
| 5.1.29       | QA_STATUS                 | Quality assurance status            | NUMBER(1)          |

| Key Type   | Column(s) order                | Tables to link    | Abbreviated notation   |
|------------|--------------------------------|-------------------|------------------------|
| Primary    | CN                             | N/A               | DVT_PK                 |
| Unique     | PLT_CN                         | N/A               | DVT_UK                 |
| Natural    | STATECD, INVYR, COUNTYCD, PLOT | N/A               | DVT_NAT_I              |
| Foreign    | PLT_CN                         | DWM_VISIT to PLOT | DVT_PLT_FK             |

## 5.1.1 CN

Sequence number. A unique sequence number used to identify a down woody material visit record.

## 5.1.2 PLT\_CN

Plot sequence number. Foreign key linking the down woody material visit record to the plot record.

## 5.1.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 5.1.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each state. Refer to appendix B.

## 5.1.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a state. FIPS codes from the Bureau of the Census are used. Refer to appendix B.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 5.1.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, and COUNTYCD, PLOT may be used to uniquely identify a plot.

## 5.1.7 MEASDAY

Measurement day. The day on which the plot was completed.

## 5.1.8 MEASMON

Measurement month. The month in which the plot was completed.

## Codes: MEASMON

|   Code | Description   |
|--------|---------------|
|      1 | January.      |
|      2 | February.     |
|      3 | March.        |