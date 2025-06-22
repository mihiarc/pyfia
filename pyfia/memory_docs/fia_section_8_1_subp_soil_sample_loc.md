# Section 8.1: Subplot Soil Sample Location Table
**Oracle Table Name:** SUBP_SOIL_SAMPLE_LOC
**Extracted Pages:** 451-456 (Chapter pages 8-3 to 8-8)
**Source:** FIA Database Handbook v9.3
**Chapter:** 8 - Database Tables - Soils, Pacific Northwest Research Station (PNWRS)

---

## 8.1 Subplot Soil Sample Location Table (Oracle table name: SUBP\_SOIL\_SAMPLE\_LOC)

| Subsection   | Column name (attribute)   | Descriptive name                        | Oracle data type   |
|--------------|---------------------------|-----------------------------------------|--------------------|
| 8.1.1        | CN                        | Sequence number                         | VARCHAR2(34)       |
| 8.1.2        | PLT_CN                    | Plot sequence number                    | VARCHAR2(34)       |
| 8.1.3        | STATECD                   | State code                              | NUMBER(2)          |
| 8.1.4        | COUNTYCD                  | County code                             | NUMBER(3)          |
| 8.1.5        | PLOT                      | Plot number                             | NUMBER(5)          |
| 8.1.6        | INV_VST_NBR               | Inventory visit number                  | NUMBER(2)          |
| 8.1.7        | INVYR                     | Inventory year                          | NUMBER(4)          |
| 8.1.8        | CYCLE                     | Inventory cycle number                  | NUMBER(2)          |
| 8.1.9        | SUBCYCLE                  | Inventory subcycle number               | NUMBER(2)          |
| 8.1.10       | UNITCD                    | Survey unit code                        | NUMBER(2)          |
| 8.1.11       | SUBP                      | Subplot number                          | NUMBER(1)          |
| 8.1.12       | VSTNBR                    | Visit number                            | NUMBER(1)          |
| 8.1.13       | CONDID                    | Condition class number                  | NUMBER(1)          |
| 8.1.14       | SOILS_SAMPLE_METHOD_CD    | Soils sample method code                | NUMBER(1)          |
| 8.1.15       | SOILS_SAMPLE_STATUS_CD    | Soils sample status code                | NUMBER(2)          |
| 8.1.16       | CORE_SIZE                 | Soil core size                          | NUMBER(4,3)        |
| 8.1.17       | CORE_LENGTH               | Soil core length                        | NUMBER(3,1)        |
| 8.1.18       | CORE_BOTTOM_CD            | Core bottom code                        | NUMBER(1)          |
| 8.1.19       | HOLE_DEPTH                | Hole depth                              | NUMBER(3,1)        |
| 8.1.20       | RESTRICTION_DEPTH_CD_1    | Restriction depth code 1                | NUMBER(1)          |
| 8.1.21       | RESTRICTION_DEPTH_CD_2    | Restriction depth code 2                | NUMBER(1)          |
| 8.1.22       | RESTRICTION_DEPTH_CD_3    | Restriction depth code 3                | NUMBER(1)          |
| 8.1.23       | RESTRICTION_DEPTH_CD_4    | Restriction depth code 4                | NUMBER(1)          |
| 8.1.24       | RESTRICTION_DEPTH_1       | Restriction depth 1                     | NUMBER(3,1)        |
| 8.1.25       | RESTRICTION_DEPTH_2       | Restriction depth 2                     | NUMBER(3,1)        |
| 8.1.26       | RESTRICTION_DEPTH_3       | Restriction depth 3                     | NUMBER(3,1)        |
| 8.1.27       | RESTRICTION_DEPTH_4       | Restriction depth 4                     | NUMBER(3,1)        |
| 8.1.28       | C_TOT_3IN_MG_AC           | Total carbon per acre, 3 inches depth   | NUMBER(10,6)       |
| 8.1.29       | N_TOT_3IN_MG_AC           | Total nitrogen per acre, 3 inches depth | NUMBER(10,6)       |
| 8.1.30       | USED_IN_ESTIMATION_CD     | Used in estimation code                 | NUMBER(1)          |
| 8.1.31       | CREATED_BY                | Created by                              | VARCHAR2(30)       |
| 8.1.32       | CREATED_DATE              | Created date                            | DATE               |

| Subsection   | Column name (attribute)   | Descriptive name     | Oracle data type   |
|--------------|---------------------------|----------------------|--------------------|
| 8.1.33       | CREATED_IN_INSTANCE       | Created in instance  | VARCHAR2(6)        |
| 8.1.34       | MODIFIED_BY               | Modified by          | VARCHAR2(30)       |
| 8.1.35       | MODIFIED_DATE             | Modified date        | DATE               |
| 8.1.36       | MODIFIED_IN_INSTANCE      | Modified in instance | VARCHAR2(6)        |

| Key Type   | Column(s) order   | Tables to link               | Abbreviated notation   |
|------------|-------------------|------------------------------|------------------------|
| Primary    | CN                | N/A                          | SSSL_PK                |
| Foreign    | PLT_CN            | SUBP_SOIL_SAMPLE_LOC to PLOT | SSSL_FK                |

## 8.1.1 CN

Sequence number. A unique sequence number used to identify a subplot soil sample location record.

## 8.1.2 PLT\_CN

Plot sequence number. Foreign key linking the subplot soil sample location record to the plot record.

## 8.1.3 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 8.1.4 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 8.1.5 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 8.1.6 INV\_VST\_NBR

Inventory visit number. Visit number within a cycle. A plot is usually visited once per cycle, but may be visited again for quality assurance visits or other measurements.

## 8.1.7 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 8.1.8 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 8.1.9 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 8.1.10 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 8.1.11 SUBP

Subplot number. The number assigned to the subplot adjacent to the soil sampling site. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Soils protocols use only subplots 2-4.

## Codes: SUBP

|   Code | Description        |
|--------|--------------------|
|      2 | North subplot.     |
|      3 | Southeast subplot. |
|      4 | Southwest subplot. |

## 8.1.12 VSTNBR

Visit number. The number of the soil sampling location at which the soil sample was collected. Values are 1-9.

Figure 8-1: Location of soil sampling site.

<!-- image -->

## 8.1.13 CONDID

Condition class number. The unique identifying number assigned to the condition where the soil sample was collected. If the condition class of the soil sample site differs from any

condition class mapped on the four subplots, then CONDID = 0. If no soil sample was collected, this attribute is blank (null).

## 8.1.14 SOILS\_SAMPLE\_METHOD\_CD

Soils sample method code. A code indicating which soils protocol was used.

Codes: SOILS\_SAMPLE\_METHOD\_CD

|   Code | Description                                |
|--------|--------------------------------------------|
|      1 | P3 soils sample method.                    |
|      2 | Interior Alaska pilot soils sample method. |
|      3 | Interior Alaska soils sample method.       |
|      4 | Hawaii soils sample method.                |

## 8.1.15 SOILS\_SAMPLE\_STATUS\_CD

Soils sample status code. A code indicating whether some or all of the soils protocol was applied (sampled) at the soil sampling location, and if not, the reason the location was not sampled.

Codes: SOILS\_SAMPLE\_STATUS\_CD (When SOILS\_SAMPLE\_METHOD\_CD = 1)

|   Code | Description                                                              |
|--------|--------------------------------------------------------------------------|
|     01 | Sampled: forest that has been identified as a condition on the plot.     |
|     02 | Not sampled: non-forest.                                                 |
|     03 | Not sampled forest condition: too rocky to sample.                       |
|     04 | Not sampled forest condition: water or boggy.                            |
|     05 | Not sampled forest condition: access denied.                             |
|     06 | Not sampled forest condition: too dangerous to sample.                   |
|     07 | Not sampled forest condition: obstruction in sampling area.              |
|     08 | Not sampled forest condition: broken or lost equipment.                  |
|     09 | Not sampled forest condition: other.                                     |
|     11 | Sampled: forest that has NOT been identified as a condition on the plot. |

Codes:  SOILS\_SAMPLE\_STATUS\_CD (When SOILS\_SAMPLE\_METHOD\_CD = 2, 3, 4)

|   Code | Description                  |
|--------|------------------------------|
|      1 | Sampled.                     |
|      2 | Not sampled: standing water. |
|      3 | Not sampled: access denied.  |
|      4 | Not sampled: hazardous.      |
|      5 | Not sampled: other.          |

## 8.1.16 CORE\_SIZE

Soil core size. The inner diameter (inches) of the sample soil core collected.

## 8.1.17 CORE\_LENGTH

Soil core length. The length of the soil core to the nearest 0.1 inch.

## 8.1.18 CORE\_BOTTOM\_CD

Soil core bottom code. A code indicating the substrate at the bottom of the core.

## Codes: CORE\_BOTTOM\_CD

|   Code | Description                 |
|--------|-----------------------------|
|      1 | Identifiable plant parts.   |
|      2 | Unidentifiable plant parts. |
|      5 | Mineral soil.               |
|      6 | Unknown material.           |

## 8.1.19 HOLE\_DEPTH

Hole depth. The depth of the cored hole to the nearest 0.1 inch.

## 8.1.20 RESTRICTION\_DEPTH\_CD\_1

Restriction depth code 1. A code indicating the estimated substrate encountered when probing at location 1.

## Codes: RESTRICTION\_DEPTH\_CD\_1

|   Code | Description                       |
|--------|-----------------------------------|
|      1 | Frozen soil.                      |
|      2 | Gravel.                           |
|      3 | Substrate not reached >40 inches. |
|      4 | Substrate unknown.                |
|      5 | Bedrock.                          |
|      6 | Sand.                             |

## 8.1.21 RESTRICTION\_DEPTH\_CD\_2

Restriction depth code 2. A code indicating the estimated substrate encountered when probing at location 2. See RESTRICTION\_DEPTH\_CD\_1 for codes and definitions.

## 8.1.22 RESTRICTION\_DEPTH\_CD\_3

Restriction depth code 3. A code indicating the estimated substrate encountered when probing at location 3. See RESTRICTION\_DEPTH\_CD\_1 for codes and definitions.

## 8.1.23 RESTRICTION\_DEPTH\_CD\_4

Restriction depth code 4. A code indicating the estimated substrate encountered when probing at location 4. See RESTRICTION\_DEPTH\_CD\_1 for codes and definitions.

## 8.1.24 RESTRICTION\_DEPTH\_1

Restriction depth 1. The maximum depth, to the nearest 0.5 inch, encountered when probing for a restriction at location 1.

## 8.1.25 RESTRICTION\_DEPTH\_2

Restriction depth 2. The maximum depth, to the nearest 0.5 inch, encountered when probing for a restriction at location 2.

## 8.1.26 RESTRICTION\_DEPTH\_3

Restriction depth 3. The maximum depth, to the nearest 0.5 inch, encountered when probing for a restriction at location 3.

## 8.1.27 RESTRICTION\_DEPTH\_4

Restriction depth 4. The maximum depth, to the nearest 0.5 inch, encountered when probing for a restriction at location 4.

## 8.1.28 C\_TOT\_3IN\_MG\_AC

Total carbon per acre, 3 inches depth. The total carbon content (Mg) per acre to a standard depth of 3 inches.

## 8.1.29 N\_TOT\_3IN\_MG\_AC

Total nitrogen per acre, 3 inches depth. The total nitrogen content (Mg) per acre to a standard depth of 3 inches.

## 8.1.30 USED\_IN\_ESTIMATION\_CD

Used in estimation code. A code indicating whether or not the soil core is included in population estimates.

## Codes: USED IN ESTIMATION\_CD

|   Code | Description                                            |
|--------|--------------------------------------------------------|
|      0 | The soil core is not included in population estimates. |
|      1 | The soil core is included in population estimates.     |

## 8.1.31 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 8.1.32 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 8.1.33 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 8.1.34 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 8.1.35 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 8.1.36 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.