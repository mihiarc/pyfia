# Section 4.1: Invasive Subplot Species Table
**Oracle Table Name:** INVASIVE_SUBPLOT_SPP
**Extracted Pages:** 321-324 (Chapter pages 4-3 to 4-6)
**Source:** FIA Database Handbook v9.3
**Chapter:** 4 - Database Tables - Invasive Species; Understory Vegetation

---

## 4.1 Invasive Subplot Species Table

(Oracle table name: INVASIVE\_SUBPLOT\_SPP)

| Subsection   | Column name (attribute)   | Descriptive name              | Oracle data type   |
|--------------|---------------------------|-------------------------------|--------------------|
| 4.1.1        | CN                        | Sequence number               | VARCHAR2(34)       |
| 4.1.2        | PLT_CN                    | Plot sequence number          | VARCHAR2(34)       |
| 4.1.3        | INVYR                     | Inventory year                | NUMBER(4)          |
| 4.1.4        | STATECD                   | State code                    | NUMBER(4)          |
| 4.1.5        | UNITCD                    | Survey unit code              | NUMBER(2)          |
| 4.1.6        | COUNTYCD                  | County code                   | NUMBER(3)          |
| 4.1.7        | PLOT                      | Plot number                   | NUMBER             |
| 4.1.8        | SUBP                      | Subplot number                | NUMBER             |
| 4.1.9        | CONDID                    | Condition class number        | NUMBER(1)          |
| 4.1.10       | VEG_FLDSPCD               | Vegetation field species code | VARCHAR2(10)       |
| 4.1.11       | UNIQUE_SP_NBR             | Unique species number         | NUMBER(2)          |
| 4.1.12       | VEG_SPCD                  | Vegetation species code       | VARCHAR2(10)       |
| 4.1.13       | COVER_PCT                 | Cover percent                 | NUMBER(3)          |
| 4.1.14       | CREATED_BY                | Created by                    | VARCHAR2(30)       |
| 4.1.15       | CREATED_DATE              | Created date                  | DATE               |
| 4.1.16       | CREATED_IN_INSTANCE       | Created in instance           | VARCHAR2(6)        |
| 4.1.17       | MODIFIED_BY               | Modified by                   | VARCHAR2(30)       |
| 4.1.18       | MODIFIED_DATE             | Modified date                 | DATE               |
| 4.1.19       | MODIFIED_IN_INSTANCE      | Modified in instance          | VARCHAR2(6)        |
| 4.1.20       | CYCLE                     | Inventory cycle number        | NUMBER(2)          |
| 4.1.21       | SUBCYCLE                  | Inventory subcycle number     | NUMBER(2)          |

| Key Type   | Column(s) order                                  | Tables to link                    | Abbreviated notation   |
|------------|--------------------------------------------------|-----------------------------------|------------------------|
| Primary    | CN                                               | N/A                               | ISS_PK                 |
| Unique     | PLT_CN, VEG_FLDSPCD, UNIQUE_SP_NBR, SUBP, CONDID | N/A                               | ISS_UK                 |
| Foreign    | PLT_CN                                           | INVASIVE_SUBPLOT_SPP to PLOT      | ISS_PLT_FK             |
| Foreign    | PLT_CN, SUBP, CONDID                             | INVASIVE_SUBPLOT_SPP to SUBP_COND | ISS_SCD_FK             |

FIA identifies species and other taxonomic ranks for plants using symbols (SYMBOL) as assigned by NRCS (Natural Resources Conservation Service) for the PLANTS database (https://plants.usda.gov) on a periodic basis. The most recent NRCS download for the FIA program was September 15, 2017.

## 4.1.1 CN

Sequence number. A unique sequence number used to identify an invasive subplot species record.

## 4.1.2 PLT\_CN

Plot sequence number. Foreign key linking the invasive subplot species record to the plot record for this location.

## 4.1.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 4.1.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 4.1.5 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. Refer to appendix B for codes.

## 4.1.6 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 4.1.7 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 4.1.8 SUBP

Subplot number. The number assigned to the subplot where the invasive species is located. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4.

## Codes: SUBP

|   Code | Description        |
|--------|--------------------|
|      1 | Center subplot.    |
|      2 | North subplot.     |
|      3 | Southeast subplot. |
|      4 | Southwest subplot. |

## 4.1.9 CONDID

Condition class number. The unique identifying number assigned to a condition on which the invasive species is located, and is defined in the COND table. See COND.CONDID for details on the attributes which delineate a condition.

## 4.1.10 VEG\_FLDSPCD

Vegetation field species code. Species code assigned by the field crew, conforming to the NRCS PLANTS database.

## 4.1.11 UNIQUE\_SP\_NBR

Unique species number. A unique number assigned to each invasive species encountered on the plot.

## 4.1.12 VEG\_SPCD

Vegetation species code. A code indicating each sampled vascular invasive plant species found rooted in or overhanging the sampled condition of the subplot at any height. Species codes are the standardized codes in the NRCS PLANTS database.

## 4.1.13 COVER\_PCT

Cover percent. For each species recorded, the canopy cover present on the subplot condition to the nearest 1 percent. Canopy cover is based on a vertically projected polygon described by the outline of the foliage, ignoring any normal spaces occurring between the leaves of plants (Daubenmire 1959), and ignoring overlap among multiple layers of a species. For each species, cover can never exceed 100 percent.

Note: Cover is always recorded as a percent of the full subplot area, even if the condition that was assessed did not cover the full subplot. Canopy cover for species is assigned to the dominant layer.

## 4.1.14 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 4.1.15 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 4.1.16 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 4.1.17 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 4.1.18 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 4.1.19 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 4.1.20 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 4.1.21 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.