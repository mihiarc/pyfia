# Section 9.1: Population Estimation Unit Table
**Oracle Table Name:** POP_ESTN_UNIT
**Extracted Pages:** 469-472 (Chapter pages 9-3 to 9-6)
**Source:** FIA Database Handbook v9.3
**Chapter:** 9 - Database Tables - Population

---

## 9.1 Population Estimation Unit Table (Oracle table name: POP\_ESTN\_UNIT)

| Subsection   | Column name (attribute)   | Descriptive name                             | Oracle data type   |
|--------------|---------------------------|----------------------------------------------|--------------------|
| 9.1.1        | CN                        | Sequence number                              | VARCHAR2(34)       |
| 9.1.2        | EVAL_CN                   | Evaluation sequence number                   | VARCHAR2(34)       |
| 9.1.3        | RSCD                      | Region or station code                       | NUMBER(2)          |
| 9.1.4        | EVALID                    | Evaluation identifier                        | NUMBER(6)          |
| 9.1.5        | ESTN_UNIT                 | Estimation unit                              | NUMBER(6)          |
| 9.1.6        | ESTN_UNIT_DESCR           | Estimation unit description                  | VARCHAR2(255)      |
| 9.1.7        | STATECD                   | State code                                   | NUMBER(4)          |
| 9.1.8        | AREALAND_EU               | Land area within the estimation unit         | NUMBER(12,2)       |
| 9.1.9        | AREATOT_EU                | Total area within the estimation unit        | NUMBER(12,2)       |
| 9.1.10       | AREA_USED                 | Area used to calculate all expansion factors | NUMBER(12,2)       |
| 9.1.11       | AREA_SOURCE               | Area source                                  | VARCHAR2(50)       |
| 9.1.12       | P1PNTCNT_EU               | Phase 1 point count for the estimation unit  | NUMBER(12)         |
| 9.1.13       | P1SOURCE                  | Phase 1 source                               | VARCHAR2(50)       |
| 9.1.14       | CREATED_BY                | Created by                                   | VARCHAR2(30)       |
| 9.1.15       | CREATED_DATE              | Created date                                 | DATE               |
| 9.1.16       | CREATED_IN_INSTANCE       | Created in instance                          | VARCHAR2(6)        |
| 9.1.17       | MODIFIED_BY               | Modified by                                  | VARCHAR2(30)       |
| 9.1.18       | MODIFIED_DATE             | Modified date                                | DATE               |
| 9.1.19       | MODIFIED_IN_INSTANCE      | Modified in instance                         | VARCHAR2(6)        |

| Key Type   | Column(s) order         | Tables to link            | Abbreviated notation   |
|------------|-------------------------|---------------------------|------------------------|
| Primary    | CN                      | N/A                       | PEU_PK                 |
| Unique     | RSCD, EVALID, ESTN_UNIT | N/A                       | PEU_UK                 |
| Foreign    | EVAL_CN                 | POP_ESTN_UNIT to POP_EVAL | PEU_PEV_FK             |

## 9.1.1 CN

Sequence number. A unique sequence number used to identify a population estimation unit record.

## 9.1.2 EVAL\_CN

Evaluation sequence number. Foreign key linking the estimation unit record to the evaluation record.

## 9.1.3 RSCD

Region or Station code. See SURVEY.RSCD description for definition.

## 9.1.4 EVALID

Evaluation identifier. See POP\_EVAL.EVALID description for definition.

## 9.1.5 ESTN\_UNIT

Estimation unit. A number assigned to the specific geographic area that is stratified. Estimation units are often determined by a combination of geographical boundaries, sampling intensity and ownership.

## 9.1.6 ESTN\_UNIT\_DESCR

Estimation unit description. A description of the estimation unit (e.g., name of the county).

## 9.1.7 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B. For evaluations that do not conform to the boundaries of a single State the value of STATECD should be set to 99.

## 9.1.8 AREALAND\_EU

Land area within the estimation unit. The area of land, in acres, enclosed by the estimation unit. Census water is excluded.

## 9.1.9 AREATOT\_EU

Total area within the estimation unit. The area of land and census water, in acres, enclosed by the estimation unit.

## 9.1.10 AREA\_USED

Area used to calculate all expansion factors. This value is equivalent to AREATOT\_EU when estimates are for all area, including census water; and this value is equivalent to AREALAND\_EU when estimates are for land area only.

## 9.1.11 AREA\_SOURCE

Area source. A descriptor for the source of the area numbers. Usually, the area source is either the U.S. Census Bureau or area estimates based on pixel counts. Example descriptors are 'US CENSUS 2000' and 'PIXEL COUNT.'

## 9.1.12 P1PNTCNT\_EU

Phase 1 point count for the estimation unit. For remotely sensed data, this will be the total number of pixels in the estimation unit.

## 9.1.13 P1SOURCE

Phase 1 source. A descriptor for the Phase 1 data source used for this stratification. Example descriptors are 'NLCD 2001 CANOPY' and 'IKONOS.'

## 9.1.14 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 9.1.15 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 9.1.16 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 9.1.17 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 9.1.18 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 9.1.19 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

Population Estimation Unit Table

Chapter 9 (revision: 04.2024)