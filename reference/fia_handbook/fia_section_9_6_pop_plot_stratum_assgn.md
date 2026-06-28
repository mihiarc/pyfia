# Section 9.6: Population Plot Stratum Assignment Table
**Oracle Table Name:** POP_PLOT_STRATUM_ASSGN
**Extracted Pages:** 483-486 (Chapter pages 9-17 to 9-20)
**Source:** FIA Database Handbook v9.3
**Chapter:** 9 - Database Tables - Population

---

## 9.6 Population Plot Stratum Assignment Table (Oracle table name: POP\_PLOT\_STRATUM\_ASSGN)

| Subsection   | Column name (attribute)   | Descriptive name        | Oracle data type   |
|--------------|---------------------------|-------------------------|--------------------|
| 9.6.1        | CN                        | Sequence number         | VARCHAR2(34)       |
| 9.6.2        | STRATUM_CN                | Stratum sequence number | VARCHAR2(34)       |
| 9.6.3        | PLT_CN                    | Plot sequence number    | VARCHAR2(34)       |
| 9.6.4        | STATECD                   | State code              | NUMBER(4)          |
| 9.6.5        | INVYR                     | Inventory year          | NUMBER(4)          |
| 9.6.6        | UNITCD                    | Survey unit code        | NUMBER(2)          |
| 9.6.7        | COUNTYCD                  | County code             | NUMBER(3)          |
| 9.6.8        | PLOT                      | Plot number             | NUMBER(5)          |
| 9.6.9        | RSCD                      | Region or station code  | NUMBER(2)          |
| 9.6.10       | EVALID                    | Evaluation identifier   | NUMBER(6)          |
| 9.6.11       | ESTN_UNIT                 | Estimation unit         | NUMBER(6)          |
| 9.6.12       | STRATUMCD                 | Stratum code            | NUMBER(6)          |
| 9.6.13       | CREATED_BY                | Created by              | VARCHAR2(30)       |
| 9.6.14       | CREATED_DATE              | Created date            | DATE               |
| 9.6.15       | CREATED_IN_INSTANCE       | Created in instance     | VARCHAR2(6)        |
| 9.6.16       | MODIFIED_BY               | Modified by             | VARCHAR2(30)       |
| 9.6.17       | MODIFIED_DATE             | Modified date           | DATE               |
| 9.6.18       | MODIFIED_IN_INSTANCE      | Modified in instance    | VARCHAR2(6)        |

| Key Type   | Column(s) order                       | Tables to link                        | Abbreviated notation   |
|------------|---------------------------------------|---------------------------------------|------------------------|
| Primary    | CN                                    | N/A                                   | PPSA_PK                |
| Unique     | RSCD, EVALID, STATECD, COUNTYCD, PLOT | N/A                                   | PPSA_UK                |
| Foreign    | PLT_CN                                | POP_PLOT_STRATUM_ASSGN to PLOT        | PPSA_PLT_FK            |
| Foreign    | STRATUM_CN                            | POP_PLOT_STRATUM_ASSGN to POP_STRATUM | PPSA_PSM_FK            |

## 9.6.1 CN

Sequence number. A unique sequence number used to identify a population plot stratum assignment record.

## 9.6.2 STRATUM\_CN

Stratum sequence number. Foreign key linking the population plot stratum assignment record to the population stratum record.

## 9.6.3 PLT\_CN

Plot sequence number. Foreign key linking the population plot stratum assignment record to the plot record.

## 9.6.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 9.6.5 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 9.6.6 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 9.6.7 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 9.6.8 PLOT

Plot number. An identifier for a plot. Along with INVYR, STATECD, UNITCD, COUNTYCD, PLOT may be used to uniquely identify a plot.

## 9.6.9 RSCD

Region or Station code. See SURVEY.RSCD description for definition.

## 9.6.10 EVALID

Evaluation identifier. See POP\_EVAL.EVALID description for definition.

## 9.6.11 ESTN\_UNIT

Estimation unit. A number assigned to the specific geographic area that is stratified. Estimation units are often determined by a combination of geographical boundaries, sampling intensity and ownership.

## 9.6.12 STRATUMCD

Stratum code. A code uniquely identifying a stratum within an estimation unit.

## 9.6.13 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 9.6.14 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATEdescription for definition.

## 9.6.15 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 9.6.16

MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 9.6.17 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 9.6.18 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

Population Plot Stratum Assignment Table

Chapter 9 (revision: 04.2024)