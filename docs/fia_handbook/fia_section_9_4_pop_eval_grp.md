# Section 9.4: Population Evaluation Group Table
**Oracle Table Name:** POP_EVAL_GRP
**Extracted Pages:** 479-480 (Chapter pages 9-13 to 9-14)
**Source:** FIA Database Handbook v9.3
**Chapter:** 9 - Database Tables - Population

---

## 9.4 Population Evaluation Group Table

(Oracle table name: POP\_EVAL\_GRP)

| Subsection   | Column name (attribute)   | Descriptive name             | Oracle data type   |
|--------------|---------------------------|------------------------------|--------------------|
| 9.4.1        | CN                        | Sequence number              | VARCHAR2(34)       |
| 9.4.2        | RSCD                      | Region or station code       | NUMBER(2)          |
| 9.4.3        | EVAL_GRP                  | Evaluation group             | NUMBER(6)          |
| 9.4.4        | EVAL_GRP_DESCR            | Evaluation group description | VARCHAR2(255)      |
| 9.4.5        | STATECD                   | State code                   | NUMBER(4)          |
| 9.4.6        | NOTES                     | Notes                        | VARCHAR2(2000)     |
| 9.4.7        | CREATED_BY                | Created by                   | VARCHAR2(30)       |
| 9.4.8        | CREATED_DATE              | Created date                 | DATE               |
| 9.4.9        | CREATED_IN_INSTANCE       | Created in instance          | VARCHAR2(6)        |
| 9.4.10       | MODIFIED_BY               | Modified by                  | VARCHAR2(30)       |
| 9.4.11       | MODIFIED_DATE             | Modified date                | DATE               |
| 9.4.12       | MODIFIED_IN_INSTANCE      | Modified in instance         | VARCHAR2(6)        |

| Key Type   | Column(s) order   | Tables to link   | Abbreviated notation   |
|------------|-------------------|------------------|------------------------|
| Primary    | CN                | N/A              | PEG_PK                 |
| Unique     | RSCD, EVAL_GRP    | N/A              | PEG_UK                 |
| Index      | EVAL_GRP          | N/A              | PEG_EVAL_I             |

## 9.4.1 CN

Sequence number. A unique sequence number used to identify a population evaluation group record.

## 9.4.2 RSCD

Region or Station code. See SURVEY.RSCD description for definition.

## 9.4.3 EVAL\_GRP

Evaluation group. An identifier for the evaluation group. This identifier includes the "State code" (first 2 digits) and the "year" (last 4 digits) used to identify the evaluation group. The last year of a measurement interval (which is a "range of years" that is typically 5, 7, or 10 years in length) is used for the identifier label.

## 9.4.4 EVAL\_GRP\_DESCR

Evaluation group description. A brief description for the evaluation group. This description includes the State and year used to identify the evaluation group, and the types of estimates that can be computed using the evaluation group (e.g., area, volume, growth, removals, mortality). The last year of a measurement interval (which is a "range of years" that is typically 5, 7, or 10 years in length) is used for the description. For example, 'MINNESOTA 2017: ALL AREA, CURRENT AREA, CURRENT VOLUME, AREA

CHANGE, GROWTH, REMOVALS, MORTALITY, DWM, REGENERATION' is an evaluation group description.

## 9.4.5 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B. For evaluations that do not conform to the boundaries of a single State the value of STATECD should be set to 99.

## 9.4.6 NOTES

Notes. Population evaluation group notes.

## 9.4.7 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 9.4.8 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 9.4.9 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 9.4.10 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 9.4.11 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 9.4.12 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.