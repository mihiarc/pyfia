# Section 9.3: Population Evaluation Attribute Table
**Oracle Table Name:** POP_EVAL_ATTRIBUTE
**Extracted Pages:** 477-478 (Chapter pages 9-11 to 9-12)
**Source:** FIA Database Handbook v9.3
**Chapter:** 9 - Database Tables - Population

---

## 9.3 Population Evaluation Attribute Table (Oracle table name: POP\_EVAL\_ATTRIBUTE)

| Subsection   | Column name (attribute)   | Descriptive name           | Oracle data type   |
|--------------|---------------------------|----------------------------|--------------------|
| 9.3.1        | CN                        | Sequence number            | VARCHAR2(34)       |
| 9.3.2        | EVAL_CN                   | Evaluation sequence number | VARCHAR2(34)       |
| 9.3.3        | ATTRIBUTE_NBR             | Attribute number           | NUMBER(6)          |
| 9.3.4        | STATECD                   | State code                 | NUMBER(4)          |
| 9.3.5        | CREATED_BY                | Created by                 | VARCHAR2(30)       |
| 9.3.6        | CREATED_DATE              | Created date               | DATE               |
| 9.3.7        | CREATED_IN_INSTANCE       | Created in instance        | VARCHAR2(6)        |
| 9.3.8        | MODIFIED_BY               | Modified by                | VARCHAR2(30)       |
| 9.3.9        | MODIFIED_DATE             | Modified date              | DATE               |
| 9.3.10       | MODIFIED_IN_INSTANCE      | Modified in instance       | VARCHAR2(6)        |

| Key Type   | Column(s) order        | Tables to link                 | Abbreviated notation   |
|------------|------------------------|--------------------------------|------------------------|
| Unique     | EVAL_CN, ATTRIBUTE_NBR | N/A                            | PEA_UK                 |
| Foreign    | EVAL_CN                | POP_EVAL_ATTRIBUTE to POP_EVAL | PEA_PEV_FK             |

## 9.3.1 CN

Sequence number. A unique sequence number used to identify a population evaluation attribute record.

## 9.3.2 EVAL\_CN

Evaluation sequence number. Foreign key linking the population evaluation attribute record to the population evaluation record.

## 9.3.3 ATTRIBUTE\_NBR

Attribute number. Foreign key linking the population evaluation attribute record to the reference population attribute record.

## 9.3.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 9.3.5 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 9.3.6 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 9.3.7 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 9.3.8 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 9.3.9 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 9.3.10 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.