# Section 9.5: Population Evaluation Type Table
**Oracle Table Name:** POP_EVAL_TYP
**Extracted Pages:** 481-482 (Chapter pages 9-15 to 9-16)
**Source:** FIA Database Handbook v9.3
**Chapter:** 9 - Database Tables - Population

---

## 9.5 Population Evaluation Type Table (Oracle table name: POP\_EVAL\_TYP)

| Subsection   | Column name (attribute)   | Descriptive name                 | Oracle data type   |
|--------------|---------------------------|----------------------------------|--------------------|
| 9.5.1        | CN                        | Sequence number                  | VARCHAR2(34)       |
| 9.5.2        | EVAL_GRP_CN               | Evaluation group sequence number | VARCHAR2(34)       |
| 9.5.3        | EVAL_CN                   | Evaluation sequence number       | VARCHAR2(34)       |
| 9.5.4        | EVAL_TYP                  | Evaluation type                  | VARCHAR2(15)       |
| 9.5.5        | CREATED_BY                | Created by                       | VARCHAR2(30)       |
| 9.5.6        | CREATED_DATE              | Created date                     | DATE               |
| 9.5.7        | CREATED_IN_INSTANCE       | Created in instance              | VARCHAR2(6)        |
| 9.5.8        | MODIFIED_BY               | Modified by                      | VARCHAR2(30)       |
| 9.5.9        | MODIFIED_DATE             | Modified date                    | DATE               |
| 9.5.10       | MODIFIED_IN_INSTANCE      | Modified in instance             | VARCHAR2(6)        |

| Key Type   | Column(s) order                | Tables to link                         | Abbreviated notation   |
|------------|--------------------------------|----------------------------------------|------------------------|
| Primary    | CN                             | N/A                                    | PET_PK                 |
| Unique     | EVAL_GRP_CN, EVAL_CN, EVAL_TYP | N/A                                    | PET_UK1                |
| Unique     | EVAL_GRP_CN, EVAL_TYP          | N/A                                    | PET_UK2                |
| Foreign    | EVAL_GRP_CN                    | POP_EVAL_TYP to POP_EVAL_GRP           | PET_PEG_FK             |
| Foreign    | EVAL_CN                        | POP_EVAL_TYP to POP_EVAL               | PET_PEV_FK             |
| Foreign    | EVAL_TYP                       | POP_EVAL_TYP to REF_POP_EVAL_TYP_DESCR | PET_PED_FK             |

## 9.5.1 CN

Sequence number. A unique sequence number used to identify a population evaluation type record.

## 9.5.2 EVAL\_GRP\_CN

Evaluation group sequence number. Foreign key linking the population evaluation type record to the population evaluation group record.

## 9.5.3 EVAL\_CN

Evaluation sequence number. Foreign key linking the population evaluation type record to the population evaluation record.

## 9.5.4 EVAL\_TYP

Evaluation type. An identifier describing the type of evaluation. Evaluation type is needed to generate summary reports for an inventory. For example, a specific evaluation is

associated with the evaluation for tree volume (EXPVOL). See REF\_POP\_EVAL\_TYP\_DESCR.EVAL\_TYP\_CD for codes.

## 9.5.5 CREATED\_BY

Created by. See SURVEY.CREATED\_BYdescription for definition.

## 9.5.6 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 9.5.7 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 9.5.8 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 9.5.9 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 9.5.10 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.