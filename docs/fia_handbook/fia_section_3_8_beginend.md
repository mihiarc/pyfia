# Section 3.8: Begin and End Table
**Oracle Table Name:** BEGINEND
**Extracted Pages:** 299-300 (Chapter pages 3-141 to 3-142)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.8 Begin and End Table

## (Oracle table name: BEGINEND)

| Subsection   | Column name (attribute)   | Descriptive name     | Oracle data type   |
|--------------|---------------------------|----------------------|--------------------|
| 3.8.1        | ONEORTWO                  | One or two           | NUMBER             |
| 3.8.2        | CREATED_BY                | Created by           | VARCHAR2(30)       |
| 3.8.3        | CREATED_DATE              | Created date         | DATE               |
| 3.8.4        | CREATED_IN_INSTANCE       | Created in instance  | VARCHAR2(6)        |
| 3.8.5        | MODIFIED_BY               | Modified by          | VARCHAR2(30)       |
| 3.8.6        | MODIFIED_DATE             | Modified date        | DATE               |
| 3.8.7        | MODIFIED_IN_INSTANCE      | Modified in instance | VARCHAR2(6)        |

| Key Type   | Column(s) order   | Tables to link   | Abbreviated notation   |
|------------|-------------------|------------------|------------------------|
| Unique     | ONEORTWO          | N/A              | BE_UK                  |

## 3.8.1 ONEORTWO

One or two. A counter to establish how many times to access a tree record in the TREE\_GRM\_ESTN table. Possible values of ONEORTWO are 1 and 2. This attribute is used when calculating net growth accounting estimates. It should not be used when summarizing net growth attributes stored in the TREE table (i.e., when not summarizing by the accounting temporal basis). The first time the record is accessed, TREE\_GRM\_ESTN.EST\_BEGIN is acquired along with the classification attribute value at time 1. The second time the record is accessed, TREE\_GRM\_ESTN.EST\_END is acquired along with the classification attribute value at time 2. If TREE\_GRM\_ESTN.EST\_END is null, then TREE\_GRM\_ESTN.EST\_MIDPT is substituted. See The Forest Inventory and

Analysis Database: Population Estimation User Guide for examples of use.

## 3.8.2 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.8.3 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.8.4 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.8.5 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.8.6 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.8.7 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

Begin and End Table

Chapter 3 (revision: 12.2024)