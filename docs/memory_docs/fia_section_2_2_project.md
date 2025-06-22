# Section 2.2: Project Table
**Oracle Table Name:** PROJECT
**Extracted Pages:** 51-52 (Chapter pages 2-7 to 2-8)
**Source:** FIA Database Handbook v9.3
**Chapter:** 2 - Database Tables - Location Level

---

## 2.2 Project Table

## (Oracle table name: PROJECT)

| Subsection   | Column name (attribute)   | Descriptive name       | Oracle data type   |
|--------------|---------------------------|------------------------|--------------------|
| 2.2.1        | CN                        | Sequence number        | VARCHAR2(34)       |
| 2.2.2        | RSCD                      | Region or Station code | NUMBER(2)          |
| 2.2.3        | NAME                      | Project name           | VARCHAR2(200)      |
| 2.2.4        | CREATED_BY                | Created by             | VARCHAR2(30)       |
| 2.2.5        | CREATED_DATE              | Created date           | DATE               |
| 2.2.6        | CREATED_IN_INSTANCE       | Created in instance    | VARCHAR2(6)        |
| 2.2.7        | MODIFIED_BY               | Modified by            | VARCHAR2(30)       |
| 2.2.8        | MODIFIED_DATE             | Modified date          | DATE               |
| 2.2.9        | MODIFIED_IN_INSTANCE      | Modified in instance   | VARCHAR2(6)        |

| Key Type   | Column(s) order   | Tables to link   | Abbreviated notation   |
|------------|-------------------|------------------|------------------------|
| Primary    | CN                | N/A              | PRJ_PK                 |
| Unique     | RSCD, NAME        | N/A              | PRJ_UK                 |

## 2.2.1 CN

Sequence number. A unique sequence number used to identify a project record.

## 2.2.2 RSCD

Region or Station code. See SURVEY.RSCD description for definition.

## 2.2.3 NAME

Project name. The name of the project.

## 2.2.4 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 2.2.5 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 2.2.6 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 2.2.7 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 2.2.8 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

Chapter 2 (revision: 12.2024)

## 2.2.9 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.