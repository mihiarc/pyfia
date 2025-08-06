# Section 2.3: County Table
**Oracle Table Name:** COUNTY
**Extracted Pages:** 53-54 (Chapter pages 2-9 to 2-10)
**Source:** FIA Database Handbook v9.3
**Chapter:** 2 - Database Tables - Location Level

---

## 2.3 County Table

## (Oracle table name: COUNTY)

| Subsection   | Column name (attribute)   | Descriptive name     | Oracle data type   |
|--------------|---------------------------|----------------------|--------------------|
| 2.3.1        | STATECD                   | State code           | NUMBER(4)          |
| 2.3.2        | UNITCD                    | Survey unit code     | NUMBER(2)          |
| 2.3.3        | COUNTYCD                  | County code          | NUMBER(3)          |
| 2.3.4        | COUNTYNM                  | County name          | VARCHAR2(50)       |
| 2.3.5        | CN                        | Sequence number      | VARCHAR2(34)       |
| 2.3.6        | CREATED_BY                | Created by           | VARCHAR2(30)       |
| 2.3.7        | CREATED_DATE              | Created date         | DATE               |
| 2.3.8        | CREATED_IN_INSTANCE       | Created in instance  | VARCHAR2(6)        |
| 2.3.9        | MODIFIED_BY               | Modified by          | VARCHAR2(30)       |
| 2.3.10       | MODIFIED_DATE             | Modified date        | DATE               |
| 2.3.11       | MODIFIED_IN_INSTANCE      | Modified in instance | VARCHAR2(6)        |

| Key Type   | Column(s) order           | Tables to link   | Abbreviated notation   |
|------------|---------------------------|------------------|------------------------|
| Primary    | CN                        | N/A              | CTY_PK                 |
| Unique     | STATECD, UNITCD, COUNTYCD | N/A              | CTY_UK                 |

## 2.3.1 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 2.3.2 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 2.3.3 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 2.3.4 COUNTYNM

County name. County name as recorded by the Bureau of the Census for individual counties, or the name given to a similar governmental unit by the FIA program. Only the first 50 characters of the name are used. Refer to appendix B for names.

## 2.3.5 CN

Sequence number. A unique sequence number used to identify a county record.

## 2.3.6 CREATED\_BY

Created by.

See SURVEY.CREATED\_BY description for definition.

## 2.3.7 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 2.3.8 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 2.3.9 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 2.3.10 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 2.3.11 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.