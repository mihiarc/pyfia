# Section 2.9: Subplot Condition Change Matrix
**Oracle Table Name:** SUBP_COND_CHNG_MTRX
**Extracted Pages:** 155-158 (Chapter pages 2-111 to 2-114)
**Source:** FIA Database Handbook v9.3
**Chapter:** 2 - Database Tables - Location Level

---

## 2.9 Subplot Condition Change Matrix (Oracle table name: SUBP\_COND\_CHNG\_MTRX)

| Subsection   | Column name (attribute)   | Descriptive name                | Oracle data type   |
|--------------|---------------------------|---------------------------------|--------------------|
| 2.9.1        | CN                        | Sequence number                 | VARCHAR2(34)       |
| 2.9.2        | STATECD                   | State code                      | NUMBER(4)          |
| 2.9.3        | SUBP                      | Subplot number                  | NUMBER(1)          |
| 2.9.4        | SUBPTYP                   | Plot type code                  | NUMBER(1)          |
| 2.9.5        | PLT_CN                    | Plot sequence number            | VARCHAR2(34)       |
| 2.9.6        | CONDID                    | Condition class number          | NUMBER(1)          |
| 2.9.7        | PREV_PLT_CN               | Previous plot sequence number   | VARCHAR2(34)       |
| 2.9.8        | PREVCOND                  | Previous condition class number | NUMBER(1)          |
| 2.9.9        | SUBPTYP_PROP_CHNG         | Plot type proportion change     | NUMBER(5,4)        |
| 2.9.10       | CREATED_BY                | Created by                      | VARCHAR2(30)       |
| 2.9.11       | CREATED_DATE              | Created date                    | DATE               |
| 2.9.12       | CREATED_IN_INSTANCE       | Created in instance             | VARCHAR2(6)        |
| 2.9.13       | MODIFIED_BY               | Modified by                     | VARCHAR2(30)       |
| 2.9.14       | MODIFIED_DATE             | Modified date                   | DATE               |
| 2.9.15       | MODIFIED_IN_INSTANCE      | Modified in instance            | VARCHAR2(6)        |

| Key Type   | Column(s) order                                      | Tables to link              | Abbreviated notation   |
|------------|------------------------------------------------------|-----------------------------|------------------------|
| Primary    | CN                                                   | N/A                         | CMX_PK                 |
| Unique     | PLT_CN, PREV_PLT_CN, SUBP, SUBPTYP, CONDID, PREVCOND | N/A                         | CMX_UK                 |
| Foreign    | PREV_PLT_CN                                          | SUBP_COND_CHNG_MTRX to PLOT | CMX_PLT_FK             |
| Foreign    | PLT_CN                                               | SUBP_COND_CHNG_MTRX to PLOT | CMX_PLT_FK2            |

This table contains information about the mix of current and previous conditions that occupy the same area on the subplot. Figure 2-1 provides an illustration of how the information in this table is derived using data from two points in time that are stored in the BOUNDARY and COND tables.

Figure 2-1: Illustration of the SUBP\_COND\_CHNG\_MTRX table function.

<!-- image -->

## 2.9.1 CN

Sequence number. A unique sequence number used to identify a subplot condition change matrix record.

## 2.9.2 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 2.9.3 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values.

## 2.9.4 SUBPTYP

Plot type code. A code indicating whether the record is for a subplot, microplot, or macroplot.

## Codes: SUBPTYP

|   Code | Description   |
|--------|---------------|
|      1 | Subplot.      |
|      2 | Microplot.    |
|      3 | Macroplot.    |

## 2.9.5 PLT\_CN

Plot sequence number. The foreign key linking the subplot condition change matrix record to the plot record for the current inventory.

## 2.9.6 CONDID

Condition class number. The unique identifying number assigned to a condition that exists on the subplot, and is defined in the COND table. See COND.CONDID for details on the attributes which delineate a condition.

## 2.9.7 PREV\_PLT\_CN

Previous plot sequence number. The foreign key linking the subplot condition change matrix record to the plot record from the previous inventory.

Note: If the previous plot was classified as periodic, PREV\_PLT\_CN will not link to the periodic record.

## 2.9.8 PREVCOND

Previous condition class number. Identifies the condition class number from the previous inventory.

## 2.9.9 SUBPTYP\_PROP\_CHNG

Plot type proportion change. The unadjusted proportion of the subplot that is in the same geographic area condition for both the previous and current inventory. For details, see chapter 7.7 in The Forest Inventory and Analysis Database: Population Estimation User Guide.

## 2.9.10 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 2.9.11 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 2.9.12 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 2.9.13 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 2.9.14 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 2.9.15 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.