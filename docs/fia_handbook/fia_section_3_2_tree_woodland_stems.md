# Section 3.2: Tree Woodland Stems Table
**Oracle Table Name:** TREE_WOODLAND_STEMS
**Extracted Pages:** 233-236 (Chapter pages 3-75 to 3-78)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.2 Tree Woodland Stems Table (Oracle table name: TREE\_WOODLAND\_STEMS)

| Subsection   | Column name (attribute)   | Descriptive name          | Oracle data type   |
|--------------|---------------------------|---------------------------|--------------------|
| 3.2.1        | CN                        | Sequence number           | VARCHAR2(34)       |
| 3.2.2        | PLT_CN                    | Plot sequence number      | VARCHAR2(34)       |
| 3.2.3        | INVYR                     | Inventory year            | NUMBER(4)          |
| 3.2.4        | STATECD                   | State code                | NUMBER(4)          |
| 3.2.5        | UNITCD                    | Survey unit code          | NUMBER(2)          |
| 3.2.6        | COUNTYCD                  | County code               | NUMBER(3)          |
| 3.2.7        | PLOT                      | Plot number               | NUMBER             |
| 3.2.8        | SUBP                      | Subplot number            | NUMBER             |
| 3.2.9        | TREE                      | Woodland tree number      | NUMBER(9)          |
| 3.2.10       | TRE_CN                    | Tree sequence number      | VARCHAR2(34)       |
| 3.2.11       | DIA                       | Woodland stem diameter    | NUMBER(5,2)        |
| 3.2.12       | STATUSCD                  | Woodland stem status code | NUMBER(1)          |
| 3.2.13       | STEM_NBR                  | Woodland stem number      | NUMBER(3)          |
| 3.2.14       | CYCLE                     | Inventory cycle number    | NUMBER(2)          |
| 3.2.15       | SUBCYCLE                  | Inventory subcycle number | NUMBER(2)          |
| 3.2.16       | CREATED_BY                | Created by                | VARCHAR2(30)       |
| 3.2.17       | CREATED_DATE              | Created date              | DATE               |
| 3.2.18       | CREATED_IN_INSTANCE       | Created in instance       | VARCHAR2(6)        |
| 3.2.19       | MODIFIED_BY               | Modified by               | VARCHAR2(30)       |
| 3.2.20       | MODIFIED_DATE             | Modified date             | DATE               |
| 3.2.21       | MODIFIED_IN_INSTANCE      | Modified in instance      | VARCHAR2(6)        |

| Key Type   | Column(s) order              | Tables to link              | Abbreviated notation   |
|------------|------------------------------|-----------------------------|------------------------|
| Primary    | CN                           | N/A                         | WOODS_PK               |
| Unique     | TRE_CN, STEM_NBR             | N/A                         | WOODS_UK               |
| Unique     | PLT_CN, SUBP, TREE, STEM_NBR | N/A                         | WOODS_UK2              |
| Foreign    | PLT_CN                       | TREE_WOODLAND_STEMS to PLOT | WOODS_PLT_FK           |
| Foreign    | TRE_CN                       | TREE_WOODLAND_STEMS to TREE | WOODS_TRE_FK           |

## 3.2.1 CN

Sequence number. A unique sequence number used to identify a tree woodland stems record.

## 3.2.2 PLT\_CN

Plot sequence number. Foreign key linking the tree woodland stems record to the plot record.

## 3.2.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 3.2.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 3.2.5 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 3.2.6 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 3.2.7 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 3.2.8 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 3.2.9 TREE

Woodland tree number. A number that uniquely identifies the woodland tree on the subplot to which the individual qualifying stem belongs.

Woodland species are often multi-stemmed. Individual stems (live or dead) must be at least 1 foot in length and at least 1.0 inch in diameter 1 foot up from the stem diameter measurement point to qualify for measurement.

## 3.2.10 TRE\_CN

Tree sequence number. Foreign key linking the tree woodland stem record to the tree record.

## 3.2.11 DIA

Woodland stem diameter. The current diameter, in inches, at the point of diameter measurement for the individual qualifying stem on the woodland tree. Individual stems (live or dead) must be at least 1 foot in length and at least 1.0 inch in diameter 1 foot up from the stem diameter measurement point to qualify for measurement.

For woodland species, which are often multi-stemmed, diameter is measured at the ground line or at the stem root collar (d.r.c.), whichever is higher. The overall diameter for woodland species tree (DRC) is computed using the following formula:

DRC = SQRT [SUM (stem diameter 2 )]

The computed DRC value for the woodland tree is stored in the TREE.DIA column.

## 3.2.12 STATUSCD

Woodland stem status code. A code indicating whether the individual qualifying stem on the woodland tree is live or dead.

Woodland species are often multi-stemmed. Individual stems (live or dead) must be at least 1 foot in length and at least 1.0 inch in diameter 1 foot up from the stem diameter measurement point to qualify for measurement.

## Codes: STATUSCD

|   Code | Description   |
|--------|---------------|
|      1 | Live stem.    |
|      2 | Dead stem.    |

## 3.2.13 STEM\_NBR

Woodland stem number. A number that uniquely identifies the individual qualifying stem on the woodland tree, which was used to measure the tree diameter.

Woodland species are often multi-stemmed. Individual stems (live or dead) must be at least 1 foot in length and at least 1.0 inch in diameter 1 foot up from the stem diameter measurement point to qualify for measurement.

The total number of live and dead stems used to calculate the diameter (TREE.DIA) on a woodland tree is stored in the tree table (TREE.WDLDSTEM).

## 3.2.14 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 3.2.15 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 3.2.16 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.2.17 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.2.18 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.2.19 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.2.20 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.2.21 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.