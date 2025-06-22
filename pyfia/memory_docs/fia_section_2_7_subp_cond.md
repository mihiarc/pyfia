# Section 2.7: Subplot Condition Table
**Oracle Table Name:** SUBP_COND
**Extracted Pages:** 149-152 (Chapter pages 2-105 to 2-108)
**Source:** FIA Database Handbook v9.3
**Chapter:** 2 - Database Tables - Location Level

---

## 2.7 Subplot Condition Table

## (Oracle table name: SUBP\_COND)

| Subsection   | Column name (attribute)   | Descriptive name                             | Oracle data type   |
|--------------|---------------------------|----------------------------------------------|--------------------|
| 2.7.1        | CN                        | Sequence number                              | VARCHAR2(34)       |
| 2.7.2        | PLT_CN                    | Plot sequence number                         | VARCHAR2(34)       |
| 2.7.3        | INVYR                     | Inventory year                               | NUMBER(4)          |
| 2.7.4        | STATECD                   | State code                                   | NUMBER(4)          |
| 2.7.5        | UNITCD                    | Survey unit code                             | NUMBER(2)          |
| 2.7.6        | COUNTYCD                  | County code                                  | NUMBER(3)          |
| 2.7.7        | PLOT                      | Plot number                                  | NUMBER(5)          |
| 2.7.8        | SUBP                      | Subplot number                               | NUMBER(3)          |
| 2.7.9        | CONDID                    | Condition class number                       | NUMBER(1)          |
| 2.7.10       | CREATED_BY                | Created by                                   | VARCHAR2(30)       |
| 2.7.11       | CREATED_DATE              | Created date                                 | DATE               |
| 2.7.12       | CREATED_IN_INSTANCE       | Created in instance                          | VARCHAR2(6)        |
| 2.7.13       | MODIFIED_BY               | Modified by                                  | VARCHAR2(30)       |
| 2.7.14       | MODIFIED_DATE             | Modified date                                | DATE               |
| 2.7.15       | MODIFIED_IN_INSTANCE      | Modified in instance                         | VARCHAR2(6)        |
| 2.7.16       | MICRCOND_PROP             | Microplot-condition proportion               | NUMBER             |
| 2.7.17       | SUBPCOND_PROP             | Subplot-condition proportion                 | NUMBER             |
| 2.7.18       | MACRCOND_PROP             | Macroplot-condition proportion               | NUMBER             |
| 2.7.19       | NONFR_INCL_PCT_SUBP       | Nonforest inclusions percentage of subplot   | NUMBER(3)          |
| 2.7.20       | NONFR_INCL_PCT_MACRO      | Nonforest inclusions percentage of macroplot | NUMBER(3)          |
| 2.7.21       | CYCLE                     | Inventory cycle number                       | NUMBER(2)          |
| 2.7.22       | SUBCYCLE                  | Inventory subcycle number                    | NUMBER(2)          |

| Key Type   | Column(s) order                                      | Tables to link       | Abbreviated notation   |
|------------|------------------------------------------------------|----------------------|------------------------|
| Primary    | CN                                                   | N/A                  | SCD_PK                 |
| Unique     | PLT_CN, SUBP, CONDID                                 | N/A                  | SCD_UK                 |
| Natural    | STATECD, INVYR, UNITCD, COUNTYCD, PLOT, SUBP, CONDID | N/A                  | SCD_NAT_I              |
| Foreign    | PLT_CN, CONDID                                       | SUBP_COND to COND    | SCD_CND_FK             |
| Foreign    | PLT_CN                                               | SUBP_COND to PLOT    | SCD_PLT_FK             |
| Foreign    | PLT_CN, SUBP                                         | SUBP_COND to SUBPLOT | SCD_SBP_FK             |

Note:

The SUBP\_COND record may not exist for some periodic inventory data.

## 2.7.1 CN

Sequence number. A unique sequence number used to identify a subplot condition record.

## 2.7.2 PLT\_CN

Plot sequence number. Foreign key linking the subplot condition record to the plot record.

## 2.7.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 2.7.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 2.7.5 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 2.7.6 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 2.7.7 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combination of variables, PLOT may be used to uniquely identify a plot.

## 2.7.8 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 2.7.9 CONDID

Condition class number. The unique identifying number assigned to a condition that exists on the subplot, and is defined in the COND table. See COND.CONDID for details on the attributes which delineate a condition.

## 2.7.10 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 2.7.11 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 2.7.12 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 2.7.13 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 2.7.14 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 2.7.15 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 2.7.16 MICRCOND\_PROP

Microplot-condition proportion. Proportion of this microplot in this condition.

## 2.7.17 SUBPCOND\_PROP

Subplot-condition proportion. Proportion of this subplot in this condition.

## 2.7.18 MACRCOND\_PROP

Macroplot-condition proportion. Proportion of this macroplot in this condition.

## 2.7.19 NONFR\_INCL\_PCT\_SUBP

Nonforest inclusions percentage of subplot. Nonforest area estimate, expressed as a percentage, of the 24.0-foot radius subplot present within a mapped, accessible forest land condition class in Oregon, Washington, and California. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 2.7.20 NONFR\_INCL\_PCT\_MACRO

Nonforest inclusions percentage of macroplot. Nonforest area estimate, expressed as a percentage, of the 58.9-foot radius macroplot present within a mapped, accessible forest land condition class in Oregon, Washington, and California. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 2.7.21 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 2.7.22 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

Subplot Condition Table

Chapter 2 (revision: 12.2024)