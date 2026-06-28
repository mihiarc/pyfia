# Section 5.5: Down Woody Material Microplot Fuel Table
**Oracle Table Name:** DWM_MICROPLOT_FUEL
**Extracted Pages:** 373-378 (Chapter pages 5-37 to 5-42)
**Source:** FIA Database Handbook v9.3
**Chapter:** 5 - Database Tables - Down Woody Material

---

## 5.5 Down Woody Material Microplot Fuel Table (Oracle table name: DWM\_MICROPLOT\_FUEL)

| Subsection   | Column name (attribute)   | Descriptive name        | Oracle data type   |
|--------------|---------------------------|-------------------------|--------------------|
| 5.5.1        | CN                        | Sequence number         | VARCHAR2(34)       |
| 5.5.2        | PLT_CN                    | Plot sequence number    | VARCHAR2(34)       |
| 5.5.3        | INVYR                     | Inventory year          | NUMBER(4)          |
| 5.5.4        | STATECD                   | State code              | NUMBER(4)          |
| 5.5.5        | COUNTYCD                  | County code             | NUMBER(3)          |
| 5.5.6        | PLOT                      | Plot number             | NUMBER(5)          |
| 5.5.7        | SUBP                      | Subplot number          | NUMBER(1)          |
| 5.5.8        | MEASYEAR                  | Measurement year        | NUMBER(4)          |
| 5.5.9        | LVSHRBCD                  | Live shrub code         | NUMBER(2)          |
| 5.5.10       | DSHRBCD                   | Dead shrub code         | NUMBER(2)          |
| 5.5.11       | LVHRBCD                   | Live herb code          | NUMBER(2)          |
| 5.5.12       | DHRBCD                    | Dead herb code          | NUMBER(2)          |
| 5.5.13       | LITTERCD                  | Litter code             | NUMBER             |
| 5.5.14       | LVSHRBHT                  | Live shrub height       | NUMBER             |
| 5.5.15       | DSHRBHT                   | Dead shrub height       | NUMBER             |
| 5.5.16       | LVHRBHT                   | Live herb height        | NUMBER             |
| 5.5.17       | DHRBHT                    | Dead herb height        | NUMBER             |
| 5.5.18       | CREATED_BY                | Created by              | VARCHAR2(30)       |
| 5.5.19       | CREATED_DATE              | Created date            | DATE               |
| 5.5.20       | CREATED_IN_INSTANCE       | Created in instance     | VARCHAR2(6)        |
| 5.5.21       | MODIFIED_BY               | Modified by             | VARCHAR2(30)       |
| 5.5.22       | MODIFIED_DATE             | Modified date           | DATE               |
| 5.5.23       | MODIFIED_IN_INSTANCE      | Modified in instance    | VARCHAR2(6)        |
| 5.5.24       | MICR_SAMPLE_METHOD        | Microplot sample method | VARCHAR2(6)        |

| Key Type   | Column(s) order                      | Tables to link   | Abbreviated notation   |
|------------|--------------------------------------|------------------|------------------------|
| Primary    | CN                                   | N/A              | DMF_PK                 |
| Unique     | PLT_CN, SUBP                         | N/A              | DMF_UK                 |
| Natural    | STATECD, INVYR, COUNTYCD, PLOT, SUBP | N/A              | DMF_NAT_I              |

## 5.5.1 CN

Sequence number. A unique sequence number used to identify a down woody material microplot fuel record.

## 5.5.2 PLT\_CN

Plot sequence number. Foreign key linking the down woody material microplot fuel record to the plot record.

## 5.5.3 INVYR

Inventory year. See SURVEY.IINVYR description for definition.

## 5.5.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 5.5.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 5.5.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combination of attributes, PLOT may be used to uniquely identify a plot.

## 5.5.7 SUBP

Subplot number. A code indicating the number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 5.5.8 MEASYEAR

Measurement year. The year in which the plot was completed. MEASYEAR may differ from INVYR.

## 5.5.9 LVSHRBCD

Live shrub code. A cover class code indicating the percent cover of the forested microplot area covered with live shrubs.

## Codes: LVSHRBCD

|   Code | Description       |
|--------|-------------------|
|     00 | Absent            |
|     01 | Trace (<1% cover) |
|     10 | 1-10%             |
|     20 | 11-20%            |
|     30 | 21-30%            |
|     40 | 31-40%            |
|     50 | 41-50%            |

|   Code | Description   |
|--------|---------------|
|     60 | 51-60%        |
|     70 | 61-70%        |
|     80 | 71-80%        |
|     90 | 81-90%        |
|     99 | 91-100%       |

## 5.5.10 DSHRBCD

Dead shrub code. A cover class code indicating the percent cover of the forested microplot area covered with dead shrubs and dead branches attached to live shrubs if visible from above.

## Codes: DSHRBCD

|   Code | Description       |
|--------|-------------------|
|     00 | Absent            |
|     01 | Trace (<1% cover) |
|     10 | 1-10%             |
|     20 | 11-20%            |
|     30 | 21-30%            |
|     40 | 31-40%            |
|     50 | 41-50%            |
|     60 | 51-60%            |
|     70 | 61-70%            |
|     80 | 71-80%            |
|     90 | 81-90%            |
|     99 | 91-100%           |

## 5.5.11 LVHRBCD

Live herb code. A cover class code indicating the percent cover of the forested microplot area covered with live herbaceous plants.

## Codes: LVHRBCD

|   Code | Description       |
|--------|-------------------|
|     00 | Absent            |
|     01 | Trace (<1% cover) |
|     10 | 1-10%             |
|     20 | 11-20%            |
|     30 | 21-30%            |
|     40 | 31-40%            |
|     50 | 41-50%            |
|     60 | 51-60%            |
|     70 | 61-70%            |

|   Code | Description   |
|--------|---------------|
|     80 | 71-80%        |
|     90 | 81-90%        |
|     99 | 91-100%       |

## 5.5.12 DHRBCD

Dead herb code. A cover class code indicating the percent cover of the forested microplot area covered with dead herbaceous plants and dead leaves attached to live plants if visible from above.

## Codes: DHRBCD

|   Code | Description       |
|--------|-------------------|
|     00 | Absent            |
|     01 | Trace (<1% cover) |
|     10 | 1-10%             |
|     20 | 11-20%            |
|     30 | 21-30%            |
|     40 | 31-40%            |
|     50 | 41-50%            |
|     60 | 51-60%            |
|     70 | 61-70%            |
|     80 | 71-80%            |
|     90 | 81-90%            |
|     99 | 91-100%           |

## 5.5.13 LITTERCD

Litter code. A cover class code indicating the percent cover of the forested microplot area covered with litter. Litter is the layer of freshly fallen leaves, twigs, dead moss, dead lichens, and other fine particles of organic matter found on the surface of the forest floor. Decomposition is minimal.

## Codes: LITTERCD

|   Code | Description       |
|--------|-------------------|
|     00 | Absent            |
|     01 | Trace (<1% cover) |
|     10 | 1-10%             |
|     20 | 11-20%            |
|     30 | 21-30%            |
|     40 | 31-40%            |
|     50 | 41-50%            |
|     60 | 51-60%            |
|     70 | 61-70%            |

|   Code | Description   |
|--------|---------------|
|     80 | 71-80%        |
|     90 | 81-90%        |
|     99 | 91-100%       |

## 5.5.14 LVSHRBHT

Live shrub height. Indicates the height of the tallest live shrub to the nearest 0.1 foot. Heights &lt;6 feet are measured and heights  6 feet are estimated. 

## 5.5.15 DSHRBHT

Dead shrub height. Indicates the height of the tallest dead shrub to the nearest 0.1 foot. Heights &lt;6 feet are measured and heights  6 feet are estimated. 

## 5.5.16 LVHRBHT

Live herb height. Indicates the height (at the tallest point) of the live herbaceous layer to the nearest 0.1 foot. Maximum height is 6 feet.

## 5.5.17 DHRBHT

Dead herb height. Indicates the height (at the tallest point) of the dead herbaceous layer to the nearest 0.1 foot. Maximum height is 6 feet.

## 5.5.18 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 5.5.19 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 5.5.20 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 5.5.21 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 5.5.22 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 5.5.23 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 5.5.24 MICR\_SAMPLE\_METHOD

Microplot sample method. A code indicating the sampling protocol used to collect microplot fuels data.

Note:

Starting with PLOT.MANUAL = 5.1, DWM sampling on microplots was discontinued.

## Codes: MICR\_SAMPLE\_METHOD

|   Code | Description                                                                                                                                                                                           | Distance measurement   |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------|
|      0 | Microplot fuel not sampled.                                                                                                                                                                           | Not applicable.        |
|      1 | National P2 and P3 protocol. Percent cover in 10% classes of fuels on all forested conditions combined on the microplot. Fuel classes were: live shrubs, dead shrubs, live herbs, dead herbs, litter. | Horizontal.            |
|      2 | RMRS P2 protocol. No microplot fuels sampled.                                                                                                                                                         | Not applicable.        |
|      3 | SRS P2 protocol. Percent cover in 10% classes and height of fuels on 6-foot transect on subplot 1. Fuel classes were shrubs and herbs, live and dead combined.                                        | Slope.                 |