# Section 5.4: Down Woody Material Fine Woody Debris Table
**Oracle Table Name:** DWM_FINE_WOODY_DEBRIS
**Extracted Pages:** 367-372 (Chapter pages 5-31 to 5-36)
**Source:** FIA Database Handbook v9.3
**Chapter:** 5 - Database Tables - Down Woody Material

---

## 5.4 Down Woody Material Fine Woody Debris Table (Oracle table name: DWM\_FINE\_WOODY\_DEBRIS)

| Subsection   | Column name (attribute)   | Descriptive name                                      | Oracle data type   |
|--------------|---------------------------|-------------------------------------------------------|--------------------|
| 5.4.1        | CN                        | Sequence number                                       | VARCHAR2(34)       |
| 5.4.2        | PLT_CN                    | Plot sequence number                                  | VARCHAR2(34)       |
| 5.4.3        | INVYR                     | Inventory year                                        | NUMBER(4)          |
| 5.4.4        | STATECD                   | State code                                            | NUMBER(4)          |
| 5.4.5        | COUNTYCD                  | County code                                           | NUMBER(3)          |
| 5.4.6        | PLOT                      | Plot number                                           | NUMBER(5)          |
| 5.4.7        | TRANSECT                  | Transect                                              | NUMBER(3)          |
| 5.4.8        | SUBP                      | Subplot number                                        | NUMBER(1)          |
| 5.4.9        | CONDID                    | Condition class number                                | NUMBER(1)          |
| 5.4.10       | MEASYEAR                  | Measurement year                                      | NUMBER(4)          |
| 5.4.11       | SMALLCT                   | Small-size class count                                | NUMBER(3)          |
| 5.4.12       | MEDIUMCT                  | Medium-size class count                               | NUMBER(3)          |
| 5.4.13       | LARGECT                   | Large-size class count                                | NUMBER(3)          |
| 5.4.14       | RSNCTCD                   | Reason count code                                     | NUMBER(1)          |
| 5.4.15       | PILESCD                   | Piles code                                            | NUMBER(1)          |
| 5.4.16       | SMALL_TL_COND             | Small-size class transect length in condition         | NUMBER             |
| 5.4.17       | SMALL_TL_PLOT             | Small-size class transect length on plot              | NUMBER             |
| 5.4.18       | SMALL_TL_UNADJ            | Small-size class transect length on plot, unadjusted  | NUMBER             |
| 5.4.19       | MEDIUM_TL_COND            | Medium-size class transect length in condition        | NUMBER             |
| 5.4.20       | MEDIUM_TL_PLOT            | Medium-size class transect length on plot             | NUMBER             |
| 5.4.21       | MEDIUM_TL_UNADJ           | Medium-size class transect length on plot, unadjusted | NUMBER             |
| 5.4.22       | LARGE_TL_COND             | Large-size class transect length in condition         | NUMBER             |
| 5.4.23       | LARGE_TL_PLOT             | Large-size class transect length on plot              | NUMBER             |
| 5.4.24       | LARGE_TL_UNADJ            | Large-size class transect length on plot, unadjusted  | NUMBER             |
| 5.4.25       | CREATED_BY                | Created by                                            | VARCHAR2(30)       |
| 5.4.26       | CREATED_DATE              | Created date                                          | DATE               |
| 5.4.27       | CREATED_IN_INSTANCE       | Created in instance                                   | VARCHAR2(6)        |
| 5.4.28       | MODIFIED_BY               | Modified by                                           | VARCHAR2(30)       |

| Subsection   | Column name (attribute)   | Descriptive name                         | Oracle data type   |
|--------------|---------------------------|------------------------------------------|--------------------|
| 5.4.29       | MODIFIED_DATE             | Modified date                            | DATE               |
| 5.4.30       | MODIFIED_IN_INSTANCE      | Modified in instance                     | VARCHAR2(6)        |
| 5.4.31       | FWD_STATUS_CD             | Fine woody debris sample status          | NUMBER(1)          |
| 5.4.32       | FWD_NONSAMPLE_REASN_CD    | Fine woody debris nonsampled reason code | NUMBER(2)          |
| 5.4.33       | FWD_SAMPLE_METHOD         | Fine woody debris sample method          | VARCHAR2(6)        |
| 5.4.34       | SLOPE                     | Transect percent slope                   | NUMBER(3)          |

| Key Type   | Column(s) order                                        | Tables to link   | Abbreviated notation   |
|------------|--------------------------------------------------------|------------------|------------------------|
| Primary    | CN                                                     | N/A              | DFW_PK                 |
| Unique     | PLT_CN, TRANSECT, SUBP, CONDID                         | N/A              | DFW_UK                 |
| Natural    | STATECD, INVYR, COUNTYCD, PLOT, TRANSECT, SUBP, CONDID | N/A              | DFW_NAT_I              |

## 5.4.1 CN

Sequence number. A unique sequence number used to identify a down woody material fine woody debris (FWD) record.

## 5.4.2 PLT\_CN

Plot sequence number. Foreign key linking the down woody material fine woody debris record to the plot record.

## 5.4.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 5.4.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 5.4.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 5.4.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combination of attributes, PLOT may be used to uniquely identify a plot.

## 5.4.7 TRANSECT

Transect. The azimuth, in degrees, of the transect on which fine woody debris was sampled, extending out from subplot center.

## 5.4.8 SUBP

Subplot number. A code indicating the number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 5.4.9 CONDID

Condition class number. The unique identifying number assigned to the condition where the fine woody debris (FWD) was sampled. See COND.CONDID for details on the attributes which delineate a condition.

## 5.4.10 MEASYEAR

Measurement year. The year in which the plot was completed. MEASYEAR may differ from INVYR.

## 5.4.11 SMALLCT

Small-size class count. The number of pieces of 1-hr fuels counted in the small-size class (0.01- to 0.24-inch diameter) in one condition along the transect segment on the plot specified in the sample design to measure small-size class FWD. Individual pieces are tallied up to 50, then ocularly estimated over a tally of 50.

## 5.4.12 MEDIUMCT

Medium-size class count. The number of pieces of 10-hr fuels counted in the medium-size class (0.25- to 0.9-inch diameter) in one condition along the transect segment on the plot specified in the sample design to measure medium-size class FWD. Individual pieces are tallied up to 50, then ocularly estimated over a tally of 50.

## 5.4.13 LARGECT

Large-size class count. The number of pieces of 100-hr fuels counted in the large-size class (1.0- to 2.9-inch diameter) in one condition along the transect segment on the plot specified in the sample design to measure large-size class FWD. Individual pieces are tallied up to 20, then ocularly estimated over a tally of 20.

## 5.4.14 RSNCTCD

Reason count code. A code indicating the reason that SMALLCT, MEDIUMCT, or LARGECT has more than 100 pieces tallied.

## Codes: RSNCTCD

|   Code | Description                                                              |
|--------|--------------------------------------------------------------------------|
|      0 | FWD is not unusually high (<100).                                        |
|      1 | High count is due to an overall high density of FWD across the transect. |
|      2 | Wood rat's nest located on transect.                                     |
|      3 | Tree or shrub laying across transect.                                    |
|      4 | Other reason.                                                            |

## 5.4.15 PILESCD

Piles code. A code indicating whether a residue pile intersects the FWD transect segment. If the code is 1 (Yes), then FWD is not sampled.

## Codes: PILESCD

|   Code | Description                                                  |
|--------|--------------------------------------------------------------|
|      0 | No pile is present on the transect. FWD was sampled.         |
|      1 | Yes, a pile is present on the transect. FWD was not sampled. |

## 5.4.16 SMALL\_TL\_COND

Small-size class transect length in condition. Sum of the transect segment lengths, in feet, that were installed to measure small-sized FWD in one condition on the plot.

## 5.4.17 SMALL\_TL\_PLOT

Small-size class transect length on plot. Sum of the transect segment lengths, in feet, that were installed to measure small-sized FWD on the plot. This total length includes all sampled conditions, excluding hazardous or access denied conditions.

## 5.4.18 SMALL\_TL\_UNADJ

Small-size class transect length on plot, unadjusted. Sum of all transect segment lengths, in feet, on the plot that were specified in the sample design to measure small-sized FWD. Includes transects in all conditions, sampled and nonsampled. This value must be adjusted using POP\_STRATUM.ADJ\_FACTOR\_FWD\_SM to derive population estimates.

## 5.4.19 MEDIUM\_TL\_COND

Medium-size class transect length in condition. Sum of transect segment lengths, in feet, that were installed to measure medium-sized FWD in one condition on the plot.

## 5.4.20 MEDIUM\_TL\_PLOT

Medium-size class transect length on plot. Sum of transect segment lengths, in feet, that were installed to measure medium-sized FWD on the plot. This total length includes segment in all sampled conditions, excluding hazardous or access denied conditions.

## 5.4.21 MEDIUM\_TL\_UNADJ

Medium-size class transect length on plot, unadjusted. Sum of all transect segment lengths, in feet, on the plot that were specified in the sample design to measure medium-sized FWD. Includes transects in all conditions, sampled and nonsampled. This value must be adjusted using POP\_STRATUM.ADJ\_FACTOR\_FWD\_SM to derive population estimates.

## 5.4.22 LARGE\_TL\_COND

Large-size class transect length in condition. Sum of transect segment lengths, in feet, that were installed to measure large-sized FWD in one condition on the plot.

## 5.4.23 LARGE\_TL\_PLOT

Large-size class transect length on plot. Sum of transect segment lengths, in feet, that were installed to measure large-sized FWD on the entire plot. This total length includes segments in all sampled conditions, excluding hazardous or access denied conditions.

## 5.4.24 LARGE\_TL\_UNADJ

Large-size class transect length on plot, unadjusted. Sum of all transect segment lengths, in feet, that were installed to measure large-sized FWD on the entire plot. Includes transects in all conditions, sampled and nonsampled. This value must be adjusted using POP\_STRATUM.ADJ\_FACTOR\_FWD\_LG to derive population estimates.

## 5.4.25 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 5.4.26 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 5.4.27 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 5.4.28 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 5.4.29 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 5.4.30 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 5.4.31 FWD\_STATUS\_CD

Fine woody debris sample status. A code indicating the sampling status of the fine woody debris transect segment.

## Codes: FWD\_STATUS\_CD

|   Code | Description                       |
|--------|-----------------------------------|
|      0 | FWD transect segment not sampled. |
|      1 | FWD transect segment sampled.     |

## 5.4.32 FWD\_NONSAMPLE\_REASN\_CD

Fine woody debris nonsampled reason code. A code indicating the reason fine woody debris was not measured.

## Codes: FWD\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                              |
|--------|------------------------------------------------------------------------------------------------------------------------------------------|
|     04 | Time limitation.                                                                                                                         |
|     05 | Lost data.                                                                                                                               |
|     10 | Other - The point was not measured (for example, snow/water covering transect segment, or some other obstruction prevented measurement). |

## 5.4.33 FWD\_SAMPLE\_METHOD

Fine woody debris sample method. A code indicating the sampling protocol used to collect fine woody debris data.

## Codes: FWD\_SAMPLE\_METHOD

|   Code | Description                                                                                                                            | Transect distance measurement   |
|--------|----------------------------------------------------------------------------------------------------------------------------------------|---------------------------------|
|      0 | FWD not sampled.                                                                                                                       | Not applicable.                 |
|      1 | National P2 and P3 protocol. One 10-foot transect for small and medium FWD and one 20-foot transect for large FWD per subplot.         | Slope.                          |
|      2 | National P2 and P3 protocol. One 6-foot transect for small and medium FWD and one 10-foot transect for large FWD per subplot.          | Slope.                          |
|      3 | National P2 protocol (all options). One 6-foot transect for small and medium FWD and one 10-foot transect for large FWD per subplot.   | Horizontal.                     |
|      4 | SRS P2 protocol. One 6-foot transect for small and medium FWD, and one 10-foot transect for large FWD on subplot 1.                    | Slope.                          |
|      5 | RMRS P2 protocol. One 6-foot transect for small and medium FWD and one 10-foot transect for large FWD on each of subplots 2, 3, and 4. | Slope.                          |

## 5.4.34 SLOPE

Transect percent slope. The average percent slope of the transect within the condition class being sampled. Slope ranges from 0 to 155 percent.