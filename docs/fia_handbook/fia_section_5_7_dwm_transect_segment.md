# Section 5.7: Down Woody Material Transect Segment Table
**Oracle Table Name:** DWM_TRANSECT_SEGMENT
**Extracted Pages:** 387-392 (Chapter pages 5-51 to 5-56)
**Source:** FIA Database Handbook v9.3
**Chapter:** 5 - Database Tables - Down Woody Material

---

## 5.7 Down Woody Material Transect Segment Table (Oracle table name: DWM\_TRANSECT\_SEGMENT)

| Subsection   | Column name (attribute)   | Descriptive name                                      | Oracle data type   |
|--------------|---------------------------|-------------------------------------------------------|--------------------|
| 5.7.1        | CN                        | Sequence number                                       | VARCHAR2(34)       |
| 5.7.2        | PLT_CN                    | Plot sequence number                                  | VARCHAR2(34)       |
| 5.7.3        | INVYR                     | Inventory year                                        | NUMBER(4)          |
| 5.7.4        | STATECD                   | State code                                            | NUMBER(4)          |
| 5.7.5        | COUNTYCD                  | County code                                           | NUMBER(3)          |
| 5.7.6        | PLOT                      | Plot number                                           | NUMBER(5)          |
| 5.7.7        | SUBP                      | Subplot number                                        | NUMBER(1)          |
| 5.7.8        | TRANSECT                  | Transect                                              | NUMBER(3)          |
| 5.7.9        | SEGMNT                    | Segment number                                        | NUMBER(1)          |
| 5.7.10       | MEASYEAR                  | Measurement year                                      | NUMBER(4)          |
| 5.7.11       | CONDID                    | Condition class number                                | NUMBER(1)          |
| 5.7.12       | SLOPE_BEGNDIST            | Beginning slope distance of the transect segment      | NUMBER             |
| 5.7.13       | SLOPE_ENDDIST             | Ending slope distance of the transect segment         | NUMBER             |
| 5.7.14       | SLOPE                     | Transect percent slope                                | NUMBER(3)          |
| 5.7.15       | HORIZ_LENGTH              | Horizontal length of the transect segment             | NUMBER             |
| 5.7.16       | HORIZ_BEGNDIST            | Beginning horizontal distance of the transect segment | NUMBER             |
| 5.7.17       | HORIZ_ENDDIST             | Ending horizontal distance of the transect segment    | NUMBER             |
| 5.7.18       | CREATED_BY                | Created by                                            | VARCHAR2(30)       |
| 5.7.19       | CREATED_DATE              | Created date                                          | DATE               |
| 5.7.20       | CREATED_IN_INSTANCE       | Created in instance                                   | VARCHAR2(6)        |
| 5.7.21       | MODIFIED_BY               | Modified by                                           | VARCHAR2(30)       |
| 5.7.22       | MODIFIED_IN_INSTANCE      | Modified in instance                                  | VARCHAR2(6)        |
| 5.7.23       | MODIFIED_DATE             | Modified date                                         | DATE               |
| 5.7.24       | SEGMNT_STATUS_CD          | Segment sample status code                            | NUMBER(1)          |
| 5.7.25       | SEGMNT_NONSAMPLE_REASN_CD | Segment nonsampled reason code                        | NUMBER(2)          |
| 5.7.26       | TRANSECT_LENGTH           | Transect length                                       | NUMBER(4,1)        |

| Key Type   | Column(s) order                                        | Tables to link   | Abbreviated notation   |
|------------|--------------------------------------------------------|------------------|------------------------|
| Primary    | CN                                                     | N/A              | DTS_PK                 |
| Unique     | PLT_CN,SUBP,TRANSECT,SEGMNT                            | N/A              | DTS_UK                 |
| Natural    | STATECD, INVYR, COUNTYCD, PLOT, SUBP, TRANSECT, SEGMNT | N/A              | DTS_NAT_I              |

## 5.7.1 CN

Sequence number. A unique sequence number used to identify a down woody material transect segment record.

## 5.7.2 PLT\_CN

Plot sequence number. Foreign key linking the down woody material transect segment record to the plot record.

## 5.7.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 5.7.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 5.7.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 5.7.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combination of attributes, PLOT may be used to uniquely identify a plot.

## 5.7.7 SUBP

Subplot number. A code indicating the number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 5.7.8 TRANSECT

Transect. The azimuth, in degrees, of the transect, extending out from subplot center.

## 5.7.9 SEGMNT

Segment number. A number identifying a segment on the transect within one condition, recorded sequentially from subplot center out to the end of the transect. Each condition is given a segment number as it is encountered and mapped along the transect. A segment is a continuous length of line within one condition. Segment number 8 is an office generated segment, indicating field crews did not actually measure or install the segment. Most often, this is for entire subplots that are nonsampled nonforest land.

## 5.7.10 MEASYEAR

Measurement year. The year in which the plot was completed. MEASYEAR may differ from INVYR.

## 5.7.11 CONDID

Condition class number. The unique identifying number assigned to the condition where the transect segment is located. See COND.CONDID for details on the attributes which delineate a condition.

## 5.7.12 SLOPE\_BEGNDIST

Beginning slope distance of the transect segment. The location on the transect where the segment begins, in slope distance in feet. A segment is a continuous length of line within one condition. The beginning distance is the point on the transect line where the condition class changes and a new segment begins. If the beginning distance is zero, this is the start of the transect at subplot center. Each segment has a beginning and ending distance recorded as slope distance in the field, measured from the subplot center.

## 5.7.13 SLOPE\_ENDDIST

Ending slope distance of the transect segment. The location on the transect where the segment ends, in slope distance in feet. A segment is a continuous length of line within one condition. The ending distance is the point on the transect line where the condition class of the current segment changes, or the point where the transect ends on the subplot. Each segment has a beginning and ending distance recorded as slope distance in the field, measured from the subplot center.

## 5.7.14 SLOPE

Transect percent slope. The average percent slope of the transect within the condition class being sampled. Slope ranges from 0 to 155 percent.

## 5.7.15 HORIZ\_LENGTH

Horizontal length of the transect segment. The horizontal length of the individual transect segment in feet.

## 5.7.16 HORIZ\_BEGNDIST

Beginning horizontal distance of the transect segment. The location on the transect where the segment begins, in horizontal distance in feet. A segment is a continuous length of line within one condition. The beginning distance is the point on the transect line where the condition class changes and a new segment begins. If the beginning distance is zero, this is the start of the transect at subplot center. Each segment has a beginning and

ending distance recorded as slope distance in the field, which is then converted to horizontal distance.

## 5.7.17 HORIZ\_ENDDIST

Ending horizontal distance of the transect segment. The location on the transect where the segment ends, in horizontal distance in feet. A segment is a continuous length of line within one condition. The ending distance is the point on the transect line where the condition class of the current segment changes, or the point where the transect ends on the subplot. Each segment has a beginning and ending distance recorded as slope distance in the field, which is then converted to horizontal distance.

## 5.7.18 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 5.7.19 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 5.7.20 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 5.7.21 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 5.7.22 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 5.7.23 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 5.7.24 SEGMNT\_STATUS\_CD

Segment sample status code. A code indicating the sampling status of the transect segment. Populated for all options of the National P2 DWM protocol.

## Codes: SEGMNT\_STATUS\_CD

|   Code | Description                   |
|--------|-------------------------------|
|      0 | Transect segment not sampled. |
|      1 | Transect segment sampled.     |

## 5.7.25 SEGMNT\_NONSAMPLE\_REASN\_CD

Segment nonsampled reason code. A code indicating the reason DWM measurement was not conducted on a transect segment.

## Codes: SEGMNT\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                                         |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
|     04 | Time limitation.                                                                                                                                    |
|     05 | Lost data.                                                                                                                                          |
|     10 | Other - The transect segment was not measured (for example, snow/water covering transect segment, or some other obstruction prevented measurement). |

## 5.7.26 TRANSECT\_LENGTH

Transect length. The target length of the full transect, in horizontal distance in feet. This is an office-generated value.

Down Woody Material Transect Segment Table

Chapter 5 (revision: 04.2024)