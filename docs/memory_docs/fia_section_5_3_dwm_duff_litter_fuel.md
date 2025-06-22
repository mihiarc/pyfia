# Section 5.3: Down Woody Material Duff, Litter, Fuel Table
**Oracle Table Name:** DWM_DUFF_LITTER_FUEL
**Extracted Pages:** 361-366 (Chapter pages 5-25 to 5-30)
**Source:** FIA Database Handbook v9.3
**Chapter:** 5 - Database Tables - Down Woody Material

---

## 5.3 Down Woody Material Duff, Litter, Fuel Table (Oracle table name: DWM\_DUFF\_LITTER\_FUEL)

| Subsection   | Column name (attribute)    | Descriptive name                    | Oracle data type   |
|--------------|----------------------------|-------------------------------------|--------------------|
| 5.3.1        | CN                         | Sequence number                     | VARCHAR2(34)       |
| 5.3.2        | PLT_CN                     | Plot sequence number                | VARCHAR2(34)       |
| 5.3.3        | INVYR                      | Inventory year                      | NUMBER(4)          |
| 5.3.4        | STATECD                    | State code                          | NUMBER(4)          |
| 5.3.5        | COUNTYCD                   | County code                         | NUMBER(3)          |
| 5.3.6        | PLOT                       | Plot number                         | NUMBER(5)          |
| 5.3.7        | TRANSECT                   | Transect                            | NUMBER(3)          |
| 5.3.8        | SUBP                       | Subplot number                      | NUMBER(1)          |
| 5.3.9        | SMPLOCCD                   | Sample location code                | NUMBER(1)          |
| 5.3.10       | MEASYEAR                   | Measurement year                    | NUMBER(4)          |
| 5.3.11       | CONDID                     | Condition class number              | NUMBER(1)          |
| 5.3.12       | DUFFDEP                    | Duff depth                          | NUMBER             |
| 5.3.13       | LITTDEP                    | Litter depth                        | NUMBER             |
| 5.3.14       | FUELDEP                    | Fuelbed depth                       | NUMBER             |
| 5.3.15       | CREATED_BY                 | Created by                          | VARCHAR2(30)       |
| 5.3.16       | CREATED_DATE               | Created date                        | DATE               |
| 5.3.17       | CREATED_IN_INSTANCE        | Created in instance                 | VARCHAR2(6)        |
| 5.3.18       | MODIFIED_BY                | Modified by                         | VARCHAR2(30)       |
| 5.3.19       | MODIFIED_DATE              | Modified date                       | DATE               |
| 5.3.20       | MODIFIED_IN_INSTANCE       | Modified in instance                | VARCHAR2(6)        |
| 5.3.21       | DLF_SAMPLE_METHOD          | Duff, litter, fuelbed sample method | VARCHAR2(6)        |
| 5.3.22       | DUFF_METHOD                | Duff measurement method             | NUMBER(1)          |
| 5.3.23       | DUFF_NONSAMPLE_REASN_CD    | Duff nonsampled reason code         | NUMBER(2)          |
| 5.3.24       | LITTER_METHOD              | Litter measurement method           | NUMBER(1)          |
| 5.3.25       | LITTER_NONSAMPLE_REASN_CD  | Litter nonsampled reason code       | NUMBER(2)          |
| 5.3.26       | FUELBED_METHOD             | Fuelbed measurement method          | NUMBER(1)          |
| 5.3.27       | FUELBED_NONSAMPLE_REASN_CD | Fuelbed nonsampled reason code      | NUMBER(2)          |
| 5.3.28       | DL_STATUS_CD               | Duff and litter sample status code  | NUMBER(1)          |

| Key Type   | Column(s) order   | Tables to link   | Abbreviated notation   |
|------------|-------------------|------------------|------------------------|
| Primary    | CN                | N/A              | DDL_PK                 |

| Key Type   | Column(s) order                                       | Tables to link   | Abbreviated notation   |
|------------|-------------------------------------------------------|------------------|------------------------|
| Unique     | PLT_CN, TRANSECT, SUBP, SMPLOCCD                      | N/A              | DDL_UK                 |
| Natural    | STATECD, INVYR, COUNTYCD, PLOT,TRANSECT,SUBP,SMPLOCCD | N/A              | DDL_NAT_I              |

## 5.3.1 CN

Sequence number. A unique sequence number used to identify a down woody material duff, litter, fuel record.

## 5.3.2 PLT\_CN

Plot sequence number. Foreign key linking the down woody material duff, litter, fuel record to the plot record.

## 5.3.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 5.3.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 5.3.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 5.3.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combination of attributes, PLOT may be used to uniquely identify a plot.

## 5.3.7 TRANSECT

Transect. The azimuth, in degrees, of the transect on which duff, litter, and/or fuel were sampled, extending out from subplot center.

## 5.3.8 SUBP

Subplot number. A code indicating the number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 5.3.9 SMPLOCCD

Sample location code. A code indicating the location along the transect where duff, litter, and fuelbed samples were taken. One transect is sampled on each subplot. Prior to 2002,

there were 2 sample locations on the transect (at 14 and 24 feet). Starting in 2002, there is only 1 sample location on the transect (at 24 feet).

## Codes: SMPLOCCD

|   Code | Description                                   |
|--------|-----------------------------------------------|
|      1 | Duff, litter, and fuelbed sampled at 14 feet. |
|      2 | Duff, litter, and fuelbed sampled at 24 feet. |

## 5.3.10 MEASYEAR

Measurement year. The year in which the plot was completed. MEASYEAR may differ from INVYR.

## 5.3.11 CONDID

Condition class number. The unique identifying number assigned to the condition where the duff/litter/fuel measurement(s) was taken. See COND.CONDID for details on the attributes which delineate a condition.

## 5.3.12 DUFFDEP

Duff depth. Depth of duff layer to the nearest 0.1 inch. The measurement is taken at an exact point on the transect (see SMPLOCCD for location; see TRANSECT for azimuth; see DLF\_SAMPLE\_METHOD to determine if the measurement was taken at slope or horizontal distance). Duff is the layer just below litter. It consists of decomposing leaves and other organic material. There are no recognizable plant parts; the duff layer is usually dark decomposed organic matter. When moss is present, the top of the duff layer is just below the green portion of the moss. The bottom of this layer is the point where mineral soil begins. To use these data, calculate an average depth for the condition.

## 5.3.13 LITTDEP

Litter depth. Depth of litter layer to the nearest 0.1 inch. The measurement is taken at an exact point on the transect (see SMPLOCCD for location; see TRANSECT for azimuth; see DLF\_SAMPLE\_METHOD to determine if the measurement was taken at slope or horizontal distance). Litter is the layer of freshly fallen leaves, needles, twigs (&lt;0.25 inch in diameter), cones, detached bark chunks, dead moss, dead lichens, detached small chunks of rotted wood, dead herbaceous stems, and flower parts (detached and not upright). Litter is the loose plant material found on the top surface of the forest floor. Little decomposition has begun in this layer. To use these data, calculate an average depth for the condition.

## 5.3.14 FUELDEP

Fuelbed depth. Depth of the fuelbed to the nearest 0.1 foot. The measurement is taken at an exact point on the transect (see SMPLOCCD for location; see TRANSECT for azimuth; see DLF\_SAMPLE\_METHOD to determine if the measurement was taken at slope or horizontal distance). The fuelbed is the accumulated mass of dead, woody material on the surface of the forest floor. It begins at the top of the duff layer, and includes litter, FWD, CWD, and dead woody shrubs. In this definition, the fuelbed does not include dead hanging branches from standing trees. To use these data, calculate an average depth for the condition.

## 5.3.15 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 5.3.16 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 5.3.17 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 5.3.18 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 5.3.19 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 5.3.20 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 5.3.21 DLF\_SAMPLE\_METHOD

Duff, litter, fuelbed sample method. A code indicating the sampling protocol used to collect duff, litter, and fuelbed data.

## Codes: DLF\_SAMPLE\_METHOD

|   Code | Description                                                                                                         | Distance measurement   |
|--------|---------------------------------------------------------------------------------------------------------------------|------------------------|
|      0 | Duff, litter, fuelbed not sampled.                                                                                  | Not applicable.        |
|      1 | National P3 protocol. Sampled at 2 points (14 and 24 feet) along each transect with average recorded.               | Slope.                 |
|      2 | National P3 protocol. Sampled at a point located 24 feet along each transect.                                       | Slope.                 |
|      3 | National P2 protocol (all options). Sampled at a point 24 feet along each transect.                                 | Horizontal.            |
|      4 | RMRS P2 protocol. One duff and litter point sampled at a point 24 feet along each transect on subplots 2, 3, and 4. | Horizontal.            |
|      5 | SRS P2 protocol. Duff and litter points sampled at 2 points (0 and 48 feet) along a transect on subplot 1.          | Horizontal.            |

## 5.3.22 DUFF\_METHOD

Duff measurement method. A code indicating the measurement of duff depth.

## Codes: DUFF\_METHOD

| Code   | Description                           |
|--------|---------------------------------------|
| NULL   | Not included in protocol.             |
| 0      | Included in protocol but not sampled. |
| 1      | Measured.                             |
| 2      | Estimated.                            |
| 3      | Measured up to maximum depth.         |

## 5.3.23 DUFF\_NONSAMPLE\_REASN\_CD

Duff nonsampled reason code. A code indicating the reason duff depth was not measured.

## Codes: DUFF\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                          |
|--------|--------------------------------------------------------------------------------------------------------------------------------------|
|     04 | Time limitation.                                                                                                                     |
|     05 | Lost data.                                                                                                                           |
|     10 | Other - The point was not measured (for example, snow/water covering sample point, or some other obstruction prevented measurement). |

## 5.3.24 LITTER\_METHOD

Litter measurement method. A code indicating the measurement of litter depth.

## Codes: LITTER\_METHOD

| Code   | Description                           |
|--------|---------------------------------------|
| NULL   | Not included in protocol.             |
| 0      | Included in protocol but not sampled. |
| 1      | Measured.                             |
| 2      | Estimated.                            |
| 3      | Measured up to maximum depth.         |

## 5.3.25 LITTER\_NONSAMPLE\_REASN\_CD

Litter nonsampled reason code. A code indicating the reason litter depth was not measured.

## Codes: LITTER\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                          |
|--------|--------------------------------------------------------------------------------------------------------------------------------------|
|     04 | Time limitation.                                                                                                                     |
|     05 | Lost data.                                                                                                                           |
|     10 | Other - The point was not measured (for example, snow/water covering sample point, or some other obstruction prevented measurement). |

## 5.3.26 FUELBED\_METHOD

Fuelbed measurement method. A code indicating the measurement of fuelbed depth.

## Codes: FUELBED\_METHOD

| Code   | Description                           |
|--------|---------------------------------------|
| NULL   | Not included in protocol.             |
| 0      | Included in protocol but not sampled. |
| 1      | Measured.                             |
| 2      | Estimated.                            |
| 3      | Measured up to maximum depth.         |

## 5.3.27 FUELBED\_NONSAMPLE\_REASN\_CD

Fuelbed nonsampled reason code. A code indicating the reason fuelbed depth was not measured.

## FUELBED\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                          |
|--------|--------------------------------------------------------------------------------------------------------------------------------------|
|     04 | Time limitation.                                                                                                                     |
|     05 | Lost data.                                                                                                                           |
|     10 | Other - The point was not measured (for example, snow/water covering sample point, or some other obstruction prevented measurement). |

## 5.3.28 DL\_STATUS\_CD

Duff and litter sample status code. A code indicating the sample status for duff and litter depth on the transect. If the measurement point is on a sampled condition, but the duff/litter depth is not measurable (e.g., due to snow), a value of 0 is recorded for this attribute. If the measurement point is on a sampled condition, but the DUFFDEP and LITTDEP = 0, a value of 1 is recorded for this attribute.

Note: This attribute is set to a value of 1 for noncensus water conditions (COND.COND\_STATUS\_CD = 3) and nonsampled nonforest conditions (COND.NF\_COND\_STATUS\_CD = 5).

## Codes: DL\_STATUS\_CD

|   Code | Description                        |
|--------|------------------------------------|
|      0 | Duff and litter point not sampled. |
|      1 | Duff and litter point sampled.     |