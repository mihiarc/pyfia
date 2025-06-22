# Section 5.1: Down Woody Material Visit Table
**Oracle Table Name:** DWM_VISIT
**Extracted Pages:** 339-346 (Chapter pages 5-3 to 5-10)
**Source:** FIA Database Handbook v9.3
**Chapter:** 5 - Database Tables - Down Woody Material

---

## 5.1 Down Woody Material Visit Table (Oracle table name: DWM\_VISIT)

| Subsection   | Column name (attribute)   | Descriptive name                    | Oracle data type   |
|--------------|---------------------------|-------------------------------------|--------------------|
| 5.1.1        | CN                        | Sequence number                     | VARCHAR2(34)       |
| 5.1.2        | PLT_CN                    | Plot sequence number                | VARCHAR2(34)       |
| 5.1.3        | INVYR                     | Inventory year                      | NUMBER(4)          |
| 5.1.4        | STATECD                   | State code                          | NUMBER(4)          |
| 5.1.5        | COUNTYCD                  | County code                         | NUMBER(3)          |
| 5.1.6        | PLOT                      | Plot number                         | NUMBER(5)          |
| 5.1.7        | MEASDAY                   | Measurement day                     | NUMBER(2)          |
| 5.1.8        | MEASMON                   | Measurement month                   | NUMBER(2)          |
| 5.1.9        | MEASYEAR                  | Measurement year                    | NUMBER(4)          |
| 5.1.10       | QASTATCD                  | Quality assurance status code       | NUMBER(1)          |
| 5.1.11       | CRWTYPCD                  | Crew type code                      | NUMBER(1)          |
| 5.1.12       | SMPKNDCD                  | Sample kind code                    | NUMBER(2)          |
| 5.1.13       | CREATED_BY                | Created by                          | VARCHAR2(30)       |
| 5.1.14       | CREATED_DATE              | Created date                        | DATE               |
| 5.1.15       | CREATED_IN_INSTANCE       | Created in instance                 | VARCHAR2(6)        |
| 5.1.16       | MODIFIED_BY               | Modified by                         | VARCHAR2(30)       |
| 5.1.17       | MODIFIED_DATE             | Modified date                       | DATE               |
| 5.1.18       | MODIFIED_IN_INSTANCE      | Modified in instance                | VARCHAR2(6)        |
| 5.1.19       | CWD_SAMPLE_METHOD         | Coarse woody debris sample method   | VARCHAR2(6)        |
| 5.1.20       | FWD_SAMPLE_METHOD         | Fine woody debris sample method     | VARCHAR2(6)        |
| 5.1.21       | MICR_SAMPLE_METHOD        | Microplot sample method             | VARCHAR2(6)        |
| 5.1.22       | DLF_SAMPLE_METHOD         | Duff, litter, fuelbed sample method | VARCHAR2(6)        |
| 5.1.23       | PILE_SAMPLE_METHOD        | Pile sample method                  | VARCHAR2(6)        |
| 5.1.24       | DWM_SAMPLING_STATUS_CD    | DWM sampling status code            | NUMBER(1)          |
| 5.1.25       | DWM_NBR_SUBP              | DWM number of subplots              | NUMBER(1)          |
| 5.1.26       | DWM_NBR_SUBP_TRANSECT     | DWM number of transects on subplot  | NUMBER(1)          |
| 5.1.27       | DWM_SUBPLIST              | DWM subplot list                    | NUMBER(4)          |
| 5.1.28       | DWM_TRANSECT_LENGTH       | DWM transect length                 | NUMBER(4,1)        |
| 5.1.29       | QA_STATUS                 | Quality assurance status            | NUMBER(1)          |

| Key Type   | Column(s) order                | Tables to link    | Abbreviated notation   |
|------------|--------------------------------|-------------------|------------------------|
| Primary    | CN                             | N/A               | DVT_PK                 |
| Unique     | PLT_CN                         | N/A               | DVT_UK                 |
| Natural    | STATECD, INVYR, COUNTYCD, PLOT | N/A               | DVT_NAT_I              |
| Foreign    | PLT_CN                         | DWM_VISIT to PLOT | DVT_PLT_FK             |

## 5.1.1 CN

Sequence number. A unique sequence number used to identify a down woody material visit record.

## 5.1.2 PLT\_CN

Plot sequence number. Foreign key linking the down woody material visit record to the plot record.

## 5.1.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 5.1.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each state. Refer to appendix B.

## 5.1.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a state. FIPS codes from the Bureau of the Census are used. Refer to appendix B.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 5.1.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, and COUNTYCD, PLOT may be used to uniquely identify a plot.

## 5.1.7 MEASDAY

Measurement day. The day on which the plot was completed.

## 5.1.8 MEASMON

Measurement month. The month in which the plot was completed.

## Codes: MEASMON

|   Code | Description   |
|--------|---------------|
|      1 | January.      |
|      2 | February.     |
|      3 | March.        |

|   Code | Description   |
|--------|---------------|
|      4 | April.        |
|      5 | May.          |
|      6 | June.         |
|      7 | July.         |
|      8 | August.       |
|      9 | September.    |
|     10 | October.      |
|     11 | November.     |
|     12 | December.     |

## 5.1.9 MEASYEAR

Measurement year. The year in which the plot was completed. MEASYEAR may differ from INVYR.

## 5.1.10 QASTATCD

Quality assurance status code. A code indicating the type of plot data collected. Production plots have QASTATCD = 1 or 7.

## Codes: QASTATCD

|   Code | Description                                                                                                                                |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Standard production plot.                                                                                                                  |
|      2 | Cold check.                                                                                                                                |
|      3 | Reference plot (off grid).                                                                                                                 |
|      4 | Training/practice plot (off grid).                                                                                                         |
|      5 | Botched plot file (disregard during data processing).                                                                                      |
|      6 | Blind check.                                                                                                                               |
|      7 | Hot check - This is the same as a standard production plot but the measurement is taken under the supervision of a quality assurance crew. |

## 5.1.11 CRWTYPCD

Crew type code. A code identifying the type of crew measuring the plot.

## Codes: CRWTYPCD

|   Code | Description                                           |
|--------|-------------------------------------------------------|
|      1 | Standard field crew.                                  |
|      2 | QA crew (any QA crew member present collecting data). |

## 5.1.12 SMPKNDCD

Sample kind code. A code indicating the type of plot installation.

## Codes: SMPKNDCD

|   Code | Description                                                         |
|--------|---------------------------------------------------------------------|
|      0 | Periodic inventory plot.                                            |
|      1 | Initial installation of a national design plot.                     |
|      2 | Remeasurement of previously installed national design plot.         |
|      3 | Replacement of previously installed national design plot.           |
|      4 | Modeled periodic inventory plot (Northeast and North Central only). |

## 5.1.13 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 5.1.14 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 5.1.15 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 5.1.16 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 5.1.17 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 5.1.18 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 5.1.19 CWD\_SAMPLE\_METHOD

Coarse woody debris sample method. A code indicating the sampling protocol used to collect coarse woody debris data.

## Codes: CWD\_SAMPLE\_METHOD

|   Code | Description                                                                                       | Distance measurement   |
|--------|---------------------------------------------------------------------------------------------------|------------------------|
|      0 | CWD not sampled.                                                                                  | Not applicable.        |
|      1 | National P3 protocol. Three 24-foot transects on all subplots.                                    | Slope.                 |
|      2 | PNWRS P2 protocol. Two 58.9-foot transects per subplot.                                           | Slope.                 |
|      3 | PNWRS P2 and National P3 protocols overlaid. One 24-foot and two 58.9-foot transects per subplot. | Slope.                 |
|      4 | PNWRS juniper protocol.                                                                           | Slope.                 |
|      5 | PNWRS P2 protocol. Two 24-foot transects per subplot.                                             | Slope.                 |
|      6 | National P2 protocol, base option.                                                                | Horizontal.            |
|      7 | National P2 protocol, wildlife option.                                                            | Horizontal.            |
|      8 | National P2 protocol, rapid assessment option.                                                    | Horizontal.            |

|   Code | Description                                                                                           | Distance measurement   |
|--------|-------------------------------------------------------------------------------------------------------|------------------------|
|      9 | National P3 protocol. Two 24-foot transects per subplot.                                              | Slope.                 |
|     10 | RMRS P2 protocol. Three 120-foot transects per plot.                                                  | Slope.                 |
|     11 | SRS P2 protocol. One 48-foot transect only on subplot 1 (random orientation).                         | Horizontal.            |
|     12 | PNWRS P2 protocol, transition wildlife. Two 24-foot transects per subplot.                            | Horizontal.            |
|     13 | PNWRS P2 protocol for National Forest System, transition wildlife. Two 24 foot transects per subplot. | Horizontal.            |
|     14 | National P2 protocol, wildlife for National Forest System. Two 24-foot transects per subplot.         | Horizontal.            |
|     15 | PNWRS periodic protocol. Three 55.6-foot transects per subplot.                                       | Horizontal.            |
|     16 | PNWRS periodic protocol. Three 55.8-foot transects per subplot.                                       | Horizontal.            |
|     17 | National P2 and P3 protocol (2001). Three 58.9-foot transects per subplot.                            | Horizontal.            |

## 5.1.20 FWD\_SAMPLE\_METHOD

Fine woody debris sample method. A code indicating the sampling protocol used to collect fine woody debris data.

## Codes: FWD\_SAMPLE\_METHOD

|   Code | Description                                                                                                                           | Distance measurement   |
|--------|---------------------------------------------------------------------------------------------------------------------------------------|------------------------|
|      0 | FWD not sampled.                                                                                                                      | Not applicable.        |
|      1 | National P2 and P3 protocol. One 10-foot transect for small and medium FWD and one 20-foot transect for large FWD per subplot.        | Slope.                 |
|      2 | National P2 and P3 protocol. One 6-foot transect for small and medium FWD and one 10-foot transect for large FWD per subplot.         | Slope.                 |
|      3 | National P2 protocol (all options). One 6-foot transect for small and medium FWD and one 10-foot transect for large FWD per subplot.  | Horizontal.            |
|      4 | SRS P2 protocol. One 6-foot transect for small and medium FWD, and one 10-foot transect for large FWD on subplot 1.                   | Slope.                 |
|      5 | RMRS P2 protocol. One 6-foot transect for small and medium FWD and one 10-foot transect for large FWD on each of subplots 2, 3 and 4. | Slope.                 |

## 5.1.21 MICR\_SAMPLE\_METHOD

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

## 5.1.22 DLF\_SAMPLE\_METHOD

Duff, litter, fuelbed sample method. A code indicating the sampling protocol used to collect duff, litter, and fuelbed data.

## Codes: DLF\_SAMPLE\_METHOD

|   Code | Description                                                                                                         | Distance measurement   |
|--------|---------------------------------------------------------------------------------------------------------------------|------------------------|
|      0 | Duff, litter, fuel not sampled.                                                                                     | Not applicable.        |
|      1 | National P3 protocol. Sampled at 2 points (14 and 24 feet) along each transect with average recorded.               | Slope.                 |
|      2 | National P3 protocol. Sampled at a point located 24 feet along each transect.                                       | Slope.                 |
|      3 | National P2 protocol (all options). Sampled at a point 24 feet along each transect.                                 | Horizontal.            |
|      4 | RMRS P2 protocol. One duff and litter point sampled at a point 24 feet along each transect on subplots 2, 3, and 4. | Horizontal.            |
|      5 | SRS P2 protocol. Duff and litter points sampled at 2 points (0 and 48 feet) along a transect on subplot 1.          | Horizontal.            |

## 5.1.23 PILE\_SAMPLE\_METHOD

Pile sample method. A code indicating the sampling protocol used to collect residue pile data.

## Codes: PILE\_SAMPLE\_METHOD

|   Code | Description                                                                                                                                 | Distance measurement   |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------|------------------------|
|      0 | Piles not sampled.                                                                                                                          | Not applicable.        |
|      1 | PNWRS P2 protocol. Pile measured if center located within the 58.9-foot macroplot radius.                                                   | Horizontal.            |
|      2 | National P3 protocol. Pile measured if center located within the 24-foot subplot radius.                                                    | Horizontal.            |
|      3 | National P2 protocol (all options). Pile measured if it intersects the transect (see DWM_VISIT.DWM_TRANSECT_LENGTH for length of transect). | Horizontal.            |

|   Code | Description                                                                                                       | Distance measurement   |
|--------|-------------------------------------------------------------------------------------------------------------------|------------------------|
|      4 | Pile is on 58.9-foot transect.                                                                                    | Horizontal.            |
|      5 | Pile measured if center located within the 58.9-foot transect conditions were mapped only on the 24-foot subplot. | Horizontal.            |

## 5.1.24 DWM\_SAMPLING\_STATUS\_CD

DWM sampling status code. A code indicating the type of National P2 DWM data collected.

## Codes: DWM\_SAMPLING\_STATUS\_CD

|   Code | Description                                                                                                                                                                                                                              |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | Not sampled for National P2 DWM.                                                                                                                                                                                                         |
|      1 | BASE sampling option; includes DWM attributes needed to estimate volume, biomass, and carbon of down wood on land conditions sampled with the National P2 DWM protocol.                                                                  |
|      2 | Wildlife/Ecological sampling option; includes BASE attributes along with other attributes needed to estimate components of wildlife habitats or ecological functions collected on land conditions sampled with National P2 DWM protocol. |
|      3 | Rapid assessment sampling option; includes BASE attributes along with other optional attributes selected for individual situations on land conditions sampled under National P2 DWM protocol.                                            |

## 5.1.25 DWM\_NBR\_SUBP

DWM number of subplots. The number of subplots on which National P2 DWM data were collected: 1, 2, 3, or 4.

## 5.1.26 DWM\_NBR\_SUBP\_TRANSECT

DWM number of transects on subplot. The number of transects per subplot on which National P2 DWM data were collected: 1, 2, or 3.

## 5.1.27 DWM\_SUBPLIST

DWM subplot list. The list of subplots on which National P2 DWM data were collected. The list is a concatenation of the four subplots. Subplots not included are coded as 0. For example, if National P2 DWM data are collected on subplots 1, 2, and 3, then DWM\_SUBPLIST = 1230.

## 5.1.28 DWM\_TRANSECT\_LENGTH

DWM transect length. The length of National P2 DWM transects in feet. Values must be between 24.0 and 58.9 feet.

## 5.1.29 QA\_STATUS

Quality assurance status. A code indicating the type of plot data collected. Production plots have QA\_STATUS = 1 or 7. Codes 2-6 indicate additional quality assurance data. May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0.

Note: QASTATCD and QA\_STATUS both reside in this table and have the same description and codes. QASTATCD is a remnant from the Forest Health Monitoring and Phase 3 data collection files, and is retained in this table for continuity with older data.

## Codes: QA\_STATUS

|   Code | Description                                                                                                                                |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Standard production plot.                                                                                                                  |
|      2 | Cold check.                                                                                                                                |
|      3 | Reference plot (off grid).                                                                                                                 |
|      4 | Training/practice plot (off grid).                                                                                                         |
|      5 | Botched plot file (disregard during data processing).                                                                                      |
|      6 | Blind check.                                                                                                                               |
|      7 | Hot check - This is the same as a standard production plot but the measurement is taken under the supervision of a quality assurance crew. |