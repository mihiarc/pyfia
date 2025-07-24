# Section 5.2: Down Woody Material Coarse Woody Debris Table
**Oracle Table Name:** DWM_COARSE_WOODY_DEBRIS
**Extracted Pages:** 347-360 (Chapter pages 5-11 to 5-24)
**Source:** FIA Database Handbook v9.3
**Chapter:** 5 - Database Tables - Down Woody Material

---

## 5.2 Down Woody Material Coarse Woody Debris Table

## (Oracle table name: DWM\_COARSE\_WOODY\_DEBRIS)

| Subsection   | Column name (attribute)   | Descriptive name                                                | Oracle data type   |
|--------------|---------------------------|-----------------------------------------------------------------|--------------------|
| 5.2.1        | CN                        | Sequence number                                                 | VARCHAR2(34)       |
| 5.2.2        | PLT_CN                    | Plot sequence number                                            | VARCHAR2(34)       |
| 5.2.3        | INVYR                     | Inventory year                                                  | NUMBER(4)          |
| 5.2.4        | STATECD                   | State code                                                      | NUMBER(4)          |
| 5.2.5        | COUNTYCD                  | County code                                                     | NUMBER(3)          |
| 5.2.6        | PLOT                      | Plot number                                                     | NUMBER(5)          |
| 5.2.7        | SUBP                      | Subplot number                                                  | NUMBER(1)          |
| 5.2.8        | TRANSECT                  | Transect                                                        | NUMBER(3)          |
| 5.2.9        | CWDID                     | Coarse woody debris piece (log) number                          | NUMBER             |
| 5.2.10       | MEASYEAR                  | Measurement year                                                | NUMBER(4)          |
| 5.2.11       | CONDID                    | Condition class number                                          | NUMBER(1)          |
| 5.2.12       | SLOPDIST                  | Slope distance                                                  | NUMBER             |
| 5.2.13       | HORIZ_DIST                | Horizontal distance                                             | NUMBER             |
| 5.2.14       | SPCD                      | Species code                                                    | NUMBER             |
| 5.2.15       | DECAYCD                   | Decay class code                                                | NUMBER(1)          |
| 5.2.16       | TRANSDIA                  | Transect diameter                                               | NUMBER(3)          |
| 5.2.17       | SMALLDIA                  | Small diameter                                                  | NUMBER(3)          |
| 5.2.18       | LARGEDIA                  | Large diameter                                                  | NUMBER(3)          |
| 5.2.19       | LENGTH                    | Length of the piece                                             | NUMBER(3)          |
| 5.2.20       | HOLLOWCD                  | Hollow code                                                     | VARCHAR2(1)        |
| 5.2.21       | CWDHSTCD                  | Coarse woody debris history code                                | NUMBER(1)          |
| 5.2.22       | VOLCF                     | Gross cubic-foot volume of the piece                            | NUMBER             |
| 5.2.23       | DRYBIO                    | Dry biomass of the piece                                        | NUMBER             |
| 5.2.24       | CARBON                    | Carbon weight of the piece                                      | NUMBER             |
| 5.2.25       | COVER_PCT                 | Percent cover represented by each coarse woody debris piece     | NUMBER             |
| 5.2.26       | LPA_UNADJ                 | Number of logs (pieces) per acre, unadjusted                    | NUMBER             |
| 5.2.27       | LPA_PLOT                  | Number of logs (pieces) per acre on the plot, unadjusted        | NUMBER             |
| 5.2.28       | LPA_COND                  | Number of logs (pieces) per acre in the condition, unadjusted   | NUMBER             |
| 5.2.29       | LPA_UNADJ_RGN             | Number of logs (pieces) per acre, unadjusted, regional protocol | NUMBER             |

| Subsection   | Column name (attribute)   | Descriptive name                                                                                  | Oracle data type   |
|--------------|---------------------------|---------------------------------------------------------------------------------------------------|--------------------|
| 5.2.30       | LPA_PLOT_RGN              | Number of logs (pieces) per acre on the plot, unadjusted, regional protocol                       | NUMBER             |
| 5.2.31       | LPA_COND_RGN              | Number of logs (pieces) per acre in the condition, unadjusted, regional protocol                  | NUMBER             |
| 5.2.32       | COVER_PCT_RGN             | Percent cover, represented by each coarse woody debris piece, regional protocol                   | NUMBER             |
| 5.2.33       | CHARRED_CD                | Charred by fire code                                                                              | NUMBER(1)          |
| 5.2.34       | ORNTCD_PNWRS              | Orientation code, Pacific Northwest Research Station                                              | VARCHAR2(1)        |
| 5.2.35       | CREATED_BY                | Created by                                                                                        | VARCHAR2(30)       |
| 5.2.36       | CREATED_DATE              | Created date                                                                                      | DATE               |
| 5.2.37       | CREATED_IN_INSTANCE       | Created in instance                                                                               | VARCHAR2(6)        |
| 5.2.38       | MODIFIED_BY               | Modified by                                                                                       | VARCHAR2(30)       |
| 5.2.39       | MODIFIED_DATE             | Modified date                                                                                     | DATE               |
| 5.2.40       | MODIFIED_IN_INSTANCE      | Modified in instance                                                                              | VARCHAR2(6)        |
| 5.2.41       | CWD_SAMPLE_METHOD         | Coarse woody debris sample method                                                                 | VARCHAR2(6)        |
| 5.2.42       | HOLLOW_DIA                | Hollow diameter at the point of intersection                                                      | NUMBER(3)          |
| 5.2.43       | HORIZ_DIST_CD             | Horizontal distance code                                                                          | NUMBER(1)          |
| 5.2.44       | INCLINATION               | Piece inclination                                                                                 | NUMBER(2)          |
| 5.2.45       | LARGE_END_DIA_CLASS       | Large end diameter class code                                                                     | NUMBER(1)          |
| 5.2.46       | LENGTH_CD                 | Coarse woody debris length code                                                                   | NUMBER(1)          |
| 5.2.47       | VOLCF_AC_UNADJ            | Gross cubic-foot volume per acre based on target plot transect length, unadjusted                 | NUMBER             |
| 5.2.48       | VOLCF_AC_PLOT             | Gross cubic-foot volume per acre based on plot transect length actually measured, unadjusted      | NUMBER             |
| 5.2.49       | VOLCF_AC_COND             | Gross cubic-foot volume per acre based on condition transect length actually measured, unadjusted | NUMBER             |
| 5.2.50       | DRYBIO_AC_UNADJ           | Dry biomass per acre based on target plot transect length, unadjusted                             | NUMBER             |
| 5.2.51       | DRYBIO_AC_PLOT            | Dry biomass per acre based on plot transect length actually measured, unadjusted                  | NUMBER             |

| Subsection   | Column name (attribute)   | Descriptive name                                                                      | Oracle data type   |
|--------------|---------------------------|---------------------------------------------------------------------------------------|--------------------|
| 5.2.52       | DRYBIO_AC_COND            | Dry biomass per acre based on condition transect length actually measured, unadjusted | NUMBER             |
| 5.2.53       | CARBON_AC_UNADJ           | Carbon per acre based on target plot transect length, unadjusted                      | NUMBER             |
| 5.2.54       | CARBON_AC_PLOT            | Carbon per acre based on plot transect length actually measured, unadjusted           | NUMBER             |
| 5.2.55       | CARBON_AC_COND            | Carbon per acre based on condition transect length actually measured, unadjusted      | NUMBER             |

| Key Type   | Column(s) order                                       | Tables to link   | Abbreviated notation   |
|------------|-------------------------------------------------------|------------------|------------------------|
| Primary    | CN                                                    | N/A              | DCW_PK                 |
| Unique     | PLT_CN, TRANSECT, SUBP, CWDID                         | N/A              | DCW_UK                 |
| Natural    | STATECD, INVYR, COUNTYCD, PLOT, TRANSECT, SUBP, CWDID | N/A              | DCW_NAT_I              |

## 5.2.1 CN

Sequence number. A unique sequence number used to identify a down woody material coarse woody debris (CWD) record.

## 5.2.2 PLT\_CN

Plot sequence number. Foreign key linking the down woody material coarse woody debris record to the plot record.

## 5.2.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 5.2.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 5.2.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 5.2.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combination of attributes, PLOT may be used to uniquely identify a plot.

## 5.2.7 SUBP

Subplot number. A code indicating the number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 5.2.8 TRANSECT

Transect. The azimuth, in degrees, of the transect on which coarse woody debris was sampled, extending out from subplot center.

## 5.2.9 CWDID

Coarse woody debris piece (log) number. A number that uniquely identifies each piece that was tallied along one transect.

## 5.2.10 MEASYEAR

Measurement year. The year in which the plot was completed. MEASYEAR may differ from INVYR.

## 5.2.11 CONDID

Condition class number. The unique identifying number assigned to the condition where the piece was sampled. See COND.CONDID for details on the attributes which delineate a condition.

## 5.2.12 SLOPDIST

Slope distance. The slope distance, in feet, between the subplot center and the point where the transect intersects the longitudinal center of the piece.

## 5.2.13 HORIZ\_DIST

Horizontal distance. The horizontal distance, in feet, between subplot center and the point where the transect intersects the longitudinal center of the piece.

## 5.2.14 SPCD

Species code. An FIA tree species code. Refer to appendix F for codes. If the piece is the woody stem of a shrub, a code of 001 is recorded.

## 5.2.15 DECAYCD

Decay class code. A code indicating the stage of decay that predominates along the recorded total length of the piece. DECAYCD is used to reduce biomass based on ratios stored in the REF\_SPECIES table.

Note: Pieces within decay class 5 must still resemble a log; the pieces must be  5.0 inches in diameter,  5.0 inches from the surface of the ground, and at least 3.0 feet long.

## Codes: DECAYCD

|   Decay class | Structural integrity                                                              | Texture of rotten portions                                                                | Color of wood                     | Invading roots   | Branches and twigs                                                                            |
|---------------|-----------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|-----------------------------------|------------------|-----------------------------------------------------------------------------------------------|
|             1 | Sound, freshly fallen, intact logs.                                               | Intact, no rot; conks of stem decay absent.                                               | Original color.                   | Absent.          | If branches are present, fine twigs are still attached and have tight bark.                   |
|             2 | Sound.                                                                            | Mostly intact; sapwood partly soft (starting to decay) but can't be pulled apart by hand. | Original color.                   | Absent.          | If branches are present, many fine twigs are gone and remaining fine twigs have peeling bark. |
|             3 | Heartwood sound; piece supports its own weight.                                   | Hard, large pieces; sapwood can be pulled apart by hand or sapwood absent.                | Reddish- brown or original color. | Sapwood only.    | Branch stubs will not pull out.                                                               |
|             4 | Heartwood rotten; piece does not support its own weight, but maintains its shape. | Soft, small blocky pieces; a metal pin can be pushed into heartwood.                      | Reddish or light brown.           | Throughout.      | Branch stubs pull out.                                                                        |
|             5 | None; piece no longer maintains its shape, it spreads out on ground.              | Soft; powdery when dry.                                                                   | Red-brown to dark brown.          | Throughout.      | Branch stubs and pitch pockets have usually rotted down.                                      |

## 5.2.16 TRANSDIA

Transect diameter. The diameter, in inches, at the point where the longitudinal center of the piece intersects the transect.

## 5.2.17 SMALLDIA

Small diameter. The diameter, in inches, at the small end of the piece, or at the point where the piece tapers down to 3 inches. If the small end is splintered or decomposing, the diameter is measured at a point that best represents the overall volume of the piece.

## 5.2.18 LARGEDIA

Large diameter. The diameter, in inches, at the large end of the piece, or at the point just above the root collar. If the end is splintered or decomposing, the diameter is measured at a point that best represents the overall volume of the piece.

## 5.2.19 LENGTH

Length of the piece. Length, in feet, of the piece, measured between the small- and large-end diameters, or if the piece is decay class 5, between the physical ends of the piece.

## 5.2.20 HOLLOWCD

Hollow code. A code indicating whether or not the piece is hollow. If the piece has a cavity that extends at least 2 feet along the central longitudinal axis and the diameter of the cavity entrance is at least ¼ of the diameter at the end of the piece, it is classified as hollow.

## Codes: HOLLOWCD

| Code   | Description              |
|--------|--------------------------|
| Y      | The piece is hollow.     |
| N      | The piece is not hollow. |

## 5.2.21 CWDHSTCD

Coarse woody debris history code. A code indicating whether or not the piece is on the ground as a result of harvesting operations or as a result of natural circumstances.

## Codes: CWDHSTCD

|   Code | Description                                                                                 |
|--------|---------------------------------------------------------------------------------------------|
|      1 | CWD piece is on the ground as a result of natural causes.                                   |
|      2 | CWD piece is on the ground as a result of major recent harvest activity (  15 yrs old).    |
|      3 | CWD piece is on the ground as a result of older harvest activity (>15 yrs old).             |
|      4 | CWD piece is on the ground as a result of an incidental harvest (such as firewood cutting). |
|      5 | Exact reason unknown.                                                                       |

## 5.2.22 VOLCF

Gross cubic-foot volume of the piece. The gross volume, in cubic feet, estimated for the piece, based on length and either the small- and large-end diameter or just the transect diameter. This is a per piece value and must be multiplied by one of the logs per acre (LPA) to obtain per acre information.

## 5.2.23 DRYBIO

Dry biomass of the piece. The oven-dry biomass, in pounds, estimated for the piece, adjusted for the degree of decomposition based on DECAYCD. This is a per piece value and must be multiplied by one of the logs per acre (LPA) to obtain per acre information.

## 5.2.24 CARBON

Carbon weight of the piece. The weight of carbon, in pounds, estimated for the piece, adjusted for the degree of decomposition based on DECAYCD. This is a per piece value and must be multiplied by one of the logs per acre (LPA) to obtain per acre information.

## 5.2.25 COVER\_PCT

Percent cover represented by each coarse woody debris piece. An estimate of the percent of the condition area covered by the piece.

## 5.2.26 LPA\_UNADJ

Number of logs (pieces) per acre, unadjusted. This estimate is the number of logs per acre the individual piece represents. The estimate is based on the target transect length (COND\_DWM\_CALC.CWD\_TL\_UNADJ), which is the total length of transect that could

potentially be installed on the plot, before adjustment for partially nonsampled plots in the stratum. This attribute is used to calculate population estimates and not to derive estimates for one condition or individual plot. It should be summed for a condition or plot, adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table, and then expanded by the acres in POP\_STRATUM.EXPNS to produce population totals for number of CWD logs in an area of interest (e.g., State). It is important to select the appropriate EVALID and use the LPA column associated with that evaluation (see LPA\_UNADJ\_RGN).

## 5.2.27 LPA\_PLOT

Number of logs (pieces) per acre on the plot, unadjusted. This estimate is the number of logs per acre the individual piece represents on the plot. The estimate is based on the actual length of transect installed and sampled on the plot. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual plot locations, and is not adjusted or used to develop population estimates. It is important to select the appropriate EVALID and use the LPA column associated with that evaluation (see LPA\_PLOT\_RGN).

## 5.2.28 LPA\_COND

Number of logs (pieces) per acre in the condition, unadjusted. This estimate is the number of logs per acre the individual piece represents on one condition on the plot. The estimate is based on the actual length of transect installed and sampled on that condition. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual conditions within a plot, and is not adjusted or used to develop population estimates. It is important to select the appropriate EVALID and use the LPA column associated with that evaluation (see LPA\_COND\_RGN).

## 5.2.29 LPA\_UNADJ\_RGN

Number of logs (pieces) per acre, unadjusted, regional protocol. This estimate is the number of logs per acre the individual piece represents when sampled using a regional protocol that differs from the national core design. The estimate is based on the target transect length (COND\_DWM\_CALC.CWD\_TL\_UNADJ), which is the total length of transect that could potentially be installed on the plot using the regional sampling protocol, before adjustment for partially nonsampled plots in the stratum. This attribute is used to calculate population estimates and not to derive estimates for one condition or individual plot. It should be summed for a condition or plot, adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table, and then expanded by the acres in POP\_STRATUM.EXPNS to produce population totals for number of CWD logs in an area of interest (e.g., State). This column will be populated for all plots sampled with a regional protocol, where transect length and configuration differ from the core design. When regional protocols and core designs are overlaid, those pieces that fall only on the core design will have null in this field (e.g., this column contains data for RSCD = 26, where a regional protocol was used to sample all Phase 2 plots in the inventory). Contact FIA work units (table 1-1) for information on regional sampling protocol. It is important to select the appropriate EVALID and use the LPA column associated with that evaluation (see LPA\_UNADJ).

## 5.2.30 LPA\_PLOT\_RGN

Number of logs (pieces) per acre on the plot, unadjusted, regional protocol. This estimate is the number of logs per acre the individual piece represents on the plot when sampled using a regional protocol that differs from the national core design. The estimate is based on the actual length of transect installed and sampled on the plot. This attribute

is useful for analysis projects that involve modeling, mapping, or classifying individual plot locations, and is not adjusted or used to develop population estimates. This column will be populated for all plots sampled with a regional protocol, where transect length and configuration differ from the core design. When regional protocols and core designs are overlaid, those pieces that fall only on the core design will have null in this field (e.g., this column contains data for RSCD = 26, where a regional protocol was used to sample all Phase 2 plots in the inventory). Contact FIA work units (table 1-1) for information on regional sampling protocol. It is important to select the appropriate EVALID and use the LPA column associated with that evaluation (see LPA\_PLOT).

## 5.2.31 LPA\_COND\_RGN

Number of logs (pieces) per acre in the condition, unadjusted, regional protocol. This estimate is the number of logs per acre the individual piece represents on one condition on the plot when sampled using a regional protocol that differs from the national core design. The estimate is based on the actual length of transect installed and sampled on that condition. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual conditions within a plot, and is not adjusted or used to develop population estimates. This column will be populated for all plots sampled with a regional protocol, where transect length and configuration differ from the core design. When regional protocols and core designs are overlaid, those pieces that fall only on the core design will have null in this field (e.g., this column contains data for RSCD = 26, where a regional protocol was used to sample all Phase 2 plots in the inventory). Contact FIA work units (table 1-1) for information on regional sampling protocol. It is important to select the appropriate EVALID and use the LPA column associated with that evaluation (see LPA\_COND).

## 5.2.32 COVER\_PCT\_RGN

Percent cover, represented by each coarse woody debris piece, regional protocol. An estimate of the percent of the condition area covered by the piece, when sampled using a regional protocol.

## 5.2.33 CHARRED\_CD

Charred by fire code. A code indicating the percentage of the piece's surface that has been charred by fire. This attribute was required by some regional protocols and is optional for the National P2 DWM protocol. CHARRED\_CD replaces CHRCD\_PNWRS; the code sets are the same.

## Codes: CHARRED\_CD

|   Code | Description                                  |
|--------|----------------------------------------------|
|      0 | None of the piece is charred by fire.        |
|      1 | Up to 1/3 of the piece is charred by fire.   |
|      2 | 1/3 to 2/3 of the piece is charred by fire.  |
|      3 | 2/3 or more of the piece is charred by fire. |

## 5.2.34 ORNTCD\_PNWRS

Orientation code, Pacific Northwest Research Station. A code indicating the orientation of the piece on the slope. Data collected for field guide (PLOT.MANUAL) versions 1.4-1.7 (INVYR = 2000-2004).

## Codes: ORNTCD\_PNWRS

| Code   | Description                                                                     |
|--------|---------------------------------------------------------------------------------|
| A      | Across - Piece is oriented between vertical and horizontal.                     |
| F      | Flat - Piece is on flat ground (<10% slope).                                    |
| H      | Horizontal - Piece is oriented within 15 degrees of the contour.                |
| V      | Vertical - Piece is oriented within 15 degrees of perpendicular to the contour. |

## 5.2.35 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 5.2.36 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 5.2.37 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 5.2.38 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 5.2.39 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 5.2.40 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 5.2.41 CWD\_SAMPLE\_METHOD

Coarse woody debris sample method. A code indicating the sampling protocol used to collect coarse woody debris data.

## Codes: CWD\_SAMPLE\_METHOD

|   Code | Description                                                                                       | Transect distance measurement   |
|--------|---------------------------------------------------------------------------------------------------|---------------------------------|
|      0 | CWD not sampled.                                                                                  | Not applicable.                 |
|      1 | National P3 protocol. Three 24-foot transects on all subplots.                                    | Slope.                          |
|      2 | PNWRS P2 protocol. Two 58.9-foot transects per subplot.                                           | Slope.                          |
|      3 | PNWRS P2 and National P3 protocols overlaid. One 24-foot and two 58.9-foot transects per subplot. | Slope.                          |
|      4 | PNWRS juniper protocol.                                                                           | Slope.                          |
|      5 | PNWRS P2 protocol. Two 24-foot transects per subplot.                                             | Slope.                          |
|      6 | National P2 protocol, base option.                                                                | Horizontal.                     |
|      7 | National P2 protocol, wildlife option.                                                            | Horizontal.                     |
|      8 | National P2 protocol, rapid assessment option.                                                    | Horizontal.                     |
|      9 | National P3 protocol. Two 24-foot transects per subplot.                                          | Slope.                          |
|     10 | RMRS P2 protocol. Three 120-foot transects per plot.                                              | Slope.                          |

|   Code | Description                                                                                           | Transect distance measurement   |
|--------|-------------------------------------------------------------------------------------------------------|---------------------------------|
|     11 | SRS P2 protocol. One 48-foot transect only on subplot 1 (random orientation).                         | Horizontal.                     |
|     12 | PNWRS P2 protocol, transition wildlife. Two 24-foot transects per subplot.                            | Horizontal.                     |
|     13 | PNWRS P2 protocol for National Forest System, transition wildlife. Two 24-foot transects per subplot. | Horizontal.                     |
|     14 | National P2 protocol, wildlife for National Forest System. Two 24-foot transects per subplot.         | Horizontal.                     |
|     15 | PNWRS periodic protocol. Three 55.6-foot transects per subplot.                                       | Horizontal.                     |
|     16 | PNWRS periodic protocol. Three 55.8-foot transects per subplot.                                       | Horizontal.                     |
|     17 | National P2 and P3 protocol (2001). Three 58.9-foot transects per subplot.                            | Horizontal.                     |

## 5.2.42 HOLLOW\_DIA

Hollow diameter at the point of intersection. The diameter of the hollow portion of a piece at the point of intersection with the transect, measured in inches. Required for all options of the National P2 DWM protocol.

## 5.2.43 HORIZ\_DIST\_CD

Horizontal distance code. A code indicating if a piece intersects the transect on the subplot or macroplot. Required for all options of the National P2 DWM protocol.

## Codes: HORIZ\_DIST\_CD

|   Code | Description                                                                                              |
|--------|----------------------------------------------------------------------------------------------------------|
|      1 | Central longitudinal axis of piece intersects the transect on the subplot (  24.0 horizontal feet).     |
|      2 | Central longitudinal axis of piece intersects the transect on the macroplot (24.1-58.9 horizontal feet). |

## 5.2.44 INCLINATION

Piece inclination. (core optional) The inclination of the piece from horizontal measured in degrees (0 to 90). This is an optional measurement and might not be populated on every record.

## 5.2.45 LARGE\_END\_DIA\_CLASS

Large end diameter class code. (core optional) A code indicating the diameter class of the large end of a piece of coarse woody debris. This is an optional measurement and might not be populated on every record.

## Codes: LARGE\_END\_DIA\_CLASS

|   Code | Description          |
|--------|----------------------|
|      1 | 3.0 to 4.9 inches.   |
|      2 | 5.0 to 8.9 inches.   |
|      3 | 9.0 to 14.9 inches.  |
|      4 | 15.0 to 20.9 inches. |
|      5 | 21.0 to 39.9 inches. |
|      6 | 40.0+ inches.        |

## 5.2.46 LENGTH\_CD

Coarse woody debris length code. A code indicating the length class of the piece. Codes identify whether the piece is between 0.5 feet and less than 3.0 feet in length, or greater than or equal to 3.0 feet. This is used to correctly filter pieces when combining plots from different protocols. Older protocols only measured pieces  3.0 feet in length.

## Codes: LENGTH\_CD

|   Code | Description                                    |
|--------|------------------------------------------------|
|      1 | CWD piece length is  3.0 feet.               |
|      2 | CWD piece length is  0.5 feet and  3.0 feet. |

## 5.2.47 VOLCF\_AC\_UNADJ

Gross cubic-foot volume per acre based on target plot transect length, unadjusted. This estimate is the gross cubic-foot volume per acre the individual piece represents. The estimate is based on the target transect length (COND\_DWM\_CALC.CWD\_TL\_UNADJ), which is the total length of transect that could potentially be installed on the plot, before adjustment for partially nonsampled plots in the stratum. This attribute is used to calculate population estimates and not to derive estimates for one condition or individual plot. It should be summed for a condition or plot, adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table, and then expanded by the acres in POP\_STRATUM.EXPNS to produce population totals for gross cubic-foot volume of CWD logs in an area of interest (e.g., State).

## 5.2.48 VOLCF\_AC\_PLOT

Gross cubic-foot volume per acre based on plot transect length actually measured, unadjusted. This estimate is the gross cubic-foot volume per acre the individual piece represents on the plot. The estimate is based on the actual length of transect installed and sampled on the plot. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual plot locations, and is not adjusted or used to develop population estimates.

## 5.2.49 VOLCF\_AC\_COND

Gross cubic-foot volume per acre based on condition transect length actually measured, unadjusted. This estimate is the gross cubic-foot volume per acre the individual piece represents on one condition on the plot. The estimate is based on the actual length of transect installed and sampled on that condition. This attribute is useful

for analysis projects that involve modeling, mapping, or classifying individual conditions within a plot, and is not adjusted or used to develop population estimates.

## 5.2.50 DRYBIO\_AC\_UNADJ

Dry biomass per acre based on target plot transect length, unadjusted. This estimate is the oven-dry weight of biomass, in pounds per acre, that the individual piece represents. The estimate is based on the target transect length

(COND\_DWM\_CALC.CWD\_TL\_UNADJ), which is the total length of transect that could potentially be installed on the plot, before adjustment for partially nonsampled plots in the stratum. This attribute is used to calculate population estimates and not to derive estimates for one condition or individual plot. It should be summed for a condition or plot, adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table, and then expanded by the acres in POP\_STRATUM.EXPNS to produce population totals for biomass of CWD logs in an area of interest (e.g., State).

## 5.2.51 DRYBIO\_AC\_PLOT

Dry biomass per acre based on plot transect length actually measured, unadjusted.

This estimate is the oven-dry weight of biomass, in pounds per acre, that the individual piece represents on the plot. The estimate is based on the actual length of transect installed and sampled on the plot. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual plot locations, and is not adjusted or used to develop population estimates.

## 5.2.52 DRYBIO\_AC\_COND

## Dry biomass per acre based on condition transect length actually measured,

unadjusted. This estimate is the oven-dry weight of biomass, in pounds per acre, that the individual piece represents on one condition on the plot. The estimate is based on the actual length of transect installed and sampled on that condition. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual conditions within a plot, and is not adjusted or used to develop population estimates.

## 5.2.53 CARBON\_AC\_UNADJ

Carbon per acre based on target plot transect length, unadjusted. This estimate is the weight of carbon, in pounds per acre, that the individual piece represents. The estimate is based on the target transect length (COND\_DWM\_CALC.CWD\_TL\_UNADJ), which is the total length of transect that could potentially be installed on the plot, before adjustment for partially nonsampled plots in the stratum. This attribute is used to calculate population estimates and not to derive estimates for one condition or individual plot. It should be summed for a condition or plot, adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table, and then expanded by the acres in POP\_STRATUM.EXPNS to produce population totals for carbon of CWD logs in an area of interest (e.g., State).

## 5.2.54 CARBON\_AC\_PLOT

Carbon per acre based on plot transect length actually measured, unadjusted. This estimate is the weight of carbon, in pounds per acre, that the individual piece represents on the plot. The estimate is based on the actual length of transect installed and sampled on the plot. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual plot locations, and is not adjusted or used to develop population estimates.

## 5.2.55 CARBON\_AC\_COND

Carbon per acre based on condition transect length actually measured, unadjusted. This estimate is the weight of carbon, in pounds per acre, that the individual piece represents on one condition on the plot. The estimate is based on the actual length of transect installed and sampled on that condition. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual conditions within a plot, and is not adjusted or used to develop population estimates.