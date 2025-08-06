# Section 5.6: Down Woody Material Residual Pile Table
**Oracle Table Name:** DWM_RESIDUAL_PILE
**Extracted Pages:** 379-386 (Chapter pages 5-43 to 5-50)
**Source:** FIA Database Handbook v9.3
**Chapter:** 5 - Database Tables - Down Woody Material

---

## 5.6 Down Woody Material Residual Pile Table (Oracle table name: DWM\_RESIDUAL\_PILE)

| Subsection   | Column name (attribute)   | Descriptive name                                     | Oracle data type   |
|--------------|---------------------------|------------------------------------------------------|--------------------|
| 5.6.1        | CN                        | Sequence number                                      | VARCHAR2(34)       |
| 5.6.2        | PLT_CN                    | Plot sequence number                                 | VARCHAR2(34)       |
| 5.6.3        | INVYR                     | Inventory year                                       | NUMBER(4)          |
| 5.6.4        | STATECD                   | State code                                           | NUMBER(4)          |
| 5.6.5        | COUNTYCD                  | County code                                          | NUMBER(3)          |
| 5.6.6        | PLOT                      | Plot number                                          | NUMBER(5)          |
| 5.6.7        | SUBP                      | Subplot number                                       | NUMBER(1)          |
| 5.6.8        | PILE                      | Pile number                                          | NUMBER             |
| 5.6.9        | MEASYEAR                  | Measurement year                                     | NUMBER(4)          |
| 5.6.10       | CONDID                    | Condition class number                               | NUMBER(1)          |
| 5.6.11       | SHAPECD                   | Shape code                                           | NUMBER(1)          |
| 5.6.12       | AZIMUTH                   | Azimuth                                              | NUMBER(3)          |
| 5.6.13       | DENSITY                   | Density                                              | NUMBER(2)          |
| 5.6.14       | HEIGHT1                   | Height first measurement                             | NUMBER(2)          |
| 5.6.15       | WIDTH1                    | Width first measurement                              | NUMBER(2)          |
| 5.6.16       | LENGTH1                   | Length first measurement                             | NUMBER(2)          |
| 5.6.17       | HEIGHT2                   | Height second measurement                            | NUMBER(2)          |
| 5.6.18       | WIDTH2                    | Width second measurement                             | NUMBER(2)          |
| 5.6.19       | LENGTH2                   | Length second measurement                            | NUMBER(2)          |
| 5.6.20       | VOLCF                     | Gross cubic-foot volume of the residual pile         | NUMBER             |
| 5.6.21       | DRYBIO                    | Dry biomass of the residual pile                     | NUMBER             |
| 5.6.22       | CARBON                    | Carbon weight of the residual pile                   | NUMBER             |
| 5.6.23       | PPA_UNADJ                 | Piles per acre, unadjusted, for population estimates | NUMBER             |
| 5.6.24       | PPA_PLOT                  | Piles per acre, unadjusted, for plot estimates       | NUMBER             |
| 5.6.25       | PPA_COND                  | Piles per acre, unadjusted, for condition estimates  | NUMBER             |
| 5.6.26       | CREATED_BY                | Created by                                           | VARCHAR2(30)       |
| 5.6.27       | CREATED_DATE              | Created date                                         | DATE               |
| 5.6.28       | CREATED_IN_INSTANCE       | Created in instance                                  | VARCHAR2(6)        |
| 5.6.29       | MODIFIED_BY               | Modified by                                          | VARCHAR2(30)       |
| 5.6.30       | MODIFIED_IN_INSTANCE      | Modified in instance                                 | VARCHAR2(6)        |
| 5.6.31       | MODIFIED_DATE             | Modified date                                        | DATE               |

| Subsection   | Column name (attribute)   | Descriptive name                                   | Oracle data type   |
|--------------|---------------------------|----------------------------------------------------|--------------------|
| 5.6.32       | COMP_HT                   | Compacted height of the residual pile              | NUMBER(2)          |
| 5.6.33       | DECAYCD                   | Decay class code of the residual pile              | NUMBER(1)          |
| 5.6.34       | HORIZ_BEGNDIST            | Beginning horizontal distance of the residual pile | NUMBER(3,1)        |
| 5.6.35       | HORIZ_ENDDIST             | Ending horizontal distance of the residual pile    | NUMBER(3,1)        |
| 5.6.36       | PILE_SAMPLE_METHOD        | Pile sample method                                 | VARCHAR2(6)        |
| 5.6.37       | SPCD                      | Species code for the residual pile                 | NUMBER(4)          |
| 5.6.38       | TRANSECT                  | Transect                                           | NUMBER(3)          |

| Key Type   | Column(s) order                            | Tables to link   | Abbreviated notation   |
|------------|--------------------------------------------|------------------|------------------------|
| Primary    | CN                                         | N/A              | DRP_PK                 |
| Unique     | PLT_CN, SUBP, TRANSECT, PILE               | N/A              | DRP_UK                 |
| Natural    | STATECD, INVYR, COUNTYCD, PLOT, SUBP, PILE | N/A              | DRP_NAT_I              |

## 5.6.1 CN

Sequence number. A unique sequence number used to identify a down woody material residual pile record.

## 5.6.2 PLT\_CN

Plot sequence number. Foreign key linking the down woody material residual pile record to the plot record.

## 5.6.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 5.6.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 5.6.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 5.6.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combination of attributes, PLOT may be used to uniquely identify a plot.

## 5.6.7 SUBP

Subplot number. A code indicating the number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 5.6.8 PILE

Pile number. A number that uniquely identifies each pile tallied on a subplot.

## 5.6.9 MEASYEAR

Measurement year. The year in which the plot was completed. MEASYEAR may differ from INVYR.

## 5.6.10 CONDID

Condition class number. The unique identifying number assigned to the condition where the pile center is located. See COND.CONDID for details on the attributes that delineate a condition.

## 5.6.11 SHAPECD

Shape code. A code indicating the shape of the pile. The type of shape is used to select an equation to estimate pile cubic volume. See figure 5-1 below.

Figure 5-1: PILE SHAPE codes (Hardy 1996). Figure 14-12 from the Forest Inventory and Analysis National Core Field Guide (Phase 3, version 3.0).

<!-- image -->

## Codes: SHAPECD

|   Code | Description           |
|--------|-----------------------|
|      1 | Paraboloids.          |
|      2 | Half-cylinder.        |
|      3 | Half-frustum of cone. |
|      4 | Irregular solid.      |

## 5.6.12 AZIMUTH

Azimuth. The azimuth, to the nearest degree, from the subplot center to the pile. This azimuth centers on the pile so that it can be relocated. Due north is recorded as 360 degrees.

## 5.6.13 DENSITY

Density. A code indicating the percent of the pile that consists of woody material  3 inches. Air, soil, rock, and live plants are not included in the estimate. Estimated to the nearest 10 percent.

## Codes: DENSITY

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

## 5.6.14 HEIGHT1

Height first measurement. The estimated height, in feet, of either end of the pile. Pile HEIGHT1 may equal pile HEIGHT2. See figure 5-1 under SHAPECD.

## 5.6.15 WIDTH1

Width first measurement. The estimated width, in feet, of the side of HEIGHT1. Pile WIDTH1 may equal pile WIDTH2. See figure 5-1 under SHAPECD.

## 5.6.16 LENGTH1

Length first measurement. The estimated length, in feet, of either side of the pile. Pile LENGTH1 may equal pile LENGTH2. See figure 5-1 under SHAPECD.

## 5.6.17 HEIGHT2

Height second measurement. The estimated height, in feet, of either end of the pile. Pile HEIGHT1 may equal pile HEIGHT2. See figure 5-1 under SHAPECD.

## 5.6.18 WIDTH2

Width second measurement. The estimated width, in feet, of the side of HEIGHT2. Pile WIDTH1 may equal pile WIDTH2. See figure 5-1 under SHAPECD.

## 5.6.19 LENGTH2

Length second measurement. The length, in feet, of either side of the pile. Pile LENGTH1 may equal pile LENGTH2. See figure 5-1 under SHAPECD.

## 5.6.20 VOLCF

Gross cubic-foot volume of the residual pile. The gross volume, in cubic feet, of the pile, calculated with equations based on shape code and pile dimensions. This is an individual pile value and must be multiplied by one of the piles per acre (PPA) columns to obtain per acre information.

## 5.6.21 DRYBIO

Dry biomass of the residual pile. The oven-dry weight, in pounds, estimated for the pile. This is an individual pile value and must be multiplied by one of the piles per acre (PPA) columns to obtain per acre information.

## 5.6.22 CARBON

Carbon weight of the residual pile. The weight of carbon, in pounds, estimated for the pile. This is an individual pile value and must be multiplied by one of the piles per acre (PPA) columns to obtain per acre information.

## 5.6.23 PPA\_UNADJ

Piles per acre, unadjusted, for population estimates. The number of piles per acre that the pile represents before adjustment for partially nonsampled plots in the stratum. The estimate must be adjusted using factors stored on the POP\_STRATUM table to derive population estimates.

Note: A per acre estimate of the pile is calculated by multiplying PPA\_UNADJ and any pile attribute of interest (e.g., DRYBIO).

## 5.6.24 PPA\_PLOT

Piles per acre, unadjusted, for plot estimates. The number of piles per acre that the pile represents on the individual plot. This estimate is based on the condition area actually sampled on the plot; therefore, it excludes access denied or hazardous conditions. It is used to expand pile attributes for plot-level analyses, where it is important to have an estimate for an individual plot location. This PPA is never adjusted and is not used to derive population estimates.

## 5.6.25 PPA\_COND

Piles per acre, unadjusted, for condition estimates. The number of piles per acre that the pile represents on one condition on the plot. This estimate is based on the condition area actually sampled on the plot, therefore excludes access denied or hazardous conditions. It is used to expand pile attributes for condition-level analyses, where it is important to have an estimate for an individual condition. This PPA is never adjusted and is not used to derive population estimates.

## 5.6.26 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 5.6.27 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 5.6.28 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 5.6.29 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 5.6.30 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 5.6.31 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 5.6.32 COMP\_HT

Compacted height of the residual pile. The average height of a pile of woody debris in feet, visually compacted to exclude air, debris, and pieces less than 3 inches at the point of intersection with the transect. Populated for all options of the National P2 DWM protocol.

## 5.6.33 DECAYCD

Decay class code of the residual pile. A code indicating the predominant decay class of the pile. Populated for all options of the National P2 DWM protocol.

## Codes: DECAYCD

|   Decay class | Structural integrity                                                              | Texture of rotten portions                                                                | Color of wood                     | Invading roots   | Branches and twigs                                                                            |
|---------------|-----------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|-----------------------------------|------------------|-----------------------------------------------------------------------------------------------|
|             1 | Sound, freshly fallen, intact logs.                                               | Intact, no rot; conks of stem decay absent.                                               | Original color.                   | Absent.          | If branches are present, fine twigs are still attached and have tight bark.                   |
|             2 | Sound.                                                                            | Mostly intact; sapwood partly soft (starting to decay) but can't be pulled apart by hand. | Original color.                   | Absent.          | If branches are present, many fine twigs are gone and remaining fine twigs have peeling bark. |
|             3 | Heartwood sound; piece supports its own weight.                                   | Hard, large pieces; sapwood can be pulled apart by hand or sapwood absent.                | Reddish- brown or original color. | Sapwood only.    | Branch stubs will not pull out.                                                               |
|             4 | Heartwood rotten; piece does not support its own weight, but maintains its shape. | Soft, small blocky pieces; a metal pin can be pushed into heartwood.                      | Reddish or light brown.           | Throughout.      | Branch stubs pull out.                                                                        |
|             5 | None; piece no longer maintains its shape, it spreads out on ground.              | Soft; powdery when dry.                                                                   | Red-brown to dark brown.          | Throughout.      | Branch stubs and pitch pockets have usually rotted down.                                      |

## 5.6.34 HORIZ\_BEGNDIST

Beginning horizontal distance of the residual pile. The horizontal length of the transect in feet from subplot center to the beginning of the pile where pieces cannot be tallied individually. Populated for all options of the National P2 DWM protocol.

## 5.6.35 HORIZ\_ENDDIST

Ending horizontal distance of the residual pile. The horizontal length of the transect in feet from subplot center to the end of the pile where pieces can be tallied individually again.

## 5.6.36 PILE\_SAMPLE\_METHOD

Pile sample method. A code indicating the sampling protocol used to collect residue pile data.

## Codes: PILE\_SAMPLE\_METHOD

|   Code | Description                                                                                                                                 | Distance measurement   |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------|------------------------|
|      0 | Piles not sampled.                                                                                                                          | Not applicable.        |
|      1 | PNWRS P2 protocol. Pile measured if center located within the 58.9-foot macroplot radius.                                                   | Horizontal.            |
|      2 | National P3 protocol. Pile measured if center located within the 24-foot subplot radius.                                                    | Horizontal.            |
|      3 | National P2 protocol (all options). Pile measured if it intersects the transect (see DWM_VISIT.DWM_TRANSECT_LENGTH for length of transect). | Horizontal.            |

## 5.6.37 SPCD

Species code for the residual pile. A code indicating the predominant species, or species group, of pieces in the pile. If it was not possible to determine the species, or if there was a mixture of species, the genus or hardwood/softwood was recorded.

## 5.6.38 TRANSECT

Transect. The azimuth, in degrees, of the transect on which the pile was sampled, extending out from subplot center.

Down Woody Material Residual Pile Table

Chapter 5 (revision: 04.2024)