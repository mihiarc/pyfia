# Section 7.1: Ground Cover Table
**Oracle Table Name:** GRND_CVR
**Extracted Pages:** 431-436 (Chapter pages 7-3 to 7-8)
**Source:** FIA Database Handbook v9.3
**Chapter:** 7 - Database Tables - Ground Cover, Pacific Northwest Research Station (PNWRS)

---

## 7.1 Ground Cover Table

## (Oracle table name: GRND\_CVR)

| Subsection   | Column name (attribute)   | Descriptive name            | Oracle data type   |
|--------------|---------------------------|-----------------------------|--------------------|
| 7.1.1        | CN                        | Sequence number             | VARCHAR2(34)       |
| 7.1.2        | PLT_CN                    | Plot sequence number        | VARCHAR2(34)       |
| 7.1.3        | INVYR                     | Inventory year              | NUMBER(4)          |
| 7.1.4        | STATECD                   | State code                  | NUMBER(4)          |
| 7.1.5        | UNITCD                    | Survey unit code            | NUMBER(2)          |
| 7.1.6        | COUNTYCD                  | County code                 | NUMBER(3)          |
| 7.1.7        | PLOT                      | Plot number                 | NUMBER(5)          |
| 7.1.8        | SUBP                      | Subplot number              | NUMBER             |
| 7.1.9        | TRANSECT                  | Transect                    | NUMBER(3)          |
| 7.1.10       | CVR_PCT                   | Cover percent               | NUMBER(3)          |
| 7.1.11       | GRND_CVR_SEG              | Ground cover segment number | NUMBER(1)          |
| 7.1.12       | GRND_CVR_TYP              | Ground cover type           | VARCHAR2(4)        |
| 7.1.13       | CYCLE                     | Inventory cycle number      | NUMBER(2)          |
| 7.1.14       | SUBCYCLE                  | Inventory subcycle number   | NUMBER(2)          |
| 7.1.15       | CREATED_BY                | Created by                  | VARCHAR2(30)       |
| 7.1.16       | CREATED_DATE              | Created date                | DATE               |
| 7.1.17       | CREATED_IN_INSTANCE       | Created in instance         | VARCHAR2(6)        |
| 7.1.18       | MODIFIED_BY               | Modified by                 | VARCHAR2(30)       |
| 7.1.19       | MODIFIED_DATE             | Modified date               | DATE               |
| 7.1.20       | MODIFIED_IN_INSTANCE      | Modified in instance        | VARCHAR2(6)        |

| Key Type   | Column(s) order                                    | Tables to link   | Abbreviated notation   |
|------------|----------------------------------------------------|------------------|------------------------|
| Primary    | CN                                                 | N/A              | GRND_CVR_PK            |
| Unique     | PLT_CN, SUBP, TRANSECT, GRND_CVR_SEG, GRND_CVR_TYP | N/A              | GRND_CVR_UK            |
| Foreign    | PLT_CN                                             | GRND_CVR to PLOT | GRND_CVR_PLT_FK        |

This table contains ground cover measurement data for National Forest System (NFS) ownership protocols. Currently, this table is populated only by the PNWRS FIA work unit (SURVEY.RSCD = 26). Ground surface cover data for the RMRS FIA work unit (SURVEY.RSCD = 22) is stored in the SUBPLOT table.

## 7.1.1 CN

Sequence number. A unique sequence number used to identify a ground cover record.

## 7.1.2 PLT\_CN

Plot sequence number. Foreign key linking the ground cover record to the plot record.

## 7.1.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 7.1.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 7.1.5 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 7.1.6 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 7.1.7 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 7.1.8 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 7.1.9 TRANSECT

Transect. A code indicating the transect on which ground cover was measured. Each code represents the azimuth of the transect line, extending out from subplot center.

## Codes: TRANSECT (INVYR  2012) 

|   Code | Description                                       |
|--------|---------------------------------------------------|
|     30 | Transect extends 30 degrees from subplot center.  |
|    150 | Transect extends 150 degrees from subplot center. |
|    270 | Transect extends 270 degrees from subplot center. |

## Codes: TRANSECT (INVYR  2013) 

|   Subplot |   Code | Description                                       |
|-----------|--------|---------------------------------------------------|
|         1 |     90 | Transect extends 90 degrees from subplot center.  |
|         1 |    270 | Transect extends 270 degrees from subplot center. |
|         2 |    360 | Transect extends 360 degrees from subplot center. |
|         2 |    180 | Transect extends 180 degrees from subplot center. |
|         3 |    135 | Transect extends 135 degrees from subplot center. |
|         3 |    315 | Transect extends 315 degrees from subplot center. |
|         4 |     45 | Transect extends 45 degrees from subplot center.  |
|         4 |    225 | Transect extends 225 degrees from subplot center. |

## 7.1.10 CVR\_PCT

Cover percent. The percentage of cover to the nearest 1 percent, for a ground cover type found on each transect segment. If multiple ground cover types (e.g., BARE, LITT, ROCK) are present on a segment, a separate record is populated for each category. Individual categories add up to 100 percent for each 10-foot segment along the transect.

## 7.1.11 GRND\_CVR\_SEG

Ground cover segment number. A code indicating a 10-foot segment on the ground cover transect. A segment is a continuous length of line within one condition, and is based on slope distance from point center.

## Codes: GRND\_CVR\_SEG

|   Code | Description                                  |
|--------|----------------------------------------------|
|      1 | Segment for 4.0-14.0 feet (slope distance).  |
|      2 | Segment for 14.0-24.0 feet (slope distance). |

## 7.1.12 GRND\_CVR\_TYP

Ground cover type. A code indicating the ground cover type found on each transect segment. If multiple ground cover types (e.g., BARE, LITT, ROCK) are present on a segment, a separate record is populated for each category. Individual categories add up to 100 percent for each 10-foot segment along the transect.

Ground cover items must be in contact with the ground (e.g., a log suspended 1-foot above the ground over the transect does not count as ground cover). If items overlay each other (e.g., MOSS over ROCK, LITT over WOOD), the item viewed from above is measured.

Ground cover type is only recorded for condition classes on R5 or R6 Forest Service administered lands (COND.ADFORCD = 501-699); the category 'NONS' is recorded for portions of the transect not on R5 or R6 Forest Service administered land.

## Codes: GRND\_CVR\_TYP

| Code   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ASH    | Residue after wood and other combustible material has been burned off. Does not include ash from aerial volcanic expulsions.                                                                                                                                                                                                                                                                                                                         |
| BARE   | Exposed Soil: Bare soil, composed of particles less than 1/8 inch in diameter, which is not covered by rock, cryptogams, or organic material. Does not include any part of a road (see definition for road).                                                                                                                                                                                                                                         |
| BAVE   | The basal area cover, at ground surface, of any plants occupying the ground surface area (this category only includes area where plant stems come out of the ground). Includes any trees, shrubs, basal grasses, and forbs (live, or senesced from the current year). Senesced = live during the current year's growing season, but now dead.                                                                                                        |
| CRYP   | Thin, biotically dominated ground or surface crusts on soil in dry rangeland conditions; e.g., cryptogamic crust (algae, lichen, mosses or cyanobacteria).                                                                                                                                                                                                                                                                                           |
| DEVP   | Surface area occupied or covered by any man-made structure other than a road, such as a building, dam, parking lot, electronic site/structure.                                                                                                                                                                                                                                                                                                       |
| LICH   | An organism generally recognized as a single plant consisting of a fungus and an alga or cyanobacterium living in a symbiotic association. This code does not apply to lichen growing on bare soil in dry rangeland conditions. For rangeland conditions see cryptogamic crusts.                                                                                                                                                                     |
| LITT   | Leaf and needle litter, and duff not yet incorporated into the decomposed top humus layer (includes animal droppings).                                                                                                                                                                                                                                                                                                                               |
| MOSS   | Nonvascular, terrestrial green plant, including mosses, hornworts, and liverworts. Always herbaceous. This code does not apply to moss growing on bare soil in dry rangeland conditions. For rangeland conditions see cryptogamic crusts.                                                                                                                                                                                                            |
| NOIN   | Non-inventoried condition classes on R5 or R6 Forest Service administered land: Census water, noncensus water, or nonsampled (hazardous, access denied, outside U.S. boundary).                                                                                                                                                                                                                                                                      |
| NONS   | Nonsampled: Condition class is not on R5 or R6 Forest Service administered land.                                                                                                                                                                                                                                                                                                                                                                     |
| PEIS   | Surface area covered by ice and snow at the time of plot measurement, considered permanent.                                                                                                                                                                                                                                                                                                                                                          |
| ROAD   | Includes improved roads used to assign condition class, which are generally constructed using machinery, and is the area where the original topography has been disturbed by cutbanks and fill. Also includes unimproved trails impacted by regular use of motorized machines (e.g., motorcycles, jeeps, and off road vehicles). Non-motorized trails and unimproved traces, and roads created by occasional use for skidding logs are not included. |
| ROCK   | Relatively hard, naturally formed mineral or petrified matter greater than 1/8 inch in diameter appearing on the soil surface, as small to large fragments, or as relatively large bodies, cliffs, outcrops or peaks. Includes bedrock. Does not include tephra or pyroclastic material (see definition for TEPH).                                                                                                                                   |
| TEPH   | All material formed by volcanic explosion or aerial expulsion from a volcanic vent, such as tephra, or pyroclastic material.                                                                                                                                                                                                                                                                                                                         |
| TRIS   | Surface area covered by ice and snow at the time of plot measurement, considered transient.                                                                                                                                                                                                                                                                                                                                                          |
| WATE   | Water is coded where the water table is above the ground surface during the growing season, such as streams, bogs, swamps, marshes, and ponds.                                                                                                                                                                                                                                                                                                       |
| WOOD   | Woody Material, Slash & Debris: Any woody material, small and large woody debris, regardless of depth. Includes stumps. Litter is not included.                                                                                                                                                                                                                                                                                                      |

## 7.1.13 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 7.1.14 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 7.1.15 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 7.1.16 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 7.1.17 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 7.1.18 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 7.1.19 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 7.1.20 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.