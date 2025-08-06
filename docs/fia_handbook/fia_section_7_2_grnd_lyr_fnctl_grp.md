# Section 7.2: Ground Layer Functional Groups Table
**Oracle Table Name:** GRND_LYR_FNCTL_GRP
**Extracted Pages:** 437-442 (Chapter pages 7-9 to 7-14)
**Source:** FIA Database Handbook v9.3
**Chapter:** 7 - Database Tables - Ground Cover, Pacific Northwest Research Station (PNWRS)

---

## 7.2 Ground Layer Functional Groups Table (Oracle table name: GRND\_LYR\_FNCTL\_GRP)

| Subsection   | Column name (attribute)    | Descriptive name                                | Oracle data type   |
|--------------|----------------------------|-------------------------------------------------|--------------------|
| 7.2.1        | CN                         | Sequence number                                 | VARCHAR2(34)       |
| 7.2.2        | STATECD                    | State code                                      | NUMBER(2)          |
| 7.2.3        | COUNTYCD                   | County code                                     | NUMBER(3)          |
| 7.2.4        | PLOT                       | Plot number                                     | NUMBER(5)          |
| 7.2.5        | PLT_CN                     | Plot sequence number                            | VARCHAR2(34)       |
| 7.2.6        | INVYR                      | Inventory year                                  | NUMBER(4)          |
| 7.2.7        | INV_VST_NBR                | Inventory visit number                          | NUMBER(2)          |
| 7.2.8        | CYCLE                      | Inventory cycle number                          | NUMBER(2)          |
| 7.2.9        | SUBCYCLE                   | Inventory subcycle number                       | NUMBER(2)          |
| 7.2.10       | UNITCD                     | Survey unit code                                | NUMBER(2)          |
| 7.2.11       | SUBP                       | Subplot number                                  | NUMBER(1)          |
| 7.2.12       | TRANSECT                   | Transect (Interior Alaska)                      | NUMBER(3)          |
| 7.2.13       | MICROQUAD                  | Microquadrat number (Interior Alaska)           | NUMBER(2)          |
| 7.2.14       | FUNCTIONAL_GROUP_CD        | Functional group code (Interior Alaska)         | VARCHAR2(5)        |
| 7.2.15       | FUNCTIONAL_GROUP_UNCERTAIN | Functional group uncertain (Interior Alaska)    | VARCHAR2(1)        |
| 7.2.16       | COVER_CLASS_CD             | Cover class code (Interior Alaska)              | VARCHAR2(2)        |
| 7.2.17       | DEPTH_CLASS_CD             | Depth class code (Interior Alaska)              | VARCHAR2(2)        |
| 7.2.18       | MODIFIED_BY                | Modified by                                     | VARCHAR2(30)       |
| 7.2.19       | MODIFIED_DATE              | Modified date                                   | Date               |
| 7.2.20       | MODIFIED_IN_INSTANCE       | Modified in instance                            | VARCHAR2(6)        |
| 7.2.21       | CREATED_BY                 | Created by                                      | VARCHAR2(30)       |
| 7.2.22       | CREATED_DATE               | Created date                                    | Date               |
| 7.2.23       | CREATED_IN_INSTANCE        | Created in instance                             | VARCHAR2(6)        |
| 7.2.24       | GRND_LYR_CONFIG            | Ground layer configuration name                 | VARCHAR2(20)       |
| 7.2.25       | MQUADPAC_UNADJ             | Microquadrat area expansion to acre, unadjusted | NUMBER             |
| 7.2.26       | BULKDENS                   | Functional group bulk density                   | NUMBER             |
| 7.2.27       | DRYBIOT                    | Functional group biomass                        | NUMBER             |
| 7.2.28       | CARBON                     | Functional group carbon                         | NUMBER             |
| 7.2.29       | NITROGEN                   | Functional group nitrogen                       | NUMBER             |

| Key Type   | Column(s) order                                                                                       | Tables to link              | Abbreviated notation   |
|------------|-------------------------------------------------------------------------------------------------------|-----------------------------|------------------------|
| Primary    | CN                                                                                                    | N/A                         | FGLFGP_PK              |
| Unique     | PLT_CN, SUBP, TRANSECT, MICROQUAD, FUNCTIONAL_GROUP_CD                                                | N/A                         | FGLFGP_UK              |
| Unique     | STATECD, COUNTYCD, PLOT, INVYR, INV_VST_NBR, SUBP, TRANSECT, MICROQUAD, FUNCTIONAL_GROUP_CD           | N/A                         | FGLFGP_UK2             |
| Unique     | STATECD, CYCLE, SUBCYCLE, COUNTYCD, PLOT, SUBP, TRANSECT, MICROQUAD, FUNCTIONAL_GROUP_CD, INV_VST_NBR | N/A                         | FGLFGP_UK3             |
| Foreign    | PLT_CN                                                                                                | GRND_LYR_FUNCTL_GRP to PLOT | FGLFGP_PLT_FK          |

Currently, this table is populated only by the PNWRS FIA work unit (SURVEY. RSCD = 27).

## 7.2.1 CN

Sequence number. A unique sequence number used to identify a ground layer functional groups record.

## 7.2.2 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 7.2.3 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 7.2.4 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 7.2.5 PLT\_CN

Plot sequence number. Foreign key linking the ground layer functional groups record to the plot record.

## 7.2.6 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 7.2.7 INV\_VST\_NBR

Inventory visit number. Visit number within a cycle. A plot is usually visited once per cycle, but may be visited again for quality assurance visits or other measurements.

## 7.2.8 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 7.2.9 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 7.2.10 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 7.2.11 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4.

## Codes: SUBP

|   Code | Description        |
|--------|--------------------|
|      1 | Center subplot.    |
|      2 | North subplot.     |
|      3 | Southeast subplot. |
|      4 | Southwest subplot. |

## 7.2.12 TRANSECT

Transect (Interior Alaska). The transect azimuth, in degrees, to identify which transect is being sampled. Azimuth indicates direction from subplot center.

## Codes: TRANSECT (Interior Alaska)

|   Code |   Subplot |
|--------|-----------|
|     90 |         1 |
|    270 |         1 |
|    360 |         2 |
|    180 |         2 |
|    135 |         3 |
|    315 |         3 |
|     45 |         4 |
|    225 |         4 |

## 7.2.13 MICROQUAD

Microquadrat number (Interior Alaska). A code indicating the number of the microquadrat. This code identifies the placement of the microquadrat, in feet (horizontal distance), on the transect.

## Codes: MICROQUAD

|   Code | Description                                               |
|--------|-----------------------------------------------------------|
|      5 | Microquadrat located at the 5-foot mark on the transect.  |
|     10 | Microquadrat located at the 10-foot mark on the transect. |
|     15 | Microquadrat located at the 15-foot mark on the transect. |
|     20 | Microquadrat located at the 20-foot mark on the transect. |

## 7.2.14 FUNCTIONAL\_GROUP\_CD

Functional group code (Interior Alaska). A code indicating the functional group observed on the microquadrat.

## Codes: FUNCTIONAL\_GROUP\_CD

| Code   | Description                                                         |
|--------|---------------------------------------------------------------------|
| MS     | Sphagnum peat-moss.                                                 |
| MN     | N-fixing feather mosses: Pleurozium, Hylocomium .                   |
| MF     | Other feather (pleurocarp) mosses: Thuidium , Kindbergia .          |
| MT     | Turf (acrocarp) mosses: Bryum, Mnium, Polytrichum .                 |
| VF     | Flat (thalloid) liverworts: Marchantia , Conocephalum .             |
| VS     | Stem-and-leaf liverworts: Anthelia , Cephaloziella , Marsupella .   |
| LF     | Forage lichens: branched- Cladonia , Alectoria , Bryocaulon .       |
| LN     | N-fixing foliose lichens: Peltigera , Nephroma , Solorina, Sticta . |
| LU     | N-fixing fruticose lichens: Stereocaulon .                          |
| LL     | Other foliose Lichens: Parmelia , Physcia .                         |
| LR     | Other fruticose lichens: unbranched- Cladonia , Hypogymnia .        |
| CO     | Orange lichens: Xanthoria , Candelaria .                            |
| CC     | Biotic soil crust: Psora , Placidium , cyanobacteria.               |

## 7.2.15 FUNCTIONAL\_GROUP\_UNCERTAIN

Functional group uncertain (Interior Alaska). A code indicating the reliability of the functional group identification (see FUNCTIONAL\_GROUP\_CD). This attribute was collected for the 2014 Interior Alaska Pilot.

## Codes: FUNCTIONAL\_GROUP\_UNCERTAIN

| Code   | Description                                                               |
|--------|---------------------------------------------------------------------------|
| Y      | Yes - The field crew was certain in the functional group identification.  |
| N      | No - The field crew was uncertain in the functional group identification. |

## 7.2.16 COVER\_CLASS\_CD

Cover class code (Interior Alaska). A code indicating the cover class for the vertically projected percent cover over the entire microquadrat, combining together all species included in the functional group.

## Codes: COVER\_CLASS\_CD

| Code   | Percent cover   | Description                    |
|--------|-----------------|--------------------------------|
| 0      | Absent          | None.                          |
| T      | >0 to 0.1%      | Trace.                         |
| 1      | >0.1 to 1%      | Two postage stamps.            |
| 2      | >1 to 2%        | Half a standard business card. |
| 5      | >2 to 5%        | One business card.             |
| 10     | >5 to 10%       | One U.S. dollar bill.          |
| 25     | >10 to 25%      | -                              |
| 50     | >25 to 50%      | -                              |
| 75     | >50 to 75%      | -                              |
| 95     | >75 to 95%      | -                              |
| 99     | >95%            | Virtually complete cover.      |

## 7.2.17 DEPTH\_CLASS\_CD

Depth class code (Interior Alaska). A code indicating the depth class for the functional group on the microquadrat. This attribute is recorded up to a maximum depth of 16 inches.

## Codes: COVER\_CLASS\_CD

| Code   | Description                                                   |
|--------|---------------------------------------------------------------|
| 0      | Absent.                                                       |
| T      | 0 to 1/8 inch (trace, often used for thin biotic soil crusts. |
| Q      | >1/8 to 1/4 inch.                                             |
| H      | >1/4 to 1/2 inch.                                             |
| 1      | >1/2 to 1 inch.                                               |
| 2      | >1 to 2 inches.                                               |
| 4      | >2 to 4 inches                                                |
| 8      | >4 to 8 inches.                                               |
| 16     | >8 to 16 inches.                                              |

## 7.2.18 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 7.2.19 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 7.2.20 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 7.2.21 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 7.2.22 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 7.2.23 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 7.2.24 GRND\_LYR\_CONFIG

Ground layer configuration name. A descriptor identifying the ground layer configuration.

## Codes: GRND\_LYR\_CONFIG

| Code   | Description      |
|--------|------------------|
| INTAK  | Interior Alaska. |

## 7.2.25 MQUADPAC\_UNADJ

Microquadrat area expansion to acre, unadjusted. Used for the expansion of the microquadrat area to an acre, based on 32 microquadrats per plot. The value 1264.642632 is used for PLOT.DESIGNCD = 506.

## 7.2.26 BULKDENS

Functional group bulk density. The calculated bulk density of the functional group.

## 7.2.27 DRYBIOT

Functional group biomass. The calculated biomass of the functional group on the microquadrat, in pounds per acre.

## 7.2.28 CARBON

Functional group carbon. The calculated carbon of the functional group on the microquadrat, in pounds per acre.

## 7.2.29 NITROGEN

Functional group nitrogen. The calculated nitrogen of the functional group on the microquadrat, in pounds per acre.