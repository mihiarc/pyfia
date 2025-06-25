# Section 2.6: Subplot Table
**Oracle Table Name:** SUBPLOT
**Extracted Pages:** 135-148 (Chapter pages 2-91 to 2-104)
**Source:** FIA Database Handbook v9.3
**Chapter:** 2 - Database Tables - Location Level

---

## 2.6 Subplot Table

## (Oracle table name: SUBPLOT)

| Subsection   | Column name (attribute)    | Descriptive name                                                      | Oracle data type   |
|--------------|----------------------------|-----------------------------------------------------------------------|--------------------|
| 2.6.1        | CN                         | Sequence number                                                       | VARCHAR2(34)       |
| 2.6.2        | PLT_CN                     | Plot sequence number                                                  | VARCHAR2(34)       |
| 2.6.3        | PREV_SBP_CN                | Previous subplot sequence number                                      | VARCHAR2(34)       |
| 2.6.4        | INVYR                      | Inventory year                                                        | NUMBER(4)          |
| 2.6.5        | STATECD                    | State code                                                            | NUMBER(4)          |
| 2.6.6        | UNITCD                     | Survey unit code                                                      | NUMBER(2)          |
| 2.6.7        | COUNTYCD                   | County code                                                           | NUMBER(3)          |
| 2.6.8        | PLOT                       | Plot number                                                           | NUMBER(5)          |
| 2.6.9        | SUBP                       | Subplot number                                                        | NUMBER(3)          |
| 2.6.10       | SUBP_STATUS_CD             | Subplot/macroplot status code                                         | NUMBER(1)          |
| 2.6.11       | POINT_NONSAMPLE_REASN_CD   | Point nonsampled reason code                                          | NUMBER(2)          |
| 2.6.12       | MICRCOND                   | Microplot center condition                                            | NUMBER(1)          |
| 2.6.13       | SUBPCOND                   | Subplot center condition                                              | NUMBER(1)          |
| 2.6.14       | MACRCOND                   | Macroplot center condition                                            | NUMBER(1)          |
| 2.6.15       | CONDLIST                   | Subplot/macroplot condition list                                      | NUMBER(4)          |
| 2.6.16       | SLOPE                      | Subplot percent slope                                                 | NUMBER(3)          |
| 2.6.17       | ASPECT                     | Subplot aspect                                                        | NUMBER(3)          |
| 2.6.18       | WATERDEP                   | Snow/water depth                                                      | NUMBER(2,1)        |
| 2.6.19       | P2A_GRM_FLG                | Periodic to annual growth, removal, and mortality flag                | VARCHAR2(1)        |
| 2.6.20       | CREATED_BY                 | Created by                                                            | VARCHAR2(30)       |
| 2.6.21       | CREATED_DATE               | Created date                                                          | DATE               |
| 2.6.22       | CREATED_IN_INSTANCE        | Created in instance                                                   | VARCHAR2(6)        |
| 2.6.23       | MODIFIED_BY                | Modified by                                                           | VARCHAR2(30)       |
| 2.6.24       | MODIFIED_DATE              | Modified date                                                         | DATE               |
| 2.6.25       | MODIFIED_IN_INSTANCE       | Modified in instance                                                  | VARCHAR2(6)        |
| 2.6.26       | CYCLE                      | Inventory cycle number                                                | NUMBER(2)          |
| 2.6.27       | SUBCYCLE                   | Inventory subcycle number                                             | NUMBER(2)          |
| 2.6.28       | ROOT_DIS_SEV_CD_PNWRS      | Root disease severity rating code, Pacific Northwest Research Station | NUMBER(1)          |
| 2.6.29       | NF_SUBP_STATUS_CD          | Nonforest subplot/macroplot status code                               | NUMBER(1)          |
| 2.6.30       | NF_SUBP_NONSAMPLE_REASN_CD | Nonforest subplot/macroplot nonsampled reason code                    | NUMBER(2)          |
| 2.6.31       | P2VEG_SUBP_STATUS_CD       | P2 vegetation subplot status code                                     | NUMBER(1)          |

| Subsection   | Column name (attribute)        | Descriptive name                                                                               | Oracle data type   |
|--------------|--------------------------------|------------------------------------------------------------------------------------------------|--------------------|
| 2.6.32       | P2VEG_SUBP_NONSAMPLE_REASN _CD | P2 vegetation subplot nonsampled reason code                                                   | NUMBER(2)          |
| 2.6.33       | INVASIVE_SUBP_STATUS_CD        | Invasive subplot status code                                                                   | NUMBER(1)          |
| 2.6.34       | INVASIVE_NONSAMPLE_REASN_CD    | Invasive nonsampled reason code                                                                | NUMBER(2)          |
| 2.6.35       | CROWN_CLOSURE_ME_NERS          | Crown closure (Maine), Northeastern Research Station                                           | NUMBER(1)          |
| 2.6.36       | GROUND_TRAN_PTS_BARE_RMRS      | Ground surface cover transect points - bare ground, Rocky Mountain Research Station            | NUMBER(3)          |
| 2.6.37       | GROUND_TRAN_PTS_CRYP_RMRS      | Ground surface cover transect points - cryptogamic crust, Rocky Mountain Research Station      | NUMBER(3)          |
| 2.6.38       | GROUND_TRAN_PTS_DEV_RMRS       | Ground surface cover transect points - developed land, Rocky Mountain Research Station         | NUMBER(3)          |
| 2.6.39       | GROUND_TRAN_PTS_LICHEN_RMR S   | Ground surface cover transect points - lichen, Rocky Mountain Research Station                 | NUMBER(3)          |
| 2.6.40       | GROUND_TRAN_PTS_LITTER_RMR S   | Ground surface cover transect points - litter, Rocky Mountain Research Station                 | NUMBER(3)          |
| 2.6.41       | GROUND_TRAN_PTS_MOSS_RMRS      | Ground surface cover transect points - moss, Rocky Mountain Research Station                   | NUMBER(3)          |
| 2.6.42       | GROUND_TRAN_PTS_NOTSAMP_R MRS  | Ground surface cover transect points - not sampled, Rocky Mountain Research Station            | NUMBER(3)          |
| 2.6.43       | GROUND_TRAN_PTS_OTHER_RMR S    | Ground surface cover transect points - other cover, Rocky Mountain Research Station            | NUMBER(3)          |
| 2.6.44       | GROUND_TRAN_PTS_PEIS_RMRS      | Ground surface cover transect points - permanent ice and snow, Rocky Mountain Research Station | NUMBER(3)          |
| 2.6.45       | GROUND_TRAN_PTS_ROAD_RMRS      | Ground surface cover transect points - road, Rocky Mountain Research Station                   | NUMBER(3)          |
| 2.6.46       | GROUND_TRAN_PTS_ROCK_RMRS      | Ground surface cover transect points - rock, Rocky Mountain Research Station                   | NUMBER(3)          |
| 2.6.47       | GROUND_TRAN_PTS_TRIS_RMRS      | Ground surface cover transect points - transient ice and snow, Rocky Mountain Research Station | NUMBER(3)          |
| 2.6.48       | GROUND_TRAN_PTS_VEG_RMRS       | Ground surface cover transect points - basal vegetation, Rocky Mountain Research Station       | NUMBER(3)          |

| Subsection   | Column name (attribute)     | Descriptive name                                                              | Oracle data type   |
|--------------|-----------------------------|-------------------------------------------------------------------------------|--------------------|
| 2.6.49       | GROUND_TRAN_PTS_WATER_RMR S | Ground surface cover transect points - water, Rocky Mountain Research Station | NUMBER(3)          |
| 2.6.50       | GROUND_TRAN_PTS_WOOD_RMR S  | Ground surface cover transect points - wood, Rocky Mountain Research Station  | NUMBER(3)          |
| 2.6.51       | PREV_STATUSCD_RMRS          | Previous subplot status code, Rocky Mountain Research Station                 | NUMBER(1)          |
| 2.6.52       | ROOTSEVCD_RMRS              | Root disease severity rating code, Rocky Mountain Research Station            | NUMBER(1)          |

| Key Type   | Column(s) order                              | Tables to link   | Abbreviated notation   |
|------------|----------------------------------------------|------------------|------------------------|
| Primary    | CN                                           | N/A              | SBP_PK                 |
| Unique     | PLT_CN, SUBP                                 | N/A              | SBP_UK                 |
| Natural    | STATECD, INVYR, UNITCD, COUNTYCD, PLOT, SUBP | N/A              | SBP_NAT_I              |
| Foreign    | PLT_CN, SUBPCOND                             | SUBPLOT to COND  | SBP_CND_FK             |
| Foreign    | PLT_CN, MICRCOND                             | SUBPLOT to COND  | SBP_CND_FK2            |
| Foreign    | PLT_CN, MACRCOND                             | SUBPLOT to COND  | SBP_CND_FK3            |
| Foreign    | PLT_CN                                       | SUBPLOT to PLOT  | SBP_PLT_FK             |

Note: The SUBPLOT record may not exist for some periodic inventory data.

## 2.6.1 CN

Sequence number. A unique sequence number used to identify a subplot record.

## 2.6.2 PLT\_CN

Plot sequence number. Foreign key linking the subplot record to the plot record.

## 2.6.3 PREV\_SBP\_CN

Previous subplot sequence number. Foreign key linking the subplot record to the previous inventory's subplot record for this subplot. Only populated on annual remeasured plots.

## 2.6.4 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 2.6.5 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 2.6.6 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 2.6.7 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 2.6.8 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 2.6.9 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 2.6.10 SUBP\_STATUS\_CD

Subplot/macroplot status code. A code indicating whether or not forest land was sampled on the subplot/macroplot. May be blank (null) in periodic inventories and where SUBP &gt;4.

## Codes: SUBP\_STATUS\_CD

|   Code | Description                                                                 |
|--------|-----------------------------------------------------------------------------|
|      1 | Sampled - at least one accessible forest land condition present on subplot. |
|      2 | Sampled - no accessible forest land condition present on subplot.           |
|      3 | Nonsampled - possibility of forest land.                                    |

## 2.6.11 POINT\_NONSAMPLE\_REASN\_CD

Point nonsampled reason code. A code indicating the reason an entire subplot (or macroplot) was not sampled.

## Codes: POINT\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                                                                |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     01 | Outside U.S. boundary - Entire subplot (or macroplot) is outside of the U.S. border.                                                                                       |
|     02 | Denied access area - Access to the entire subplot (or macroplot) is denied by the legal owner, or by the owner of the only reasonable route to the subplot (or macroplot). |

|   Code | Description                                                                                                                                                                                                                                                                                |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     03 | Hazardous situation - Entire subplot (or macroplot) cannot be accessed because of a hazard or danger, for example cliffs, quarries, strip mines, illegal substance plantations, high water, etc.                                                                                           |
|     04 | Time limitation - Entire subplot (or macroplot) cannot be sampled due to a time restriction. This code is reserved for areas with limited access, and in situations where it is imperative for the crew to leave before the plot can be completed (e.g., scheduled helicopter rendezvous). |
|     05 | Lost data - The plot data file was discovered to be corrupt after a panel was completed and submitted for processing. This code is assigned to entire plots or full subplots that could not be processed.                                                                                  |
|     06 | Lost plot - Entire plot cannot be found. Used for the four subplots that are required for this plot.                                                                                                                                                                                       |
|     07 | Wrong location - Previous plot can be found, but its placement is beyond the tolerance limits for plot location. Used for the four subplots that are required for this plot.                                                                                                               |
|     08 | Skipped visit - Entire plot skipped. Used for plots that are not completed prior to the time a panel is finished and submitted for processing. Used for the four subplots that are required for this plot. This code is for office use only.                                               |
|     09 | Dropped intensified plot - Intensified plot dropped due to a change in grid density. Used for the four subplots that are required for this plot. This code used only by units engaged in intensification. This code is for office use only.                                                |
|     10 | Other - Entire subplot (or macroplot) not sampled due to a reason other than one of the specific reasons already listed.                                                                                                                                                                   |
|     11 | Ocean - Subplot/macroplot falls in ocean water below mean high tide line.                                                                                                                                                                                                                  |

## 2.6.12 MICRCOND

Microplot center condition. Condition number for the condition at the center of the microplot.

## 2.6.13 SUBPCOND

Subplot center condition. Condition number for the condition at the center of the subplot.

## 2.6.14 MACRCOND

Macroplot center condition. Condition number for the condition at the center of the macroplot. Blank (null) if macroplot is not measured.

## 2.6.15 CONDLIST

Subplot/macroplot condition list. (core optional) This is a listing of all condition classes located within the 24.0/58.9-foot radius around the subplot/macroplot center. A maximum of four conditions is permitted on any individual subplot/macroplot. For example, a value of 2300 indicates that conditions 2 and 3 are on the subplot/macroplot.

## 2.6.16 SLOPE

Subplot percent slope. The predominant or average angle of the slope across the subplot, to the nearest 1 percent. Valid values are 0 through 155.

## 2.6.17 ASPECT

Subplot aspect. The aspect across the subplot, to the nearest 1 degree. Aspect is measured by sighting along the direction used to determine slope. North is recorded as 360. When slope is &lt;5 percent, there is no aspect and it is recorded as 0.

## 2.6.18 WATERDEP

Snow/water depth. The approximate depth in feet of water or snow covering the subplot. Not collected for certain FIA work units in 1999 (SURVEY.RSCD = 23, 24). May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0.

## 2.6.19 P2A\_GRM\_FLG

Periodic to annual growth, removal, and mortality flag. A code indicating if this subplot is part of a periodic inventory that is only included for the purposes of computing growth, removals and/or mortality estimates, referred to as GRM throughout this document. The flag is set to 'Y' for those subplots that are needed for change estimation and otherwise is left blank (null).

## 2.6.20 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 2.6.21 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 2.6.22 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 2.6.23 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 2.6.24 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 2.6.25 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 2.6.26 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 2.6.27 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 2.6.28 ROOT\_DIS\_SEV\_CD\_PNWRS

Root disease severity rating code, Pacific Northwest Research Station. The root disease severity rating that describes the degree of root disease present. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: ROOT\_DIS\_SEV\_CD\_PNWRS

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                          |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | No evidence of root disease visible within 50 feet of the 58.9 foot macroplot.                                                                                                                                                                                                                                                                                                       |
|      1 | Root disease present within 50 feet of the macroplot, but no evidence of disease on the macroplot.                                                                                                                                                                                                                                                                                   |
|      2 | Minor evidence of root disease on the macroplot, such as suppressed tree killed by root disease, or a minor part of the overstory showing symptoms of infection. Little or no detectable reduction in canopy closure or volume.                                                                                                                                                      |
|      3 | Canopy reduction evident, up to 20 percent; usually as a result of death of 1 codominant tree on an otherwise fully stocked site. In absence of mortality, numerous trees showing symptoms of root disease infection.                                                                                                                                                                |
|      4 | Canopy reduction at least 20 percent; up to 30 percent as a result of root disease mortality. Snags and downed trees removed from canopy by disease as well as live trees with advance symptoms of disease contribute to impact.                                                                                                                                                     |
|      5 | Canopy reduction 30-50 percent as a result of root disease. At least half of the ground area of macroplot considered infested with evidence of root disease-killed trees. Macroplots representing mature stands with half of their volume in root disease-tolerant species usually do not go much above severity 5 because of the ameliorating effect of the disease-tolerant trees. |
|      6 | 50-75 percent reduction in canopy with most of the ground area considered infested as evidenced by symptomatic trees. Much of the canopy variation in this category is generally a result of root disease-tolerant species occupying infested ground.                                                                                                                                |
|      7 | At least 75 percent canopy reduction. Macroplots that reach this severity level usually are occupied by only the most susceptible species. There are very few of the original overstory trees remaining although infested ground is often densely stocked with regeneration of susceptible species.                                                                                  |
|      8 | The entire macroplot falls within a definite root disease pocket with only one or very few susceptible overstory trees present.                                                                                                                                                                                                                                                      |
|      9 | The entire macroplot falls within a definite root disease pocket with no overstory trees of the susceptible species present.                                                                                                                                                                                                                                                         |

## 2.6.29 NF\_SUBP\_STATUS\_CD

Nonforest subplot/macroplot status code. A code describing the sampling status of the other-than-forest subplot/macroplot.

Codes: NF\_SUBP\_STATUS\_CD

|   Code | Description                                                                                                                           |
|--------|---------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Sampled - at least one accessible nonforest land condition present on the subplot/macroplot.                                          |
|      2 | Sampled - no nonforest land condition present on subplot/macroplot (i.e., subplot/macroplot is either census and/or noncensus water). |
|      3 | Nonsampled nonforest.                                                                                                                 |

## 2.6.30 NF\_SUBP\_NONSAMPLE\_REASN\_CD

Nonforest subplot/macroplot nonsampled reason code. A code indicating the reason an entire nonforest subplot (or macroplot) was not sampled.

## Codes: NF\_SUBP\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                   |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     02 | Denied access - A subplot/macroplot to which access is denied by the legal owner, or to which an owner of the only reasonable route to the plot denies access. Because a denied-access subplot/macroplot can become accessible in the future, it remains in the sample and is re-examined at the next occasion to determine if access is available.                                           |
|     03 | Hazardous situation - A subplot/macroplot that cannot be accessed because of a hazard or danger, for example cliffs, quarries, strip mines, illegal substance plantations, temporary high water, etc. Although the hazard is not likely to change over time, a hazardous condition remains in the sample and is re-examined at the next occasion to determine if the hazard is still present. |
|     04 | Time limitation - This code applies to a full subplot/macroplot that cannot be sampled due to a time restriction. This code is reserved for areas with limited access, and in situations where it is imperative for the crew to leave before the plot can be completed (e.g., scheduled helicopter rendezvous).                                                                               |
|     10 | Other - This code is used whenever a subplot/macroplot is not sampled due to a reason other than one of the specific reasons already listed.                                                                                                                                                                                                                                                  |

## 2.6.31 P2VEG\_SUBP\_STATUS\_CD

P2 vegetation subplot status code. A code indicating if the subplot was sampled for P2 vegetation.

## Codes: P2VEG\_SUBP\_STATUS\_CD

|   Code | Description                            |
|--------|----------------------------------------|
|      1 | Subplot sampled for P2 vegetation.     |
|      2 | Subplot not sampled for P2 vegetation. |

## 2.6.32 P2VEG\_SUBP\_NONSAMPLE\_REASN\_CD

P2 vegetation subplot nonsampled reason code. A code indicating why vegetation on a subplot could not be sampled.

## Codes: P2VEG\_SUBP\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                            |
|--------|----------------------------------------------------------------------------------------|
|     04 | Time limitation.                                                                       |
|     05 | Lost Data (for office use only).                                                       |
|     10 | Other (for example, snow or water covering vegetation that is supposed to be sampled). |

## 2.6.33 INVASIVE\_SUBP\_STATUS\_CD

Invasive subplot status code. A code indicating if the subplot was sampled for invasive plants.

## Codes: INVASIVE\_SUBP\_STATUS\_CD

|   Code | Description                               |
|--------|-------------------------------------------|
|      1 | Subplot sampled, invasive plants present. |

|   Code | Description                                  |
|--------|----------------------------------------------|
|      2 | Subplot sampled, no invasive plants present. |
|      3 | Subplot not sampled for invasive plants.     |

## 2.6.34 INVASIVE\_NONSAMPLE\_REASN\_CD

Invasive nonsampled reason code. A code indicating why a subplot could not be sampled for invasive plants.

## Codes: INVASIVE\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                            |
|--------|----------------------------------------------------------------------------------------|
|     04 | Time limitation.                                                                       |
|     05 | Lost Data (for office use only).                                                       |
|     10 | Other (for example, snow or water covering vegetation that is supposed to be sampled). |

## 2.6.35 CROWN\_CLOSURE\_ME\_NERS

Crown closure (Maine), Northeastern Research Station. A code indicating the percent of the subplot that is covered by live trees directly overhead. Only populated by certain FIA work units (SURVEY.RSCD = 24).

## Codes: CROWN\_CLOSURE\_ME\_NERS

|   Code | Description   |
|--------|---------------|
|      0 | 0-25%         |
|      1 | 26-50%        |
|      2 | 51-75%        |
|      3 | >75%          |

## 2.6.36 GROUND\_TRAN\_PTS\_BARE\_RMRS

Ground surface cover transect points - bare ground, Rocky Mountain Research Station.

A value indicating the percent of the subplot area covered by bare ground. This value is an estimate based on the number of sampling points on the subplot area that were classified as bare ground using a ground surface cover transect sampling method. Bare ground is defined as exposed soil and rock fragments smaller than ¾ inch in diameter. Larger rocks protruding through the soil are not classified as bare ground.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.37 GROUND\_TRAN\_PTS\_CRYP\_RMRS

Ground surface cover transect points - cryptogamic crust, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by cryptogamic crust. This value is an estimate based on the number of sampling points on the subplot area that were classified as cryptogamic crust using a ground surface cover transect sampling method.

Cryptogamic crust is defined as thin, biotically dominated ground or surface crusts on soil in dry rangeland conditions (such as algae, lichen, mosses, or cyanobacteria, which are growing on bare soil).

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.38 GROUND\_TRAN\_PTS\_DEV\_RMRS

Ground surface cover transect points - developed land, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by developed land. This value is an estimate based on the number of sampling points on the subplot area that were classified as developed land using a ground surface cover transect sampling method. Developed land is defined as surface area covered by the following: (1) any man-made structure other than a road, such as a building, dam, parking lot, or electronic site/structure, (2) maintained residential yards, or (3) agricultural crops (not rangeland).

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.39 GROUND\_TRAN\_PTS\_LICHEN\_RMRS

Ground surface cover transect points - lichen, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by lichens. This value is an estimate based on the number of sampling points on the subplot area that were classified as lichen using a ground surface cover transect sampling method. A lichen is defined as an organism generally recognized as a single plant that consists of a fungus and an alga or cyanobacterium living in a symbiotic association. This category does not apply to lichens growing on bare soils in dry rangeland conditions. For rangeland conditions, see GROUND\_TRAN\_PTS\_CRYP\_RMRS (cryptogamic crusts).

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.40 GROUND\_TRAN\_PTS\_LITTER\_RMRS

Ground surface cover transect points - litter, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by litter. This value is an estimate based on the number of sampling points on the subplot area that were classified as litter using a ground surface cover transect sampling method. Litter is defined as organic debris, freshly fallen or slightly decomposed; it includes dead vegetation, animal feces, etc.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.41 GROUND\_TRAN\_PTS\_MOSS\_RMRS

Ground surface cover transect points - moss, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by moss. This value is an estimate based on the number of sampling points on the subplot area that were classified as moss using a ground surface cover transect sampling method. Moss is defined as

nonvascular, terrestrial green plants including mosses, hornworts and liverworts - always herbaceous. This category does not apply to moss growing on bare soils in dry rangeland conditions. For rangeland conditions, see GROUND\_TRAN\_PTS\_CRYP\_RMRS (cryptogamic crusts).

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.42 GROUND\_TRAN\_PTS\_NOTSAMP\_RMRS

Ground surface cover transect points - not sampled, Rocky Mountain Research Station. A value indicating the percent of the subplot area that was not sampled. This value is based on the number of sampling points on the subplot area that were classified as not sampled using a ground surface cover transect sampling method. When this category is used, the reason for not sampling any points along the transect should be described in the subplot notes.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.43 GROUND\_TRAN\_PTS\_OTHER\_RMRS

Ground surface cover transect points - other cover, Rocky Mountain Research Station. A value indicating the percent of the subplot area classified as other cover. This value is an estimate based on the number of sampling points on the subplot area that were classified as other cover using a ground surface cover transect sampling method. This category includes covers that are not defined elsewhere by one of the other ground cover transect categories (e.g., trash). When this category is used, the other cover should be described in the subplot notes.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.44 GROUND\_TRAN\_PTS\_PEIS\_RMRS

Ground surface cover transect points - permanent ice and snow, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by permanent ice and snow. This value is an estimate based on the number of sampling points on the subplot area that were classified as permanent ice and snow using a ground surface cover transect sampling method. This category is defined as surface area covered with ice and snow at the time of plot measurement, which is considered to be permanent.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.45 GROUND\_TRAN\_PTS\_ROAD\_RMRS

Ground surface cover transect points - road, Rocky Mountain Research Station. A  value indicating the percent of the subplot area covered by road. This value is an estimate based on the number of sampling points on the subplot area that were classified as road using a ground surface cover transect sampling method. This category is defined as improved roads, paved roads, gravel roads, improved dirt roads, and off-road vehicle trails, which

are regularly maintained or in long-term continuing use. These roads are generally constructed using machinery. Cutbanks and fills are included.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.46 GROUND\_TRAN\_PTS\_ROCK\_RMRS

Ground surface cover transect points - rock, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by rock. This value is an estimate based on the number of sampling points on the subplot area that were classified as rock using a ground surface cover transect sampling method. This category includes rocks and rock fragments that are greater than ¾ inch in diameter.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.47 GROUND\_TRAN\_PTS\_TRIS\_RMRS

Ground surface cover transect points - transient ice and snow, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by transient ice and snow. This value is an estimate based on the number of sampling points on the subplot area that were classified as transient ice and snow using a ground surface cover transect sampling method. This category is defined as surface area covered with ice and snow at the time of plot measurement, which is considered to be transient.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.48 GROUND\_TRAN\_PTS\_VEG\_RMRS

Ground surface cover transect points - basal vegetation, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by basal vegetation. This value is an estimate based on the number of sampling points on the subplot area that were classified as basal vegetation using a ground surface cover transect sampling method. Basal vegetation is defined as the area outline of a plant near the ground surface. For grass, this consists of the shoot system at ground level. For trees and shrubs, this consists of the stem area.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.49 GROUND\_TRAN\_PTS\_WATER\_RMRS

Ground surface cover transect points - water, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by water. This value is an estimate based on the number of sampling points on the subplot area that were classified as water using a ground surface cover transect sampling method. This category is defined as water remaining above the ground surface during the growing season, such as streams, bogs, swamps, marshes and ponds.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.50 GROUND\_TRAN\_PTS\_WOOD\_RMRS

Ground surface cover transect points - wood, Rocky Mountain Research Station. A value indicating the percent of the subplot area covered by wood. This value is an estimate based on the number of sampling points on the subplot area that were classified as wood using a ground surface cover transect sampling method. This category is defined as woody material, including slash and small and large woody debris, regardless of depth. Litter and non-continuous litter are not included.

Data only collected for subplots that have a sampled nonforest condition at subplot center (NF\_SUBP\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.6.51 PREV\_STATUSCD\_RMRS

Previous subplot status code, Rocky Mountain Research Station. A code indicating the subplot sampling at the previous inventory visit. Blank (null) values may be present for periodic inventories. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: PREV\_STATUSCD\_RMRS

|   Code | Description                                                                 |
|--------|-----------------------------------------------------------------------------|
|      1 | Sampled - at least one accessible forest land condition present on subplot. |
|      2 | Sampled - no accessible forest land condition present on subplot.           |
|      3 | Nonsampled - possibility of forest land.                                    |

## 2.6.52 ROOTSEVCD\_RMRS

Root disease severity rating code, Rocky Mountain Research Station. A code indicating the severity of root disease on the subplot area. Data only collected for plots sampled by RMRS in Region 1 (MT, ID, ND, SD) when SUBP\_STATUS\_CD = 1 or NF\_SUBP\_STATUS\_CD = 1. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: ROOTSEVCD\_RMRS

|   Code | Description                                                                                                                                                                                                                                     |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | No evidence of root disease visible within 50 feet of the subplot perimeter.                                                                                                                                                                    |
|      1 | Root disease present within 50 feet of the subplot perimeter, but no evidence of root disease on subplot.                                                                                                                                       |
|      2 | Minor evidence of root disease evident on the subplot - suppressed tree killed by root disease, or minor part of overstory showing symptoms of infection. Little or no reduction in canopy closure or volume.                                   |
|      3 | Up to 20 percent canopy reduction evident - as a result of the death of one codominant tree on an otherwise fully stocked site. In the absence of mortality, numerous trees showing symptoms of root disease infection.                         |
|      4 | 20 to 30 percent canopy reduction - as a result of root disease-caused mortality. The presence of snags and downed dead trees as a result of disease, leaving gaps in the tree canopy, as well as live trees with advanced symptoms of disease. |

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                          |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      5 | 30 to 50 percent canopy reduction - as a result of root disease. Almost half of ground area of subplot considered infested with evidence of root disease-killed trees. Note: Subplots representing mature stands with half of their volume in root disease-tolerant species usually don't go much above severity 5 because of the ameliorating effect of the disease tolerant trees. |
|      6 | 50 to 75 percent canopy reduction -- most of the ground area considered infested as evidenced by symptomatic trees. Much of the canopy variation in this category results from disease-tolerant species occupying infested ground.                                                                                                                                                   |
|      7 | 75 percent or more canopy reduction - subplots with this severity level usually were occupied by only the most susceptible species. Very few of the original overstory trees remain, although the infested ground area is often densely stocked with regeneration of the susceptible species.                                                                                        |
|      8 | Entire subplot falls within a definite root disease patch with only one or very few susceptible overstory trees present (standing/live) within the canopy.                                                                                                                                                                                                                           |
|      9 | Entire subplot falls within a definite root disease patch with no overstory trees of the susceptible species present within the canopy.                                                                                                                                                                                                                                              |