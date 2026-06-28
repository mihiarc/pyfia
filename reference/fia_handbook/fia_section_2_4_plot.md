# Section 2.4: Plot Table
**Oracle Table Name:** PLOT
**Extracted Pages:** 55-72 (Chapter pages 2-11 to 2-28)
**Source:** FIA Database Handbook v9.3
**Chapter:** 2 - Database Tables - Location Level

---

## 2.4 Plot Table

## (Oracle table name: PLOT)

| Subsection   | Column name (attribute)   | Descriptive name                          | Oracle data type   |
|--------------|---------------------------|-------------------------------------------|--------------------|
| 2.4.1        | CN                        | Sequence number                           | VARCHAR2(34)       |
| 2.4.2        | SRV_CN                    | Survey sequence number                    | VARCHAR2(34)       |
| 2.4.3        | CTY_CN                    | County sequence number                    | VARCHAR2(34)       |
| 2.4.4        | PREV_PLT_CN               | Previous plot sequence number             | VARCHAR2(34)       |
| 2.4.5        | INVYR                     | Inventory year                            | NUMBER(4)          |
| 2.4.6        | STATECD                   | State code                                | NUMBER(4)          |
| 2.4.7        | UNITCD                    | Survey unit code                          | NUMBER(2)          |
| 2.4.8        | COUNTYCD                  | County code                               | NUMBER(3)          |
| 2.4.9        | PLOT                      | Plot number                               | NUMBER(5)          |
| 2.4.10       | PLOT_STATUS_CD            | Plot status code                          | NUMBER(1)          |
| 2.4.11       | PLOT_NONSAMPLE_REASN_CD   | Plot nonsampled reason code               | NUMBER(2)          |
| 2.4.12       | MEASYEAR                  | Measurement year                          | NUMBER(4)          |
| 2.4.13       | MEASMON                   | Measurement month                         | NUMBER(2)          |
| 2.4.14       | MEASDAY                   | Measurement day                           | NUMBER(2)          |
| 2.4.15       | REMPER                    | Remeasurement period                      | NUMBER(3,1)        |
| 2.4.16       | KINDCD                    | Sample kind code                          | NUMBER(2)          |
| 2.4.17       | DESIGNCD                  | Design code                               | NUMBER(4)          |
| 2.4.18       | RDDISTCD                  | Horizontal distance to improved road code | NUMBER(2)          |
| 2.4.19       | WATERCD                   | Water on plot code                        | NUMBER(2)          |
| 2.4.20       | LAT                       | Latitude                                  | NUMBER(8,6)        |
| 2.4.21       | LON                       | Longitude                                 | NUMBER(9,6)        |
| 2.4.22       | ELEV                      | Elevation                                 | NUMBER(5)          |
| 2.4.23       | GROW_TYP_CD               | Type of annual volume growth code         | NUMBER(2)          |
| 2.4.24       | MORT_TYP_CD               | Type of annual mortality volume code      | NUMBER(2)          |
| 2.4.25       | P2PANEL                   | Phase 2 panel number                      | NUMBER(2)          |
| 2.4.26       | P3PANEL                   | Phase 3 panel number                      | NUMBER(2)          |
| 2.4.27       | MANUAL                    | Manual (field guide) version number       | NUMBER(3,1)        |
| 2.4.28       | KINDCD_NC                 | Sample kind code, North Central           | NUMBER(2)          |
| 2.4.29       | QA_STATUS                 | Quality assurance status                  | NUMBER(1)          |
| 2.4.30       | CREATED_BY                | Created by                                | VARCHAR2(30)       |
| 2.4.31       | CREATED_DATE              | Created date                              | DATE               |
| 2.4.32       | CREATED_IN_INSTANCE       | Created in instance                       | VARCHAR2(6)        |
| 2.4.33       | MODIFIED_BY               | Modified by                               | VARCHAR2(30)       |

| Subsection   | Column name (attribute)         | Descriptive name                                                          | Oracle data type   |
|--------------|---------------------------------|---------------------------------------------------------------------------|--------------------|
| 2.4.34       | MODIFIED_DATE                   | Modified date                                                             | DATE               |
| 2.4.35       | MODIFIED_IN_INSTANCE            | Modified in instance                                                      | VARCHAR2(6)        |
| 2.4.36       | MICROPLOT_LOC                   | Microplot location                                                        | VARCHAR2(12)       |
| 2.4.37       | DECLINATION                     | Declination                                                               | NUMBER(4,1)        |
| 2.4.38       | SAMP_METHOD_CD                  | Sample method code                                                        | NUMBER(1)          |
| 2.4.39       | SUBP_EXAMINE_CD                 | Subplots examined code                                                    | NUMBER(1)          |
| 2.4.40       | MACRO_BREAKPOINT_DIA            | Macroplot breakpoint diameter                                             | NUMBER(2)          |
| 2.4.41       | INTENSITY                       | Intensity                                                                 | VARCHAR2(3)        |
| 2.4.42       | CYCLE                           | Inventory cycle number                                                    | NUMBER(2)          |
| 2.4.43       | SUBCYCLE                        | Inventory subcycle number                                                 | NUMBER(2)          |
| 2.4.44       | TOPO_POSITION_PNW               | Topographic position, Pacific Northwest Research Station                  | VARCHAR2(2)        |
| 2.4.45       | NF_SAMPLING_STATUS_CD           | Nonforest sampling status code                                            | NUMBER(1)          |
| 2.4.46       | NF_PLOT_STATUS_CD               | Nonforest plot status code                                                | NUMBER(1)          |
| 2.4.47       | NF_PLOT_NONSAMPLE_REASN_CD      | Nonforest plot nonsampled reason code                                     | NUMBER(2)          |
| 2.4.48       | P2VEG_SAMPLING_STATUS_CD        | P2 vegetation sampling status code                                        | NUMBER(1)          |
| 2.4.49       | P2VEG_SAMPLING_LEVEL_DETAIL_ CD | P2 vegetation sampling level detail code                                  | NUMBER(1)          |
| 2.4.50       | INVASIVE_SAMPLING_STATUS_CD     | Invasive sampling status code                                             | NUMBER(1)          |
| 2.4.51       | INVASIVE_SPECIMEN_RULE_CD       | Invasive specimen rule code                                               | NUMBER(1)          |
| 2.4.52       | DESIGNCD_P2A                    | Design code periodic to annual                                            | NUMBER(4)          |
| 2.4.53       | MANUAL_DB                       | Manual version of the data                                                | NUMBER(3,1)        |
| 2.4.54       | SUBPANEL                        | Subpanel                                                                  | NUMBER(2)          |
| 2.4.55       | FUTFORCD_RMRS                   | Future forest potential code, Rocky Mountain Research Station             | NUMBER(1)          |
| 2.4.56       | MANUAL_NCRS                     | Manual (field guide) version number, North Central Research Station       | NUMBER(4,2)        |
| 2.4.57       | MANUAL_NERS                     | Manual (field guide) version number, Northeastern Research Station        | NUMBER(4,2)        |
| 2.4.58       | MANUAL_RMRS                     | Manual (field guide) version number, Rocky Mountain Research Station      | NUMBER(4,2)        |
| 2.4.59       | PAC_ISLAND_PNWRS                | Pacific Island name (Pacific Islands), Pacific Northwest Research Station | VARCHAR2(20)       |
| 2.4.60       | PLOT_SEASON_NERS                | Plot accessible season, Northeastern Research Station                     | NUMBER(1)          |
| 2.4.61       | PREV_MICROPLOT_LOC_RMRS         | Previous microplot location, Rocky Mountain Research Station              | VARCHAR2(12)       |
| 2.4.62       | PREV_PLOT_STATUS_CD_RMRS        | Previous plot status code, Rocky Mountain Research Station                | NUMBER(1)          |

| Subsection   | Column name (attribute)      | Descriptive name                        | Oracle data type   |
|--------------|------------------------------|-----------------------------------------|--------------------|
| 2.4.63       | REUSECD1                     | Recreation use code 1 (Pacific Islands) | NUMBER(2)          |
| 2.4.64       | REUSECD2                     | Recreation use code 2 (Pacific Islands) | NUMBER(2)          |
| 2.4.65       | REUSECD3                     | Recreation use code 3 (Pacific Islands) | NUMBER(2)          |
| 2.4.66       | GRND_LYR_SAMPLING_STATUS_C D | Ground layer sampling status code       | NUMBER(1)          |
| 2.4.67       | GRND_LYR_SAMPLING_METHOD_C D | Ground layer sampling method code       | NUMBER(1)          |

| Key Type   | Column(s) order                        | Tables to link   | Abbreviated notation   |
|------------|----------------------------------------|------------------|------------------------|
| Primary    | CN                                     | N/A              | PLT_PK                 |
| Unique     | STATECD, INVYR, UNITCD, COUNTYCD, PLOT | N/A              | PLT_UK                 |
| Foreign    | CTY_CN                                 | PLOT to COUNTY   | PLT_CTY_FK             |
| Foreign    | SRV_CN                                 | PLOT to SURVEY   | PLT_SRV_FK             |

Prior to October 2006, there were two separate research stations in the North, the Northeastern Research Station (NERS) and the North Central Research Station (NCRS).

The NERS region included the following States: Connecticut, Delaware, Maine, Maryland, Massachusetts, New Hampshire, New Jersey, New York, Pennsylvania, Ohio, Rhode Island, Vermont, and West Virginia.

The NCRS region included the following States: Illinois, Indiana, Iowa, Kansas, Michigan, Minnesota, Missouri, Nebraska, North Dakota, South Dakota, and Wisconsin.

In October 2006, these two research stations were combined into one, the Northern Research Station (NRS). Following the database structure created prior to the merger, regional data collected by the NRS are currently split into NCRS and NERS columns determined by the State of data collection.

Since the merger starting at MANUAL = 3.1, there has been only one regional field guide for all NRS States, the regional NRS field guide. In the database, however, there are attributes named MANUAL\_NERS and MANUAL\_NCRS. Only one of these attributes is populated; the other is blank (NULL), depending on the State of data collection.

## 2.4.1 CN

Sequence number. A unique sequence number used to identify a plot record.

## 2.4.2 SRV\_CN

Survey sequence number. Foreign key linking the plot record to the survey record.

## 2.4.3 CTY\_CN

County sequence number. Foreign key linking the plot record to the county record.

## 2.4.4 PREV\_PLT\_CN

Previous plot sequence number. Foreign key linking the plot record to the previous inventory's plot record for this location. Only populated on remeasurement plots.

Note: If the previous plot was classified as periodic, PREV\_PLT\_CN will not link to the periodic record.

## 2.4.5 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 2.4.6 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 2.4.7 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 2.4.8 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 2.4.9 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 2.4.10 PLOT\_STATUS\_CD

Plot status code. A code that describes the sampling status of the plot. May not be populated for some FIA work units when MANUAL &lt;1.0.

## Codes: PLOT\_STATUS\_CD

|   Code | Description                                                              |
|--------|--------------------------------------------------------------------------|
|      1 | Sampled - at least one accessible forest land condition present on plot. |
|      2 | Sampled - no accessible forest land condition present on plot.           |
|      3 | Nonsampled.                                                              |

## 2.4.11 PLOT\_NONSAMPLE\_REASN\_CD

Plot nonsampled reason code. A code indicating the reason an entire plot was not sampled.

## Codes: PLOT\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                                                                      |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     01 | Outside U.S. boundary - Entire plot is outside of the U.S. border.                                                                                                               |
|     02 | Denied access area - Access to the entire plot is denied by the legal owner, or by the owner of the only reasonable route to the plot.                                           |
|     03 | Hazardous - Entire plot cannot be accessed because of a hazard or danger, for example cliffs, quarries, strip mines, illegal substance plantations, high water, etc.             |
|     05 | Lost data - Plot data file was discovered to be corrupt after a panel was completed and submitted for processing.                                                                |
|     06 | Lost plot - Entire plot cannot be found.                                                                                                                                         |
|     07 | Wrong location - Previous plot can be found, but its placement is beyond the tolerance limits for plot location.                                                                 |
|     08 | Skipped visit - Entire plot skipped. Used for plots that are not completed prior to the time a panel is finished and submitted for processing. This code is for office use only. |
|     09 | Dropped intensified plot - Intensified plot dropped due to a change in grid density. This code used only by units engaged in intensification. This code is for office use only.  |
|     10 | Other - Entire plot not sampled due to a reason other than one of the specific reasons already listed.                                                                           |
|     11 | Ocean - Plot falls in ocean water below mean high tide line.                                                                                                                     |

## 2.4.12 MEASYEAR

Measurement year. The year in which the plot was completed. MEASYEAR may differ from INVYR. May be blank (null) for periodic inventory or when PLOT\_STATUS\_CD = 3.

## 2.4.13 MEASMON

Measurement month. The month in which the plot was completed. May be blank (null) for periodic inventory or when PLOT\_STATUS\_CD = 3.

## Codes: MEASMON

|   Code | Description   |
|--------|---------------|
|      1 | January.      |
|      2 | February.     |
|      3 | March.        |
|      4 | April.        |
|      5 | May.          |
|      6 | June.         |
|      7 | July.         |
|      8 | August.       |
|      9 | September.    |
|     10 | October.      |
|     11 | November.     |
|     12 | December.     |

## 2.4.14 MEASDAY

Measurement day. The day of the month in which the plot was completed. May be blank (null) for periodic inventory or when PLOT\_STATUS\_CD = 3.

## 2.4.15 REMPER

Remeasurement period. The number of years between measurements for remeasured plots to the nearest 0.1 year. This attribute is blank (null) for new plots or remeasured plots that are not used for growth, removals, or mortality estimates.

## 2.4.16 KINDCD

Sample kind code. A code indicating the type of plot installation. Database users may also want to examine DESIGNCD to obtain additional information about the kind of plot being selected. Revisited plots with KINDCD = 1, 3 are not used for remeasurement estimates.

## Codes: KINDCD

|   Code | Description                                                                                                                                                     |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | Periodic inventory plot.                                                                                                                                        |
|      1 | Initial installation of a national design plot or resampling of a national design plot that was coded as nonsampled (PLOT_STATUS_CD = 3) at the previous visit. |
|      2 | Remeasurement of previously installed national design plot.                                                                                                     |
|      3 | Replacement of previously installed national design plot.                                                                                                       |
|      4 | Modeled periodic inventory plot (Northeastern and North Central only).                                                                                          |

## 2.4.17 DESIGNCD

Design code. A code indicating the type of plot design used to collect the data. Refer to appendix G for a list of codes and descriptions.

## 2.4.18 RDDISTCD

Horizontal distance to improved road code. The straight-line distance from plot center to the nearest improved road, which is a road of any width that is maintained as evidenced by pavement, gravel, grading, ditching, and/or other improvements. May not be populated for some FIA work units when MANUAL &lt;1.0.

## Codes: RDDISTCD

|   Code | Description           |
|--------|-----------------------|
|      1 | 100 ft or less.       |
|      2 | 101 ft to 300 ft.     |
|      3 | 301 ft to 500 ft.     |
|      4 | 501 ft to 1000 ft.    |
|      5 | 1001 ft to 1/2 mile.  |
|      6 | 1/2 to 1 mile.        |
|      7 | 1 to 3 miles.         |
|      8 | 3 to 5 miles.         |
|      9 | Greater than 5 miles. |

## 2.4.19 WATERCD

Water on plot code. Water body &lt;1 acre in size or a stream &lt;30 feet wide that has the greatest impact on the area within the sampled portions of any of the four subplots. The coding hierarchy is listed in order from large permanent water to temporary water. May not be populated for some FIA work units.

## Codes: WATERCD

|   Code | Description                                                                                                                                             |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | None - no water sources within the sampled condition class(es).                                                                                         |
|      1 | Permanent streams or ponds too small to qualify as noncensus water.                                                                                     |
|      2 | Permanent water in the form of deep swamps, bogs, marshes without standing trees present and less than 1.0 acre in size, or with standing trees.        |
|      3 | Ditch/canal - human-made channels used as a means of moving water, e.g., for irrigation or drainage, which are too small to qualify as noncensus water. |
|      4 | Temporary streams.                                                                                                                                      |
|      5 | Flood zones - evidence of flooding when bodies of water exceed their natural banks.                                                                     |
|      9 | Other temporary water.                                                                                                                                  |

## 2.4.20 LAT

Latitude. The approximate latitude of the plot in decimal degrees using NAD 83 datum (these Pacific Islands plots use WSG84 datum - SURVEY.RSCD = 26 and SURVEY.STATECD = 60, 64, 66, 68, 69, or 70). Actual plot coordinates cannot be released because of a Privacy provision enacted by Congress in the Food Security Act of 1985. Therefore, this attribute is approximately +/- 1 mile and, for annual inventory data, most plots are within +/- ½ mile. Annual data have additional uncertainty for private plots caused by swapping plot coordinates for up to 20 percent of the plots. In some cases, the county centroid is used when the actual coordinate is not available.

## 2.4.21 LON

Longitude. The approximate longitude of the plot in decimal degrees using NAD 83 datum (these Pacific Islands plots use WSG84 datum - SURVEY.RSCD = 26 and SURVEY.STATECD = 60, 64, 66, 68, 69, or 70). Actual plot coordinates cannot be released because of a Privacy provision enacted by Congress in the Food Security Act of 1985. Therefore, this attribute is approximately +/- 1 mile and, for annual inventory data, most plots are within +/- ½ mile. Annual data have additional uncertainty for private plots caused by swapping plot coordinates for up to 20 percent of the plots. In some cases, the county centroid is used when the actual coordinate is not available.

## 2.4.22 ELEV

Elevation. The distance the plot is located above sea level. ELEV is based on approximate plot coordinates (see LAT and LON). For certain FIA work units (SURVEY.RSCD = 22, 23, 24, 33), the ELEV value is rounded to the nearest 10 feet. For other FIA work units (SURVEY.RSCD = 26, 27), the ELEV value is based on 200-foot groupings, and then a mid-point value is returned starting at 100 feet. Negative values indicate distance below sea level.

## 2.4.23 GROW\_TYP\_CD

Type of annual volume growth code. A code indicating how volume growth is estimated. Current annual growth is an estimate of the amount of volume that was added to a tree in the year before the tree was sampled, and is based on the measured diameter increment recorded when the tree was sampled or on a modeled diameter for the previous year. Periodic annual growth is an estimate of the average annual change in volume occurring between two measurements, usually the current inventory and the previous inventory, where the same plot is evaluated twice. Periodic annual growth is the increase in volume between inventories divided by the number of years between each inventory. This attribute is blank (null) if the plot does not contribute to the growth estimate.

Codes: GROW\_TYP\_CD

|   Code | Description      |
|--------|------------------|
|      1 | Current annual.  |
|      2 | Periodic annual. |

## 2.4.24 MORT\_TYP\_CD

Type of annual mortality volume code. A code indicating how mortality volume is estimated. Current annual mortality is an estimate of the volume of trees dying in the year before the plot was measured, and is based on the year of death or on a modeled estimate. Periodic annual mortality is an estimate of the average annual volume of trees dying between two measurements, usually the current inventory and previous inventory, where the same plot is evaluated twice. Periodic annual mortality is the loss of volume between inventories divided by the number of years between each inventory. Periodic average annual mortality is the most common type of annual mortality estimated. This attribute is blank (null) if the plot does not contribute to the mortality estimate.

Codes: MORT\_TYP\_CD

|   Code | Description      |
|--------|------------------|
|      1 | Current annual.  |
|      2 | Periodic annual. |

## 2.4.25 P2PANEL

Phase 2 panel number. The value for P2PANEL ranges from 1 to 5 for annual inventories and is blank (null) for periodic inventories. A panel is a sample in which the same elements are measured on two or more occasions. FIA divides the plots in each State into 5 panels that can be used to independently sample the population.

## 2.4.26 P3PANEL

Phase 3 panel number. A panel is a sample in which the same elements are measured on two or more occasions. FIA divides the plots in each State into 5 panels that can be used to independently sample the population. The value for P3PANEL ranges from 1 to 5 for those plots where Phase 3 data were collected. If the plot is not a Phase 3 plot, then this attribute is left blank (null).

## 2.4.27 MANUAL

Manual (field guide) version number. Version number of the Field Guide used to describe procedures for collecting data on the plot. The National FIA Field Guide began with version 1.0; therefore, data taken using the National Field procedures will have MANUAL  1.0. Data taken according to field instructions prior to the use of the National Field Guide have MANUAL &lt;1.0.

## 2.4.28 KINDCD\_NC

Sample kind code, North Central. This attribute is populated through 2005 for the former North Central work unit (SURVEY.RSCD = 23) and is blank (null) for all other FIA work units.

## Codes: KINDCD\_NC

|   Code | Description                      |
|--------|----------------------------------|
|      0 | New/lost.                        |
|      6 | Remeasured.                      |
|      8 | Old location but not remeasured. |
|     20 | Skipped.                         |
|     33 | Replacement of lost plot.        |

## 2.4.29 QA\_STATUS

Quality assurance status. A code indicating the type of plot data collected. Production plots have QA\_STATUS = 1 or 7. May not be populated for some FIA work units when MANUAL &lt;1.0.

## Codes: QA\_STATUS

|   Code | Description                                           |
|--------|-------------------------------------------------------|
|      1 | Standard production plot.                             |
|      2 | Cold check.                                           |
|      3 | Reference plot (off grid).                            |
|      4 | Training/practice plot (off grid).                    |
|      5 | Botched plot file (disregard during data processing). |

## 2.4.30 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 2.4.31 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 2.4.32 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 2.4.33 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 2.4.34 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 2.4.35 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 2.4.36 MICROPLOT\_LOC

Microplot location. A code indicating the location of the microplot center on the subplot. The offset microplot center is located 12 feet due east (90 degrees) of subplot center. The current standard is that the microplot is located in the 'OFFSET' location, but some earlier inventories, including some early panels of the annual inventory, may contain data where the microplot was located at the 'CENTER' location. May not be populated for some FIA work units when MANUAL &lt;1.0.

## Codes: MICROPLOT\_LOC

| Code   | Description                                             |
|--------|---------------------------------------------------------|
| OFFSET | The microplot center is offset from the subplot center. |
| CENTER | The microplot center is at the subplot center.          |

## 2.4.37 DECLINATION

Declination. (core optional) The azimuth correction used to adjust magnetic north to true north, and is defined as follows:

DECLINATION = (TRUE NORTH - MAGNETIC NORTH)

This field is only used in cases where FIA work units are adjusting azimuths to correspond to true north. This field includes a decimal place because the USGS corrections are provided to the nearest half degree. DECLINATION is set to a value of 0.0 for plots that are sampled using magnetic azimuths. Only populated by certain FIA work units (SURVEY.RSCD = 26, 27).

## 2.4.38 SAMP\_METHOD\_CD

Sample method code. A code indicating if the plot was observed in the field or remotely sensed in the office.

## Codes: SAMP\_METHOD\_CD

|   Code | Description                                                                                                                                                                                                               |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Field visited, meaning a field crew physically examined the plot and recorded information at least about subplot 1 center condition (see SUBP_EXAMINE_CD below).                                                          |
|      2 | Remotely sensed, meaning a determination was made using some type of imagery that a field visit was not necessary. When the plot is sampled remotely, the number of subplots examined (SUBP_EXAMINE_CD) usually equals 1. |

## 2.4.39 SUBP\_EXAMINE\_CD

Subplots examined code. A code indicating the number of subplots examined. By default, PLOT\_STATUS\_CD = 1 plots have all 4 subplots examined.

## Codes: SUBP\_EXAMINE\_CD

|   Code | Description                                                                                        |
|--------|----------------------------------------------------------------------------------------------------|
|      1 | Only subplot 1 center condition examined and all other subplots assumed (inferred) to be the same. |
|      4 | All four subplots fully described (no assumptions/inferences).                                     |

## 2.4.40 MACRO\_BREAKPOINT\_DIA

Macroplot breakpoint diameter. (core optional) A macroplot breakpoint diameter is the diameter (either d.b.h. or d.r.c.) above which trees are measured on the plot extending from 0.01 to 58.9 feet horizontal distance from the center of each subplot. Examples of different breakpoint diameters used by western FIA work units are 24 inches or 30 inches (Pacific Northwest), or 21 inches (Rocky Mountain). Installation of macroplots is core optional and is used to have a larger plot size in order to more adequately sample large trees. If macroplots are not being installed, this item will be left blank (null).

## 2.4.41 INTENSITY

Intensity. A code used to identify FIA base grid annual inventory plots and plots that have been added to intensify a particular sample. Under the FIA base grid, one plot is collected in each theoretical hexagonal polygon, which is approximately 6,000 acres in size. INTENSITY values of 1-200 are tied to the FIA base grid. INTENSITY = 1 approximates 1 plot per 6,000 acres. INTENSITY values = 2-200 indicate further intensification tied to the FIA base grid in a specific repeatable geometric pattern. INTENSITY values greater than 1 may not have any relation to the amount of intensification applied (e.g., INTENSITY = 2 does NOT necessarily mean 2x spatial intensification). For certain FIA work units (SURVEY.RSCD = 26, 27), INTENSITY values greater than 201 are tied to the older Continuous Vegetation Survey (CVS) plot grid (used by FS Region 6, Oregon Department of Forestry, and BLM) or other special studies. Populated when MANUAL  1.0.

## 2.4.42 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 2.4.43 SUBCYCLE

Inventory subcycle number.

See SURVEY.SUBCYCLE description for definition.

## 2.4.44 TOPO\_POSITION\_PNW

Topographic position, Pacific Northwest Research Station. The topographic position that describes the plot area. Illustrations available in Plot section of the PNWRS field guide located at the web page for PNWRS FIA Field Manuals

(https://research.fs.usda.gov/pnw/centers/datacollection). Adapted from information found in Wilson (1900). Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: TOPO\_POSITION\_PNW

|   Code | Topographic Position                                  | Common shape of slope   |
|--------|-------------------------------------------------------|-------------------------|
|      1 | Ridge top or mountain peak over 130 feet.             | Flat.                   |
|      2 | Narrow ridge top or mountain peak over 130 feet wide. | Convex.                 |
|      3 | Side hill - upper 1/3.                                | Convex.                 |
|      4 | Side hill - middle 1/3.                               | No rounding.            |

|   Code | Topographic Position                    | Common shape of slope   |
|--------|-----------------------------------------|-------------------------|
|      5 | Side hill - lower 1/3.                  | Concave.                |
|      6 | Canyon bottom less than 660 feet wide.  | Concave.                |
|      7 | Bench, terrace or dry flat.             | Flat.                   |
|      8 | Broad alluvial flat over 660 feet wide. | Flat.                   |
|      9 | Swamp or wet flat.                      | Flat.                   |

## 2.4.45 NF\_SAMPLING\_STATUS\_CD

Nonforest sampling status code. A code indicating whether or not the plot is part of a nonforest inventory. If NF\_SAMPLING\_STATUS\_CD = 1, then a subset of attributes that are measured on accessible forest lands were measured on accessible nonforest lands.

Codes: NF\_SAMPLING\_STATUS\_CD

|   Code | Description                                       |
|--------|---------------------------------------------------|
|      0 | Nonforest plots / conditions are not inventoried. |
|      1 | Nonforest plots / conditions are inventoried.     |

## 2.4.46 NF\_PLOT\_STATUS\_CD

Nonforest plot status code. A code describing the sampling status of the nonforest plot.

## Codes: NF\_PLOT\_STATUS\_CD

|   Code | Description                                                                                                 |
|--------|-------------------------------------------------------------------------------------------------------------|
|      1 | Sampled - at least one accessible nonforest land condition present on the plot.                             |
|      2 | Sampled - no nonforest land condition present on plot (i.e., plot is either census and/or noncensus water). |
|      3 | Nonsampled nonforest.                                                                                       |

## 2.4.47 NF\_PLOT\_NONSAMPLE\_REASN\_CD

Nonforest plot nonsampled reason code. A code indicating the reason the nonforest plot was not sampled.

## Codes: NF\_PLOT\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                                                                                                                                                                                                                                       |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     02 | Denied access - Access to the entire plot is denied by the legal owner, or by the owner of the only reasonable route to the plot. Because a denied-access plot can become accessible in the future, it remains in the sample and is re-examined at the next occasion to determine if access is available.                                         |
|     03 | Hazardous - Entire plot cannot be accessed because of a hazard or danger, for example cliffs, quarries, strip mines, illegal substance plantations, high water, etc. Although most hazards will not change over time, a hazardous plot remains in the sample and is re-examined at the next occasion to determine if the hazard is still present. |
|     08 | Skipped visit - Entire plot skipped. Used for plots that are not completed prior to the time a panel is finished and submitted for processing. This code is for office use only.                                                                                                                                                                  |

|   Code | Description                                                                                                                                                                     |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     09 | Dropped intensified plot - Intensified plot dropped due to a change in grid density. This code used only by units engaged in intensification. This code is for office use only. |
|     10 | Other - Entire plot not sampled due to a reason other than one of the specific reasons already listed.                                                                          |

## 2.4.48 P2VEG\_SAMPLING\_STATUS\_CD

P2 vegetation sampling status code. A code indicating whether the plot is part of the P2 (Phase 2) vegetation sample included in the inventory.

Note: For certain FIA work units (SURVEY.RSCD = 22, 26, 27), to obtain a list of all plots in the sample, include codes 1 and 2 (to limit conditions to only accessible forest land, specify COND.COND\_STATUS\_CD = 1). Code 1 is used for plot locations that are only eligible for accessible forest land condition sampling. Code 2 is used for a subset of plot locations that are eligible for either forest or nonforest land condition sampling (e.g., National Forest System lands in specified regions).

Codes: P2VEG\_SAMPLING\_STATUS\_CD

|   Code | Description                                                               |
|--------|---------------------------------------------------------------------------|
|      0 | Plot is not part of the P2 vegetation sample.                             |
|      1 | P2 vegetation data are sampled only on accessible forest land conditions. |
|      2 | P2 vegetation data are sampled on all accessible land conditions.         |

## 2.4.49 P2VEG\_SAMPLING\_LEVEL\_DETAIL\_CD

P2 vegetation sampling level detail code. Level of detail (LOD). A code indicating whether data were collected for vegetation structure growth habits only, or for individual species (that qualify as most abundant) as well. If LOD = 3, then a tree species could be recorded twice, but it would have two different species growth habits.

Codes: P2VEG\_SAMPLING\_LEVEL\_DETAIL\_CD

|   Code | Description                                                                                                                                                                                                                                                                                                                                |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Data collected for vegetation structure only; total aerial canopy cover and canopy cover by layer for tally tree species (all sizes), non-tally tree species (all sizes), shrubs/subshrubs/woody vines, forbs, and graminoids.                                                                                                             |
|      2 | Vegetation structure data (LOD = 1) plus understory species composition data collected including up to four most abundant species per GROWTH_HABIT_CD per subplot of: seedlings and saplings of any tree species (tally or non-tally) <5 inches d.b.h. (d.r.c. for woodland species), shrubs/subshrubs/woody vines, forbs, and graminoids. |
|      3 | Vegetation structure data, understory species composition data (LOD = 2), plus up to four most abundant tree species (tally or non-tally)  5 inches d.b.h. (d.r.c for woodland species) per GROWTH_HABIT_CD per subplot.                                                                                                                  |

## 2.4.50 INVASIVE\_SAMPLING\_STATUS\_CD

Invasive sampling status code. A code indicating whether the plot is part of the invasive plant sample included in the inventory.

Note: For certain FIA work units (SURVEY.RSCD = 22, 26, 27), to obtain a list of all plots in the sample, include codes 1 and 2 (to limit conditions to only accessible forest land, specify COND.COND\_STATUS\_CD = 1). Code 1 is used for plot locations that are only eligible for accessible forest land condition sampling. Code 2 is used for a subset of plot locations that are eligible for either forest or nonforest land condition sampling (e.g., National Forest System lands in specified regions).

## Codes: INVASIVE\_SAMPLING\_STATUS\_CDINVASIVE\_SPECIMEN\_RULE\_CD

|   Code | Description                                                                |
|--------|----------------------------------------------------------------------------|
|      0 | Plot is not part of invasive plant sample.                                 |
|      1 | Invasive plant data are sampled only on accessible forest land conditions. |
|      2 | Invasive plant data are sampled on all accessible land conditions.         |

## 2.4.51 INVASIVE\_SPECIMEN\_RULE\_CD

Invasive specimen rule code. A code indicating if specimen collection was required.

## Codes: INVASIVE\_SPECIMEN\_RULE\_CD

|   Code | Description                                                             |
|--------|-------------------------------------------------------------------------|
|      0 | FIA work unit does not require specimen collection for invasive plants. |
|      1 | FIA work unit requires specimen collection for invasive plants.         |

## 2.4.52 DESIGNCD\_P2A

Design code periodic to annual. The plot design for the periodic plots that were remeasured in the annual inventory (DESIGNCD = 1). Refer to appendix G for a list of codes and descriptions.

## 2.4.53 MANUAL\_DB

Version of the database. A number identifying the version of the FIADB to which the data have been standardized. When older data are standardized, they are updated, where appropriate, to adhere to the standards set by the newer version. For example, if an improved growth equation is developed, older data are re-processed and then re-loaded to the database.

## 2.4.54 SUBPANEL

Subpanel. Annual inventory subpanel assignment for the plot for FIA work units using subpaneling. FIA uses a 5-panel system (see P2PANEL), but may further subdivide the 5 panels into subpanels. The following FIA work units subdivide each P2PANEL into 2 subpanels (SUBPANEL = 1 or 2), for a total of 10 subpanels. For these FIA work units, 1 subpanel is usually scheduled for measurement each year: RMRS (SURVEY.RSCD = 22); PNWRS (SURVEY.RSCD = 26, 27); SRS (SURVEY.RSCD = 33, only for Oklahoma where UNITCD  3 . Populated for all plots using the National Field Guide protocols (MANUAL    1.0).

## Codes: SUBPANEL

|   Code | Description           |
|--------|-----------------------|
|      0 | Subpaneling not used. |
|      1 | Subpanel1.            |
|      2 | Subpanel2.            |

## 2.4.55 FUTFORCD\_RMRS

Future forest potential code, Rocky Mountain Research Station. A code indicating if the location requires a prefield examination at the time of the next inventory (10-20 years). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: FUTFORCD\_RMRS

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | No, there is no chance this plot will meet the forest definition at the next cycle. It meets one or more of the following criteria: • Located more than ½mile from the nearest forest land, and there are no trees present on or near the location. No disturbance evident (e.g., large fires, clearcut, etc.). • Located in a large reservoir. • Located in a developed urban area (on a house, building, parking lot), but the plot does not fall in a park, undeveloped yard, etc. that may revert to natural forest. • Located on barren rock, sand dunes, etc. |
|      1 | Yes, there is some chance that this plot could become forested in the next cycle; there are trees present, or forest land is present within ½mile.                                                                                                                                                                                                                                                                                                                                                                                                                  |
|      2 | There are no forest tree species (tree species codes) on the site, but other woody species not currently defined as forest species occupy the site (such as salt cedar, palo verde, ironwood, big sage).                                                                                                                                                                                                                                                                                                                                                            |

## 2.4.56 MANUAL\_NCRS

Manual (field guide) version number, North Central Research Station. The version number of the NCRS Field Guide used to describe procedures for collecting data on the plot. Only populated by certain FIA work units (SURVEY.RSCD = 23).

## 2.4.57 MANUAL\_NERS

Manual (field guide) version number, Northeastern Research Station. The version number of the NERS Field Guide used to describe procedures for collecting data on the plot. Only populated by certain FIA work units (SURVEY.RSCD = 24).

## 2.4.58 MANUAL\_RMRS

Manual (field guide) version, Rocky Mountain Research Station. The version number of the RMRS Field Guide used to describe procedures for collecting data on the plot. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.4.59 PAC\_ISLAND\_PNWRS

Pacific Island name ( Pacific Islands ), Pacific Northwest Research Station. The name of the Pacific Island where the plot is located. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 2.4.60 PLOT\_SEASON\_NERS

Plot accessible season, Northeastern Research Station. A code indicating the best time of year to access a plot. Populated for States in the NERS region (SURVEY.RSCD = 24) where MANUAL  4.0.

## Codes: PLOT\_SEASON\_NERS

|   Code | Description   |
|--------|---------------|
|      1 | Winter.       |
|      2 | Summer.       |
|      3 | Anytime.      |

## 2.4.61 PREV\_MICROPLOT\_LOC\_RMRS

Previous microplot location, Rocky Mountain Research Station. A code indicating the sampling location of the microplot in the previous inventory. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: PREV\_MICROPLOT\_LOC\_RMRS

| Code   | Description                                                                                                                              |
|--------|------------------------------------------------------------------------------------------------------------------------------------------|
| CENTER | Microplot center located at subplot center.                                                                                              |
| OFFSET | Microplot center offset from subplot center. For example, microplot center located 12 feet horizontal at 90 degrees from subplot center. |

## 2.4.62 PREV\_PLOT\_STATUS\_CD\_RMRS

Previous plot status code, Rocky Mountain Research Station. A code indicating the plot sampling status at the previous inventory visit. Blank (null) values may be present for periodic inventories. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: PREV\_PLOT\_STATUS\_CD\_RMRS

|   Code | Description                                                              |
|--------|--------------------------------------------------------------------------|
|      1 | Sampled - at least one accessible forest land condition present on plot. |
|      2 | Sampled - no accessible forest land condition present on plot.           |
|      3 | Nonsampled.                                                              |

## 2.4.63 REUSECD1

Recreation use code 1 ( Pacific Islands ). A code indicating signs of recreation use encountered within the accessible forest land portion of any of the four subplots, based on evidence such as campfire rings, compacted areas (from tents), hiking trails, bullet or shotgun casings, tree stands. Up to three different recreation uses per plot can be recorded (REUSECD1, REUSECD2, and REUSECD3). Only populated by certain FIA work units (SURVEY.RSCD = 26), only in the Pacific Islands.

## Codes: REUSECD1

|   Code | Description                                                                                                                             |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------|
|      0 | No evidence of recreation use.                                                                                                          |
|      1 | Motor vehicle (four wheel drive, ATV, motorcycle).                                                                                      |
|      2 | Horse riding.                                                                                                                           |
|      3 | Camping.                                                                                                                                |
|      4 | Hiking.                                                                                                                                 |
|      5 | Hunting/shooting.                                                                                                                       |
|      6 | Fishing.                                                                                                                                |
|      7 | Boating - physical evidence such as launch sites or docks.                                                                              |
|      9 | Other - recreation use where evidence is present, such as human litter, but purpose is not clear or does not fit into above categories. |

## 2.4.64 REUSECD2

Recreation use code 2 ( Pacific Islands ). The second recreation use code, if the plot has more than one recreation use. See REUSECD1 for more information.

## 2.4.65 REUSECD3

Recreation use code 3 ( Pacific Islands ). The third recreation use code, if the plot has more than two recreation uses. See REUSECD1 for more information.

## 2.4.66 GRND\_LYR\_SAMPLING\_STATUS\_CD

Ground layer sampling status code. A code indicating whether the plot is part of the ground layer sample included in the inventory. Only populated by certain FIA work units (SURVEY.RSCD = 27).

## Codes: GRND\_LYR\_SAMPLING\_STATUS\_CD

|   Code | Description                                                              |
|--------|--------------------------------------------------------------------------|
|      0 | Plot is not part of the ground layer sample.                             |
|      1 | Ground layer data are sampled only on accessible forest land conditions. |
|      2 | Ground layer data are sampled on all accessible land conditions.         |

## 2.4.67 GRND\_LYR\_SAMPLING\_METHOD\_CD

Ground layer sampling method code. A code indicating the method used for ground layer sampling. Only populated by certain FIA work units (SURVEY.RSCD = 27).

## Codes: GRND\_LYR\_SAMPLING\_METHOD\_CD

|   Code | Description                                                     |
|--------|-----------------------------------------------------------------|
|      1 | Ground layer microquadrats sampled at 4 locations per transect. |
|      2 | Ground layer microquadrats sampled at 2 locations per transect. |