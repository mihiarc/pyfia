# Section 2.5: Condition Table
**Oracle Table Name:** COND
**Extracted Pages:** 73-134 (Chapter pages 2-29 to 2-90)
**Source:** FIA Database Handbook v9.3
**Chapter:** 2 - Database Tables - Location Level

---

## 2.5 Condition Table

## (Oracle table name: COND)

| Subsection   | Column name (attribute)   | Descriptive name                 | Oracle data type   |
|--------------|---------------------------|----------------------------------|--------------------|
| 2.5.1        | CN                        | Sequence number                  | VARCHAR2(34)       |
| 2.5.2        | PLT_CN                    | Plot sequence number             | VARCHAR2(34)       |
| 2.5.3        | INVYR                     | Inventory year                   | NUMBER(4)          |
| 2.5.4        | STATECD                   | State code                       | NUMBER(4)          |
| 2.5.5        | UNITCD                    | Survey unit code                 | NUMBER(2)          |
| 2.5.6        | COUNTYCD                  | County code                      | NUMBER(3)          |
| 2.5.7        | PLOT                      | Plot number                      | NUMBER(5)          |
| 2.5.8        | CONDID                    | Condition class number           | NUMBER(1)          |
| 2.5.9        | COND_STATUS_CD            | Condition status code            | NUMBER(1)          |
| 2.5.10       | COND_NONSAMPLE_REASN_CD   | Condition nonsampled reason code | NUMBER(2)          |
| 2.5.11       | RESERVCD                  | Reserved status code             | NUMBER(2)          |
| 2.5.12       | OWNCD                     | Owner class code                 | NUMBER(2)          |
| 2.5.13       | OWNGRPCD                  | Owner group code                 | NUMBER(2)          |
| 2.5.14       | ADFORCD                   | Administrative forest code       | NUMBER(4)          |
| 2.5.15       | FORTYPCD                  | Forest type code                 | NUMBER(3)          |
| 2.5.16       | FLDTYPCD                  | Field forest type code           | NUMBER(3)          |
| 2.5.17       | MAPDEN                    | Mapping density                  | NUMBER(1)          |
| 2.5.18       | STDAGE                    | Stand age                        | NUMBER(4)          |
| 2.5.19       | STDSZCD                   | Stand-size class code            | NUMBER(2)          |
| 2.5.20       | FLDSZCD                   | Field stand-size class code      | NUMBER(2)          |
| 2.5.21       | SITECLCD                  | Site productivity class code     | NUMBER(2)          |
| 2.5.22       | SICOND                    | Site index for the condition     | NUMBER(3)          |
| 2.5.23       | SIBASE                    | Site index base age              | NUMBER(3)          |
| 2.5.24       | SISP                      | Site index species code          | NUMBER(4)          |
| 2.5.25       | STDORGCD                  | Stand origin code                | NUMBER(2)          |
| 2.5.26       | STDORGSP                  | Stand origin species code        | NUMBER             |
| 2.5.27       | PROP_BASIS                | Proportion basis                 | VARCHAR2(12)       |
| 2.5.28       | CONDPROP_UNADJ            | Condition proportion unadjusted  | NUMBER             |
| 2.5.29       | MICRPROP_UNADJ            | Microplot proportion unadjusted  | NUMBER             |
| 2.5.30       | SUBPPROP_UNADJ            | Subplot proportion unadjusted    | NUMBER             |
| 2.5.31       | MACRPROP_UNADJ            | Macroplot proportion unadjusted  | NUMBER             |
| 2.5.32       | SLOPE                     | Condition percent slope          | NUMBER(3)          |
| 2.5.33       | ASPECT                    | Condition aspect                 | NUMBER(3)          |
| 2.5.34       | PHYSCLCD                  | Physiographic class code         | NUMBER(2)          |

| Subsection   | Column name (attribute)   | Descriptive name                                 | Oracle data type   |
|--------------|---------------------------|--------------------------------------------------|--------------------|
| 2.5.35       | GSSTKCD                   | Growing-stock stocking code                      | NUMBER(2)          |
| 2.5.36       | ALSTKCD                   | All live stocking code                           | NUMBER(2)          |
| 2.5.37       | DSTRBCD1                  | Disturbance code 1                               | NUMBER(2)          |
| 2.5.38       | DSTRBYR1                  | Disturbance year 1                               | NUMBER(4)          |
| 2.5.39       | DSTRBCD2                  | Disturbance code 2                               | NUMBER(2)          |
| 2.5.40       | DSTRBYR2                  | Disturbance year 2                               | NUMBER(4)          |
| 2.5.41       | DSTRBCD3                  | Disturbance code 3                               | NUMBER(2)          |
| 2.5.42       | DSTRBYR3                  | Disturbance year 3                               | NUMBER(4)          |
| 2.5.43       | TRTCD1                    | Treatment code 1                                 | NUMBER(2)          |
| 2.5.44       | TRTYR1                    | Treatment year 1                                 | NUMBER(4)          |
| 2.5.45       | TRTCD2                    | Treatment code 2                                 | NUMBER(2)          |
| 2.5.46       | TRTYR2                    | Treatment year 2                                 | NUMBER(4)          |
| 2.5.47       | TRTCD3                    | Treatment code 3                                 | NUMBER(2)          |
| 2.5.48       | TRTYR3                    | Treatment year 3                                 | NUMBER(4)          |
| 2.5.49       | PRESNFCD                  | Present nonforest code                           | NUMBER(2)          |
| 2.5.50       | BALIVE                    | Basal area per acre of live trees                | NUMBER(9,4)        |
| 2.5.51       | FLDAGE                    | Field-recorded stand age                         | NUMBER(4)          |
| 2.5.52       | ALSTK                     | All-live-tree stocking percent                   | NUMBER(7,4)        |
| 2.5.53       | GSSTK                     | Growing-stock stocking percent                   | NUMBER(7,4)        |
| 2.5.54       | FORTYPCDCALC              | Forest type code calculated                      | NUMBER(3)          |
| 2.5.55       | HABTYPCD1                 | Habitat type code 1                              | VARCHAR2(10)       |
| 2.5.56       | HABTYPCD1_PUB_CD          | Habitat type code 1 publication code             | VARCHAR2(10)       |
| 2.5.57       | HABTYPCD1_DESCR_PUB_CD    | Habitat type code 1 description publication code | VARCHAR2(10)       |
| 2.5.58       | HABTYPCD2                 | Habitat type code 2                              | VARCHAR2(10)       |
| 2.5.59       | HABTYPCD2_PUB_CD          | Habitat type code 2 publication code             | VARCHAR2(10)       |
| 2.5.60       | HABTYPCD2_DESCR_PUB_CD    | Habitat type code 2 description publication code | VARCHAR2(10)       |
| 2.5.61       | MIXEDCONFCD               | Mixed conifer code                               | VARCHAR2(1)        |
| 2.5.62       | VOL_LOC_GRP               | Volume location group                            | VARCHAR2(200)      |
| 2.5.63       | SITECLCDEST               | Site productivity class code estimated           | NUMBER(2)          |
| 2.5.64       | SITETREE_TREE             | Site tree tree number                            | NUMBER(4)          |
| 2.5.65       | SITECL_METHOD             | Site class method                                | NUMBER(2)          |
| 2.5.66       | CARBON_DOWN_DEAD          | Carbon in down dead                              | NUMBER(13,6)       |
| 2.5.67       | CARBON_LITTER             | Carbon in litter                                 | NUMBER(13,6)       |
| 2.5.68       | CARBON_SOIL_ORG           | Carbon in soil organic material                  | NUMBER(13,6)       |
| 2.5.69       | CARBON_UNDERSTORY_AG      | Carbon in understory aboveground                 | NUMBER(13,6)       |

| Subsection   | Column name (attribute)        | Descriptive name                                              | Oracle data type   |
|--------------|--------------------------------|---------------------------------------------------------------|--------------------|
| 2.5.70       | CARBON_UNDERSTORY_BG           | Carbon in understory belowground                              | NUMBER(13,6)       |
| 2.5.71       | CREATED_BY                     | Created by                                                    | VARCHAR2(30)       |
| 2.5.72       | CREATED_DATE                   | Created date                                                  | DATE               |
| 2.5.73       | CREATED_IN_INSTANCE            | Created in instance                                           | VARCHAR2(6)        |
| 2.5.74       | MODIFIED_BY                    | Modified by                                                   | VARCHAR2(30)       |
| 2.5.75       | MODIFIED_DATE                  | Modified date                                                 | DATE               |
| 2.5.76       | MODIFIED_IN_INSTANCE           | Modified in instance                                          | VARCHAR2(6)        |
| 2.5.77       | CYCLE                          | Inventory cycle number                                        | NUMBER(2)          |
| 2.5.78       | SUBCYCLE                       | Inventory subcycle number                                     | NUMBER(2)          |
| 2.5.79       | SOIL_ROOTING_DEPTH_PNW         | Soil rooting depth, Pacific Northwest Research Station        | VARCHAR2(1)        |
| 2.5.80       | GROUND_LAND_CLASS_PNW          | Present ground land class, Pacific Northwest Research Station | VARCHAR2(3)        |
| 2.5.81       | PLANT_STOCKABILITY_FACTOR_P NW | Plant stockability factor, Pacific Northwest Research Station | NUMBER             |
| 2.5.82       | STND_COND_CD_PNWRS             | Stand condition code, Pacific Northwest Research Station      | NUMBER(1)          |
| 2.5.83       | STND_STRUC_CD_PNWRS            | Stand structure code, Pacific Northwest Research Station      | NUMBER(1)          |
| 2.5.84       | STUMP_CD_PNWRS                 | Stump code, Pacific Northwest Research Station                | VARCHAR2(1)        |
| 2.5.85       | FIRE_SRS                       | Fire, Southern Research Station                               | NUMBER(1)          |
| 2.5.86       | GRAZING_SRS                    | Grazing, Southern Research Station                            | NUMBER(1)          |
| 2.5.87       | HARVEST_TYPE1_SRS              | Harvest type code 1, Southern Research Station                | NUMBER(2)          |
| 2.5.88       | HARVEST_TYPE2_SRS              | Harvest type code 2, Southern Research Station                | NUMBER(2)          |
| 2.5.89       | HARVEST_TYPE3_SRS              | Harvest type code 3, Southern Research Station                | NUMBER(2)          |
| 2.5.90       | LAND_USE_SRS                   | Land use, Southern Research Station                           | NUMBER(2)          |
| 2.5.91       | OPERABILITY_SRS                | Operability, Southern Research Station                        | NUMBER(2)          |
| 2.5.92       | STAND_STRUCTURE_SRS            | Stand structure, Southern Research Station                    | NUMBER(2)          |
| 2.5.93       | NF_COND_STATUS_CD              | Nonforest condition status code                               | NUMBER(1)          |
| 2.5.94       | NF_COND_NONSAMPLE_REASN_C D    | Nonforest condition nonsampled reason code                    | NUMBER(2)          |
| 2.5.95       | CANOPY_CVR_SAMPLE_METHOD_ CD   | Canopy cover sample method code                               | NUMBER(2)          |
| 2.5.96       | LIVE_CANOPY_CVR_PCT            | Live canopy cover percent                                     | NUMBER(3)          |

| Subsection   | Column name (attribute)     | Descriptive name                                                              | Oracle data type   |
|--------------|-----------------------------|-------------------------------------------------------------------------------|--------------------|
| 2.5.97       | LIVE_MISSING_CANOPY_CVR_PCT | Live plus missing canopy cover percent                                        | NUMBER(3)          |
| 2.5.98       | NBR_LIVE_STEMS              | Number of live stems                                                          | NUMBER(5)          |
| 2.5.99       | OWNSUBCD                    | Owner subclass code                                                           | NUMBER(1)          |
| 2.5.100      | INDUSTRIALCD_FIADB          | Industrial code in FIADB                                                      | NUMBER(1)          |
| 2.5.101      | RESERVCD_5                  | Reserved status code field, versions 1.0-5.0                                  | NUMBER(1)          |
| 2.5.102      | ADMIN_WITHDRAWN_CD          | Administratively withdrawn code                                               | NUMBER(1)          |
| 2.5.103      | CHAINING_CD                 | Chaining code                                                                 | NUMBER(1)          |
| 2.5.104      | LAND_COVER_CLASS_CD_RET     | Land cover class, retired                                                     | NUMBER(2)          |
| 2.5.105      | AFFORESTATION_CD            | Current afforestation code                                                    | NUMBER(1)          |
| 2.5.106      | PREV_AFFORESTATION_CD       | Previous afforestation code                                                   | NUMBER(1)          |
| 2.5.107      | DWM_FUELBED_TYPCD           | DWM condition fuelbed type code                                               | VARCHAR2(3)        |
| 2.5.108      | NVCS_PRIMARY_CLASS          | Primary class of the National Vegetation Classification Standard (NVCS)       | VARCHAR2(8)        |
| 2.5.109      | NVCS_LEVEL_1_CD             | Level 1 code of the NVCS                                                      | VARCHAR2(25)       |
| 2.5.110      | NVCS_LEVEL_2_CD             | Level 2 code of the NVCS                                                      | VARCHAR2(25)       |
| 2.5.111      | NVCS_LEVEL_3_CD             | Level 3 code of the NVCS                                                      | VARCHAR2(25)       |
| 2.5.112      | NVCS_LEVEL_4_CD             | Level 4 code of the NVCS                                                      | VARCHAR2(25)       |
| 2.5.113      | NVCS_LEVEL_5_CD             | Level 5 code of the NVCS                                                      | VARCHAR2(25)       |
| 2.5.114      | NVCS_LEVEL_6_CD             | Level 6 code of the NVCS                                                      | VARCHAR2(25)       |
| 2.5.115      | NVCS_LEVEL_7_CD             | Level 7 code of the NVCS                                                      | VARCHAR2(25)       |
| 2.5.116      | NVCS_LEVEL_8_CD             | Level 8 code of the NVCS                                                      | VARCHAR2(25)       |
| 2.5.117      | AGE_BASIS_CD_PNWRS          | Age basis code, Pacific Northwest Research Station                            | NUMBER(2)          |
| 2.5.118      | COND_STATUS_CHNG_CD_RMRS    | Condition class status change code, Rocky Mountain Research Station           | NUMBER(1)          |
| 2.5.119      | CRCOVPCT_RMRS               | Live crown cover percent, Rocky Mountain Research Station                     | NUMBER(3)          |
| 2.5.120      | DOMINANT_SPECIES1_PNWRS     | Dominant tree species 1 (Pacific Islands), Pacific Northwest Research Station | NUMBER(4)          |
| 2.5.121      | DOMINANT_SPECIES2_PNWRS     | Dominant tree species 2 (Pacific Islands), Pacific Northwest Research Station | NUMBER(4)          |
| 2.5.122      | DOMINANT_SPECIES3_PNWRS     | Dominant tree species 3 (Pacific Islands), Pacific Northwest Research Station | NUMBER(4)          |
| 2.5.123      | DSTRBCD1_P2A                | Disturbance code 1, periodic to annual                                        | NUMBER(2)          |

| Subsection   | Column name (attribute)   | Descriptive name                                                                      | Oracle data type   |
|--------------|---------------------------|---------------------------------------------------------------------------------------|--------------------|
| 2.5.124      | DSTRBCD2_P2A              | Disturbance code 2, periodic to annual                                                | NUMBER(2)          |
| 2.5.125      | DSTRBCD3_P2A              | Disturbance code 3, periodic to annual                                                | NUMBER(2)          |
| 2.5.126      | DSTRBYR1_P2A              | Disturbance year 1, periodic to annual                                                | NUMBER(4)          |
| 2.5.127      | DSTRBYR2_P2A              | Disturbance year 2, periodic to annual                                                | NUMBER(4)          |
| 2.5.128      | DSTRBYR3_P2A              | Disturbance year 3, periodic to annual                                                | NUMBER(4)          |
| 2.5.129      | FLDTYPCD_30               | Field forest type code, version 3.0                                                   | NUMBER(3)          |
| 2.5.130      | FOREST_COMMUNITY_PNWRS    | Forest type (Pacific Islands), Pacific Northwest Research Station                     | NUMBER(3)          |
| 2.5.131      | LAND_USECD_RMRS           | Land use code, Rocky Mountain Research Station                                        | NUMBER(1)          |
| 2.5.132      | MAICF                     | Mean annual increment cubic feet                                                      | NUMBER(5,2)        |
| 2.5.133      | PCTBARE_RMRS              | Percent bare ground, Rocky Mountain Research Station                                  | NUMBER(3)          |
| 2.5.134      | QMD_RMRS                  | Quadratic mean diameter, Rocky Mountain Research Station                              | NUMBER(5,1)        |
| 2.5.135      | RANGETYPCD_RMRS           | Range type code (existing vegetation classification), Rocky Mountain Research Station | NUMBER(3)          |
| 2.5.136      | SDIMAX_RMRS               | Stand density index maximum, Rocky Mountain Research Station                          | NUMBER(4)          |
| 2.5.137      | SDIPCT_RMRS               | Stand density index percent, Rocky Mountain Research Station                          | NUMBER(4,1)        |
| 2.5.138      | SDI_RMRS                  | Stand density index for the condition, Rocky Mountain Research Station                | NUMBER(8,4)        |
| 2.5.139      | STAND_STRUCTURE_ME_NERS   | Stand structure (Maine), Northeastern Research Station                                | NUMBER(1)          |
| 2.5.140      | TREES_PRESENT_NCRS        | Trees present on nonforest, North Central Research Station                            | NUMBER(1)          |
| 2.5.141      | TREES_PRESENT_NERS        | Trees present on nonforest, Northeastern Research Station                             | NUMBER(1)          |
| 2.5.142      | TRTCD1_P2A                | Treatment code 1, periodic to annual                                                  | NUMBER(2)          |
| 2.5.143      | TRTCD2_P2A                | Treatment code 2, periodic to annual                                                  | NUMBER(2)          |
| 2.5.144      | TRTCD3_P2A                | Treatment code 3, periodic to annual                                                  | NUMBER(2)          |
| 2.5.145      | TRTOPCD                   | Treatment opportunity                                                                 | NUMBER(2)          |

| Subsection   | Column name (attribute)       | Descriptive name                                                           | Oracle data type   |
|--------------|-------------------------------|----------------------------------------------------------------------------|--------------------|
| 2.5.146      | TRTYR1_P2A                    | Treatment year 1, periodic to annual                                       | NUMBER(4)          |
| 2.5.147      | TRTYR2_P2A                    | Treatment year 2, periodic to annual                                       | NUMBER(4)          |
| 2.5.148      | TRTYR3_P2A                    | Treatment year 3, periodic to annual                                       | NUMBER(4)          |
| 2.5.149      | LAND_COVER_CLASS_CD           | Land cover class code                                                      | NUMBER(2)          |
| 2.5.150      | SIEQN_REF_CD                  | Site index equation reference code                                         | VARCHAR2(10)       |
| 2.5.151      | SICOND_FVS                    | Site index for the condition, used by the Forest Vegetation Simulator      | NUMBER(3)          |
| 2.5.152      | SIBASE_FVS                    | Site index base age used by the Forest Vegetation Simulator                | NUMBER(3)          |
| 2.5.153      | SISP_FVS                      | Site index species code used by the Forest Vegetation Simulator            | NUMBER(4)          |
| 2.5.154      | SIEQN_REF_CD_FVS              | Site index equation reference code used by the Forest Vegetation Simulator | VARCHAR2(10)       |
| 2.5.155      | MQUADPROP_UNADJ               | Microquadrat proportion unadjusted                                         | NUMBER(11,10)      |
| 2.5.156      | SOILPROP_UNADJ                | Soil proportion unadjusted                                                 | NUMBER(11,10)      |
| 2.5.157      | FOREST_COND_STATUS_CHANGE_ CD | Forest land condition status change code                                   | NUMBER(1)          |

| Key Type   | Column(s) order                                | Tables to link    | Abbreviated notation   |
|------------|------------------------------------------------|-------------------|------------------------|
| Primary    | CN                                             | N/A               | CND_PK                 |
| Unique     | PLT_CN, CONDID                                 | N/A               | CND_UK                 |
| Natural    | STATECD, INVYR, UNITCD, COUNTYCD, PLOT, CONDID | N/A               | CND_NAT_I              |
| Foreign    | PLT_CN                                         | CONDITION to PLOT | CND_PLT_FK             |

## 2.5.1 CN

Sequence number. A unique sequence number used to identify a condition record.

## 2.5.2 PLT\_CN

Plot sequence number. Foreign key linking the condition record to the plot record.

## 2.5.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 2.5.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 2.5.5 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 2.5.6 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 2.5.7 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combination of variables, PLOT may be used to uniquely identify a plot.

## 2.5.8 CONDID

Condition class number. Unique identifying number assigned to each condition on a plot. A condition is initially defined by condition class status. Differences in reserved status, owner group, forest type, stand-size class, regeneration status, and stand density further define condition for forest land. Mapped nonforest conditions are also assigned numbers. At the time of the plot establishment, the condition class at plot center (the center of subplot 1) is usually designated as condition class 1. Other condition classes are assigned numbers sequentially at the time each condition class is delineated. On a plot, each sampled condition class must have a unique number that can change at remeasurement to reflect new conditions on the plot.

## 2.5.9 COND\_STATUS\_CD

Condition status code. A code indicating the basic land classification.

Note: Starting with PLOT.MANUAL  6.0, codes 1 and 2 have been modified to match FIA's  new definition for accessible forest land and nonforest land. The current wording of "at least 10 percent canopy cover" replaces older wording of "at least 10 percent stocked" as the qualifying criterion in classification. This criterion applies to any tally tree species, including woodland tree species.

## Codes: COND\_STATUS\_CD

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Accessible forest land - Land within the population of interest that can be occupied safely and has at least 10 percent canopy cover by live tally trees of any size or has had at least 10 percent canopy cover of live tally species in the past, based on the presence of stumps, snags, or other evidence. To qualify, the area must be at least 1.0 acre in size and 120.0 feet wide. Forest land includes transition zones, such as areas between forest and nonforest lands that meet the minimal tree canopy cover and forest areas adjacent to urban and built-up lands. Roadside, streamside, and shelterbelt strips of trees must have a width of at least 120 feet and continuous length of at least 363 feet to qualify as forest land. Unimproved roads and trails, streams, and clearings in forest areas are classified as forest if they are less than 120 feet wide or less than an acre in size. Tree-covered areas in agricultural production settings, such as fruit orchards, or tree-covered areas in urban settings, such as city parks, are not considered forest land.                                                                                                                                                                                                                                                                                                                                                                                                |
|      2 | Nonforest land - Land that has less than 10 percent canopy cover of tally tree species of any size and, in the case of afforested land, fewer than 150 established trees per acre; or land that has sufficient canopy cover or stems, but is classified as nonforest land use (see criteria under PRESNFCD). Nonforest includes areas that have sufficient cover or live stems to meet the forest land definition, but do not meet the dimensional requirements. Note: Nonforest land includes "other wooded land" that has at least 5 percent, but less than 10 percent, canopy cover of live tally tree species of any size or has had at least 5 percent, but less than 10 percent, canopy cover of tally species in the recent past, based on the presence of stumps, snags, or other evidence. Other wooded land is recognized as a subset of nonforest land, and therefore is not currently considered a separate condition class. Other wooded land is not subject to nonforest use(s) that prevent normal tree regeneration and succession, such as regular mowing, intensive grazing, or recreation activities. In addition, other wooded land is classified according to the same nonforest land use rules as forest land (e.g., 6 percent cover in an urban setting is not considered other wooded land). Other wooded land is therefore defined as having >5 percent and <10 percent canopy cover at present, or evidence of such in the past, and PRESNFCD = 20, 40, 42, 43 or 45. |
|      3 | Noncensus water - Lakes, reservoirs, ponds, and similar bodies of water 1.0 acre to 4.5 acre in size. Rivers, streams, canals, etc., 30.0 feet to 200 feet wide. This definition was used in the 1990 census and applied when the data became available. Earlier inventories defined noncensus water differently.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|      4 | Census water - Lakes, reservoirs, ponds, and similar bodies of water 4.5 acre in size and larger; and rivers, streams, canals, etc., more than 200 feet wide.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|      5 | Nonsampled, possibility of forest land - Any portion of a plot within accessible forest land that cannot be sampled is delineated as a separate condition. There is no minimum size requirement. The reason the condition was not sampled is provided in COND_NONSAMPLE_REASN_CD.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |

## 2.5.10 COND\_NONSAMPLE\_REASN\_CD

Condition nonsampled reason code. A code indicating the reason a condition class was not sampled.

## Codes: COND\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                                                                                                                                    |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     01 | Outside U.S. boundary - Condition class is outside the U.S. border.                                                                                                                                                                            |
|     02 | Denied access area - Access to the condition class is denied by the legal owner, or by the owner of the only reasonable route to the condition class.                                                                                          |
|     03 | Hazardous situation - Condition class cannot be accessed because of a hazard or danger, for example cliffs, quarries, strip mines, illegal substance plantations, temporary high water, etc.                                                   |
|     05 | Lost data - The data file was discovered to be corrupt after a panel was completed and submitted for processing. Used for the single condition that is required for this plot. This code is for office use only.                               |
|     06 | Lost plot - Entire plot cannot be found. Used for the single condition that is required for this plot.                                                                                                                                         |
|     07 | Wrong location - Previous plot can be found, but its placement is beyond the tolerance limits for plot location. Used for the single condition that is required for this plot.                                                                 |
|     08 | Skipped visit - Entire plot skipped. Used for plots that are not completed prior to the time a panel is finished and submitted for processing. Used for the single condition that is required for this plot. This code is for office use only. |
|     09 | Dropped intensified plot - Intensified plot dropped due to a change in grid density. Used for the single condition that is required for this plot. This code used only by units engaged in intensification. This code is for office use only.  |
|     10 | Other - Condition class not sampled due to a reason other than one of the specific reasons listed.                                                                                                                                             |
|     11 | Ocean - Condition falls in ocean water below mean high tide line.                                                                                                                                                                              |

## 2.5.11 RESERVCD

Reserved status code. (core for accessible forest land; core optional for other sampled land) A code indicating the reserved status of the condition on publicly owned land. Starting with PLOT.MANUAL  6.0, the description has been modified to match FIA's new application of the definition for reserved land. Reserved land is permanently prohibited from being managed for the production of wood products through statute or agency mandate; the prohibition cannot be changed through decision of the land manager. Logging may occur to meet protected area objectives. Examples include designated Federal wilderness areas, national parks and monuments, and most State parks. Private land cannot be reserved. RESERVCD differs from RESERVCD\_5, which stores reserved status based on the previous definition. See appendix L for applications of RESERVCD by FIA region and State.

## Codes: RESERVCD

|   Code | Description   |
|--------|---------------|
|      0 | Not reserved. |
|      1 | Reserved.     |

## 2.5.12 OWNCD

Owner class code. (core for all accessible forest land; core optional for other sampled land) A code indicating the ownership category of the land for the condition. When PLOT.DESIGNCD = 999, OWNCD may be blank (null).

## Codes: OWNCD

|   Code | Description                                                     |
|--------|-----------------------------------------------------------------|
|     11 | National Forest.                                                |
|     12 | National Grassland and/or Prairie.                              |
|     13 | Other Forest Service land.                                      |
|     21 | National Park Service.                                          |
|     22 | Bureau of Land Management.                                      |
|     23 | Fish and Wildlife Service.                                      |
|     24 | Departments of Defense/Energy.                                  |
|     25 | Other Federal.                                                  |
|     31 | State including State public universities.                      |
|     32 | Local (County, Municipality, etc.) including water authorities. |
|     33 | Other non-Federal public.                                       |
|     46 | Undifferentiated private and Native American.                   |

The following detailed private owner land codes are not available in this database because of the FIA data confidentiality policy. Users needing this type of information should contact the FIA Spatial Data Services (SDS) group by following the instructions provided at: https://research.fs.usda.gov/programs/fia/sds.

## Codes: OWNCD

|   Code | Description                                                                  |
|--------|------------------------------------------------------------------------------|
|     41 | Corporate, including Native Corporations in Alaska and private universities. |
|     42 | Non-governmental conservation/natural resources organization.                |
|     43 | Unincorporated local partnership/association/club.                           |
|     44 | Native American.                                                             |
|     45 | Individual and family, including trusts, estates, and family partnerships.   |

## 2.5.13 OWNGRPCD

Owner group code. (core for all accessible forest land; core optional for other sampled land) A code indicating the ownership group of the land for the condition. When PLOT.DESIGNCD = 999, OWNGRPCD may be blank (null).

Note: OWNGRPCD = 40 includes Native American lands.

## Codes: OWNGRPCD

|   Code | Description                               |
|--------|-------------------------------------------|
|     10 | Forest Service (OWNCD = 11, 12, 13).      |
|     20 | Other Federal (OWNCD 21, 22, 23, 24, 25). |

|   Code | Description                                      |
|--------|--------------------------------------------------|
|     30 | State and local government (OWNCD = 31, 32, 33). |
|     40 | Private (OWNCD = 41, 42, 43, 44, 45, 46).        |

## 2.5.14 ADFORCD

Administrative forest code. A code indicating the administrative unit (Forest Service Region and National Forest) in which the condition is located. The first 2 digits of the 4-digit code are for the region number and the last 2 digits are for the Administrative National Forest number. Refer to appendix C for codes. Populated for U.S. Forest Service lands OWNGRPCD = 10 and blank (null) for all other owners, except in a few cases where an administrative forest manages land owned by another Federal agency; in this case OWNGRPCD = 20 and ADFORCD &gt;0.

## 2.5.15 FORTYPCD

Forest type code. This is the forest type used for reporting purposes. It is primarily derived using a computer algorithm, except when less than 25 percent of the plot samples a particular forest condition or in a few cases where the derived FORTYPCDCALC does not accurately reflect the actual condition.

Nonstocked forest land is land that currently has less than 10 percent stocking but formerly met the definition of forest land. Forest conditions meeting this definition have few, if any, trees sampled. In these instances, the algorithm cannot assign a specific forest type and the resulting forest type code is 999, meaning nonstocked. See ALSTKCD for information on estimates of nonstocked areas.

Refer to appendix D for the complete list of forest type codes and names.

## 2.5.16 FLDTYPCD

Field forest type code. A code indicating the forest type, assigned by the field crew, based on the tree species or species groups forming a plurality of all live stocking. The field crew assesses the forest type based on the acre of forest land around the plot, in addition to the species sampled on the condition. Refer to appendix D for a detailed list of forest type codes and names. Nonstocked forest land is land that currently has less than 10 percent stocking but formerly met the definition of forest land. When PLOT.MANUAL &lt;2.0, forest conditions that do not meet this stocking level were coded FLDTYPCD = 999. Starting with PLOT.MANUAL = 2.0, the crew no longer recorded nonstocked as 999. Instead, they recorded FLDSZCD = 0 to identify nonstocked conditions and entered an estimated forest type for the condition. The crew determined the estimated forest type by either recording the previous forest type on remeasured plots or, on all other plots, the most appropriate forest type to the condition based on the seedlings present or the forest type of the adjacent forest stands. Periodic inventories will differ in the way FLDTYPCD was recorded - it is best to check with individual FIA work units (table 1-1) for details. In general, when FLDTYPCD is used for analysis, it is necessary to examine the values of both FLDTYPCD and FLDSZCD to identify nonstocked forest land.

## 2.5.17 MAPDEN

Mapping density. A code indicating the relative tree density of the condition. Codes other than 1 are used as an indication that a significant difference in tree density is the only factor causing another condition to be recognized and mapped on the plot. May be blank (null) for periodic inventories.

## Condition Table

## Codes: MAPDEN

|   Code | Description                                                                                                    |
|--------|----------------------------------------------------------------------------------------------------------------|
|      1 | Initial tree density class.                                                                                    |
|      2 | Density class 2 - density different than density of the condition assigned a tree density class of 1.          |
|      3 | Density class 3 - density different than densities of the conditions assigned tree density classes of 1 and 2. |

## 2.5.18 STDAGE

Stand age. For annual inventories (PLOT.MANUAL  1.0), stand age is equal to the field-recorded stand age (FLDAGE) with some exceptions:

- · When FLDAGE = 999, tree cores are first sent to the office for the counting of rings. Stand age is then estimated based upon the average total age of live trees that fall within the calculated stand-size assignment.
- · When FLDAGE = 998, STDAGE may be blank (null) because no trees were cored in the field.
- · If no tree ages are available, then RMRS (SURVEY.RSCD = 22) sets this attribute equal to FLDAGE.

For annual inventories, nonstocked stands have STDAGE set to 0. When FLDSZCD = 0 (nonstocked) but STDSZCD &lt;5 (not nonstocked), STDAGE may be set to 0 because FLDAGE = 0. Annual inventory data will contain stand ages assigned to the nearest year. For periodic data, stand age was calculated using various methods. Contact the appropriate FIA work unit (table 1-1) for details.

## 2.5.19 STDSZCD

Stand-size class code. A classification of the predominant (based on stocking) diameter class of live trees within the condition assigned using an algorithm. Large diameter trees are at least 11.0 inches diameter for hardwoods and at least 9.0 inches diameter for softwoods. Medium diameter trees are at least 5.0 inches diameter and smaller than large diameter trees. Small diameter trees are &lt;5.0 inches diameter. When &lt;25 percent of the plot samples the forested condition (CONDPROP\_UNADJ &lt;0.25), this attribute is set to the equivalent field-recorded stand-size class (FLDSZCD). Populated for forest conditions. This attribute is blank (null) for periodic plots that are used only for growth, mortality and removal estimates, and modeling of reserved and unproductive conditions.

## Codes: STDSZCD

|   Code | Description                                                                                                                                                                                                                                                                    |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Large diameter - Stands with an all live stocking value of at least 10 (base 100); with more than 50 percent of the stocking in medium and large diameter trees; and with the stocking of large diameter trees equal to or greater than the stocking of medium diameter trees. |
|      2 | Medium diameter - Stands with an all live stocking value of at least 10 (base 100); with more than 50 percent of the stocking in medium and large diameter trees; and with the stocking of large diameter trees less than the stocking of medium diameter trees.               |

|   Code | Description                                                                                                                                                |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      3 | Small diameter - Stands with an all live stocking value of at least 10 (base 100) on which at least 50 percent of the stocking is in small diameter trees. |
|      5 | Nonstocked - Forest land with all live stocking value <10.                                                                                                 |

## 2.5.20 FLDSZCD

Field stand-size class code. A code indicating the field-assigned classification of the predominant (based on stocking) diameter class of live trees within the condition. May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0.

## Codes: FLDSZCD

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | Nonstocked - Meeting the definition of accessible land and one of the following applies (1) <10 percent stocked by trees, seedlings, and saplings and not classified as cover trees, or 10 percent canopy cover if stocking standards are not available, or (2) for several woodland species where stocking standards are not available, <10 percent canopy cover of trees, seedlings, and saplings.                                                                             |
|      1 |  4.9 inches (seedlings/saplings). At least 10 percent stocking (or 10 percent canopy cover if stocking standards are not available) in trees, seedlings, and saplings, and at least 2/3 of the canopy cover is in trees <5.0 inches d.b.h./d.r.c.                                                                                                                                                                                                                               |
|      2 | 5.0-8.9 inches (softwoods and woodland trees)/ 5.0-10.9 inches (hardwoods). At least 10 percent stocking (or 10 percent canopy cover if stocking standards are not available) in trees, seedlings, and saplings; and at least one-third of the canopy cover is in trees >5.0 inches d.b.h./d.r.c. and the plurality of the canopy cover is in softwoods 5.0-8.9 inches diameter and/or hardwoods 5.0-10.9 inches d.b.h., and/or woodland trees 5.0-8.9 inches d.r.c.             |
|      3 | 9.0-19.9 inches (softwoods and woodland trees)/ 11.0-19.9 inches (hardwoods). At least 10 percent stocking (or 10 percent canopy cover if stocking standards are not available) in trees, seedlings, and sapling; and at least one-third of the canopy cover is in trees >5.0 inches d.b.h./d.r.c. and the plurality of the canopy cover is in softwoods 9.0-19.9 inches diameter and/or hardwoods between 11.0-19.9 inches d.b.h., and/or woodland trees 9.0-19.9 inches d.r.c. |
|      4 | 20.0-39.9 inches. At least 10 percent stocking (or 10 percent canopy cover if stocking standards are not available) in trees, seedlings, and saplings; and at least one-third of the canopy cover is in trees >5.0 inches d.b.h./d.r.c. and the plurality of the canopy cover is in trees 20.0-39.9 inches d.b.h.                                                                                                                                                                |
|      5 | 40.0+ inches. At least 10 percent stocking (or 10 percent canopy cover if stocking standards are not available) in trees, seedlings, and saplings; and at least one-third of the canopy cover is in trees >5.0 inches d.b.h./d.r.c. and the plurality of the canopy cover is in trees  40.0 inches d.b.h.                                                                                                                                                                       |

## 2.5.21 SITECLCD

Site productivity class code. A code indicating the classification of forest land in terms of inherent capacity to grow crops of industrial wood. Identifies the potential growth in cubic feet/acre/year and is based on the culmination of mean annual increment of fully stocked natural stands. This attribute may be assigned based on the site trees available for the plot, or, if no valid site trees are available, this attribute is set equal to SITECLCDEST, a default value that is either an estimated or predicted site productivity class. If SITECLCDEST is used to populate SITECLCD, the attribute SITECL\_METHOD is set to 6.

## Condition Table

## Codes: SITECLCD

|   Code | Description                   |
|--------|-------------------------------|
|      1 | 225+ cubic feet/acre/year.    |
|      2 | 165-224 cubic feet/acre/year. |
|      3 | 120-164 cubic feet/acre/year. |
|      4 | 85-119 cubic feet/acre/year.  |
|      5 | 50-84 cubic feet/acre/year.   |
|      6 | 20-49 cubic feet/acre/year.   |
|      7 | 0-19 cubic feet/acre/year.    |

## 2.5.22 SICOND

Site index for the condition. This represents the average total length in feet that dominant and co-dominant trees are expected to attain in well-stocked, even-aged stands at the specified base age (SIBASE). Site index is estimated for the condition by either using an individual tree or by averaging site index values that have been calculated for individual site trees (see SITETREE.SITREE) of the same species (SISP). As a result, it may be possible to find additional site index values that are not used in the calculation of SICOND in the SITETREE tables when site index has been calculated for more than one species in a condition. Site index values in SICOND are often used to calculate productivity class and other condition-level attributes. This attribute is blank (null) when no site index data are available.

## 2.5.23 SIBASE

Site index base age. The base age (sometimes called reference age), in years, of the site index curve used to derive site index. Base age may be breast height age or total age, depending on the specifications of the site index curves being used. This attribute is blank (null) when no site tree data are available.

## 2.5.24 SISP

Site index species code. The species upon which the site index is based. In most cases, the site index species will be one of the species that define the forest type of the condition (FORTYPCD). In cases where there are no suitable site trees of the type species, other suitable species may be used. This attribute is blank (null) when no site tree data are available.

## 2.5.25 STDORGCD

Stand origin code. A code indicating the method of stand regeneration for the trees in the condition. An artificially regenerated stand is established by planting or artificial seeding. Populated for forest conditions.

## Codes: STDORGCD

|   Code | Description                                |
|--------|--------------------------------------------|
|      0 | Natural stands.                            |
|      1 | Clear evidence of artificial regeneration. |

## 2.5.26 STDORGSP

Stand origin species code. The species code for the predominant artificially regenerated species (only populated when STDORGCD = 1). See appendix F. May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0.

## 2.5.27 PROP\_BASIS

Proportion basis. A value indicating what type of fixed-size subplots were installed when this plot was sampled. This information is needed to use the proper adjustment factor for the stratum in which the plot occurs (see POP\_STRATUM.ADJ\_FACTOR\_SUBP and POP\_STRATUM.ADJ\_FACTOR\_MACR).

Note: This attribute may not be populated for periodic inventories.

## Codes: PROP\_BASIS

| Code   | Description                                |
|--------|--------------------------------------------|
| SUBP   | Subplots (24.0-foot radius per subplot).   |
| MACR   | Macroplots (58.9-foot radius per subplot). |

## 2.5.28 CONDPROP\_UNADJ

Condition proportion unadjusted. The unadjusted proportion of the plot that is in the condition. This attribute is retained for ease of area calculations. It is equal to either SUBPPROP\_UNADJ or MACRPROP\_UNADJ, depending on the value of PROP\_BASIS. The sum of all condition proportions for a plot equals 1. When generating population area estimates, this proportion is adjusted by either the POP\_STRATUM.ADJ\_FACTOR\_MACR or the POP\_STRATUM.ADJ\_FACTOR\_SUBP to account for partially nonsampled plots (access denied or hazardous portions).

## 2.5.29 MICRPROP\_UNADJ

Microplot proportion unadjusted. The unadjusted proportion of the microplots that are in the condition. The sum of all microplot condition proportions for a plot equals 1.

## 2.5.30 SUBPPROP\_UNADJ

Subplot proportion unadjusted. The unadjusted proportion of the subplots that are in the condition. The sum of all subplot condition proportions for a plot equals 1.

## 2.5.31 MACRPROP\_UNADJ

Macroplot proportion unadjusted. The unadjusted proportion of the macroplots that are in the condition. When macroplots are installed, the sum of all macroplot condition proportions for a plot equals 1; otherwise this attribute is left blank (null).

## 2.5.32 SLOPE

Condition percent slope. The predominant or average angle of the slope across the condition to the nearest 1 percent. Valid values are 0 through 155 for data collected when PLOT.MANUAL  1.0, and 0 through 200 on data collected when PLOT.MANUAL &lt;1.0.

When PLOT.MANUAL &lt;1.0, the field crew measured slope at a condition level by sighting along the average incline or decline of the condition. When PLOT.MANUAL  1.0, slope is collected at a subplot level (see SUBPLOT.SLOPE), and then the slope from the subplot representing the greatest proportion of the condition is assigned as a surrogate. In the

event that two or more subplots represent the same area in the condition, the slope from the lower numbered subplot is used.

Note: When PLOT.MANUAL &lt;1.0, this attribute is populated for all forest periodic plots and all NCRS periodic plots that were measured as "nonforest with trees" (e.g., wooded pasture, windbreaks).

## 2.5.33 ASPECT

Condition aspect. The aspect across the condition to the nearest 1 degree. North is recorded as 360. When slope is &lt;5 percent, there is no aspect and this item is set to 0.

When PLOT.MANUAL &lt;1.0, the field crew measured aspect at the condition level. When PLOT.MANUAL  1.0, aspect is collected at a subplot level (see SUBPLOT.ASPECT), and then the aspect from the subplot representing the greatest proportion of the condition is assigned as a surrogate. In the event that two or more subplots represent the same area in the condition, the slope from the lower numbered subplot is used.

Note: When PLOT.MANUAL &lt;1.0, this attribute is populated for all forest periodic plots and all NCRS periodic plots that were measured as "nonforest with trees" (e.g. wooded pasture, windbreaks).

## 2.5.34 PHYSCLCD

Physiographic class code. A code indicating the general effect of land form, topographical position, and soil on moisture available to trees.

Note: When PLOT.MANUAL &lt;1.0, this attribute is populated for all forest periodic plots and all NCRS periodic plots that were measured as "nonforest with trees" (e.g., wooded pasture, windbreaks).

## Codes: PHYSCLCD

| Code   | Description                                                                                                                                                                                                                          |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| -      | Xeric sites (normally low or deficient in available moisture)                                                                                                                                                                        |
| 11     | Dry Tops - Ridge tops with thin rock outcrops and considerable exposure to sun and wind.                                                                                                                                             |
| 12     | Dry Slopes - Slopes with thin rock outcrops and considerable exposure to sun and wind. Includes most mountain/steep slopes with a southern or western exposure.                                                                      |
| 13     | Deep Sands - Sites with a deep, sandy surface subject to rapid loss of moisture following precipitation. Typical examples include sand hills, ridges, and flats in the South, sites along the beach and shores of lakes and streams. |
| 19     | Other Xeric - All dry physiographic sites not described above.                                                                                                                                                                       |
| -      | Mesic sites (normally moderate but adequate available moisture)                                                                                                                                                                      |
| 21     | Flatwoods - Flat or fairly level sites outside of floodplains. Excludes deep sands and wet, swampy sites.                                                                                                                            |
| 22     | Rolling Uplands - Hills and gently rolling, undulating terrain and associated small streams. Excludes deep sands, all hydric sites, and streams with associated floodplains.                                                         |
| 23     | Moist Slopes and Coves - Moist slopes and coves with relatively deep, fertile soils. Often these sites have a northern or eastern exposure and are partially shielded from wind and sun. Includes moist mountain tops and saddles.   |

| Code   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 24     | Narrow floodplains/Bottomlands - Floodplains and bottomlands less than 1/4 mile in width along rivers and streams. These sites are normally well drained but are subjected to occasional flooding during periods of heavy or extended precipitation. Includes associated levees, benches, and terraces within a 1/4 mile limit. Excludes swamps, sloughs, and bogs.                                                                                      |
| 25     | Broad Floodplains/Bottomlands - Floodplains and bottomlands 1/4-mile or wider along rivers and streams. These sites are normally well drained but are subjected to occasional flooding during periods of heavy or extended precipitation. Includes associated levees, benches, and terraces. Excludes swamps, sloughs, and bogs with year-round water problems.                                                                                          |
| 29     | Other Mesic - All moderately moist physiographic sites not described above.                                                                                                                                                                                                                                                                                                                                                                              |
| -      | Hydric sites (normally abundant or overabundant moisture all year)                                                                                                                                                                                                                                                                                                                                                                                       |
| 31     | Swamps/Bogs - Low, wet, flat, forested areas usually quite extensive that are flooded for long periods except during periods of extreme drought. Excludes cypress ponds and small drains.                                                                                                                                                                                                                                                                |
| 32     | Small Drains - Narrow, stream-like, wet strands of forest land often without a well-defined stream channel. These areas are poorly drained or flooded throughout most of the year and drain the adjacent higher ground.                                                                                                                                                                                                                                  |
| 33     | Bays and wet pocosins - Low, wet, boggy sites characterized by peaty or organic soils. May be somewhat dry during periods of extended drought. Examples include sites in the Carolina bays in the Southeast United States.                                                                                                                                                                                                                               |
| 34     | Beaver ponds.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 35     | Cypress ponds.                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 36     | Forest or Nonforest over Permafrost - Low-lying, sometimes wet, flat areas, often characterized by a thick moss layered ground surface, sometimes comprised of tussocks that tend to form a waterlogged soils layer as the active layer thaws seasonally. Permafrost may be visible or detected with a soil probe. At later periods in the season when permafrost cannot be detected, waterlogged soils layered on top of deeper permafrost are possible |
| 39     | Other hydric - All other hydric physiographic sites.                                                                                                                                                                                                                                                                                                                                                                                                     |

## 2.5.35 GSSTKCD

Growing-stock stocking code. A code indicating the stocking of the condition by growing-stock trees and seedlings. Growing-stock trees are those where tree class (TREE.TREECLCD) equals 2. The following species groups (TREE.SPGRPCD) are not included: 23 (woodland softwoods), 43 (eastern noncommercial hardwoods), and 48 (woodland hardwoods). Populated for forest conditions.

Estimates (e.g., forest land area, tree volume) associated with nonstocked areas identified with stocking code (GSSTKCD and ALSTKCD), stand-size class (STDSZCD and FLDSZCD), and forest type (FORTYPCDCALC, FORTYPCD, and FLDTYPCD) can differ. Stand-size class (STDSZCD) and forest type (FORTYPCD) use a field-crew recorded stand-size class (FLDSZCD) and forest type (FLDTYPCD) when a condition is less than 25 percent of the plot area (CONDPROP\_UNADJ &lt;0.25); otherwise, stand-size class and forest type are assigned with an algorithm using trees tallied on the plot (for historical documentation, see "National Algorithms for Determining Stocking Class, Stand Size Class, and Forest Type for Forest Inventory and Analysis Plots" (Arner and others 2001) at https://www.fs.usda.gov/fmsc/ftp/fvs/docs/gtr/Arner2001.pdf or contact the appropriate FIA work unit in table 1-1). Stocking code and forest type code calculated

(FORTYPCDCALC) also use the algorithm to assign stocking to every condition on the plot, regardless of condition size. When estimates include conditions less than 25 percent of the plot area, small differences among estimates can result when summarizing by stocking code or forest type code calculated versus stand-size class or forest type. Differences are expected between field crew and algorithm assignments; the field crew assigns stand-size class and forest type considering trees on and adjacent to the plot, while the algorithm only uses trees tallied on the plot.

## Codes: GSSTKCD

|   Code | Description              |
|--------|--------------------------|
|      1 | Overstocked (100+%).     |
|      2 | Fully stocked (60-99%).  |
|      3 | Medium stocked (35-59%). |
|      4 | Poorly stocked (10-34%). |
|      5 | Nonstocked (0-9%).       |

Note: When PLOT.MANUAL &lt;1.0, this attribute is also populated for all forest plots, and all NCRS periodic plots that were measured as "nonforest with trees" (e.g., wooded pasture, windbreaks). It is blank (null) for periodic plots that are used only for growth, mortality, and removal estimates, and modeling of reserved and unproductive conditions. Some periodic survey data are in the form of an absolute stocking value (0-167). More detailed information on how stocking values were determined from plot data in a particular State can be obtained directly from the FIA work units (table 1-1).

Codes: GSSTKCD (Absolute stocking value - used for some periodic inventory data)

|   Code | Description                    |
|--------|--------------------------------|
|      1 | Overstocked (130+%).           |
|      2 | Fully stocked (100 - 129.9%).  |
|      3 | Medium stocked (60 - 99.9%).   |
|      4 | Poorly stocked (16.7 - 59.9%). |
|      5 | Nonstocked (<16.7%).           |

## 2.5.36 ALSTKCD

All live stocking code. A code indicating the stocking of the condition by live trees, including seedlings. Data are in classes as listed for GSSTKCD above. Populated for forest conditions. May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0.

Estimates (e.g., forest land area, tree volume) associated with nonstocked areas identified with stocking code (GSSTKCD and ALSTKCD), stand-size class (STDSZCD and FLDSZCD), and forest type (FORTYPCDCALC, FORTYPCD, and FLDTYPCD) can differ. Stand-size class (STDSZCD) and forest type (FORTYPCD) use a field-crew recorded stand-size class (FLDSZCD) and forest type (FLDTYPCD) when a condition is less than 25 percent of the plot area (CONDPROP\_UNADJ &lt;0.25); otherwise, stand-size class and forest type are assigned with an algorithm using trees tallied on the plot (for historical documentation, see "National Algorithms for Determining Stocking Class, Stand Size Class, and Forest Type for Forest Inventory and Analysis Plots" (Arner and others 2001) at

https://www.fs.usda.gov/fmsc/ftp/fvs/docs/gtr/Arner2001.pdf or contact the appropriate FIA work unit in table 1-1). Stocking code and forest type code calculated (FORTYPCDCALC) also use the algorithm to assign stocking to every condition on the plot, regardless of condition size. When estimates include conditions less than 25 percent of the plot area, small differences among estimates can result when summarizing by stocking code or forest type code calculated versus stand-size class or forest type. Differences are expected between field crew and algorithm assignments; the field crew assigns stand-size class and forest type considering trees on and adjacent to the plot, while the algorithm only uses trees tallied on the plot.

Note: Some periodic survey data are in the form of an absolute stocking value (0-167). More detailed information on how stocking values were determined from plot data in a particular State can be obtained directly from the FIA work units (table 1-1).

## 2.5.37 DSTRBCD1

Disturbance code 1. A code indicating the kind of disturbance occurring since the last measurement or within the last 5 years for new plots. The area affected by the disturbance must be at least 1 acre in size. A significant level of disturbance (mortality or damage to 25 percent of the trees in the condition) is required. Up to three different disturbances per condition can be recorded, from most important to least important (DSTRBCD1, DSTRBCD2, and DSTRBCD1). May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0. Codes 11, 12, 21, and 22 are valid where PLOT.MANUAL  2.0.

## Codes: DSTRBCD1

|   Code | Description                                                             |
|--------|-------------------------------------------------------------------------|
|      0 | No visible disturbance.                                                 |
|     10 | Insect damage.                                                          |
|     11 | Insect damage to understory vegetation.                                 |
|     12 | Insect damage to trees, including seedlings and saplings.               |
|     20 | Disease damage.                                                         |
|     21 | Disease damage to understory vegetation.                                |
|     22 | Disease damage to trees, including seedlings and saplings.              |
|     30 | Fire damage (from crown and ground fire, either prescribed or natural). |
|     31 | Ground fire damage.                                                     |
|     32 | Crown fire damage.                                                      |
|     40 | Animal damage.                                                          |
|     41 | Beaver (includes flooding caused by beaver).                            |
|     42 | Porcupine.                                                              |
|     43 | Deer/ungulate.                                                          |
|     44 | Bear ( core optional ).                                                 |
|     45 | Rabbit ( core optional ).                                               |
|     46 | Domestic animal/livestock (includes grazing).                           |
|     50 | Weather damage.                                                         |
|     51 | Ice.                                                                    |
|     52 | Wind (includes hurricane, tornado).                                     |
|     53 | Flooding (weather induced).                                             |

## Condition Table

|   Code | Description                                                                                                                               |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------|
|     54 | Drought.                                                                                                                                  |
|     60 | Vegetation (suppression, competition, vines).                                                                                             |
|     70 | Unknown / not sure / other.                                                                                                               |
|     80 | Human-induced damage - any significant threshold of human-caused damage not described in the DISTURBANCE codes or in the TREATMENT codes. |
|     90 | Geologic disturbances.                                                                                                                    |
|     91 | Landslide.                                                                                                                                |
|     92 | Avalanche track.                                                                                                                          |
|     93 | Volcanic blast zone.                                                                                                                      |
|     94 | Other geologic event.                                                                                                                     |
|     95 | Earth movement / avalanches.                                                                                                              |

## 2.5.38 DSTRBYR1

Disturbance year 1. The year in which disturbance 1 (DSTRBCD1) is estimated to have occurred. If the disturbance occurs continuously over a period of time, the value 9999 is used. If DSTRBCD1 = 0, then DSTRBYR1 = blank (null) or 0. May not be populated for some FIA work units when PLOT.MANUAL&lt;1.0.

## 2.5.39 DSTRBCD2

Disturbance code 2. The second disturbance code, if the stand has experienced more than one disturbance. See DSTRBCD1 for more information.

## 2.5.40 DSTRBYR2

Disturbance year 2. The year in which disturbance 2 (DSTRBCD2) occurred. See DSTRBYR1 for more information.

## 2.5.41 DSTRBCD3

Disturbance code 3. The third disturbance code, if the stand has experienced more than two disturbances. See DSTRBCD1 for more information.

## 2.5.42 DSTRBYR3

Disturbance year 3. The year in which disturbance 3 (DSTRBCD3) occurred. See DSTRBYR1 for more information.

## 2.5.43 TRTCD1

Treatment code 1. A code indicating the type of stand treatment that has occurred since the last measurement or within the last 5 years for new plots. The area affected by the treatment must be at least 1 acre in size. Populated for all forested conditions using the National Field Guide protocols (PLOT.MANUAL  1.0) and populated by some FIA work units where PLOT.MANUAL &lt;1.0. When PLOT.MANUAL &lt;1.0, inventories may record treatments occurring within the last 20 years for new plots. Up to three different treatments per condition can be recorded, from most important to least important (TRTCD1, TRTCD2, and TRTCD3).

## Codes: TRTCD1

|   Code | Description                                                                                                                                                                                                                                                                              |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     00 | No observable treatment.                                                                                                                                                                                                                                                                 |
|     10 | Cutting - The removal of one or more trees from a stand.                                                                                                                                                                                                                                 |
|     20 | Site preparation - Clearing, slash burning, chopping, disking, bedding, or other practices clearly intended to prepare a site for either natural or artificial regeneration.                                                                                                             |
|     30 | Artificial regeneration - Following a disturbance or treatment (usually cutting), a new stand where at least 50 percent of the live trees present resulted from planting or direct seeding.                                                                                              |
|     40 | Natural regeneration - Following a disturbance or treatment (usually cutting), a new stand where at least 50 percent of the live trees present (of any size) were established through the growth of existing trees and/or natural seeding or sprouting.                                  |
|     50 | Other silvicultural treatment - The use of fertilizers, herbicides, girdling, pruning, or other activities (not covered by codes 10-40) designed to improve the commercial value of the residual stand; or chaining, which is a practice used on woodlands to encourage wildlife forage. |

## 2.5.44 TRTYR1

Treatment year 1. The year in which treatment 1 (TRTCD1) is estimated to have occurred. Populated for all forested conditions that have some treatment using the National Field Guide protocols (PLOT.MANUAL  1.0) and populated by some FIA work units where PLOT.MANUAL &lt;1.0. If TRTCD1 = 00 then TRTYR1 = blank (null) or 0.

## 2.5.45 TRTCD2

Treatment code 2. The second treatment code, if the stand has experienced more than one treatment since the last measurement or within the last 5 years for new plots. See TRTCD1 for more information.

## 2.5.46 TRTYR2

Treatment year 2. The year in which treatment 2 (TRTCD2) is estimated to have occurred. See TRTYR1 for more information.

## 2.5.47 TRTCD3

Treatment code 3. The third treatment code, if the stand has experienced more than two treatments since the last measurement or within the last 5 years for new plots. See TRTCD1 for more information.

## 2.5.48 TRTYR3

Treatment year 3. The year in which treatment 3 (TRTCD3) is estimated to have occurred. See TRTYR1 for more information.

## 2.5.49 PRESNFCD

Present nonforest code. A code indicating the current land use for a nonforest condition, which meets the minimum area and width requirements (except those cases where the condition has been solely defined due to developed land uses, such as roads and rights-of-way). PRESNFCD is recorded for the following: (1) conditions that were

previously classified as forest but are now classified as nonforest, and (2) sampled nonforest conditions, regardless of the previous condition.

Note: This attribute is core starting with FIADB version 6.0 (PLOT.MANUAL  6.0), but for all prior annual inventories, it was core for remeasured conditions that were forest before and are now nonforest, and core optional for all conditions where current condition class status is nonforest, regardless of the previous condition.

## Codes: PRESNFCD

|   Code | Description                                                     |
|--------|-----------------------------------------------------------------|
|     10 | Agricultural land.                                              |
|     11 | Cropland.                                                       |
|     12 | Pasture (improved through cultural practices).                  |
|     13 | Idle farmland.                                                  |
|     16 | Maintained wildlife opening.                                    |
|     17 | Windbreak/Shelterbelt.                                          |
|     20 | Rangeland.                                                      |
|     30 | Developed.                                                      |
|     31 | Cultural (business, residential, other intense human activity). |
|     32 | Rights-of-way (improved road, railway, power line).             |
|     40 | Other (undeveloped beach, marsh, bog, snow, ice).               |
|     41 | Nonvegetated.                                                   |
|     42 | Wetland.                                                        |
|     43 | Beach.                                                          |
|     45 | Nonforest-Chaparral.                                            |

The following detailed current nonforest land use codes are not available in this database because of the FIA data confidentiality policy. Users needing this type of information should contact the FIA Spatial Data Services (SDS) group by following the instructions provided at https://research.fs.usda.gov/programs/fia/sds.

Note: Codes 14 and 15 are included in code 10. Codes 33 and 34 are included in code 30.

## Codes: PRESNFCD

|   Code | Description                              |
|--------|------------------------------------------|
|     14 | Orchard.                                 |
|     15 | Christmas tree plantation.               |
|     33 | Recreation (park, golf course, ski run). |
|     34 | Mining.                                  |

## 2.5.50 BALIVE

Basal area per acre of live trees. Basal area in square feet per acre of all live trees  1.0 inch d.b.h./d.r.c. sampled in the condition. Populated for forest conditions.

## 2.5.51 FLDAGE

Field-recorded stand age. The stand age as assigned by the field crew. Based on the average total age, to the nearest year, of the trees in the field-recorded stand-size class of the condition, determined using local procedures. For nonstocked stands, a value of 0 is stored. If all of the trees in a condition class are of a species that by regional standards cannot be cored for age (e.g., mountain mahogany, tupelo), 998 is recorded. If tree cores are not counted in the field, but are collected and sent to the office for the counting of rings, 999 is recorded.

## 2.5.52 ALSTK

All-live-tree stocking percent. The sum of stocking percent values of all live trees, including seedlings, on the condition. The percent is then assigned to a stocking class, which is found in ALSTKCD. Populated for forest conditions. May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0.

Note: Some periodic survey data are in the form of an absolute stocking value (0-167). More detailed information on how stocking values were determined from plot data in a particular State can be obtained directly from the FIA work units (table 1-1).

## 2.5.53 GSSTK

Growing-stock stocking percent. The sum of stocking percent values of all growing-stock trees and seedlings on the condition. The percent is then assigned to a stocking class, which is found in GSSTKCD. Growing-stock trees are those where tree class (TREE.TREECLCD) equals 2. The following species groups (TREE.SPGRPCD) are not included: 23 (woodland softwoods), 43 (eastern noncommercial hardwoods), and 48 (woodland hardwoods). Populated for forest conditions. May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0.

Note: Some periodic survey data are in the form of an absolute stocking value (0-167). More detailed information on how stocking values were determined from plot data in a particular State can be obtained directly from the FIA work units (table 1-1).

## 2.5.54 FORTYPCDCALC

Forest type code calculated. Forest type is calculated based on the tree species sampled on the condition. The forest typing algorithm is a hierarchical procedure applied to the tree species sampled on the condition. The algorithm begins by comparing the live tree stocking of softwoods and hardwoods and continues in a stepwise fashion comparing successively smaller subgroups of the preceding aggregation of initial type groups, selecting the group with the largest aggregate stocking value. The comparison proceeds in most cases until a plurality of a forest type is identified.

In instances where the condition is more than 10 percent stocked, but the algorithm cannot identify a forest type, FORTYPCDCALC is blank (null). Nonstocked forest land is land that currently has less than 10 percent stocking but formerly met the definition of forest land. Forest conditions meeting this definition have few, if any, trees sampled. In these instances, the algorithm cannot assign a specific forest type and the resulting forest type code is 999, meaning nonstocked.

FORTYPCDCALC is only used for computational purposes. It is a direct output from the algorithm, and is used to populate FORTYPCD when the condition is at least 25 percent of the plot area (CONDPROP\_UNADJ  .25). See also FORTYPCD and FLDTYPCD. Refer to appendix D for a complete list of forest type codes and names.

## 2.5.55 HABTYPCD1

Habitat type code 1. A code indicating the primary habitat type (or community type) for this condition. Unique codes are determined by combining both habitat type code and publication code (HABTYPCD1 and HABTYPCD1\_PUB\_CD). Habitat type captures information about both the overstory and understory vegetation and usually describes the vegetation that is predicted to become established after all successional stages of the ecosystem are completed without any disturbance. This code can be translated using the publication in which it was named and described (see HABTYPCD1\_PUB\_CD and HABTYPCD1\_DESCR\_PUB\_CD). Only populated by certain FIA work units (SURVEY.RSCD = 22, 23, 26).

Note: For Caribbean Islands, life zone codes are populated in this column (see VOL\_LOC\_GRP for definitions). Only populated by certain FIA work units (SURVEY.RSCD = 33, STATECD = 72, 78).

## 2.5.56 HABTYPCD1\_PUB\_CD

Habitat type code 1 publication code. A code indicating the publication that lists the name for habitat type code 1 (HABTYPCD1). Publication information is documented in the REF\_HABTYP\_PUBLICATION table. Only used by certain FIA work units (SURVEY.RSCD = 22, 23, 26).

## 2.5.57 HABTYPCD1\_DESCR\_PUB\_CD

Habitat type code 1 description publication code. A code indicating the publication that gives a description for habitat type code 1 (HABTYPCD1). This publication may or may not be the same publication that lists the name of the habitat type (HABTYPCD1\_PUB\_CD). Publication information is documented in REF\_HABTYP\_PUBLICATION table. Only used by certain FIA work units (SURVEY.RSCD = 22, 23, 26).

## 2.5.58 HABTYPCD2

Habitat type code 2. A code indicating the secondary habitat type (or community type) for this condition. See HABTYPCD1 for description.

## 2.5.59 HABTYPCD2\_PUB\_CD

Habitat type code 2 publication code. A code indicating the publication that lists the name for habitat type code 2 (HABTYPCD2). See HABTYPCD1\_PUB\_CD for description.

## 2.5.60 HABTYPCD2\_DESCR\_PUB\_CD

Habitat type code 2 description publication code. A code indicating the publication that gives a description for habitat type code 2 (HABTYPCD2). See HABTYPCD1\_DESCR\_PUB\_CD for description.

## 2.5.61 MIXEDCONFCD

Mixed conifer code. An indicator to show that the forest condition is a mixed conifer site in California. These sites are a complex association of ponderosa pine, sugar pine, Douglas-fir, white fir, red fir, and/or incense-cedar. Mixed conifer sites use a specific site index equation. This is a yes/no attribute. This attribute is left blank (null) for all other States. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: MIXEDCONFCD

| Code   | Description                                                  |
|--------|--------------------------------------------------------------|
| Y      | Yes, the condition is a mixed conifer site in California.    |
| N      | No, the condition is not a mixed conifer site in California. |

## 2.5.62 VOL\_LOC\_GRP

Volume location group. An identifier indicating what equations are used for volume, biomass, site index, etc. A volume group is usually designated for a geographic area, such as a State, multiple States, a group of counties, or an ecoregion.

Codes: VOL\_LOC\_GRP

| Code      | Description                                                               |
|-----------|---------------------------------------------------------------------------|
| S22LAZN   | Northern Arizona Ecosections.                                             |
| S22LAZS   | Southern Arizona Ecosections.                                             |
| S22LCOE   | Eastern Colorado Ecosections.                                             |
| S22LCOW   | Western Colorado Ecosections.                                             |
| S22LID    | Idaho Ecosections.                                                        |
| S22LMTE   | Eastern Montana Ecosections.                                              |
| S22LMTW   | Western Montana Ecosections.                                              |
| S22LNMN   | Northern New Mexico Ecosections.                                          |
| S22LNMS   | Southern New Mexico Ecosections.                                          |
| S22LNV    | Nevada Ecosections.                                                       |
| S22LUTNE  | Northern and Eastern Utah Ecosections.                                    |
| S22LUTSW  | Southern and Western Utah Ecosections.                                    |
| S22LWYE   | Eastern Wyoming Ecosections.                                              |
| S22LWYW   | Western Wyoming Ecosections.                                              |
| S23LCS    | Central States (IL, IN, IA, MO).                                          |
| S23LLS    | Lake States (MI, MN, WI).                                                 |
| S23LPS    | Plains States (KS, NE, ND, SD).                                           |
| S24       | Northeastern States (CT, DE, ME, MD, MA, NH, NJ, NY, OH, PA, RI, VT, WV). |
| S26LCA    | California other than mixed conifer forest type.                          |
| S26LCAMIX | California mixed conifer forest type.                                     |
| S26LEOR   | Eastern Oregon.                                                           |
| S26LEWA   | Eastern Washington.                                                       |
| S26LORJJ  | Oregon, Jackson and Josephine Counties.                                   |
| S26LPI    | Pacific Islands.                                                          |
| S26LWACF  | Washington Silver Fir Zone.                                               |
| S26LWOR   | Western Oregon.                                                           |
| S26LWWA   | Western Washington.                                                       |
| S27LAK    | Alaska - coastal and interior.                                            |
| S27LAK1AB | Coastal Alaska Southeast and Central.                                     |

| Code          | Description                                                                                                          |
|---------------|----------------------------------------------------------------------------------------------------------------------|
| S27LAK1C      | Coastal Alaska Kodiak and Afognak Islands.                                                                           |
| S33           | Southern States - excluding Puerto Rico and the Virgin Islands (AL, AR, FL, GA, LA, KY, MS, OK, NC, SC, TN, TX, VA). |
| S33CARIBDRY   | Caribbean Islands - Subtropical dry forest life zones.                                                               |
| S33CARIBLMWR  | Caribbean Islands - Lower montane wet and rain forest life zones.                                                    |
| S33CARIBMOIST | Caribbean Islands - Subtropical moist forest life zones.                                                             |
| S33CARIBWET   | Caribbean Islands - Subtropical wet and rain forest life zones.                                                      |

## 2.5.63 SITECLCDEST

Site productivity class code estimated. This is a field-recorded code that is an estimated or predicted indicator of site productivity. It is used as the value for SITECLCD if no valid site tree is available. When SITECLCDEST is used as SITECLCD, SITECL\_METHOD is set to 6. May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0. Only populated by certain FIA work units (SURVEY.RSCD = 23, 24, 26, 27, 33).

## Codes: SITECLCDEST

|   Code | Description                   |
|--------|-------------------------------|
|      1 | 225+ cubic feet/acre/year.    |
|      2 | 165-224 cubic feet/acre/year. |
|      3 | 120-164 cubic feet/acre/year. |
|      4 | 85-119 cubic feet/acre/year.  |
|      5 | 50-84 cubic feet/acre/year.   |
|      6 | 20-49 cubic feet/acre/year.   |
|      7 | 0-19 cubic feet/acre/year.    |

## 2.5.64 SITETREE\_TREE

Site tree tree number. If an individual site index tree is used to calculate SICOND, this is the tree number of the site tree (SITETREE.TREE column) used. Only populated by certain FIA work units (SURVEY.RSCD = 23, 33).

## 2.5.65 SITECL\_METHOD

Site class method. A code identifying the method for determining site index or estimated site productivity class. May not be populated for some FIA work units when PLOT.MANUAL &lt;1.0.

## Codes: SITECL\_METHOD

|   Code | Description                                                                                           |
|--------|-------------------------------------------------------------------------------------------------------|
|      1 | Tree measurement (length, age, etc.) collected during this inventory.                                 |
|      2 | Tree measurement (length, age, etc.) collected during a previous inventory.                           |
|      3 | Site index or site productivity class estimated either in the field or office.                        |
|      4 | Site index or site productivity class estimated by the height-intercept method during this inventory. |

|   Code | Description                                                                |
|--------|----------------------------------------------------------------------------|
|      5 | Site index or site productivity class estimated using multiple site trees. |
|      6 | Site index or site productivity class estimated using default values.      |

## 2.5.66 CARBON\_DOWN\_DEAD

Carbon in down dead. Carbon, in tons per acre, of woody material &gt;3 inches in diameter on the ground, and stumps and their roots &gt;3 inches in diameter. Estimated from models based on geographic area, forest type, and live tree carbon density (Smith and Heath 2008). This modeled attribute is not a direct sum of Phase 2 or Phase 3 measurements. This is a per acre estimate and must be multiplied by CONDPROP\_UNADJ and the appropriate expansion and adjustment factor located in the POP\_STRATUM table.

## 2.5.67 CARBON\_LITTER

Carbon in litter. Carbon, in tons per acre, of organic material on the floor of the forest, including fine woody debris, humus, and fine roots in the organic forest floor layer above mineral soil. Estimated from a model based on on litter carbon measurements from a subset of FIA plots, geographic area, elevation, forest type group, aboveground live tree carbon, and climate variables (Domke and others 2016). This modeled attribute, while based on litter carbon observations on FIA plots, is not a direct sum of Phase 2 or Phase 3 measurements. This is a per acre estimate and must be multiplied by CONDPROP\_UNADJ and the appropriate expansion and adjustment factor located in the POP\_STRATUM table. Not populated for the Caribbean Islands, Pacific Islands, and Interior Alaska.

## 2.5.68 CARBON\_SOIL\_ORG

Carbon in soil organic material. Carbon, in tons per acre, in fine organic material below the soil surface to a depth of 1 meter. Does not include roots. Estimated from a model based on soil organic carbon measurements from a subset of FIA plots, geographic area, elevation, forest type group, climate variables, soil order, and surficial geology (Domke and others 2017). This modeled attribute, while based on soil organic carbon observations on FIA plots, is not a direct sum of Phase 2 or Phase 3 measurements. This is a per acre estimate and must be multiplied by CONDPROP\_UNADJ and the appropriate expansion and adjustment factor located in the POP\_STRATUM table. Not populated for the Caribbean Islands, Pacific Islands, and Interior Alaska.

## 2.5.69 CARBON\_UNDERSTORY\_AG

Carbon in understory aboveground. Carbon, in tons per acre, in the aboveground portions of seedlings and woody shrubs. Estimated from models based on geographic area, forest type, and (except for nonstocked and pinyon-juniper stands) live tree carbon density (Smith and Health 2008). This modeled attribute is a component of the EPA's Greenhouse Gas Inventory and is not a direct sum of Phase 2 or Phase 3 measurements. This is a per acre estimate and must be multiplied by CONDPROP\_UNADJ and the appropriate expansion and adjustment factor located in the POP\_STRATUM table.

## 2.5.70 CARBON\_UNDERSTORY\_BG

Carbon in understory belowground. Carbon, in tons per acre, in the belowground portions of seedlings and woody shrubs. Estimated from models based on geographic area, forest type, and (except for nonstocked and pinyon-juniper stands) live tree carbon density (Smith and Heath 2008). This modeled attribute is a component of the EPA's Greenhouse Gas Inventory and is not a direct sum of Phase 2 or Phase 3 measurements.

This is a per acre estimate and must be multiplied by CONDPROP\_UNADJ and the appropriate expansion and adjustment factor located in the POP\_STRATUM table.

## 2.5.71 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 2.5.72 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 2.5.73 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 2.5.74 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 2.5.75 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 2.5.76 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 2.5.77 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 2.5.78 SUBCYCLE

Inventory subcycle number.

See SURVEY.SUBCYCLE description for definition.

## 2.5.79 SOIL\_ROOTING\_DEPTH\_PNW

Soil rooting depth, Pacific Northwest Research Station. A code indicating the soil depth (the depth to which tree roots can penetrate) within each forest land condition class. Required for all forest condition classes. This attribute is coded 1 when more than half of area in the condition class is estimated to be  20 inches deep. Ground pumice, decomposed granite, and sand all qualify as types of soil. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: SOIL\_ROOTING\_DEPTH\_PNW

|   Code | Description   |
|--------|---------------|
|      1 |  20 inches.  |
|      2 | >20 inches.   |

## 2.5.80 GROUND\_LAND\_CLASS\_PNW

Present ground land class, Pacific Northwest Research Station. A code indicating a ground land class (GLC) category, which is used to further refine the forest land classification for the condition. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: GROUND\_LAND\_CLASS\_PNW

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|    120 | Timberland - Forest land that is potentially capable of producing at least 20 cubic feet/acre/year at culmination in fully stocked, natural stands of continuous crops of trees to industrial roundwood size and quality. Industrial roundwood requires species that grow to size and quality adequate to produce lumber and other manufactured products (exclude fence posts and fuel wood that are not considered manufactured). Timberland is characterized by no severe limitations on artificial or natural restocking with species capable of producing industrial roundwood.                                                                                                                                                                                                                                                                                        |
|    141 | Other forest rocky - Other forest land that can produce tree species of industrial roundwood size and quality, but that is unmanageable because the site is steep, hazardous, and rocky, or is predominantly nonstockable rock or bedrock, with trees growing in cracks and pockets. Other forest-rocky sites may be incapable of growing continuous crops due to inability to obtain adequate regeneration success.                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|    142 | Other forest unsuitable site (wetland, subalpine, or coastal conifer scrub; California only) - Other forest land that is unsuited for growing industrial roundwood because of one of the following environment factors: willow bogs, spruce bogs, sites with high water tables or even standing water for a portion of the year, and harsh sites due to extreme climatic and soil conditions. Trees present are often extremely slow growing and deformed. Examples: whitebark pine, lodgepole, or mountain hemlock stands at timberline; shore pine along the Pacific Ocean (Monterey, Bishop, and Douglas-fir); willow wetlands with occasional cottonwoods present; Sitka spruce-shrub communities bordering tidal flats and channels along the coast. Includes aspen stands in high-desert areas or areas where juniper/mountain mahogany are the predominant species. |
|    143 | Other forest pinyon-juniper - Areas currently capable of 10 percent or more tree stocking with forest trees, with juniper species predominating. These areas are not now, and show no evidence of ever having been 10 percent or more stocked with trees of industrial roundwood form and quality. Stocking capabilities indicated by live juniper trees or juniper stumps and juniper snags less than 25 years dead or cut. Ten percent juniper stocking means 10 percent canopy cover at stand maturity. For woodland juniper species, ten percent stocking means 5 percent canopy cover at stand maturity.                                                                                                                                                                                                                                                              |
|    144 | Other forest-oak (formally oak woodland) - Areas currently 10 percent or more stocked with forest trees, with low quality forest trees of oak, gray pine, madrone, or other hardwood species predominating, and that are not now, and show no evidence of ever having been 10 percent or more stocked with trees of industrial roundwood form and quality. Trees on these sites are usually short, slow growing, gnarled, poorly formed, and generally suitable only for fuel wood. The following types are included: blue oak, white oak, live oak, oak-gray pine.                                                                                                                                                                                                                                                                                                        |
|    146 | Other forest unsuitable site (Oregon and Washington only) - Other forest land that is unsuited for growing industrial roundwood because of one of the following environment factors: willow bogs, spruce bogs, sites with high water tables or even standing water for a portion of the year, and harsh sites due to climatic conditions. Trees present are often extremely slow growing and deformed. Examples: whitebark pine or mountain hemlock stands at timberline, shore pine along the Pacific Ocean, willow wetlands with occasional cottonwoods present, and Sitka spruce-shrub communities bordering tidal flats and channels along the coast. Aspen stands in high-desert areas or areas where juniper/mountain mahogany are the predominant species are considered other forest-unsuitable site.                                                              |
|    148 | Other forest-Cypress (California only) - Forest land with forest trees with cypress predominating. Shows no evidence of having had 10 percent or more cover of trees of industrial roundwood quality and species.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                    |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|    149 | Other forest-low productivity (this code is calculated in the office) - Forest land capable of growing crops of trees to industrial roundwood quality, but not able to grow wood at the rate of 20 cubic feet/acre/year. Included are areas of low stocking potential and/or very low site index.                                                                                                              |
|    150 | Other forest curlleaf mountain mahogany - Areas currently capable of 10 percent or more tree stocking with forest trees, with curlleaf mountain mahogany species predominating. These areas are not now, and show no evidence of ever having been 10 percent or more stocked with trees of industrial roundwood form and quality; 10 percent mahogany stocking means 5 percent canopy cover at stand maturity. |

## 2.5.81 PLANT\_STOCKABILITY\_FACTOR\_PNW

Plant stockability factor, Pacific Northwest Research Station. Some plots in PNWRS have forest land condition classes that are low productivity sites, and are incapable of attaining normal yield table levels of stocking. For such classes, potential productivity (mean annual increment at culmination) must be discounted. Most forested conditions have a default value of 1 assigned; those conditions that meet the low site criteria have a value between 0.1 and 1. Key plant indicators and plant communities are used to assign discount factors, using procedures outlined in MacLean and Bolsinger (1974) and Hanson and others (2002). Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 2.5.82 STND\_COND\_CD\_PNWRS

Stand condition code, Pacific Northwest Research Station. A code that best describes the condition of the stand within forest condition classes. Stand condition is defined here as "the size, density, and species composition of a plant community following disturbance and at various time intervals after disturbance." Information on stand condition is used in describing wildlife habitat. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: STND\_COND\_CD\_PNWRS

|   Code | Stand Condition                  | Description                                                                                                                                                                                                                      |
|--------|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | Not applicable.                  | Condition class is juniper, chaparral, or curlleaf mountain mahogany forest type.                                                                                                                                                |
|      1 | Grass-forb.                      | Shrubs <40 percent canopy cover and <5 feet tall; plot may range from being largely devoid of vegetation to dominance by herbaceous species (grasses and forbs); tree regeneration generally <5 feet tall and <40 percent cover. |
|      2 | Shrub.                           | Shrubs 40 percent canopy cover or greater, of any height; trees <40 percent canopy cover and <1.0 inch d.b.h./d.r.c. When average stand diameter exceeds 1.0 inch d.b.h./d.r.c., plot is "open sapling" or "closed sapling."     |
|      3 | Open sapling, poletimber.        | Average stand diameter 1.0-8.9 inches d.b.h./d.r.c., and canopy cover <60 percent.                                                                                                                                               |
|      4 | Closed sapling, pole, sawtimber. | Average stand diameter is 1.0-21.0 inches d.b.h./d.r.c. and canopy cover is 60 percent or greater.                                                                                                                               |
|      5 | Open sawtimber.                  | Average stand diameter is 9.0-21.0 inches d.b.h./d.r.c., and canopy cover is <60 percent.                                                                                                                                        |

|   Code | Stand Condition   | Description                                                                                                                                                                                                                                                                                                               |
|--------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      6 | Large sawtimber.  | Average stand diameter exceeds 21.0 inches d.b.h./d.r.c.; canopy cover may be <100 percent; decay and decadence required for old-growth characteristics is generally lacking, successional trees required by old-growth may be lacking, and dead and down material required by old-growth is lacking.                     |
|      7 | Old-growth.       | Average stand diameter exceeds 21.0 inches d.b.h./d.r.c. Stands over 200 years old with at least two tree layers (overstory and understory), decay in living trees, snags, and down woody material. Some of the overstory layer may be composed of long-lived successional species (e.g., Douglas-fir, western redcedar). |

## 2.5.83 STND\_STRUC\_CD\_PNWRS

Stand structure code, Pacific Northwest Research Station. A code indicating the overall structure of the stand. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: STND\_STRUC\_CD\_PNWRS

|   Code | Stand Condition           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|--------|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Even-aged single-storied. | A single even canopy characterizes the stand. The greatest numbers of trees are in a height class represented by the average height of the stand; there are substantially fewer trees in height classes above and below this mean. The smaller trees are usually tall spindly members that have fallen behind their associates. The ages of trees usually do not differ by more than 20 years.                                                                                                                                                                                                                                                                                                                                                      |
|      2 | Even-aged two-storied.    | Stands composed of two distinct canopy layers, such as, an overstory with an understory sapling layer possibly from seed tree and shelterwood operations. This may also be true in older plantations, where shade-tolerant trees may become established. Two relatively even canopy levels can be recognized in the stand. Understory or overtopped trees are common. Neither canopy level is necessarily continuous or closed, but both canopy levels tend to be uniformly distributed across the stand. The average age of each level differs significantly from the other.                                                                                                                                                                       |
|      3 | Uneven-aged.              | Theoretically, these stands contain trees of every age on a continuum from seedlings to mature canopy trees. In practice, uneven-aged stands are characterized by a broken or uneven canopy layer. Usually the largest number of trees is in the smaller diameter classes. As trees increase in diameter, their numbers diminish throughout the stand. Many times, instead of producing a negative exponential distribution of diminishing larger diameters, uneven-aged stands behave irregularly with waves of reproduction and mortality. Consider any stand with three or more structural layers as uneven-aged. Logging disturbances (examples are selection, diameter limit, and salvage cutting) will give a stand an uneven-aged structure. |
|      4 | Mosaic.                   | At least two distinct size classes are represented and these are not uniformly distributed but are grouped in small repeating aggregations, or occur as stringers <120 feet wide, throughout the stand. Each size class aggregation is too small to be recognized and mapped as an individual stand. The aggregations may or may not be even-aged.                                                                                                                                                                                                                                                                                                                                                                                                  |

## 2.5.84 STUMP\_CD\_PNWRS

Stump code, Pacific Northwest Research Station. A code indicating whether or not stumps are present on a condition. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: STUMP\_CD\_PNWRS

| Code   | Description                                                        |
|--------|--------------------------------------------------------------------|
| Y      | Yes, evidence of cutting or management exists; stumps are present. |
| N      | No, evidence of cutting was not observed; stumps are not present.  |

## 2.5.85 FIRE\_SRS

Fire, Southern Research Station. A code indicating the presence or absence of fire on the condition since the last survey or within the last 5 years on new/replacement plots. Evidence of fire must occur within the subplot. Only populated by certain FIA work units (SURVEY.RSCD = 33).

## Codes: FIRE\_SRS

|   Code | Description                                          |
|--------|------------------------------------------------------|
|      0 | No evidence of fire since last survey.               |
|      1 | Evidence of burning (either prescribed or wildfire). |

## 2.5.86 GRAZING\_SRS

Grazing, Southern Research Station. A code indicating the presence or absence of domestic animal grazing on the condition since the last survey or within the last 5 years on new/replacement plots. Evidence of grazing must occur within the subplot. Only populated by certain FIA work units (SURVEY.RSCD = 33).

## Codes: GRAZING\_SRS

|   Code | Description                                                 |
|--------|-------------------------------------------------------------|
|      0 | No evidence of livestock use (by domestic animals).         |
|      1 | Evidence of grazing (including dung, tracks, trails, etc.). |

## 2.5.87 HARVEST\_TYPE1\_SRS

Harvest type code 1, Southern Research Station. A code indicating the harvest type. This attribute is populated when the corresponding attribute TRTCD = 10. Only populated by certain FIA work units (SURVEY.RSCD = 33). Not populated for the Caribbean Islands.

## Codes: HARVEST\_TYPE1\_SRS

|   Code | Description                                                                                                                                                                                                                            |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     11 | Clearcut harvest - The removal of the majority of the merchantable trees in a stand; residual stand stocking is under 50 percent.                                                                                                      |
|     12 | Partial harvest - Removal primarily consisting of highest quality trees. Residual consists of lower quality trees because of high grading or selection harvest. (e.g., uneven aged, group selection, high grading, species selection). |

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                     |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     13 | Seed-tree/shelterwood harvest - Crop trees are harvested leaving seed source trees either in a shelterwood or seed tree. Also includes the final harvest of the seed trees.                                                                                                                                                                                                     |
|     14 | Commercial thinning - The removal of trees (usually poletimber sized) from poletimber-sized stands leaving sufficient stocking of growing-stock trees to feature in future stand development. Also included are thinning in sawtimber-sized stands where poletimber-sized (or log-sized) trees have been removed to improve quality of those trees featured in a final harvest. |
|     15 | Timber stand improvement (cut trees only) - The cleaning, release or other stand improvement involving non-commercial cutting applied to an immature stand that leaves sufficient stocking.                                                                                                                                                                                     |
|     16 | Salvage cutting - The harvesting of dead or damaged trees or of trees in danger of being killed by insects, disease, flooding, or other factors in order to save their economic value.                                                                                                                                                                                          |

## 2.5.88 HARVEST\_TYPE2\_SRS

Harvest type code 2, Southern Research Station. See HARVEST\_TYPE1\_SRS.

## 2.5.89 HARVEST\_TYPE3\_SRS

Harvest type code 3, Southern Research Station. See HARVEST\_TYPE1\_SRS.

## 2.5.90 LAND\_USE\_SRS

Land use, Southern Research Station. A classification indicating the present land use of the condition. Collected on all condition records where SURVEY.RSCD = 33 and PLOT.DESIGNCD = 1, 230, 231, 232, or 233. It may not be populated for other SRS plot designs. Only populated by certain FIA work units (SURVEY.RSCD = 33).

## Codes: LAND\_USE\_SRS

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     01 | Timberland (SITECLCD = 1, 2, 3, 4, 5, or 6).                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|     02 | Other forest land (SITECLCD = 7).                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|     10 | Agricultural land - Land managed for crops, pasture, or other agricultural use and is not better described by one of the following detailed codes. The area must be at least 1.0 acre in size and 120.0 feet wide. Note: Codes 14, 15 and 16 are collected only where PLOT.MANUAL  1. If PLOT.MANUAL <1, then codes 14 and 15 were coded 11. There was no single rule for coding maintained wildlife openings where PLOT.MANUAL <1, so code 16 may have been coded 10, 11 or 12. |
|     11 | Cropland.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|     12 | Pasture (improved through cultural practices).                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|     13 | Idle farmland.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|     14 | Orchard.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|     15 | Christmas tree plantation.                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|     16 | Maintained wildlife openings.                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     17 | Windbreak/Shelterbelt - Windbreaks or shelterbelts are plantings of single or multiple rows of trees or shrubs that are established for environmental purposes. Windbreaks or shelterbelts are generally established to protect or shelter nearby leeward areas from troublesome winds. SRS Note: If the dimensions of the windbreak or shelterbelt meet the minimum dimensions of forest land (1.0 acre in size and 120.0 feet wide), then the area is considered accessible forest land (COND_STATUS_CD = 1). |
|     20 | Rangeland - Land primarily composed of grasses, forbs, or shrubs. This includes lands vegetated naturally or artificially to provide a plant cover managed like native vegetation and does not meet the definition of pasture. The area must be at least 1.0 acre in size and 120.0 feet wide.                                                                                                                                                                                                                  |
|     30 | Developed - Land used primarily by humans for purposes other than forestry or agriculture and is not better described by one of the following detailed codes. Note: Code 30 is used to describe all developed land where PLOT.MANUAL <1. The following detailed codes only apply to PLOT.MANUAL  1.                                                                                                                                                                                                            |
|     31 | Cultural - business, residential, and other places of intense human activity.                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|     32 | Rights-of-way - improved roads, railway, power lines, maintained canal.                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|     33 | Recreation - parks, skiing, golf courses.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|     34 | Mining.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|     40 | Other - Land parcels greater than 1.0 acre in size and greater than 120.0 feet wide that do not fall into one of the uses described above or below.                                                                                                                                                                                                                                                                                                                                                             |
|     41 | Nonvegetated.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|     42 | Wetland.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|     43 | Beach.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|     45 | Nonforest - Chaparral.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|     91 | Census Water - Lakes, reservoirs, ponds, and similar bodies of water 4.5 acres in size and larger; and rivers, streams, canals, etc., 30 to 200 feet wide.                                                                                                                                                                                                                                                                                                                                                      |
|     92 | Noncensus water - Lakes, reservoirs, ponds, and similar bodies of water 1.0 acre to 4.5 acres in size. Rivers, streams, canals, etc., more than 200 feet wide.                                                                                                                                                                                                                                                                                                                                                  |
|     99 | Nonsampled - Condition not sampled (see COND_NONSAMPLE_REASN_CD for exact reason).                                                                                                                                                                                                                                                                                                                                                                                                                              |

## 2.5.91 OPERABILITY\_SRS

Operability, Southern Research Station. A code indicating the viability of operating logging equipment in the vicinity of the condition. The code represents the most limiting class code that occurs on each forest condition. Only populated by certain FIA work units (SURVEY.RSCD = 33).

## Codes: OPERABILITY\_SRS

|   Code | Description                                                                                             |
|--------|---------------------------------------------------------------------------------------------------------|
|      0 | No problems.                                                                                            |
|      1 | Seasonal access due to water conditions in wet weather.                                                 |
|      2 | Mixed wet and dry areas typical of multi-channeled streams punctuated with dry islands.                 |
|      3 | Broken terrain, cliffs, gullies, outcroppings, etc. that would severely limit equipment, access or use. |

|   Code | Description                                   |
|--------|-----------------------------------------------|
|      4 | Year-round water problems (includes islands). |
|      5 | Slopes 20-40 percent.                         |
|      6 | Slope greater than 40 percent.                |

## 2.5.92 STAND\_STRUCTURE\_SRS

Stand structure, Southern Research Station. A code indicating the description of the predominant canopy structure for the condition. Only the vertical position of the dominant and codominant trees in the stand are considered. Only populated by certain FIA work units (SURVEY.RSCD = 33).

## Codes: STAND\_STRUCTURE\_SRS

|   Code | Description                                                                                                                                                               |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | Nonstocked - The condition is less than 10 percent stocked.                                                                                                               |
|      1 | Single-storied - Most of the dominant/codominant tree crowns form a single canopy (i.e., most of the trees are approximately the same height).                            |
|      2 | Two-storied - The dominant/codominant tree crowns form two distinct canopy layers or stories.                                                                             |
|      3 | Multi-storied - More than two recognizable levels characterize the crown canopy. Dominant/codominant trees of many sizes (diameters and heights) for a multilevel canopy. |

## 2.5.93 NF\_COND\_STATUS\_CD

Nonforest condition status code. A code indicating the sampling status of the condition class.

## Codes: NF\_COND\_STATUS\_CD

|   Code | Description                |
|--------|----------------------------|
|      2 | Accessible nonforest land. |
|      5 | Nonsampled nonforest.      |

## 2.5.94 NF\_COND\_NONSAMPLE\_REASN\_CD

Nonforest condition nonsampled reason code. A code indicating the reason a nonforest portion of a plot was not sampled.

## Codes: NF\_COND\_NONSAMPLE\_REASN\_CD

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     02 | Denied access - Any area within the sampled area of a plot to which access is denied by the legal owner, or to which an owner of the only reasonable route to the plot denies access. There are no minimum area or width requirements for a condition class delineated by denied access. Because a denied-access condition can become accessible in the future, it remains in the sample and is re-examined at the next occasion to determine if access is available.                                                         |
|     03 | Hazardous situation - Any area within the sampled area on plot that cannot be accessed because of a hazard or danger, for example cliffs, quarries, strip mines, illegal substance plantations, temporary high water, etc. Although the hazard is not likely to change over time, a hazardous condition remains in the sample and is re-examined at the next occasion to determine if the hazard is still present. There are no minimum size or width requirements for a condition class delineated by a hazardous condition. |
|     10 | Other - This code is used whenever a condition class is not sampled due to a reason other than one of the specific reasons listed.                                                                                                                                                                                                                                                                                                                                                                                            |

## 2.5.95 CANOPY\_CVR\_SAMPLE\_METHOD\_CD

Canopy cover sample method code. A code indicating the canopy cover sample method used to determine LIVE\_CANOPY\_CVR\_PCT, LIVE\_MISSING\_CANOPY\_CVR\_PCT, and NBR\_LIVE\_STEMS. Codes 1-4 are used for field-measured canopy cover, and codes 11-14 are generated from imagery.

## Codes: CANOPY\_CVR\_SAMPLE\_METHOD\_CD

|   Code | Method Name     | Description                                                                                                                                                                                                                                                                                                                           |
|--------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Ocular method.  | Visual inspection of what is on the ground along with various types of aerial imagery to help determine LIVE_CANOPY_CVR_PCT and LIVE_MISSING_CANOPY_CVR_PCT. Used only in areas that are obviously 0 percent LIVE_MISSING_CANOPY_CVR_PCT or obviously greater than 10 percent LIVE_MISSING_CANOPY_CVR_PCT.                            |
|      2 | Subplot method. | Used when the ocular method is not appropriate and in cases where the terrain, vegetation, and dimensions of a condition or the size of the field crew DO NOT allow a safe or practical sample using the acre method. The crew measures the crowns of all live trees, seedlings, and saplings on each of the four 1/24 acre subplots. |
|      3 | Acre method.    | Used when the ocular method is not appropriate and when it is safe and practical to sample on the entire acre. To determine if minimum 10 percent LIVE_MISSING_CANOPY_CVR_PCT is reached, the crew samples all live, dead, and missing tree canopies on the one-acre sample plot as described above in LIVE_MISSING_CANOPY_CVR_PCT.   |

|   Code | Method Name                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                    |
|--------|------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      4 | Sub-acre method.                   | Used only when the ocular method is not appropriate and only when the acre or subplot methods cannot be established due to the condition's shape, dimensions or accessibility. The crew samples all live, dead, and missing tree canopies on the canopy cover sample plot as described above in LIVE_MISSING_CANOPY_CVR_PCT. The 10 percent threshold is dependent on the sample plot size and respective area in square feet. |
|     11 | Dot grid method.                   | The preferred method for estimating LIVE_CANOPY_CVR_PCT. Under this method, 109 dots are systematically arranged within the 144 foot radius prefield plot and LIVE_CANOPY_CVR_PCT is calculated based on the proportion of dots that fall on a tree crown.                                                                                                                                                                     |
|     12 | Ocular image-based assessment.     | Only used for plots that fall in the ocean or when the dot grid method is not possible.                                                                                                                                                                                                                                                                                                                                        |
|     13 | Other image-based assessment.      | Used when the codes 11 and 12 do not apply.                                                                                                                                                                                                                                                                                                                                                                                    |
|     14 | No canopy cover estimate possible. | Used when an estimate of canopy cover was not made because of lacking or poor-quality imagery.                                                                                                                                                                                                                                                                                                                                 |

## 2.5.96 LIVE\_CANOPY\_CVR\_PCT

Live canopy cover percent. The percentage of live canopy cover for the condition. Included are live tally trees, saplings, and seedlings that cover the sample area.

## 2.5.97 LIVE\_MISSING\_CANOPY\_CVR\_PCT

Live plus missing canopy cover percent. The percentage of live and missing canopy cover for the condition. This percentage for the condition is determined in the field by adding LIVE\_CANOPY\_CVR\_PCT to the estimated missing canopy cover. Included are all dead, harvested, and removed trees, saplings, and seedlings as well as dead portions of live trees. Missing canopy that has been replaced by the current live canopy or missing canopy that existed before the most recent conversion of a forested condition to a nonforest condition is not included.  The estimate is based on field observations, aerial photos, historical aerial imagery, and similar evidence in adjacent stands that do not have dead, harvested, or removed trees. The total LIVE\_MISSING\_CANOPY\_CVR\_PCT cannot exceed 100 percent.

## 2.5.98 NBR\_LIVE\_STEMS

Number of live stems. The estimated number of live stems per acre on the condition. The estimate in the field is based on actual stem count of tally tree species within the canopy cover for sample area.

## 2.5.99 OWNSUBCD

Owner subclass code. (core optional for accessible forest land) A code that further subdivides the owner class into detailed subcategories. Currently, only populated for the "State" owner class subcategories (OWNCD = 31).

## Codes: OWNSUBCD

|   Code | Description            |
|--------|------------------------|
|      1 | State forestry agency. |
|      2 | State wildlife agency. |
|      3 | State park agency.     |
|      4 | Other State lands.     |

## 2.5.100 INDUSTRIALCD\_FIADB

Industrial code in FIADB. A code indicating the status of the owner with regard to their objectives towards commercial timber production. This attribute is new starting with FIADB version 6.0 (PLOT.MANUAL  6.0). Industrial lands are of sufficient size to produce a continual flow of timber, and are owned by companies, organizations, and individuals who engage in commercially oriented forest management activities, such as harvesting, thinning, and planting.

## Codes: INDUSTRIALCD\_FIADB

|   Code | Description     |
|--------|-----------------|
|      0 | Non-industrial. |
|      1 | Industrial.     |

## 2.5.101 RESERVCD\_5

Reserved status code field, versions 1.0-5.0. A code indicating the reserved designation for the condition at the time of the field survey. This attribute is new starting with FIADB version 6.0 (PLOT.MANUAL  6.0), and is used to account for a change in the application of the definition of RESERVCD. In PLOT.MANUAL&lt;6.0, publicly owned land was considered reserved only if it was withdrawn by law(s) prohibiting the management of land for the production of wood products. Conditions measured prior to PLOT.MANUAL = 6.0 may have different values in RESERVCD and RESERVCD\_5 due to changes in the application of the RESERVCD definition. RESERVCD\_5 holds the reserved status associated with the previous definition of RESERVCD. Only populated for PLOT.MANUAL  1.0 and PLOT.MANUAL &lt;6.0.

## Codes: RESERVCD\_5

|   Code | Description   |
|--------|---------------|
|      0 | Not reserved. |
|      1 | Reserved.     |

## 2.5.102 ADMIN\_WITHDRAWN\_CD

Administratively withdrawn code. (core optional) A code indicating whether or not a condition has an administratively withdrawn designation. Administratively withdrawn land is public land withdrawn by management plans or government regulations prohibiting the management of land for the production of wood products (not merely controlling or prohibiting wood-harvesting methods). Such plans and regulations are formally adopted by land managers and the prohibition against management for wood products cannot be changed through decision of the land manager except by a formal modification of management plans or regulations.

## Codes: ADMIN\_WITHDRAWN\_CD

|   Code | Description                     |
|--------|---------------------------------|
|      0 | Not administratively withdrawn. |
|      1 | Administratively withdrawn.     |

## 2.5.103 CHAINING\_CD

Chaining code. A code indicating that a condition has been chained, shear bladed, roller chopped, etc., for the purpose of increased forage production. These treatments contrast with silvicultural removals in that little or none of the woody material is removed from the site and there are few residual live trees.

## Codes: CHAINING\_CD

|   Code | Description   |
|--------|---------------|
|      0 | No.           |
|      1 | Yes.          |

## 2.5.104 LAND\_COVER\_CLASS\_CD\_RET

Land cover class, retired. A code indicating the type of land cover for a condition that meets the minimum area and width requirements (except those cases where the condition has been solely defined due to developed land uses, such as roads and rights-of-way). If the condition was less than 1 acre, a land cover classification key was used to assign a land cover class.

This attribute is retired when PLOT.MANUAL  8.0 and replaced by a newer version by the previous name (LAND\_COVER\_CLASS\_CD). Many of the codes are the same between the retired and the current code sets. The cover classification used by crews has been modified to remove all aspects of land use and focus on land cover. There is no national crosswalk to translate the retired codes into the new codes (see LAND\_COVER\_CLASS\_CD for the new code list).

Codes: LAND\_COVER\_CLASS\_CD\_RET (codes that are  10% vegetative cover) 

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     01 | Treeland: Areas on which trees provide 10% or greater canopy cover and are part of the dominant (uppermost) vegetation layer, including areas that have been planted to produce woody crops. Only tree species that can be tallied in the region are considered. Example areas include forests, forest plantations, reverting fields with  10% tree canopy cover, clearcuts with  10% tree canopy cover. This category includes cypress swamps and mangroves.                                                                         |
|     02 | Shrubland: Areas on which shrubs or subshrubs provide 10% or greater cover and are part of the dominant (uppermost) vegetation layer, provided these areas do not qualify as Treeland. Shrub/Subshrub - a woody plant that generally has several erect, spreading, or prostrate stems which give it a bushy appearance. This includes dwarf shrubs, and low or short woody vines (NVCS 2008) and excludes any species on FIA's tree list. Examples include cranberry bogs and other shrub-dominated wetlands, chaparral, and sagebrush. |

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     03 | Grassland: Areas on which herbaceous vegetation provide 10% or greater cover and are part of the dominant (uppermost) vegetation layer, provided these areas do not qualify as Treeland or Shrubland. This includes herbs, forbs, and graminoid species. Examples include meadows and prairies. Grazed land is also included, but not if the pasture is improved to such an extent that it meets the requirements for Agricultural Vegetation. This category also includes emergent wetland vegetation like seasonally flooded grasslands, cattail marshes, etc.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|     04 | Non-vascular Vegetation: Areas on which non-vascular vegetation provide 10% or greater cover and are part of the dominant vegetation layer, provided these areas do not qualify as Treeland, Shrubland, or Grassland. Examples include mosses, sphagnum moss bogs, liverworts, hornworts, lichens, and algae.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|     05 | Mixed Vegetation: Areas with 10% or greater vegetative cover but no one life form has 10% or more cover. That is, these areas do not qualify as Treeland, Shrubland, Grassland, or Non-vascular Vegetation, and thus are a mixture of plant life forms. Examples can include early stages of reverting fields and high deserts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|     06 | Agricultural Vegetation: Areas that are dominated by vegetation grown for the production of crops (food, non-woody fiber and/or ornamental horticulture), including land in any stage of annual crop production, and land being regularly cultivated for production of crops from perennial plants. Agricultural vegetation shows a) rapid turnover in structure, typically at least on an annual basis, either through harvesting and/or planting, or by continual removal of above ground structure (e.g., cutting, haying, or intensive grazing), or b) showing strong linear (planted) features. The herbaceous layer may be bare at various times of the year (NVCS 2008). Examples include row crops and closely sown crops; sod farms, hay and silage crops; orchards (tree fruits and nuts, Christmas trees, nurseries of trees and shrubs), small fruits, and berries; vegetables and melons; unharvested crops; cultivated or improved pasture; idle cropland (can include land in cover and soil-improvement crops and cropland on which no crops were planted) (NRI Field guide). When idle or fallow land ceases to be predominantly covered with manipulated vegetation, then it is no longer Agricultural Vegetation. |
|     07 | Developed, Vegetated: Areas predominantly covered by vegetation with highly manipulated growth forms (usually by mechanical pruning, mowing, clipping, etc.), but are not Agricultural. This vegetation type typically contains an almost continuous herbaceous (typically grass) layer, with a closely cropped physiognomy, typically through continual removal of above ground structure (e.g., cutting, mowing), and where tree cover is highly variable, or other highly manipulated planted gardens (NVCS 2008). Examples can include lawns, maintained utility rights-of-way, office parks, and cemeteries.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

Codes: LAND\_COVER\_CLASS\_CD\_RET (codes that are &lt;10% vegetative cover)

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     08 | Barren: Natural areas of limited plant life (<10%). Areas generally characterized by bare rock, gravel, sand, silt, clay, or other earthen material, with little or no "green" vegetation present regardless of its inherent ability to support life. Examples include naturally barren areas such as lava fields, gravel bars and sand dunes, as well as areas where land clearance has removed the vegetative cover. Can include the natural material portions of quarries, mines, gravel pits, and cut or burned land <10% vegetation. |

|   Code | Description                                                                                                                                                                                                                                       |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     09 | Developed: Areas predominantly covered with constructed materials with limited plant life (<10%). Examples include completely paved surfaces like roads, parking lots and densely developed urban areas.                                          |
|     10 | Water: Areas persistently covered and predominated by water and have <10% emergent vegetative cover. Examples include census and noncensus water and permanent snow and ice. For example, only the open water portion of a bog is to be included. |

## 2.5.105 AFFORESTATION\_CD

Current afforestation code. A code indicating a condition that has no evidence of prior forest, but does have evidence suggesting deliberate afforestation attempts (planted or prepared to promote tree establishment) to convert to forest in the current inventory cycle or since the last measurement.

## Codes: AFFORESTATION\_CD

|   Code | Description   |
|--------|---------------|
|      0 | No.           |
|      1 | Yes.          |

## 2.5.106 PREV\_AFFORESTATION\_CD

Previous afforestation code. A code indicating a condition that has no evidence of prior forest, but does have evidence suggesting deliberate afforestation attempts (planted or prepared to promote tree establishment) to convert to forest in the prior inventory cycle or prior to the last measurement.

## Codes: PREV\_AFFORESTATION\_CD

|   Code | Description   |
|--------|---------------|
|      0 | No.           |
|      1 | Yes.          |

## 2.5.107 DWM\_FUELBED\_TYPCD

DWM condition fuelbed type code. A code indicating the fuels available for consumption by fire. Codes are from Scott and Burgan (2005).

## Codes: DWM\_FUELBED\_TYPCD

| Code   | Description                                 |
|--------|---------------------------------------------|
| GR1    | Short, sparse dry climate grass.            |
| GR2    | Low load, dry climate grass.                |
| GR3    | Low load, very coarse, humid climate grass. |
| GR4    | Moderate load, dry climate grass.           |
| GR5    | Low load, humid climate grass.              |
| GR6    | Moderate load, humid climate grass.         |
| GR7    | High load, dry climate grass.               |

## Condition Table

| Code   | Description                                        |
|--------|----------------------------------------------------|
| GR8    | High load, very coarse, humid climate grass.       |
| GR9    | Very high load, humid climate grass.               |
| GS1    | Low load, dry climate grass-shrub.                 |
| GS2    | Moderate load, dry climate grass-shrub.            |
| GS3    | Moderate load, humid climate grass-shrub.          |
| GS4    | High load, humid climate grass-shrub.              |
| SB1    | Slash-blowdown: low load activity fuel.            |
| SB2    | Moderate load activity fuel or low load blowdown.  |
| SB3    | High load activity fuel or moderate load blowdown. |
| SB4    | High load blowdown.                                |
| SH1    | Low load dry climate shrub.                        |
| SH2    | Moderate load dry climate shrub.                   |
| SH3    | Moderate load, humid climate shrub.                |
| SH4    | Low load, humid climate timber-shrub.              |
| SH5    | High load, dry climate shrub.                      |
| SH6    | Low load, humid climate shrub.                     |
| SH7    | Very high load, dry climate shrub.                 |
| SH8    | High load, humid climate shrub.                    |
| SH9    | Very high load, humid climate shrub.               |
| TL1    | Low load compact conifer litter.                   |
| TL2    | Low load broadleaf litter.                         |
| TL3    | Moderate load conifer litter.                      |
| TL4    | Small downed logs.                                 |
| TL5    | High load conifer litter.                          |
| TL6    | Moderate load broadleaf litter.                    |
| TL7    | Large downed logs.                                 |
| TL8    | Long-needle litter.                                |
| TL9    | Very high load broadleaf litter.                   |
| TU1    | Low load dry climate timber-grass-shrub.           |
| TU2    | Moderate load, humid climate timber-shrub.         |
| TU3    | Moderate load, humid climate timber-grass-shrub.   |
| TU4    | Dwarf conifer with understory.                     |
| TU5    | Very high load, dry climate timber-shrub.          |
| NB1    | Nonburnable urban/developed.                       |
| NB2    | Nonburnable snow/ice.                              |
| NB3    | Nonburnable agricultural.                          |
| NB8    | Nonburnable open water.                            |
| NB9    | Nonburnable bare ground.                           |

## 2.5.108 NVCS\_PRIMARY\_CLASS

Primary class. The primary classification determined by the NVCS classification algorithm. 'NATURAL' or 'CULTURAL' are the valid values. As of August 2017, the classification algorithm has only been developed for the eastern continental United States excluding the western edges of the Plains States, Oklahoma, and Texas. This column will only be populated for forested conditions in the supported area.

Note: For more information on the 'NATURAL' and 'CULTURAL' vegetation classifications, refer to the Data Standard web page on the USNVC website (available at web address: https://www.usnvc.org).

## 2.5.109 NVCS\_LEVEL\_1\_CD

Level 1 code of the NVCS. The NVCS code describing the vegetative community of the condition at the first level of the NVCS hierarchy. It is populated for both the 'NATURAL' and 'CULTURAL' primary classifications. Code definitions can be found in the NVCS\_LEVEL\_1\_CODES table. Joins to this table must use both the NVCS\_PRIMARY\_CLASS and NVCS\_LEVEL\_1\_CD values as shown in the following example.

```
SELECT c.cn AS cnd_cn, c.nvcs_primary_class, c.nvcs_level_1_cd, r.meaning FROM cond c, ref_nvcs_level_1_codes r WHERE c.nvcs_primary_class = r.primary_class AND c.nvcs_level_1_cd = r.nvcs_code ;
```

## 2.5.110 NVCS\_LEVEL\_2\_CD

Level 2 code of the NVCS. The NVCS code describing the vegetative community of the condition at the second level of the NVCS hierarchy. It is populated for both the 'NATURAL' and 'CULTURAL' primary classifications. Code definitions can be found in the NVCS\_LEVEL\_2\_CODES table. Joins to this table must use both the NVCS\_PRIMARY\_CLASS and NVCS\_LEVEL\_2\_CD values as shown in the following example.

```
SELECT c.cn AS cnd_cn, c.nvcs_primary_class, c.nvcs_level_2_cd, r.meaning FROM cond c, ref_nvcs_level_2_codes r WHERE c.nvcs_primary_class = r.primary_class AND c.nvcs_level_2_cd = r.nvcs_code ;
```

## 2.5.111 NVCS\_LEVEL\_3\_CD

Level 3 code of the NVCS. The NVCS code describing the vegetative community of the condition at the third level of the NVCS hierarchy. It is populated for both the 'NATURAL' and 'CULTURAL' primary classifications. Code definitions can be found in the NVCS\_LEVEL\_3\_CODES table. Joins to this table must use both the NVCS\_PRIMARY\_CLASS and NVCS\_LEVEL\_3\_CD values as shown in the following example.

```
SELECT c.cn AS cnd_cn, c.nvcs_primary_class, c.nvcs_level_3_cd, r.meaning FROM cond c, ref_nvcs_level_3_codes r WHERE c.nvcs_primary_class = r.primary_class AND c.nvcs_level_3_cd = r.nvcs_code ;
```

## 2.5.112 NVCS\_LEVEL\_4\_CD

Level 4 code of the NVCS. The NVCS code describing the vegetative community of the condition at the fourth level of the NVCS hierarchy. It is populated for both the 'NATURAL' and 'CULTURAL' primary classifications. Code definitions can be found in the NVCS\_LEVEL\_4\_CODES table. Joins to this table must use both the NVCS\_PRIMARY\_CLASS and NVCS\_LEVEL\_4\_CD values as shown in the following example.

```
SELECT c.cn AS cnd_cn, c.nvcs_primary_class, c.nvcs_level_4_cd, r.meaning FROM cond c, ref_nvcs_level_4_codes r WHERE c.nvcs_primary_class = r.primary_class AND c.nvcs_level_4_cd = r.nvcs_code ;
```

## 2.5.113 NVCS\_LEVEL\_5\_CD

Level 5 code of the NVCS. The NVCS code describing the vegetative community of the condition at the fifth level of the NVCS hierarchy. It is populated for both the 'NATURAL' and 'CULTURAL' primary classifications. Code definitions can be found in the NVCS\_LEVEL\_5\_CODES table. Joins to this table must use both the NVCS\_PRIMARY\_CLASS and NVCS\_LEVEL\_5\_CD values as shown in the following example.

```
SELECT c.cn AS cnd_cn, c.nvcs_primary_class, c.nvcs_level_5_cd, r.meaning FROM cond c, ref_nvcs_level_5_codes r WHERE c.nvcs_primary_class = r.primary_class AND c.nvcs_level_5_cd = r.nvcs_code ;
```

## 2.5.114 NVCS\_LEVEL\_6\_CD

Level 6 code of the NVCS. The NVCS code describing the vegetative community of the condition at the sixth level of the NVCS hierarchy. It is populated for the 'CULTURAL' primary classification. Code definitions can be found in the NVCS\_LEVEL\_6\_CODES table. Joins to this table must use both the NVCS\_PRIMARY\_CLASS and NVCS\_LEVEL\_6\_CD values as shown in the following example.

```
SELECT c.cn AS cnd_cn, c.nvcs_primary_class, c.nvcs_level_6_cd,
```

```
r.meaning FROM cond c, ref_nvcs_level_6_codes r WHERE c.nvcs_primary_class = r.primary_class AND c.nvcs_level_6_cd = r.nvcs_code ;
```

## 2.5.115 NVCS\_LEVEL\_7\_CD

Level 7 code of the NVCS. The NVCS code describing the vegetative community of the condition at the seventh level of the NVCS hierarchy. It is populated for the 'CULTURAL' primary classification. Code definitions can be found in the NVCS\_LEVEL\_7\_CODES table. Joins to this table must use both the NVCS\_PRIMARY\_CLASS and NVCS\_LEVEL\_7\_CD values as shown in the following example.

```
SELECT c.cn AS cnd_cn, c.nvcs_primary_class, c.nvcs_level_7_cd, r.meaning FROM cond c, ref_nvcs_level_7_codes r WHERE c.nvcs_primary_class = r.primary_class AND c.nvcs_level_7_cd = r.nvcs_code ;
```

## 2.5.116 NVCS\_LEVEL\_8\_CD

Level 8 code of the NVCS. The NVCS code describing the vegetative community of the condition at the eighth level of the NVCS hierarchy. It is populated for the 'CULTURAL' primary classification. Code definitions can be found in the NVCS\_LEVEL\_8\_CODES table. Joins to this table must use both the NVCS\_PRIMARY\_CLASS and NVCS\_LEVEL\_8\_CD values as shown in the following example.

```
SELECT c.cn AS cnd_cn, c.nvcs_primary_class, c.nvcs_level_8_cd, r.meaning FROM cond c, ref_nvcs_level_8_codes r WHERE c.nvcs_primary_class = r.primary_class AND c.nvcs_level_8_cd = r.nvcs_code ;
```

## 2.5.117 AGE\_BASIS\_CD\_PNWRS

Age basis code, Pacific Northwest Research Station. A code that indicates the method used to determine stand age. Only populated by certain FIA work units (SURVEY.RSCD = 26, 27). Not populated for the Pacific Islands.

## Codes: AGE\_BASIS\_CD\_PNWRS

|   Code | Description                                                               |
|--------|---------------------------------------------------------------------------|
|     00 | Stand is nonstocked.                                                      |
|     10 | Weighted average of trees bored for age (on macroplot).                   |
|     11 | Weighted average of trees bored for age (off macroplot).                  |
|     20 | Whorl counted only (on or off macroplot).                                 |
|     30 | Mixed method of whorl-count and/or bored age (on or off macroplot).       |
|     40 | Time since last inventory - years added to previously recorded stand age. |

## Condition Table

|   Code | Description                                                              |
|--------|--------------------------------------------------------------------------|
|     50 | Age based on documentary evidence or landowner discussion.               |
|     51 | Age based on crew call considering site and tree diameters.              |
|     60 | All trees in the condition are of a species which cannot be bored.       |
|     70 | Tree cores not counted in the field, but taken to field office to count. |
|     80 | Stand age >997 years.                                                    |

## 2.5.118 COND\_STATUS\_CHNG\_CD\_RMRS

Condition class status change code, Rocky Mountain Research Station. A code that describes the type of change that has occurred for the condition class since the previous inventory. Only populated by certain FIA work units (SURVEY.RSCD = 22).

Note: For condition classes that have changed, the past condition class number (CONDID) remains with the condition that is most similar to the previous classification.

Codes: COND\_STATUS\_CHNG\_CD\_RMRS

|   Code | Present                                                   | Past                                                                                         |
|--------|-----------------------------------------------------------|----------------------------------------------------------------------------------------------|
|      1 | Accessible forest land (COND_STATUS_CD = 1).              | Previously all accessible forest land (COND_STATUS_CD = 1).                                  |
|      2 | Not accessible forest land (COND_STATUS_CD = 2, 3, 4, 5). | Previously all not accessible forest land (COND_STATUS_CD = 2, 3, 4, 5).                     |
|      3 | Accessible forest land (COND_STATUS_CD = 1).              | Some portion of this condition was not accessible forest land (COND_STATUS_CD = 2, 3, 4, 5). |
|      4 | Not accessible forest land (COND_STATUS_CD = 2, 3, 4, 5). | Some portion of this condition was accessible forest land (COND_STATUS_CD = 1).              |

## 2.5.119 CRCOVPCT\_RMRS

Live crown cover percent, Rocky Mountain Research Station. The percentage of live crown cover, to the nearest 1 percent, of all established tally seedlings, saplings, and trees. Crown cover is the percentage of ground surface area covered by a vertical projection of the live crowns. Only populated by certain FIA work units (SURVEY.RSCD = 22).

Note: The CRCOVPCT\_RMRS and LIVE\_CANOPY\_CVR\_PCT attributes both list the percentage of live crown cover; however, they differ in the methods that are used. For CRCOVPCT\_RMRS, a line transect method is used for determining cover. For LIVE\_CANOPY\_CVR\_PCT, individual crown widths within the sample area are measured, and then an "ellipse formula" is used to calculate canopy area.

## 2.5.120 DOMINANT\_SPECIES1\_PNWRS

Dominant tree species 1 ( Pacific Islands ), Pacific Northwest Research Station. A code corresponding to the tree species with the plurality of cover for all live trees in the condition class that are not overtopped. Recorded for all accessible forest land condition classes. Only populated by certain FIA work units (SURVEY.RSCD = 26).

Refer to appendix F for codes.

## 2.5.121 DOMINANT\_SPECIES2\_PNWRS

Dominant tree species 2 ( Pacific Islands ), Pacific Northwest Research Station. A code for the second most abundant tree species in each condition class. See DOMINANT\_SPECIES1\_PNWRS for further detail. If a second species does not exist, a code of 0000 is recorded. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 2.5.122 DOMINANT\_SPECIES3\_PNWRS

Dominant tree species 3 ( Pacific Islands ), Pacific Northwest Research Station. A code for the third most abundant tree species in each condition class. See DOMINANT\_SPECIES1\_PNWRS for further detail. If a third species does not exist, a code of 0000 is recorded. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 2.5.123 DSTRBCD1\_P2A

Disturbance code 1, periodic to annual. A code indicating the kind of disturbance occurring since the last measurement. The area affected by the disturbance must be at least 1 acre in size. A significant level of disturbance (mortality or damage to 25 percent of the trees in the condition) is required. Up to three different disturbances per condition can be recorded, from most important to least important (DSTRBCD1\_P2A, DSTRBCD2\_P2A, and DSTRBCD3\_P2A). Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States.

Periodic to annual (P2A) remeasurement includes plots where the newly established annual plot is located at the same center point as the previously established periodic plot.

Note: For RMRS, both the periodic and the annual plot have DESIGNCD = 1.

## Codes: DSTRBCD1\_P2A

|   Code | Description                                                             |
|--------|-------------------------------------------------------------------------|
|      0 | No visible disturbance.                                                 |
|     10 | Insect damage.                                                          |
|     11 | Insect damage to understory vegetation.                                 |
|     12 | Insect damage to trees, including seedlings and saplings.               |
|     20 | Disease damage.                                                         |
|     21 | Disease damage to understory vegetation.                                |
|     22 | Disease damage to trees, including seedlings and saplings.              |
|     30 | Fire damage (from crown and ground fire, either prescribed or natural). |
|     31 | Ground fire damage.                                                     |
|     32 | Crown fire damage.                                                      |
|     40 | Animal damage.                                                          |
|     41 | Beaver (includes flooding caused by beaver).                            |
|     42 | Porcupine.                                                              |
|     43 | Deer/ungulate.                                                          |
|     44 | Bear ( core optional ).                                                 |
|     45 | Rabbit ( core optional ).                                               |
|     46 | Domestic animal/livestock (includes grazing).                           |
|     50 | Weather damage.                                                         |

|   Code | Description                                                                                                                                      |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------|
|     51 | Ice.                                                                                                                                             |
|     52 | Wind (includes hurricane, tornado).                                                                                                              |
|     53 | Flooding (weather induced).                                                                                                                      |
|     54 | Drought.                                                                                                                                         |
|     60 | Vegetation (suppression, competition, vines).                                                                                                    |
|     70 | Unknown / not sure / other (include in NOTES).                                                                                                   |
|     80 | Human-induced damage - any significant threshold of human-caused damage not described in the disturbance codes or in the treatment codes listed. |
|     90 | Geologic disturbances.                                                                                                                           |
|     91 | Landslide.                                                                                                                                       |
|     92 | Avalanche track.                                                                                                                                 |
|     93 | Volcanic blast zone.                                                                                                                             |
|     94 | Other geologic event.                                                                                                                            |
|     95 | Earth movement / avalanches.                                                                                                                     |

## 2.5.124 DSTRBCD2\_P2A

Disturbance code 2, periodic to annual. The second disturbance code, if the stand has experienced more than one disturbance. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States. See DSTRBCD1\_P2A for more information.

## 2.5.125 DSTRBCD3\_P2A

Disturbance code 3, periodic to annual. The third disturbance code, if the stand has experienced more than two disturbances. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States. See DSTRBCD1\_P2A for more information.

## 2.5.126 DSTRBYR1\_P2A

Disturbance year 1, periodic to annual. The year in which disturbance 1 (DSTRBCD1\_P2A) is estimated to have occurred. If the disturbance occurs continuously over a period of time, the value '9999' is used. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States.

Periodic to annual (P2A) remeasurement includes plots where the newly established annual plot is located at the same center point as the previously established periodic plot.

Note: For RMRS, both the periodic and the annual plot have DESIGNCD = 1.

## 2.5.127 DSTRBYR2\_P2A

Disturbance year 2, periodic to annual. The year in which disturbance 2 (DSTRBCD2\_P2A) is estimated to have occurred. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States. See DSTRBYR1\_P2A for more information.

## 2.5.128 DSTRBYR3\_P2A

Disturbance year 3, periodic to annual. The year in which disturbance 3 (DSTRBCD3\_P2A) is estimated to have occurred. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States. See DSTRBYR1\_P2A for more information.

## 2.5.129 FLDTYPCD\_30

Field forest type code, version 3.0. Forest type codes when PLOT.MANUAL &lt;4.0, assigned by the field crew, based on the tree species or species groups forming a plurality of all live stocking. The field crew assesses the forest type based on the acre of forest land around the plot, in addition to the species sampled on the condition.

Nonstocked forest land is land that currently has less than 10 percent stocking but formerly met the definition of forest land. For nonstocked forest land, the crew determined the forest type by either recording the previous forest type on remeasured plots or, on all other plots, the most appropriate forest type to the condition based on the seedlings present or the forest type of the adjacent forest stands. When PLOT.MANUAL &lt;2.0, forest conditions that did not meet the 10 percent stocking level were coded FLDTYPCD = 999. Starting with PLOT.MANUAL = 2.0, the crew no longer recorded nonstocked as 999. Instead, they recorded FLDSZCD = 0 to identify nonstocked conditions and entered a forest type for the condition. In general, when FLDTYPCD is used for analysis, it is necessary to examine the values of both FLDTYPCD and FLDSZCD to identify nonstocked forest land.

Changes to forest type codes from PLOT.MANUAL = 3.0 to 4.0 are listed below. For a current list of forest type codes and names, refer to appendix D.

## Retired codes:

| Forest type group or forest type   |   Code | Description                                                                                     |
|------------------------------------|--------|-------------------------------------------------------------------------------------------------|
| Forest type group                  |    950 | Other western hardwoods.                                                                        |
| Forest type                        |    181 | Eastern redcedar.                                                                               |
| Forest type                        |    183 | Western juniper.                                                                                |
| Forest type                        |    223 | Jeffrey-Coulter-bigcone Douglas-fir.                                                            |
| Forest type                        |    382 | Australian pine. Note: Australian pine now aggregated with "other exotic hardwoods" (code 995). |
| Forest type                        |    803 | Cherry-ash-yellow poplar.                                                                       |
| Forest type                        |    807 | Elm-ash-locust.                                                                                 |
| Forest type                        |    925 | Deciduous oak woodland.                                                                         |
| Forest type                        |    926 | Evergreen oak woodland.                                                                         |
|                                    |    932 | Canyon-interior live oak.                                                                       |
| Forest type                        |    951 | Pacific madrone.                                                                                |
| Forest type                        |    952 | Mesquite woodland.                                                                              |
| Forest type                        |    953 | Mountain brush woodland.                                                                        |
| Forest type                        |    954 | Intermountain maple woodland.                                                                   |

| Forest type group or forest type   |   Code | Description                                                                                                                                                                                           |
|------------------------------------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Forest type                        |    955 | Miscellaneous western hardwoods Note: When reclassified, timber species trees were recoded as 962 (other hardwoods) and woodland species tree were recoded as 976 (miscellaneous woodland hardwoods). |
| Forest type                        |    981 | Sable palm. Note: Sable palm no longer tallied as a tree; any 981 recoded to either 983 (palms) or 962 (other hardwoods).                                                                             |

## Code changes or additions:

| Forest type group or forest type   | Old code   |   New code | Description                                                                                                                                                                                     |
|------------------------------------|------------|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Forest type group                  | -          |        150 | Tropical softwoods.                                                                                                                                                                             |
| Forest type group                  | -          |        170 | Other eastern softwoods.                                                                                                                                                                        |
| Forest type group                  | -          |        390 | Other softwoods.                                                                                                                                                                                |
| Forest type group                  | -          |        960 | Other hardwoods.                                                                                                                                                                                |
| Forest type group                  | 950        |        970 | Woodland hardwoods. Note: Forest type groups recoded from code 950 to 970, with the exception of Pacific madrone (Pacific madrone moved to the "other hardwoods" forest type group - code 960). |
| Forest type                        | -          |        128 | Fraser fir.                                                                                                                                                                                     |
| Forest type                        | -          |        129 | Red spruce / Fraser fir.                                                                                                                                                                        |
| Forest type                        | -          |        151 | Tropical pines.                                                                                                                                                                                 |
| Forest type                        | 181        |        171 | Eastern redcedar.                                                                                                                                                                               |
| Forest type                        | -          |        172 | Florida softwoods.                                                                                                                                                                              |
| Forest type                        | 223        |        203 | Bigcone Douglas-Fir.                                                                                                                                                                            |
| Forest type                        | 223        |        225 | Jeffrey pine.                                                                                                                                                                                   |
| Forest type                        | 223        |        226 | Coulter pine.                                                                                                                                                                                   |
| Forest type                        | 183        |        369 | Western juniper.                                                                                                                                                                                |
| Forest type                        | -          |        384 | Norway spruce.                                                                                                                                                                                  |
| Forest type                        | -          |        385 | Introduced larch.                                                                                                                                                                               |
| Forest type                        | -          |        391 | Other softwoods.                                                                                                                                                                                |
| Forest type                        | 803        |        516 | Cherry / white ash / yellow-poplar.                                                                                                                                                             |
| Forest type                        | 807        |        517 | Elm / ash / black locust.                                                                                                                                                                       |
| Forest type                        | -          |        609 | Baldcypress / pondcypress.                                                                                                                                                                      |
| Forest type                        | -          |        903 | Gray birch.                                                                                                                                                                                     |
| Forest type                        | -          |        905 | Pin cherry.                                                                                                                                                                                     |
| Forest type                        | 932        |        933 | Canyon live oak.                                                                                                                                                                                |
| Forest type                        | 932        |        934 | Interior live oak.                                                                                                                                                                              |
| Forest type                        | -          |        935 | California white oak (valley oak).                                                                                                                                                              |
| Forest type                        | 951        |        961 | Pacific madrone.                                                                                                                                                                                |

| Forest type group or forest type   | Old code   |   New code | Description                                                         |
|------------------------------------|------------|------------|---------------------------------------------------------------------|
| Forest type                        | 955        |        962 | Other hardwoods.                                                    |
| Forest type                        | 925        |        971 | Deciduous oak woodland. Note: Gambel oak included within this type. |
| Forest type                        | 926        |        972 | Evergreen oak woodland.                                             |
| Forest type                        | 952        |        973 | Mesquite woodland.                                                  |
| Forest type                        | 953        |        974 | Cercocarpus (mountain brush) woodland.                              |
| Forest type                        | 954        |        975 | Intermountain maple woodland.                                       |
| Forest type                        | 955        |        976 | Miscellaneous woodland hardwoods.                                   |
| Forest type                        | -          |        983 | Palms.                                                              |
| Forest type                        | -          |        989 | Other tropical hardwoods.                                           |

## 2.5.130 FOREST\_COMMUNITY\_PNWRS

Forest type ( Pacific Islands ), Pacific Northwest Research Station. A code indicating the forest type that best describes the species with the plurality of crown cover for all live trees in the condition class that are not overtopped. Recorded for all accessible forest land condition classes in the Pacific Islands. Only populated by certain FIA work units (SURVEY.RSCD = 26).

Note: Pacific Island forest types are taken from Mueller-Dombois and Fosberg (1998).

## Codes: FOREST\_COMMUNITY\_PNWRS.

|   Code | Description                                                                                                                                                                                                                                                                           |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Strand or halophytic vegetation - vegetation near the shore containing species adapted to high rates of evaporation by wind and to high salt concentrations from windblown ocean spray or inundation by salt water.                                                                   |
|      2 | Mangrove swamps - trees with high salt tolerance growing on tidally inundated shores and in landlocked depressions. Many species have pneumatophores, adaptive structures for aeration of waterlogged root systems.                                                                   |
|      3 | Lowland tropical rainforest - multistoried forest with many canopy- dwelling epiphytes, open ground, and shrub layers. This forest community can extend up the lower slopes with windward rainy exposures.                                                                            |
|      4 | Montane rainforest -the predominant type on moist hilltops and mountain slopes in many tropical islands. Forests of low stature that are rich in shrubs and epiphytes.                                                                                                                |
|      5 | Cloud forest - These forests are covered with clouds or fog much of the time. The trees have low canopies and are often dripping with moisture. The trees are typically small-leafed and covered with masses of epiphytic mosses and liverworts, which also form a deep ground cover. |
|      6 | Mesophytic or moist forest - seasonally dry evergreen forests on leeward, drier slopes.                                                                                                                                                                                               |
|      7 | Xerophytic - forests found on truly dry, rain-shadow, leeward mountain slopes and lowlands.                                                                                                                                                                                           |

## Condition Table

|   Code | Description                                                                                                                                                                                         |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      8 | Agroforestry - tree species are included in crop or animal production agricultural ecosystems.                                                                                                      |
|      9 | Plantations - an area planted with tree species for the purpose of timber production. Species planted are mainly eucalypt, mahogany, and pine species that replace indigenous forests and savannas. |

## 2.5.131 LAND\_USECD\_RMRS

Land use code, Rocky Mountain Research Station. A code indicating the current land use for an accessible forest land or nonforest land condition class. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: LAND\_USECD\_RMRS

|   Code | Description                                                                                                                                                                                              |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Condition is not being manipulated by human activities such as regular mowing, intensive grazing, or recreation activities.                                                                              |
|      2 | Condition is being manipulated by human activities that prevent normal regeneration and succession such as regular mowing, intensive grazing, or recreation activities.                                  |
|      3 | Condition has been chained in the past.                                                                                                                                                                  |
|      4 | An inclusion that would generally be recognized as a separate condition, except that it is not large enough to qualify (<1 acre or <120 feet wide), regardless of live plus missing crown cover percent. |

## 2.5.132 MAICF

Mean annual increment cubic feet. A measure of the productivity of forest land for the condition expressed as the average increase in cubic feet of (growing stock) wood volume per acre per year occurring in the year that mean annual increment (MAI) culminates (peaks),  in fully stocked natural stands. This attribute is calculated using site index for the condition, entered into a yield equation, and calculates MAI at culmination. Only populated by certain FIA work units (SURVEY.RSCD = 22, 26, 27).

## Notes:

- · For RMRS (SURVEY.RSCD = 22), MAICF is assigned a default value of 10 for conditions with a woodland forest type (FORTYPCD).
- · For PNWRS (SURVEY.RSCD = 26, 27), MAICF is not calculated for conditions with a woodland forest type (FORTYPCD).

## 2.5.133 PCTBARE\_RMRS

Percent bare ground, Rocky Mountain Research Station. A value indicating the amount of bare ground on the subplot by forested condition, to the nearest percent.

Bare ground is exposed soil and rock fragments smaller than ¾ inch (longest dimension). Rocks protruding through the soil or cryptobiotic crusts are not included in bare ground estimates. In addition, areas that are part of a nonforested condition are also not included in bare ground estimates; only forested conditions are examined (e.g., if a subplot is half forested and 25 percent of the forested portion is bare ground, then the percent bare ground is recorded as 25 percent). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.5.134 QMD\_RMRS

Quadratic mean diameter, Rocky Mountain Research Station. The quadratic mean diameter, or the diameter of the tree of average basal area, on the condition. Based on live trees  inch d.b.h./d.r.c. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.5.135 RANGETYPCD\_RMRS

Range type code (existing vegetation classification), Rocky Mountain Research Station. For each nonforest condition, a code indicating the predominant existing vegetation type that is most representative of the condition. Data only collected for plots that have a sampled nonforest condition(s) (PLOT.NF\_PLOT\_STATUS\_CD = 1). Only populated by certain FIA work units (SURVEY.RSCD = 22).

A code of 999 is recorded when the type is undefined or there is not enough vegetation to classify a type. The existing vegetation classification is not necessarily the same as habitat type.

## Codes: RANGETYPCD\_RMRS

|   Code | Range type (existing vegetation classification)         |
|--------|---------------------------------------------------------|
|    101 | Bluebunch wheatgrass.                                   |
|    102 | Idaho fescue.                                           |
|    103 | Green fescue.                                           |
|    104 | Antelope bitterbrush / bluebunch wheatgrass.            |
|    105 | Antelope bitterbrush / Idaho fescue.                    |
|    106 | Bluegrass scabland.                                     |
|    107 | Western juniper / big sagebrush / bluebunch wheatgrass. |
|    108 | Alpine Idaho fescue.                                    |
|    301 | Bluebunch wheatgrass / blue grama.                      |
|    302 | Bluebunch wheatgrass / Sandberg bluegrass.              |
|    303 | Bluebunch wheatgrass / western wheatgrass.              |
|    304 | Idaho fescue / bluebunch wheatgrass.                    |
|    305 | Idaho fescue / Richardson needlegrass.                  |
|    306 | Idaho fescue / slender wheatgrass.                      |
|    307 | Idaho fescue / threadleaf sedge.                        |
|    308 | Idaho fescue / tufted hairgrass.                        |
|    309 | Idaho fescue / western wheatgrass.                      |
|    310 | Needle-and-thread / blue grama.                         |
|    311 | Rough fescue / bluebunch wheatgrass.                    |
|    312 | Rough fescue / Idaho fescue.                            |
|    313 | Tufted hairgrass / sedge.                               |
|    314 | Big sagebrush / bluebunch wheatgrass.                   |
|    315 | Big sagebrush / Idaho fescue.                           |
|    316 | Big sagebrush / rough fescue.                           |
|    317 | Bitterbrush / bluebunch wheatgrass.                     |
|    318 | Bitterbrush / Idaho fescue.                             |

|   Code | Range type (existing vegetation classification)    |
|--------|----------------------------------------------------|
|    319 | Bitterbrush / rough fescue.                        |
|    320 | Black sagebrush / bluebunch wheatgrass.            |
|    321 | Black sagebrush / Idaho fescue.                    |
|    322 | Curlleaf mountain-mahogany / bluebunch wheatgrass. |
|    323 | Shrubby cinquefoil / rough fescue.                 |
|    324 | Threetip sagebrush / Idaho fescue.                 |
|    401 | Basin big sagebrush.                               |
|    402 | Mountain big sagebrush.                            |
|    403 | Wyoming big sagebrush.                             |
|    404 | Threetip sagebrush.                                |
|    405 | Black sagebrush.                                   |
|    406 | Low sagebrush.                                     |
|    407 | Stiff sagebrush.                                   |
|    408 | Other sagebrush types.                             |
|    409 | Tall forb.                                         |
|    410 | Alpine rangeland.                                  |
|    413 | Gambel oak.                                        |
|    414 | Salt desert shrub.                                 |
|    415 | Curlleaf mountain-mahogany.                        |
|    416 | True mountain-mahogany.                            |
|    417 | Littleleaf mountain-mahogany.                      |
|    418 | Bigtooth maple.                                    |
|    419 | Bittercherry.                                      |
|    420 | Snowbrush.                                         |
|    421 | Chokecherry / serviceberry / rose.                 |
|    601 | Bluestem prarie.                                   |
|    602 | Bluestem / prarie sandreed.                        |
|    603 | Prarie sandreed / needlegrass.                     |
|    604 | Bluestem / grama prarie.                           |
|    605 | Sandsage prarie.                                   |
|    606 | Wheatgrass / bluestem / needlegrass.               |
|    607 | Wheatgrass / needlegrass.                          |
|    608 | Wheatgrass / grama / needlegrass.                  |
|    609 | Wheatgrass / grama.                                |
|    610 | Wheatgrass.                                        |
|    611 | Blue grama / buffalograss.                         |
|    612 | Sagebrush / grass.                                 |
|    613 | Fescue grassland.                                  |
|    614 | Crested wheatgrass.                                |

|   Code | Range type (existing vegetation classification)   |
|--------|---------------------------------------------------|
|    615 | Wheatgrass / saltgrass / grama.                   |
|    999 | Undefined.                                        |

## 2.5.136 SDIMAX\_RMRS

Stand density index maximum, Rocky Mountain Research Station. The maximum value for the stand density index (SDI) for a particular forest type and region, at the condition level. If the condition is nonstocked, the field-recorded forest type (FLDTYPCD) is used in place of a calculated forest type (FORTYPCD). Refer to SDI\_RMRS for further detail. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.5.137 SDIPCT\_RMRS

Stand density index percent, Rocky Mountain Research Station. A relative measure of stand density for live trees (  1.0 inch d.b.h./d.r.c.) on the condition, expressed as a percentage of the maximum stand density index (SDI).

SDIPCT\_RMRS is computed as follows:

SDIPCT\_RMRS = (SDI\_RMRS/SDIMAX\_RMRS)*100

Refer to SDI\_RMRS and SDIMAX\_RMRS for more information. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.5.138 SDI\_RMRS

Stand density index for the condition, Rocky Mountain Research Station. An index that expresses the stand density for live trees (  1.0 inch d.b.h./d.r.c.) on the condition. SDI\_RMRS is based on a quadratic mean diameter of the stand and the number of live trees per acre (TPA\_UNADJ) at the condition level. It is computed for timber and woodland species (  1.0 inch d.b.h./d.r.c.), and is equal to the sum of stand density index (SDI) values for individual trees on the condition. SDI is a widely used measure developed by Reineke (1933). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 2.5.139 STAND\_STRUCTURE\_ME\_NERS

Stand structure (Maine), Northeastern Research Station. A code indicating the basic stand structure of the trees in the condition. This attribute is ancillary, that is, contrasting conditions are never delineated based on variation in this attribute.

Only populated by certain FIA work units (SURVEY.RSCD = 24) and only in Maine.

## Codes: STAND\_STRUCTURE\_ME\_NERS

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Single-storied - Stands characterized by an even canopy of uniform height with close competition between trees. The smaller trees are usually members of the stand that were stressed or overtopped and have fallen behind their associates. Regeneration and/or tall relics from a previous stand may be present. Most of the trees in the condition are within the height class of the average stand height.                                                           |
|      2 | Two-storied - Stands composed of two relatively even but distinct canopy layers, such as a mature overstory with an understory sapling layer, possibly from seed tree and shelterwood operations, or an overstory of tall conifers with an understory of low hardwoods. Neither canopy is necessarily continuous or closed, but both canopy levels tend to be uniformly distributed across the stand. Each canopy level must cover at least 25 percent of the condition. |
|      3 | Multi-storied - Stands generally containing trees from every size group on a continuum from seedlings to mature trees and are characterized by a broken or uneven canopy layer. Usually the largest number of trees is in the smaller diameter classes. Includes any stand with three or more structural layers if each of the three or more layers covers at least 25 percent of the condition.                                                                         |
|      4 | Mosaic - Stands contain at least two distinct size classes each of which covers at least 25 percent of the condition; however, these classes are not uniformly distributed but are grouped in small repeating aggregations, or occur in stringers less than 120.0 ft. (36.6 m.) wide, throughout the stand. Each size class aggregation is too small to be recognized and mapped as an individual stand; the aggregations may or may not be single-storied.              |
|      5 | Nonstocked - Less than 10-percent tree stocking present.                                                                                                                                                                                                                                                                                                                                                                                                                 |

## 2.5.140 TREES\_PRESENT\_NCRS

Trees present on nonforest, North Central Research Station. A code indicating the presence or absence of live trees  5.0 inches d.b.h. that are within the nonforest condition represented in the "plot triangle" (the triangle formed by the 3 outer subplots, representing 0.84 acres that is used for office photo interpretation to determine whether or not a plot is sent to the field for measurement).

Only populated by certain FIA work units (SURVEY.RSCD = 23). Data collected in all States when PLOT.MANUAL = 1.0-5.1 (INVYR = 1999-2013), and continued for Indiana when PLOT.MANUAL = 6.0 (INVYR &gt;2013).

Codes: TREES\_PRESENT\_NCRS

|   Code | Description                                           |
|--------|-------------------------------------------------------|
|      1 | Nonforest land without live trees  5.0 inches d.b.h. |
|      2 | Nonforest land with live trees  5.0 inches d.b.h.    |

## 2.5.141 TREES\_PRESENT\_NERS

Trees present on nonforest, Northeastern Research Station. A code indicating the presence or absence of live trees  5.0 inches d.b.h. that are within the nonforest condition represented in the "plot triangle" (the triangle formed by the 3 outer subplots, representing 0.84 acres that is used for office photo interpretation to determine whether or not a plot is sent to the field for measurement).

Data back-populated for all States in certain FIA work units (SURVEY.RSCD = 24) for COND\_STATUS\_CD = 4 (census water) for INVYR = 1999-2006. Data collected and back-populated for all nonforest conditions (SURVEY.RSCD = 24) for INVYR = 2007-2013.

## Codes: TREES\_PRESENT\_NERS

|   Code | Description                                           |
|--------|-------------------------------------------------------|
|      1 | Nonforest land without live trees  5.0 inches d.b.h. |
|      2 | Nonforest land with live trees  5.0 inches d.b.h.    |

## 2.5.142 TRTCD1\_P2A

Treatment code 1, periodic to annual. A code indicating the type of stand treatment that has occurred since the last periodic measurement. The area affected by the treatment must be at least 1 acre in size. Up to three different treatments per condition can be recorded, from most important to least important (TRTCD1\_P2A, TRTCD2\_P2A, and TRTCD3\_P2A). Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States.

Periodic to annual (P2A) remeasurement includes plots where the newly established annual plot is located at the same center point as the previously established periodic plot.

Note: For RMRS (SURVEY.RSCD = 22), both the periodic and the annual plot have DESIGNCD = 1.

## Codes: TRTCD1\_P2A

|   Code | Description                                                                                                                                                                                                                                                                              |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     00 | No observable treatment.                                                                                                                                                                                                                                                                 |
|     10 | Cutting - The removal of one or more trees from a stand.                                                                                                                                                                                                                                 |
|     20 | Site preparation - Clearing, slash burning, chopping, disking, bedding, or other practices clearly intended to prepare a site for either natural or artificial regeneration.                                                                                                             |
|     30 | Artificial regeneration - Following a disturbance or treatment (usually cutting), a new stand where at least 50 percent of the live trees present resulted from planting or direct seeding.                                                                                              |
|     40 | Natural regeneration - Following a disturbance or treatment (usually cutting), a new stand where at least 50 percent of the live trees present (of any size) were established through the growth of existing trees and/or natural seeding or sprouting.                                  |
|     50 | Other silvicultural treatment - The use of fertilizers, herbicides, girdling, pruning, or other activities (not covered by codes 10-40) designed to improve the commercial value of the residual stand, or chaining, which is a practice used on woodlands to encourage wildlife forage. |

## 2.5.143 TRTCD2\_P2A

Treatment code 2, periodic to annual. The second treatment code, if the stand has experienced more than one treatment since the last periodic measurement. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States. See TRTCD1\_P2A for more information.

## 2.5.144 TRTCD3\_P2A

Treatment code 3, periodic to annual. The third treatment code, if the stand has experienced more than two treatments since the last periodic measurement. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States. See TRTCD1\_P2A for more information.

## 2.5.145 TRTOPCD

Treatment opportunity code. A code indicating the best possible silvicultural treatment recommended for a forest condition, based on stand size, forest type, site productivity, and other factors. Only calculated for certain FIA work units (SURVEY.RSCD = 23).

## Codes: TRTOPCD

|   Code | Description              |
|--------|--------------------------|
|      1 | Regen without site prep. |
|      2 | Regen with site prep.    |
|      3 | Stand conversion.        |
|      4 | Thin seed/sap.           |
|      5 | Thin pole.               |
|      6 | Other stocking control.  |
|      7 | Other intermediate.      |
|      8 | Clearcut.                |
|      9 | Partial harvest.         |
|     10 | Salvage harvest.         |
|     11 | None.                    |

## 2.5.146 TRTYR1\_P2A

Treatment year 1, periodic to annual. The year in which treatment 1 (TRTCD1\_P2A) is estimated to have occurred. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States.

Periodic to annual (P2A) remeasurement includes plots where the newly established annual plot is located at the same center point as the previously established periodic plot.

Note: For RMRS, both the periodic and the annual plot have DESIGNCD = 1.

## 2.5.147 TRTYR2\_P2A

Treatment year 2, periodic to annual. The year in which treatment 2 (TRTCD2\_P2A) is estimated to have occurred. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States. See TRTYR1\_P2A for more information.

## 2.5.148 TRTYR3\_P2A

Treatment year 3, periodic to annual. The year in which treatment 3 (TRTCD3\_P2A) is estimated to have occurred. Populated for forested conditions at locations with periodic to annual remeasurement. Not populated for all States. See TRTYR1\_P2A for more information.

## 2.5.149 LAND\_COVER\_CLASS\_CD

Land cover class code. A code indicating the type of cover for a condition that meets the minimum area and width requirements, except those with cases where the condition has been defined due to one of the exceptions to the size and width requirements. If the condition was less than 1 acre, a cover classification key was used to assign a cover class.

This is the revised cover class attribute implemented in PLOT.MANUAL = 8.0. Many of the codes are the same between the retired and the current code sets. The cover classification key used by crews has been modified to remove all aspects of land use and focus on land cover. There is no national crosswalk to translate the retired codes into the new codes (see LAND\_COVER\_CLASS\_CD\_RET for the old code list).

Codes:  LAND\_COVER\_CLASS\_CD (codes that are  10% live vegetative cover)

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     01 | Tree Cover: Areas on which live trees provide 10% or greater canopy cover and are part of the dominant (uppermost) vegetation layer, including areas that have been planted to produce woody crops, Christmas trees, orchards, etc. Only include tree species that are listed on the FIA Master Tree Species List (refer to Public Box folder available at web address: https://usfs-public.box.com/v/FIA-TreeSpeciesList) after taking into account the three exclusion zones. Varieties and subspecies are tallied at the species level and hybrids are based on the dominant external characteristics. Species not included on the FIA Master Tree Species List are considered shrub cover. Example areas include forests, forest plantations, reverting fields with  10% tree canopy cover, clearcuts with  10% tree canopy cover. This category includes cyp ress swamps and mangroves (not to be confused with aquatic vegetation). |
|     02 | Shrub Cover: Areas on which live shrubs or subshrubs provide 10% or greater cover and are part of the dominant (uppermost) vegetation layer, provided these areas do not qualify as Tree Cover. Shrub/Subshrub - a woody plant that generally has several erect, spreading, or prostrate stems, which give it a bushy appearance. This includes dwarf shrubs, and low or short woody vines (Federal Geographic Data Committee Vegetation Subcommittee 2008) and excludes any species on FIA's tree list. Examples include cranberry bogs, berry crops, and other shrub-dominated wetlands, chaparral, and sagebrush.                                                                                                                                                                                                                                                                                                                        |
|     03 | Herbaceous Cover: Areas on which live herbaceous vegetation (including seasonally senescent cover) provides 10% or greater cover and are part of the dominant (uppermost) vegetation layer, provided these areas do not qualify as Tree Cover or Shrub Cover. This includes herbs, forbs, and graminoid species. Examples include meadows, prairies, croplands (while crops are present), and improved pasture. This category also includes emergent wetland vegetation like seasonally flooded grasslands, cattail marshes, etc.                                                                                                                                                                                                                                                                                                                                                                                                           |
|     04 | Non-vascular Vegetation Cover: Areas on which non-vascular vegetation provides 10% or greater cover and are part of the dominant vegetation layer, provided these areas do not qualify as Tree Cover, Shrub Cover, or Herbaceous Cover. Examples include mosses, sphagnum moss bogs, liverworts, hornworts, lichens, and algae.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|     05 | Mixed Vegetation Cover: Areas with 10% or greater live vegetative cover but no one life form has 10% or more cover. That is, these areas do not qualify as Tree Cover, Shrub Cover, Herbaceous Cover or Non-vascular Vegetation Cover, and thus are a mixture of plant life forms. Examples can include early stages of reverting fields and high deserts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

## Codes: LAND\_COVER\_CLASS\_CD (codes that are &lt;10% live vegetative cover)

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     08 | Barren: Areas predominately covered by bare rock, gravel, sand, silt, clay, or other earthen material, which contains <10% vegetation cover regardless of its inherent ability to support life. Examples include naturally barren areas such as lava fields, gravel bars, sand dunes, salt flats, deserts, playas, and rock outcroppings, as well as areas of bare soil exposed by land clearing (including plowed, harvested, or planted but not yet emerged cropland), wildfire, and other forms of disturbance. Also includes minerals and other geologic materials exposed by surface mining and roads made of dirt and gravel. |
|     09 | Impervious: Areas predominantly covered with constructed materials that contain <10% vegetation cover. Examples include paved roads, parking lots, driveways, sidewalks, rooftops, and other man-made structures.                                                                                                                                                                                                                                                                                                                                                                                                                   |
|     10 | Water: Areas persistently covered and predominated by water and have <10% emergent vegetative cover. Examples include census and noncensus water and permanent snow and ice as well as glaciers. For example, only the open water portion of a bog is to be included.                                                                                                                                                                                                                                                                                                                                                               |
|     12 | Unknown: No classification was possible.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

## 2.5.150 SIEQN\_REF\_CD

Site index equation reference code. This is the internal reference code for site index equations in the FIA equation library. There are more equations in the library than are currently in use by FIA. Site index equations have not been developed for all species, so the equation reference for a given species in a given geographic range may refer to an equation developed for a different species. See REF\_SIEQN.SIEQN\_REF\_NOTES for additional information associated with each SIEQN\_REF\_CD (e.g., notes, primary reference).

## 2.5.151 SICOND\_FVS

Site index for the condition, used by the Forest Vegetation Simulator. This is similar to SICOND, but is computed using the equation required by, and species allowed by, the Forest Vegetation Simulator (FVS). Site index values in SICOND\_FVS are not used for other computations in the FIA processing system, and are primarily used when exporting FIA data for use in FVS. This attribute is blank (null) when no site index data are available.

## 2.5.152 SIBASE\_FVS

Site index base age used by the Forest Vegetation Simulator. The base age (sometimes called reference age), in years, of the site index curves used to derive site index. Base age is specific to a given family of site index curves, and is usually set close to the common rotation age or the age of culmination of mean annual increment for a species. The most commonly used base ages are 25, 50, 80, and 100 years. It is possible for a given species to have different sets of site index curves in different geographic regions, and each set of curves may use a different base age.

Note: For a given geographic location, FVS variants may require the use of site index equations that were developed using a different base age than used by the site index equations used in standard FIA compilation procedures. Because of the historical development of FIA procedures and FVS growth models, the two systems have differences in the base ages that are used.

## 2.5.153 SISP\_FVS

Site index species code used by the Forest Vegetation Simulator. Site index species code used by the Forest Vegetation Simulator. The species upon which the site index is based for use in the vegetation simulator. In most cases the site index species will be one of the species that define the forest type of the condition (FORTYPCD). However, the list of species allowed for computation of site index for use in FVS can differ from species allowed by other FIA computational processes. It is possible for SISP to be blank and SISP\_FVS to be populated. This attribute is blank (null) when no site tree data are available.

## 2.5.154 SIEQN\_REF\_CD\_FVS

Site index equation reference code used by the Forest Vegetation Simulator. This is the internal reference code for site index equations in the FIA equation library that is used to calculate site index. There are more equations in the library than are currently in use by FIA. Site index equations have not been developed for all species, so the equation reference for a given species in a given geographic range may refer to an equation developed for a different species. See REF\_SIEQN.SIEQN\_REF\_NOTES for additional information associated with each SIEQN\_REF\_CD (e.g., notes, primary reference).

## 2.5.155 MQUADPROP\_UNADJ

Microquadrat proportion unadjusted. The unadjusted proportion of the microquadrats that are in the condition. Microquadrats are used to collect data for ground layer functional groups (see GRND\_LYR\_FNCTL\_GRP.FUNCTIONAL\_GROUP\_CD). The sum of all microquadrat proportions on a plot equals 1.

## 2.5.156 SOILPROP\_UNADJ

Soil proportion unadjusted. The unadjusted proportion of the soils sample that is in the condition. The sum of all soil proportions in a plot equals 1.

## 2.5.157 FOREST\_COND\_STATUS\_CHANGE\_CD

Forest land condition status change code. A code indicating the reason why the forest land condition status changed since the last inventory. If the status did not change, FOREST\_COND\_STATUS\_CHANGE\_CD = 0 is recorded.

## Codes: FOREST\_COND\_STATUS\_CHANGE\_CD

|   Code | Description                                                                                                                                                                                                                                                                                                                |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | No change - the condition is not a new forested condition (not originating from a previous forested condition) nor is it a new condition that is the result of a previously forested condition no longer qualifying as such or the condition was previously not field visited or was previously classified as non-sampled. |
|      1 | Physical changes - condition status changed due to actual on-the-ground physical change either natural or human-caused.                                                                                                                                                                                                    |
|      2 | Crew error - condition status changed due to a previous crew's error.                                                                                                                                                                                                                                                      |
|      3 | Procedural changes - condition status changed due to a change in variable definition or procedures.                                                                                                                                                                                                                        |