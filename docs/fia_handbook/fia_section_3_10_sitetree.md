# Section 3.10: Site Tree Table
**Oracle Table Name:** SITETREE
**Extracted Pages:** 307-314 (Chapter pages 3-149 to 3-156)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.10 Site Tree Table

## (Oracle table name: SITETREE)

| Subsection   | Column name (attribute)   | Descriptive name                                                       | Oracle data type   |
|--------------|---------------------------|------------------------------------------------------------------------|--------------------|
| 3.10.1       | CN                        | Sequence number                                                        | VARCHAR2(34)       |
| 3.10.2       | PLT_CN                    | Plot sequence number                                                   | VARCHAR2(34)       |
| 3.10.3       | PREV_SIT_CN               | Previous site tree sequence number                                     | VARCHAR2(34)       |
| 3.10.4       | INVYR                     | Inventory year                                                         | NUMBER(4)          |
| 3.10.5       | STATECD                   | State code                                                             | NUMBER(4)          |
| 3.10.6       | UNITCD                    | Survey unit code                                                       | NUMBER(2)          |
| 3.10.7       | COUNTYCD                  | County code                                                            | NUMBER(3)          |
| 3.10.8       | PLOT                      | Plot number                                                            | NUMBER(5)          |
| 3.10.9       | CONDID                    | Condition class number                                                 | NUMBER(1)          |
| 3.10.10      | TREE                      | Site tree number                                                       | NUMBER(9)          |
| 3.10.11      | SPCD                      | Species code                                                           | NUMBER             |
| 3.10.12      | DIA                       | Diameter                                                               | NUMBER(5,2)        |
| 3.10.13      | HT                        | Total height                                                           | NUMBER(3)          |
| 3.10.14      | AGEDIA                    | Tree age at diameter                                                   | NUMBER(3)          |
| 3.10.15      | SPGRPCD                   | Species group code                                                     | NUMBER(2)          |
| 3.10.16      | SITREE                    | Site index for the tree                                                | NUMBER(3)          |
| 3.10.17      | SIBASE                    | Site index base age                                                    | NUMBER(3)          |
| 3.10.18      | SUBP                      | Subplot number                                                         | NUMBER(3)          |
| 3.10.19      | AZIMUTH                   | Azimuth                                                                | NUMBER(3)          |
| 3.10.20      | DIST                      | Horizontal distance                                                    | NUMBER(4,1)        |
| 3.10.21      | METHOD                    | Site tree method code                                                  | NUMBER(2)          |
| 3.10.22      | SITREE_EST                | Estimated site index for the tree                                      | NUMBER(3)          |
| 3.10.23      | VALIDCD                   | Validity code                                                          | NUMBER(1)          |
| 3.10.24      | CREATED_BY                | Created by                                                             | VARCHAR2(30)       |
| 3.10.25      | CREATED_DATE              | Created date                                                           | DATE               |
| 3.10.26      | CREATED_IN_INSTANCE       | Created in instance                                                    | VARCHAR2(6)        |
| 3.10.27      | MODIFIED_BY               | Modified by                                                            | VARCHAR2(30)       |
| 3.10.28      | MODIFIED_DATE             | Modified date                                                          | DATE               |
| 3.10.29      | MODIFIED_IN_INSTANCE      | Modified in instance                                                   | VARCHAR2(6)        |
| 3.10.30      | CYCLE                     | Inventory cycle number                                                 | NUMBER(2)          |
| 3.10.31      | SUBCYCLE                  | Inventory subcycle number                                              | NUMBER(2)          |
| 3.10.32      | AGECHKCD_RMRS             | Radial growth and tree age check code, Rocky Mountain Research Station | NUMBER(1)          |

| Subsection   | Column name (attribute)         | Descriptive name                                                           | Oracle data type   |
|--------------|---------------------------------|----------------------------------------------------------------------------|--------------------|
| 3.10.33      | AGE_DETERMINATION_METHOD_P NWRS | Age determination method, Pacific Northwest Research Station               | NUMBER(1)          |
| 3.10.34      | CCLCD_RMRS                      | Crown class code, Rocky Mountain Research Station                          | NUMBER(1)          |
| 3.10.35      | DAMAGE_AGENT_CD1_RMRS           | Damage agent code 1, Rocky Mountain Research Station                       | NUMBER(5)          |
| 3.10.36      | DAMAGE_AGENT_CD2_RMRS           | Damage agent code 2, Rocky Mountain Research Station                       | NUMBER(5)          |
| 3.10.37      | DAMAGE_AGENT_CD3_RMRS           | Damage agent code 3, Rocky Mountain Research Station                       | NUMBER(5)          |
| 3.10.38      | SIBASE_AGE_PNWRS                | Site index equation base age, Pacific Northwest Research Station           | NUMBER(3)          |
| 3.10.39      | SITETRCD_RMRS                   | Site tree code, Rocky Mountain Research Station                            | NUMBER(1)          |
| 3.10.40      | SITE_AGE_TREE_STATUS_PNWRS      | Site age tree status, Pacific Northwest Research Station                   | VARCHAR2(1)        |
| 3.10.41      | SITE_AGE_TREE_TYPE_PNWRS        | Site age tree type, Pacific Northwest Research Station                     | NUMBER(1)          |
| 3.10.42      | SITE_TREE_METHOD_PNWRS          | Site tree selection method, Pacific Northwest Research Station             | VARCHAR2(1)        |
| 3.10.43      | SITREE_EQU_NO_PNWRS             | Site index equation number, Pacific Northwest Research Station             | NUMBER(3)          |
| 3.10.44      | TREECLCD_RMRS                   | Tree class code, Rocky Mountain Research Station                           | NUMBER(2)          |
| 3.10.45      | TREE_ACT_RMRS                   | Actual tree number, Rocky Mountain Research Station                        | NUMBER(3)          |
| 3.10.46      | YEAR_AGE_TAKEN                  | Year age taken                                                             | NUMBER(4)          |
| 3.10.47      | SIEQN_REF_CD                    | Site index equation reference code                                         | VARCHAR2(10)       |
| 3.10.48      | SITREE_FVS                      | Site index for the tree, used by the Forest Vegetation Simulator           | NUMBER(3)          |
| 3.10.49      | SIBASE_FVS                      | Site index base age used by the Forest Vegetation Simulator                | NUMBER(3)          |
| 3.10.50      | SIEQN_REF_CD_FVS                | Site index equation reference code used by the Forest Vegetation Simulator | VARCHAR2(10)       |

| Key Type   | Column(s) order                                      | Tables to link   | Abbreviated notation   |
|------------|------------------------------------------------------|------------------|------------------------|
| Primary    | CN                                                   | N/A              | SIT_PK                 |
| Unique     | PLT_CN, CONDID, TREE                                 | N/A              | SIT_UK                 |
| Natural    | STATECD, INVYR, UNITCD, COUNTYCD, PLOT, CONDID, TREE | N/A              | SIT_NAT_I              |

| Key Type   | Column(s) order   | Tables to link   | Abbreviated notation   |
|------------|-------------------|------------------|------------------------|
| Foreign    | PLT_CN, CONDID    | SITETREE to COND | SIT_CND_FK             |
| Foreign    | PLT_CN            | SITETREE to PLOT | SIT_PLT_FK             |

Note:

The SITETREE record may not exist for some periodic inventory data.

## 3.10.1 CN

Sequence number. A unique sequence number used to identify a site tree record.

## 3.10.2 PLT\_CN

Plot sequence number. Foreign key linking the site tree record to the plot record.

## 3.10.3 PREV\_SIT\_CN

Previous site tree sequence number. Foreign key linking the site tree to the previous inventory's site tree record for this tree. Only populated for site trees remeasured from a previous annual inventory.

## 3.10.4 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 3.10.5 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 3.10.6 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 3.10.7 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 3.10.8 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 3.10.9 CONDID

Condition class number. The unique identifying number assigned to a condition for which the site tree is measured, and is defined in the COND table. See COND.CONDID for details on the attributes which delineate a condition.

## 3.10.10 TREE

Site tree number. A number used to uniquely identify a site tree on a condition for a plot visit. For tallied site trees, this number is not necessarily the same as the actual tally tree number that is used to uniquely identify the tree on the subplot. Site tree numbers are not permanent, and the number can be used for a different site tree on a subsequent plot visit.

## 3.10.11 SPCD

Species code. A standard tree species code. Refer to appendix F for codes.

## 3.10.12 DIA

Diameter. The diameter, in inches, of the tree at the point of diameter measurement (d.b.h.).

## 3.10.13 HT

Total height. The total length (height) of the site tree, in feet, from the ground to the top of the main stem.

## 3.10.14 AGEDIA

Tree age at diameter. Age, in years, of tree at the point of diameter measurement (d.b.h.). Age is determined by an increment sample.

## 3.10.15 SPGRPCD

Species group code. A code assigned to each tree species in order to group them for reporting purposes. Codes and their associated names (see REF\_SPECIES\_GROUP.NAME) are shown in appendix E. Refer to appendix F for individual tree species and corresponding species group codes.

## 3.10.16 SITREE

Site index for the tree. Site index is calculated for dominant and co-dominant trees using one of several methods (see The method for determining the site index.). It is expressed as height in feet that the tree is expected to attain at a base or reference age (see SIBASE). Most commonly, site index is calculated using a family of curves that show site index as a function of total length and either breast-height age or total age. The height-intercept (or growth-intercept) method is commonly used for young trees or species that produce conspicuous annual branch whorls; using this method, site index is calculated with the height growth attained for a short period (usually 3 to 5 years) after the tree has reached breast height. Neither age nor total length determination are necessary when using the height-intercept method; therefore, one or more of those variables may be null for a site tree on which the height-intercept method was used.

## 3.10.17 SIBASE

Site index base age. The base age (sometimes called reference age), in years, of the site index curves used to derive site index. Base age is specific to a given family of site index curves, and is usually set close to the common rotation age or the age of culmination of mean annual increment for a species. The most commonly used base ages are 25, 50, 80, and 100 years. It is possible for a given species to have different sets of site index curves in different geographic regions, and each set of curves may use a different base age.

## 3.10.18 SUBP

Subplot number. (core optional) The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 3.10.19 AZIMUTH

Azimuth. (core optional) This attribute now available from the FIA Spatial Data Services (SDS) group by following the instructions provided at https://research.fs.usda.gov/programs/fia/sds.

## 3.10.20 DIST

Horizontal distance. (core optional) This attribute now available from the FIA Spatial Data Services (SDS) group by following the instructions provided at https://research.fs.usda.gov/programs/fia/sds.

## 3.10.21 METHOD

Site tree method code. The method for determining the site index.

## Codes: METHOD

|   Code | Description                                                                  |
|--------|------------------------------------------------------------------------------|
|      1 | Tree measurements (length, age, etc.) collected during this inventory.       |
|      2 | Tree measurements (length, age, etc.) collected during a previous inventory. |
|      3 | Site index estimated either in the field or office.                          |
|      4 | Site index determined by the height-intercept method during this inventory.  |

## 3.10.22 SITREE\_EST

Estimated site index for the tree. The estimated site index or the site index determined by the height-intercept method.

## 3.10.23 VALIDCD

Validity code. A code indicating if this site tree provided a valid result from the site index computation. Some trees collected by the field crew yield a negative value from the equation due to their age, height or diameter being outside the range of values for which the equation was developed. Computational results for trees that fail are not used to estimate the site index or site productivity class for the condition. If the site calculation for this tree was successful, this attribute is set to 1.

## Codes: VALIDCD

|   Code | Description                                     |
|--------|-------------------------------------------------|
|      0 | Tree failed in site index calculations.         |
|      1 | Tree was successful in site index calculations. |

## 3.10.24 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.10.25

CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.10.26 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.10.27 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.10.28 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.10.29 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 3.10.30 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 3.10.31 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 3.10.32 AGECHKCD\_RMRS

Radial growth and tree age check code, Rocky Mountain Research Station. A code indicating the method used to obtain radial growth and tree age. Only populated by certain FIA work units (SURVEY.RSCD = 22).

Note: Code 3 was added starting with PLOT.MANUAL = 6.0.

## Codes: AGECHKCD\_RMRS

|   Code | Description                                                                                                                                                                                                                  |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | • Age/radial growth measured directly from core. • Age/radial growth calculated from remeasurement data (same tree).                                                                                                         |
|      1 | • Age/radial growth was estimated due to rot. • Age/radial growth was estimated because rings were difficult to count (old suppressed trees). • Age was estimated because the increment bore could not reach to tree center. |
|      2 | • Age/radial growth was calculated from a similar remeasure tree (same species and diameter class). • Age/radial growth was based on a similar tree off the subplot.                                                         |
|      3 | • Age measured from a collected tree core (for cores collected and sent into the office for aging).                                                                                                                          |

## 3.10.33 AGE\_DETERMINATION\_METHOD\_PNWRS

Age determination method, Pacific Northwest Research Station. A code indicating how the site tree age was determined in the field. Age is extrapolated for trees that are too large to reach the pith with an increment borer. Only populated by certain FIA work units (SURVEY.RSCD = 27).

## Codes: AGE\_DETERMINATION\_METHOD\_PNWRS

|   Code | Description       |
|--------|-------------------|
|      0 | Bored age.        |
|      1 | Extrapolated age. |

## 3.10.34 CCLCD\_RMRS

Crown class code, Rocky Mountain Research Station. A code indicating the amount of sunlight received and the crown position of the tree within the canopy. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: CCLCD\_RMRS

|   Code | Description                                                                                                                                                                                                                                                                                      |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Open grown - Trees with crowns that have received full light from above and from all sides throughout all or most of their life, particularly during early development.                                                                                                                          |
|      2 | Dominant - Trees with crowns extending above the general level of the canopy and receiving full light from above and partly from the sides; larger than the average trees in the stand, and with crowns well developed, but possibly somewhat crowded on the sides.                              |
|      3 | Codominant - Trees with crowns forming part of the general level of the canopy cover and receiving full light from above, but comparatively little from the side. Usually with medium crowns more or less crowded on the sides.                                                                  |
|      4 | Intermediate - Trees shorter than those in the preceding two classes, with crowns either below or extending into the canopy formed by the dominant and codominant trees, receiving little direct light from above, and none from the sides; usually with small crowns very crowded on the sides. |
|      5 | Overtopped - Trees with crowns entirely below the general canopy level and receiving no direct light either from above or the sides.                                                                                                                                                             |

## 3.10.35 DAMAGE\_AGENT\_CD1\_RMRS

Damage agent code 1, Rocky Mountain Research Station. A code indicating the first damage agent recorded by the field crew when inspecting the tree from bottom to top (roots, bole, branches, foliage). Up to three damage agents can be recorded per tree (DAMAGE\_AGENT\_CD1\_RMRS, DAMAGE\_AGENT\_CD2\_RMRS, and

DAMAGE\_AGENT\_CD3\_RMRS). Damage agents are not necessarily recorded in order of severity.

The codes used for damage agents come from the January 2012 Pest Trend Impact Plot System (PTIPS) list from the Forest Health Assessment &amp; Applied Sciences Team (FHAAST) that has been modified to meet FIA's needs.

See TREE.DAMAGE\_AGENT\_CD1 for general agent codes. See appendix H for the complete list of codes. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 3.10.36 DAMAGE\_AGENT\_CD2\_RMRS

Damage agent code 2, Rocky Mountain Research Station. See

DAMAGE\_AGENT\_CD1\_RMRS. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 3.10.37 DAMAGE\_AGENT\_CD3\_RMRS

DAMAGE\_AGENT\_CD1\_RMRS. Only populated by certain FIA work units (SURVEY.RSCD =

Damage agent code 3, Rocky Mountain Research Station. See 22).

## 3.10.38 SIBASE\_AGE\_PNWRS

Site index equation base age, Pacific Northwest Research Station. A code indicating the range that is used to define the acceptable site index values. Only populated by certain FIA work units (SURVEY.RSCD = 26, 27).

## Codes: SIBASE\_AGE\_PNWRS

|   Code | Description                                        |
|--------|----------------------------------------------------|
|     50 | 50 year base age, site index should be within 20.  |
|    100 | 100 year base age, site index should be within 30. |

## 3.10.39 SITETRCD\_RMRS

Site tree code, Rocky Mountain Research Station. A code indicating if the site tree is considered to be suitable or unsuitable. When suitable site trees are not available, the field crew may select an unsuitable site tree. Site trees are a measure of site productivity expressed by the height to age relationship of dominant and codominant trees. Site trees are not collected for woodland conditions. The requirements for classification are as follows:

## Suitable site trees:

- · Live sound tree.
- · 5.0 inches in diameter (at breast height) or larger.
- · Open grown, dominant, or codominant throughout most of its life.
- · Minimum of 35 years (d.b.h. age) for softwoods or minimum of 45 years (d.b.h. age) for hardwoods.
- · Under rotation age (80 years for aspen and paper birch, 120 years for all other timber species).
- · Undamaged top (not dead or broken).
- · Vigorous, having an uncompacted crown ratio of at least 50 percent, if possible, and having the best height/age ratio of all the trees on the site.

## Unsuitable site trees:

- · Relicts.
- · Over rotation age but less than 200 years (d.b.h. age).
- · Rough trees (i.e., not growing stock).

Only populated by certain FIA work units (SURVEY.RSCD = 22).