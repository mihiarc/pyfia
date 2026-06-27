Updates

User Guide Updates (revision: 12.2024)

## Chapter 1: Overview

## Chapter Contents:

|   Section | Heading                                                                                                                                                                                                                             |
|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|       1.1 | Introduction: • Purpose of This Guide • The FIA Program • The FIA Database                                                                                                                                                          |
|       1.2 | FIA Sampling and Estimation Procedures: • Sampling and Stratification Methodology • Plot Location • Plot Design, Condition Delineation, and Types of Data Attributes • Types of Attributes • Expansion Factors • Accuracy Standards |
|       1.3 | Database Structure: • Table Descriptions • Keys Presented with the Tables • Oracle Data Types                                                                                                                                       |

## 1.1 Introduction

## 1.1.1 Purpose of This Guide

This guide describes the database tables and attributes (columns) contained within the Forest Inventory and Analysis database (FIADB). The data within the FIADB are for the "Nationwide Forest Inventory (NFI)" conducted by the Forest Inventory and Analysis (FIA) Program. The term "NFI" refers to what was historically called "Phase 2" (P2) under FIA's three-phase sampling scheme (see "FIA Sampling and Estimation Procedures" for details). NFI is a national network of permanent plots that are remeasured every 5-10 years depending on location. Land use information is collected on all plots, with additional site and tree (live and standing dead) data collected on plot areas having forest land use present. Additionally, information about down woody material, soils, and understory vegetation is collected on a subset of plots. Note: The title for this user guide was revised to remove the "Phase 2" term. However, within this document, the use and meanings for FIA's three-phase sampling scheme terms "Phase 1" (P1), "Phase 2" (P2), and "Phase 3" (P3) remain the same.

Although this user guide is used widely within the FIA program, a substantial part, if not the majority, of the intended audience includes those outside FIA who are interested in using FIA data for their own analyses. Awareness of the potential uses of FIA data by users outside the FIA community is growing, and the data become increasingly useful as additional attributes are collected. However, as is the case with any data source, it is incumbent upon the user to understand not only the data definitions and acquisition methods, but also the context in which the data were collected. This guide is intended to help current and potential users understand the necessary details of the FIADB.

This guide has eleven chapters. The remainder of chapter 1 includes general introductions to the FIA program and the FIA database, including brief histories of both. It provides a convenient overview for those who have an interest in using FIA data, but have not yet become familiar with the FIA program. Chapter 1 also provides descriptions of FIA sampling methods, including plot location and design, data measurement and computation, and general estimation procedures. Chapters 2 through 11 describe the tables that comprise the database, the attributes stored in each table, and the linkages between tables. Descriptions of the attributes, their data format, valid values, and other important details are given, but the appropriate field guides should be consulted for exact specifications regarding data collection methods. Users with a good understanding of the database tables (chapters 2 through 11) and fundamental database management skills should be able to conduct a wide range of analyses. The supplemental document Forest Inventory and Analysis Database: Population Estimation User Guide explains the standard methods used to compile population-level estimates from FIADB, and applies the estimation procedures documented by Bechtold and Patterson (2005). These procedures are based on adoption of the annual inventory system and the mapped plot design, and constitute a major change when compared to previous compilation procedures. However, the compilation procedures should allow more flexible analyses, especially as additional panels are completed under the annual inventory system.

There are several conventions used in this guide. The names of attributes (i.e., columns within tables) and table names appear in capital letters (e.g., PLOT table). Some attribute names appear in two or more tables. In most cases, such as the State code (STATECD), the attribute has the same definition in all tables. However, there are situations where attributes with the same name are defined differently in each table. One such example is

the VALUE attribute in the REF\_FOREST\_TYPE table, which is used to identify the forest type and refers to appendix D. However, the VALUE attribute in the REF\_UNIT table is used to indicate the FIA survey unit identification number from appendix B. In most cases, such as in the table descriptions in chapters 2 through 11, the attribute name will be used alone and the affiliation with a particular table is implied by the context. In cases where an attribute name has a different meaning in two or more tables, a compound naming convention, using the table name followed by the attribute name, will be used. In the VALUE attribute example, the name REF\_FOREST\_TYPE.VALUE refers to the VALUE attribute in the REF\_FOREST\_TYPE table, while REF\_UNIT.VALUE refers to the VALUE attribute in the REF\_UNIT table.

## 1.1.2 The FIA Program

The mission of FIA is to determine the extent, condition, volume, growth, and use of trees on the Nation's forest land. FIA is the only program that collects, publishes, and analyzes data from all ownerships of forest land in the United States (Smith 2002). Throughout the long history of the program, inventories have been conducted by a number of geographically dispersed FIA work units. Currently, the national FIA program is implemented by four regionally distributed work units that are coordinated by a National Office in Washington, DC (see figure 1-1). The four FIA work units are named by the research station in which they reside. Station abbreviations are used within this document and they are defined as Pacific Northwest Research Station (PNWRS), Northern Research Station (NRS), Rocky Mountain Research Station (RMRS), and Southern Research Station (SRS). NRS was formed from the merger of North Central Research Station (NCRS) and Northeastern Research Station (NERS). Some data items still retain these designations.

Figure 1-1: Boundaries of the four regionally distributed FIA work units and locations of program offices.

<!-- image -->

Starting in 1929, FIA accomplished its mission by conducting periodic forest inventories on a State-by-State basis. With the completion of Arizona, New Mexico, and Nevada in 1962, all 48 coterminous States had at least one periodic inventory (Van Hooser and others 1993). Repeat intervals for inventorying individual States have varied widely. By the late 1990s, most States had been inventoried more than once under the periodic inventory system; however, not all periodic data are available in electronic form (appendix J lists all periodic data available in the FIADB and the year in which annual inventory began).

With the passage of the 1998 Farm Bill, the FIA program was required to move from a periodic inventory to an annualized system, with a portion of all plots within a State measured each year (Gillespie 1999). Starting in 1999, States were phased into the annual inventory system (appendix J). As an exception, Hawaii uses a 10-year remeasurement cycle, but the entire State is inventoried over a shorter time frame (e.g., 3-5 years) before it is inventoried again.Although the 1998 Farm Bill specified that 20 percent of the plots within each State would be visited annually, funding limitations have resulted in the actual portion of plots measured annually ranging between 10 and 20 percent, depending on the State.

Periodic and annual data are analyzed to produce reports at State, regional, and national levels. In addition to published reports, data are made available to the public for those who are interested in conducting their own analyses. Downloadable data, available online at FIA Data and Tools (https://research.fs.usda.gov/programs/fia#data-and-tools), follow the format described in this document. Also available at this site are tools to make

population estimates. The web-based EVALIDator tool and the DATIM tool (Design and Analysis Toolkit for Inventory and Monitoring) provide interactive access to the FIADB.

## 1.1.3 The FIA Database

The Forest Inventory and Analysis Database (FIADB) was developed to provide users with data in a consistent format, spanning all States and inventories. The first version of FIADB replaced two FIA regional databases; the Eastern States (Eastwide database) documented by Hansen and others (1992), and Western States (Westwide database) documented by Woudenberg and Farrenkopf (1995). A new national plot design (see section 1.2) provided the impetus for replacing these two databases, and FIA work units adopted the new design in all State inventories initiated after 1998. The FIADB table structure is currently derived from the National Information Management System (NIMS), which was designed to process and store annual inventory data. A number of changes in the FIADB structure have been made to accommodate the data processing and storage requirements of NIMS. As a result, data from periodic inventories are stored in a format consistent with annual inventory data.

FIADB files are available for periodic inventory data collected as early as 1968 (see appendix J). A wide variety of plot designs and regionally defined attributes were used in periodic inventories, often differing by State. Because of this, some data attributes may not be populated or certain data may have been collected or computed differently. During some periodic inventories, ground plot data were collected on timberland only. FIA defines timberland as nonreserved forest land capable of producing at least 20 cubic feet of wood volume per acre per year (the definition of forest land is in the COND\_STATUS\_CD description in the COND table.) Thus, low productivity forest land, reserved (areas reserved from timber harvesting), and nonforested areas usually were not ground sampled. To account for the total area of a State, "place holder" plots were created to represent these nonsampled areas, which are identified by plot design code 999 in FIADB (PLOT.DESIGNCD = 999). For these plots, many attributes that are normally populated for forested plots will be blank (null). Users should be aware that while place holder plots account for the area of nonsampled forest land, they do not account for the corresponding forest attributes (such as volume, growth, or mortality) that may exist in those areas.

Annual inventories, initiated sometime after 1999 depending on the State, use a nationally standardized plot design and common data collection procedures resulting in greater consistency among FIA work units than earlier inventories. However, as part of a continuing effort to improve the inventory, some changes in methodology and attribute definitions have been implemented after the new design was put into practice. Beginning in 1998, FIA started using a National Field Guide referenced as Field Guide 1.0. The database contains an attribute labeled MANUAL that stores the version number of the field guide under which the data were collected. When both the plot design is coded as being the national design (PLOT.DESIGNCD = 1) and the field guide is coded with a number greater than or equal to 1, certain attributes are defined as being core while others are allowed to be core optional. Core attributes must be collected by every FIA work unit, using the same definition and set of codes. In contrast, collection of core optional attributes are decided upon by individual FIA work units, using the same national protocol, predefined definition, and set of codes. Many attributes, regardless of whether or not they are core or core optional, are only populated for forested conditions, and are blank (null) for other conditions (such as nonforest or water). Attributes described in chapters 2 through 9 are noted if they are core optional.

Users who wish to analyze data using aggregations of multiple State inventories or multiple inventories within States should become familiar with changes in methodology and attribute definitions (see sections 1.2 and 1.3). For each attribute in the current version of FIADB, an effort has been made to provide the current definition of the attribute, as well as any variations in definition that may have been used among various FIA work units. In other words, although inventory data have been made available in a common data format, users should be aware of differences that might affect their analyses.

## 1.2 FIA Sampling and Estimation Procedures

To use the FIADB effectively, users should acquire a basic understanding of FIA sampling and estimation procedures. Generally described, FIA uses what may be characterized as a three-phase sampling scheme. Phase 1 (P1) is used for stratification, while Phase 2 (P2) consists of plots that are visited or photo-interpreted. A subset of Phase 2 plots were designated as Phase 3 (P3) plots (formerly known as Forest Health Monitoring [FHM] plots) where additional health indicator attributes were collected. Phase 3 was no longer being completed as a separate inventory as of 2012. The FIA program collects some forest health indicators (e.g., DWM, vegetation profile, invasives, soils, lichens) on a portion of the P2 plots. Damages and crown attributes are now collected on all P2 plots. Ozone damage is no longer collected.

## 1.2.1 Sampling and Stratification Methodology

## Remote Sensing (P1)

The basic level of inventory in the FIA program is the State, which begins with the interpretation of a remotely sensed sample, referred to as Phase 1 (P1). The intent of P1 is to classify the land into various classes for the purpose of developing meaningful strata. A stratum is a group of plots that have the same or similar classifications based on remote-sensing imagery. Stratification is a statistical technique used by FIA to aggregate Phase 2 ground samples into groups to reduce variance when stratified estimation methods are used. The total area of the estimation unit is assumed to be known.

Each Phase 2 ground plot is assigned to a stratum and the weight of the stratum is based on the proportion of the stratum within the estimation unit. Estimates of population totals are then based on the sum of the product of the known total area, the stratum weight, and the mean of the plot-level attribute of interest for each stratum. The expansion factor for each stratum within the estimation unit is the product of the known total area and the stratum weight divided by the number of Phase 2 plots in the stratum.

Selection criteria for remote sensing classes and computation of area expansion factors differ from State to State. Users interested in the details of how these expansion factors are assigned to ground plots for a particular State should contact the appropriate FIA work unit (table 1-1).

## Ground Sampling (P2)

FIA ground plots, or Phase 2 plots, are designed to cover a 1-acre sample area; however, not all trees on the acre are measured. Ground plots may be new plots that have never been measured, or re-measurement plots that were measured during one or more previous inventories. Recent inventories use a nationally standard, fixed-radius plot layout

for sample tree selection (see figure 1-2). Various arrangements of fixed-radius and variable-radius (prism) subplots were used to select sample trees in older inventories.

Figure 1-2: The FIA mapped plot design. Subplot 1 is the center of the cluster with subplots 2, 3, and 4 located 120 feet away at azimuths of 360°, 120°, and 240°, respectively.

<!-- image -->

## 1.2.2 Plot Location

The FIADB includes coordinates for every plot location in the database, whether it is forested or not, but these are not the precise locations of the plot centers. In an amendment to the Food Security Act of 1985 (reference 7 USC 2276 § 1770), Congress directed FIA to ensure the privacy of private landowners. Exact plot coordinates could be used in conjunction with other publicly available data to link plot data to specific landowners, in violation of requirements set by Congress. In addition to the issue of private landowner privacy, the FIA program had concerns about plot integrity and vandalism of plot locations on public lands. A revised policy has been implemented and methods for making approximate coordinates available for all plots have been developed. These methods are collectively known as "fuzzing and swapping" (Lister and others 2005).

In the past, FIA provided approximate coordinates for its periodic data in the FIADB. These coordinates were within 1.0 mile of the exact plot location (this is called fuzzing). However, because some private individuals own extensive amounts of land in certain counties, the data could still be linked to these owners. In order to maintain the privacy requirements specified in the amendments to the Food Security Act of 1985, up to 20 percent of the private plot coordinates are swapped with another similar private plot within the same county (this is called swapping). This method creates sufficient uncertainty at the scale of the individual landowner such that privacy requirements are met. It also ensures that county summaries and any breakdowns by categories, such as ownership class, will be the same as when using the true plot locations. This is because only the coordinates of the plot are swapped - all the other plot characteristics remain the

same. The only difference will occur when users want to subdivide a county using a polygon. Even then, results will be similar because swapped plots are chosen to be similar based on attributes such as forest type, stand-size class, latitude, and longitude (each FIA work unit has chosen its own attributes for defining similarity).

For plot data collected under the current plot design, plot numbers are reassigned to sever the link to other coordinates stored in the FIADB prior to the change in the law. Private plots are also swapped using the method described above; remeasured plots are swapped independent of the periodic data. All plot coordinates are fuzzed, but less than before within 0.5 mile for most plots and up to 1.0 mile on a small subset of them. This was done to make it difficult to locate the plot on the ground, while maintaining a good correlation between the plot data and map-based characteristics.

For most user applications, such as woodbasket analyses and estimates of other large areas, fuzzed and swapped coordinates provide a sufficient level of accuracy. However, some FIA customers require more accurate of plot locations in order to perform analyses by user-defined polygons and for relating FIA plot data to other map-based information, such as soils maps and satellite imagery. In order to accommodate this need, FIA provides Spatial Data Services that allow most of the desired analyses while meeting privacy requirements. The possibilities and limitations for these types of analyses are case-specific, so interested users should contact their local FIA work unit for more information.

## 1.2.3 Plot Design, Condition Delineation, and Types of Data Attributes

## Plot Designs

The current national standard FIA plot design was originally developed for the Forest Health Monitoring program (Scott and others 1993). It was adopted by FIA in the mid-1990s and used for the last few periodic inventories and all annual inventories. The standard plot consists of four 24.0-foot radius subplots (approximately 0.0415 or 1/24 acre) (see figure 1-2), on which trees 5.0 inches d.b.h./d.r.c. are measured. Within each of these subplots is nested a 6.8-foot radius microplot (approximately 1/300th acre) on which trees &lt;5.0 inches d.b.h./d.r.c. are measured. A core optional variant of the standard design includes four "macroplots," each with a radius of 58.9 feet (approximately 1/4 acre) that originate at the centers of the 24.0-foot radius subplots. Breakpoint diameters between the 24-foot radius subplots and the macroplots vary and are specified in the macroplot breakpoint diameter attribute (PLOT.MACRO\_BREAKPOINT\_DIA).

Prior to adoption of the current plot design, a wide variety of plot designs were used. Periodic inventories might include a mixture of designs, based on forest type, ownership, or time of plot measurement. In addition, similar plot designs (e.g., 20 BAF variable-radius plots) might have been used with different minimum diameter specifications (e.g., 1-inch versus 5-inch). Details on these designs are included in appendix G (plot design codes).

## Conditions

An important distinguishing feature between the current plot design and previous designs is that different conditions are "mapped" on the current design (see figure 1-3). In older plot designs, adjustments were made to the location of the plot center or the subplots were rearranged such that the entire plot sampled a single condition. In the new design, the plot location and orientation remains fixed, but boundaries between conditions are mapped and recorded. Conditions are defined by changes in land use or changes in

vegetation that occur along more-or-less distinct boundaries. Reserved status, owner group, forest type, stand-size class, regeneration status, and stand density are used to define forest conditions. For example, the subplots may cover forest and nonforest areas, or it may cover a single forested area that can be partitioned into two or more distinct stands. Although mapping is used to separate forest and nonforest conditions, different nonforest conditions occurring on a plot are not mapped during initial plot establishment. Each condition occurring on the plot is assigned a condition proportion, and all conditions on a plot add up to 1.0. For plot designs other than the mapped design, condition proportion is always equal to 1.0 in FIADB.

Figure 1-3: The FIA mapped plot design. Subplot 1 is the center of the cluster with subplots 2, 3, and 4 located 120 feet away at azimuths of 360°, 120°, and 240°, respectively. When a plot straddles two or more conditions, the plot area is divided by condition.

<!-- image -->

## 1.2.4 Types of Attributes

## Measured, Assigned, and Computed Attributes

In addition to attributes that are collected in the field, FIADB includes attributes that are populated in the office. Examples of field attributes include tree diameter and height, and slope and aspect of the plot and subplot. Attributes that are populated in the office include assigned attributes, such as county and owner group codes, or computed attributes, such as tree and area expansion factors, and tree volumes.

For measured attributes, this document provides only basic information on the methodology used in the field. The authoritative source for methodology is the Forest Inventory and Analysis National Core Field Guide used during the inventory in which the data were collected. The MANUAL attribute in the PLOT table documents the version number where data collection protocols can be found.

Values of attributes that are assigned in the office are determined in several ways, depending on the attribute. For example, ownership may be determined using geographic

data or local government records. Other attributes, such as Congressional District and Ecological Subsection are assigned values based on data management needs.

Some computed attributes in the database are derived using other attributes in the database. Ordinarily, such attributes would not be included in a database table because they could be computed using the supplied attributes. However, some data compilation routines are complex or vary within or among FIA work units, so these computed attributes are populated for the convenience of database users.

One example of a computed attribute is site index, which is computed at the condition level. Site index is generally a function of height and age, although other attributes may be used in conjunction. In addition, several different site index equations may be available for a species within its range. Height and age data are included in the TREE table, but only certain trees (see SITETREE table) are included in the site index attribute that is reported for the condition. As a result, it would be time-consuming for users to replicate the process required to calculate site index at the condition level. For convenience, the condition (COND) table includes site index (SICOND), the species for which it is calculated (SISP), and the site index base age (SIBASE).

In most cases computed attributes should be sufficient for users' needs, because the equations and algorithms used to compute them have been determined by the FIA program to be the best available for the plot location. However, for most computed attributes the relevant tree- and plot-level attributes used to compute them are included in the database, so users may do their own calculations if desired.

## Regional Attributes

A number of regionally specific attributes are available in FIADB. These regional attributes are identified by FIA work unit, both in the table structure description (e.g., the attribute is named with an extension such as NERS) and in the attribute description (e.g., the attribute description text contains the phrase "Only populated by …" . For specific questions about the data from a particular FIA work unit, please contact the individuals listed in table 1-1. More information on attribute types is included in chapters 2 through 9.

Table 1-1: Contacts at individual FIA work units.

| FIA Work Unit         |   RSCD | States                                             | Database Contact   | Phone        | Analyst Contact   | Phone              |
|-----------------------|--------|----------------------------------------------------|--------------------|--------------|-------------------|--------------------|
| Rocky Mountain (RMRS) |     22 | AZ, CO, ID, MT, NV, NM, UT, WY                     | Andrea DiTommaso   | 801-625-5397 | Kristen Pelz      | 505-216-8710       |
| North Central (NCRS)* |     23 | IL, IN, IA, KS, MI, MN, MO, NE, ND, SD,WI          | Elizabeth Burrill  | 603-868-7675 | Scott Pugh        | 906-482-6303 x1317 |
| Northeastern (NERS)*  |     24 | CT, DE, ME, MD, MA, NH, NJ, NY, OH, PA, RI, VT, WV | Elizabeth Burrill  | 603-868-7675 | Randy Morin       | 215-233-6562       |

| FIA Work Unit             | RSCD   | States                                                     | Database Contact   | Phone        | Analyst Contact   | Phone        |
|---------------------------|--------|------------------------------------------------------------|--------------------|--------------|-------------------|--------------|
| Pacific Northwest (PNWRS) | 26,27  | AK, CA, HI, OR, WA, AS, FM, GU, MH, MP, PW                 | Vicki Johnson      | 907-743-9410 | Glenn Christensen | 503-808-2064 |
| Southern (SRS)            | 33     | AL, AR, FL, GA, KY, LA, MS, NC, OK, SC, TN, TX, VA, PR, VI | Chad Keyser        | 865-862-2095 | Kerry Dooley      | 865-862-2098 |

- * The North Central Research Station (NCRS) and the Northeastern Research Station (NERS) have merged to become one research station, the Northern Research Station. The former regional designations are kept to accommodate the data.

## 1.2.5 Expansion Factors

## Tree Expansion Factors

The expansion factor(s) used to scale each tree on a plot to a per-acre basis is dependent on the plot design. The examples here are for fixed-radius plots (see appendix G for all plot designs.) For fixed-plot designs, scaling is straightforward, with the number of trees per acre (TPA) represented by one tree equal to the inverse of the plot area in acres. The general formula is shown by equation [1]:

- [1] TPA = 1/(N*A)

Where N is the number of subplots, and A is the area of each subplot.

For example, the TPA expansion factor of each tree  5.0 inches d.b.h./d.r.c. occurring on the current plot design would be calculated using equation [2]:

TPA expansion factors for standard subplot, microplot and macroplot designs

- [2] TPA per 24-foot fixed-radius subplot

Radius of a subplot = 24 feet

Area of subplot = pi*radius 2

Area of subplot = 3.141592654*24 2

Area of subplot = 1809.557368 square feet

Acres in a subplot = area of subplot in square feet / (43560 square feet /acre)

Acres in a subplot = 1809.557368 square feet / (43560 square feet /acre)

Acres in a subplot = 0.04154172 acres per subplot

Acres in a plot = 4 subplots per plot

Acres per plot = 4* 0.04154172

= 0.166166884 acres per plot

TPA = 1 / (0.166166884) = 6.018046

The TPA expansion factor of each sapling 1.0-4.9 inches d.b.h./d.r.c. occurring on the current microplot design would be calculated using equation [3]:

- [3] TPA per 6.8-foot fixed-radius microplot

Radius of a microplot = 6.8 feet

Area of microplot = pi*radius 2

Area of microplot = 3.141592654*6.8 2

Area of microplot = 145.2672443 square feet

Acres in a microplot = area of microplot in square feet /

(43560 square feet /acre)

Acres in a microplot = 145.2672443 square feet / (43560 square feet /acre)

Acres in a microplot = 0.003334877 acres per subplot

Acres in a plot = 4 microplots per plot

Acres per plot = 4* 0.003334877

= 0.013339508 acres per plot

TPA = 1 / (0.013339508) = 74.965282

The TPA expansion factor of each tree  5.0 inches d.b.h./d.r.c. occurring on the current macroplot design would be calculated using equation [4]:

[4] TPA per 58.9-foot fixed-radius macroplot

Radius of a macroplot = 58.9 feet

Area of macroplot = pi*radius 2

Area of macroplot = 3.141592654*58.9 2

Area of macroplot = 10898.84465 square feet

Acres in a macroplot = area of macroplot in square feet /

(43560 square feet /acre)

Acres in a macroplot = 10898.84465 square feet / (43560 square feet /acre)

Acres in a macroplot = 0.250203045 acres per subplot

Acres in a plot = 4 macroplots per plot

Acres per plot = 4* 0.250203045

= 1.000812181 acres per plot

TPA = 1 / (1.000812181) = 0.999188

This expansion factor can be found in the TPA\_UNADJ attribute in the TREE table (see chapter 3) for plots measured with the annual plot design.

In variable-radius plot designs, the per-acre expansion factor is determined by the diameter of the tree, the basal area factor (BAF), and the number of points used in the plot design. The general formula is shown by equation [5]:

[5] TPA = (BAF / 0.005454*DIA 2 )/N Where BAF is the variable-radius basal area factor in square feet, DIA is diameter of the tally tree in inches, and N is the number of points in the plot design.

For example, if an 11.5-inch tree is tallied using a 10 BAF prism on a variable-radius design plot that uses five points, the calculation is:

[6] TPA = (10 / 0.005454*11.5 2 )/5 = 2.773

A 5.2-inch tree will have a greater expansion factor:

[7] TPA = (10 / 0.005454*5.2 2 )/5 = 13.562

Although it is not necessary to calculate expansion factors for different plot designs because they are stored in TPA\_UNADJ, information on plot design can be found by using the code from the DESIGNCD attribute in the PLOT table to look up the plot design specifications in appendix G.

## Plot Area Expansion Factors

Some previous versions of FIADB have included area expansion factors in the PLOT table that were used to scale plot-level data to population-level estimates (see EXPCURR and related attributes in Miles and others 2001). In this version of FIADB, area expansion factors have been removed from the PLOT table. Instead, there is one area expansion factor (EXPNS) stored in the POP\_STRATUM table. This change is needed because of the way annual inventory data are compiled. Under the annual inventory system, new plots are added each year. Adjustment factors that are used to compensate for denied access, inaccessible, and other reasons for not sampling may differ each time new data replaces older data. Both the number of acres each plot represents and the adjustments for the proportion of plots not sampled may change each year. In order to allow users to obtain population estimates for any grouping of data, an adjustment factor has been calculated and stored for each set of data being compiled. There is a separate adjustment factor for each fixed plot size: microplot, subplot, and macroplot. These attributes are also stored in the POP\_STRATUM table. Each time the data are stratified differently, the adjustments and expansion factor may change. Therefore, FIA provides a different expansion factor every time the data are restratified.

FIA has chosen the term 'evaluation' to describe this process of storing different stratifications of data either for an individual set of data or for the changing sets of data through time. Each aggregation of data is given an evaluation identifier (EVALID). The user can select population estimates for the most current set of data or for previous sets of data. In addition to being able to calculate population estimates, users can now calculate sampling error information because FIA is storing all of the Phase 1 information used for the stratification. That information is stored for each estimation unit, which is usually a geographic subset of the State (see the POP\_ESTN\_UNIT table). For more information about evaluations and calculation of area expansion factors, see The Forest Inventory and Analysis Database: Population Estimation User Guide.

## 1.2.6 Accuracy Standards

Forest inventory plans are designed to meet sampling error standards for area, volume, growth, and removals provided in the Forest Service directive (FSH 4809.11) known as the Forest Survey Handbook (U.S. Department of Agriculture 2008). These standards, along with other guidelines, are aimed at obtaining comprehensive and comparable information on timber resources for all parts of the country. FIA inventories are commonly designed to meet the specified sampling errors at the State level at the 67 percent confidence limit (one standard error). The Forest Survey Handbook mandates that the sampling error for area cannot exceed 3 percent error per 1 million acres of timberland. A 5 percent (Eastern United States) or 10 percent (Western United States) error per 1 billion cubic feet of growing-stock trees on timberland is applied to volume, removals, and net annual growth. Unlike the mandated sampling error for area, sampling errors for volume, removals, and growth are only targets.

FIA inventories are extensive inventories that provide reliable estimates for large areas. As data are subdivided into smaller and smaller areas, such as a geographic unit or a county, the sampling errors increase and the reliability of the estimates goes down.

- · A State with 5 million acres of timberland would have a maximum allowable sampling error of 1.3 percent (3% x (1,000,000) 0.5  / (5,000,000) 0.5 ).
- · A geographic unit within that State with 1 million acres of timberland would have a 3.0 percent maximum allowable sampling error (3% x (1,000,000) 0.5  / (1,000,000) 0.5 ).
- · A county within that State with 100 thousand acres would have a 9.5 percent maximum allowable sampling error (3% x (1,000,000) 0.5  / (100,000) 0.5 ) at the 67 percent confidence level.

The greater allowance for sampling error in smaller areas reflects the decrease in sample size as estimation area decreases.

Estimation procedures and the calculation of confidence intervals for typical FIA tables are discussed in The Forest Inventory and Analysis Database: Population Estimation User Guide. Additional information on estimation and confidence intervals can be found in Bechtold and Patterson (2005).

## 1.3 Database Structure

This section provides information about the database tables, including detailed descriptions of all attributes within the tables. Each column or attribute in a table is listed with its unabbreviated name, followed by a description of the attribute. Attributes that are coded include a list of the codes and their meanings. The "Index of Column Names" contains an alphabetized list of all of the column names (attributes) in the database tables included within this user guide. Some overview information is presented below, followed by a section with complete information about all tables and attributes.

## 1.3.1 Table Descriptions

Refer to the "Index of Tables" for a list of FIADB database and reference tables. This index also includes a brief description for each table.

Figure 1-4 shows an Entity Relationship Diagram (ERD) for several tables in the FIADB. This diagram displays examples of how some columns in a table can be used to link to a matching column in another table (see "Foreign key" for more information). The REF\_POP\_ATTRIBUTE table can be linked to the POP\_EVAL\_ATTRIBUTE table using the ATTRIBUTE\_NBR (attribute number) column.

Figure 1-4: Entity Relationship Diagram (ERD) for several tables in the FIADB. Note: This ERD does not display the full set of columns that are available for each table.

<!-- image -->

## 1.3.2 Keys Presented with the Tables

Each summarized table in chapters 2 through 11 has a list of keys just below the bottom of the table. These keys are used to join data from different tables. The following provides a general definition of each kind of key.

## Primary key

A single column in a table whose values uniquely identify each row in an Oracle table. The primary key in each FIADB table is the CN column.

The name of the primary key for each table is listed in the table description. It follows the nomenclature of 'TABLEABBREVIATION'\_PK. The table abbreviations are as follows:

Note: The following list of entities includes a combination of Oracle tables, views, and synonyms. However, for this user guide, all of these entities are simply referred to as database "tables."

| Table name              | Table abbreviation   |
|-------------------------|----------------------|
| SURVEY                  | SRV                  |
| PROJECT                 | PRJ                  |
| COUNTY                  | CTY                  |
| PLOT                    | PLT                  |
| COND                    | CND                  |
| SUBPLOT                 | SBP                  |
| SUBP_COND               | SCD                  |
| SUBP_COND_CHNG_MTRX     | CMX                  |
| TREE                    | TRE                  |
| TREE_WOODLAND_STEMS     | WOODS                |
| TREE_GRM_COMPONENT      | TRE_GRM_CMP          |
| TREE_GRM_THRESHOLD      | TRE_GRM_THRSHLD      |
| TREE_GRM_MIDPT          | TRE_GRM_MIDPT        |
| TREE_GRM_BEGIN          | TRE_GRM_BGN          |
| TREE_GRM_ESTN           | TGE                  |
| BEGINEND                | BE                   |
| SEEDLING                | SDL                  |
| SITETREE                | SIT                  |
| INVASIVE_SUBPLOT_SPP    | ISS                  |
| P2VEG_SUBPLOT_SPP       | P2VSSP               |
| P2VEG_SUBP_STRUCTURE    | P2VSS                |
| DWM_VISIT               | DVT                  |
| DWM_COARSE_WOODY_DEBRIS | DCW                  |
| DWM_DUFF_LITTER_FUEL    | DDL                  |
| DWM_FINE_WOODY_DEBRIS   | DFW                  |

| Table name             | Table abbreviation   |
|------------------------|----------------------|
| DWM_MICROPLOT_FUEL     | DMF                  |
| DWM_RESIDUAL_PILE      | DRP                  |
| DWM_TRANSECT_SEGMENT   | DTS                  |
| COND_DWM_CALC          | CDC                  |
| PLOT_REGEN             | PLTREGEN             |
| SUBPLOT_REGEN          | SBPREGEN             |
| SEEDLING_REGEN         | SDLREGEN             |
| GRND_CVR               | GRND_CVR             |
| GRND_LYR_FNCTL_GRP     | FGLFGP               |
| GRND_LYR_MICROQUAD     | FGLMP                |
| SUBP_SOIL_SAMPLE_LOC   | SSSL                 |
| SUBP_SOIL_SAMPLE_LAYER | SSSLYR               |
| POP_ESTN_UNIT          | PEU                  |
| POP_EVAL               | PEV                  |
| POP_EVAL_ATTRIBUTE     | PEA                  |
| POP_EVAL_GRP           | PEG                  |
| POP_EVAL_TYP           | PET                  |
| POP_PLOT_STRATUM_ASSGN | PPSA                 |
| POP_STRATUM            | PSM                  |
| PLOTGEOM               | PLOTGEOM             |
| PLOTSNAP               | PLOTSNP              |
| REF_POP_ATTRIBUTE      | PAE                  |
| REF_POP_EVAL_TYP_DESCR | PED                  |
| REF_FOREST_TYPE        | RFT                  |
| REF_FOREST_TYPE_GROUP  | FTGP                 |
| REF_SPECIES            | RS                   |
| REF_PLANT_DICTIONARY   | RPD                  |
| REF_SPECIES_GROUP      | RSG                  |
| REF_INVASIVE_SPECIES   | RIS                  |
| REF_HABTYP_DESCRIPTION | RHN                  |
| REF_HABTYP_PUBLICATION | RPN                  |
| REF_CITATION           | CIT                  |
| REF_FIADB_VERSION      | RFN                  |
| REF_STATE_ELEV         | RSE                  |
| REF_UNIT               | UNT                  |
| REF_RESEARCH_STATION   | RES                  |
| REF_NVCS_LEVEL_1_CODES | RNVCSHS1             |

| Table name                   | Table abbreviation   |
|------------------------------|----------------------|
| REF_NVCS_LEVEL_2_CODES       | RNVCSHS2             |
| REF_NVCS_LEVEL_3_CODES       | RNVCSHS3             |
| REF_NVCS_LEVEL_4_CODES       | RNVCSHS4             |
| REF_NVCS_LEVEL_5_CODES       | RNVCSHS5             |
| REF_NVCS_LEVEL_6_CODES       | RNVCSHS6             |
| REF_NVCS_LEVEL_7_CODES       | RNVCSHS7             |
| REF_NVCS_LEVEL_8_CODES       | RNVCSHS8             |
| REF_DAMAGE_AGENT             | DA                   |
| REF_DAMAGE_AGENT_GROUP       | DAG                  |
| REF_FVS_VAR_NAME             | RFVN                 |
| REF_FVS_LOC_NAME             | RFLN                 |
| REF_OWNGRPCD                 | REF_OWNGRPCD         |
| REF_DIFFERENCE_TEST_PER_ACRE | RDTPA                |
| REF_DIFFERENCE_TEST_TOTALS   | RDTT                 |
| REF_SIEQN                    | REF_SIEQN            |
| REF_GRM_TYPE                 | RGT                  |
| REF_INTL_TO_DOYLE_FACTOR     | RIDF                 |
| REF_TREE_CARBON_RATIO_DEAD   | REFTCRD              |
| REF_TREE_DECAY_PROP          | REFTDP               |
| REF_TREE_STND_DEAD_CR_PROP   | REFTSDCP             |
| REF_GRND_LYR                 | REFGLYR              |
| REF_STD_NORM_DIST            | REF_SND              |

## Unique key

Multiple columns in a table whose values uniquely identify each row in an Oracle table. There can be one and only one row for each unique key value.

The unique key varies for each FIADB table. The unique key for the PLOT table is STATECD, INVYR, UNITCD, COUNTYCD, and PLOT. The unique key for the COND table is PLT\_CN and CONDID.

The name of the unique key for each table is listed in the table description. It follows the nomenclature of 'TABLEABBREVIATION'\_UK.

## Natural key

A type of unique key made from existing attributes in the table. It is stored as an index in this database.

Not all FIADB tables have a natural key. For example, there is no natural key in the PLOT table, rather the natural key and the unique key are the same. The natural key for the COND table is STATECD, INVYR, UNITCD, COUNTYCD, PLOT, and CONDID.

The name of the natural key for each table is listed in the table description. It follows the nomenclature of 'TABLEABBREVIATION'\_NAT\_I.

## Foreign key

A column in a table that is used as a link to a matching column in another Oracle table.

A foreign key connects a record in one table to one and only one record in another table. Foreign keys are used both to link records between data tables and as a check (or constraint) to prevent "unrepresented data." For example, if there are rows of data in the TREE table for a specific plot, there needs to be a corresponding data row for that same plot in the PLOT table. The foreign key in the TREE table is the attribute PLT\_CN, which links specific rows in the TREE table to one record in the PLOT table using the plot attribute CN.

The foreign key for the COND table is PLT\_CN. There is always a match of the PLT\_CN value to the CN value in the PLOT table.

The name of the foreign key for each table is listed in the table description. It follows the nomenclature of 'SOURCETABLEABBREVIATION'\_'MATCHINGTABLEABBREVIATION'\_FK, where the source table is the table containing the foreign key and the matching table is the table the foreign key matches. The foreign key usually matches the CN column of the matching table. Most tables in FIADB have only one foreign key, but tables can have multiple foreign keys.

## 1.3.3 Oracle Data Types

| Oracle data type   | Definition                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DATE               | A data type that stores the date.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| NUMBER             | A data type that contains only numbers, positive or negative, with a floating-decimal point.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| NUMBER(SIZE, D)    | A data type that contains only numbers up to a specified maximum size. The maximum size ( and optional fixed-decimal point ) is specified by the value(s) listed in the parentheses. For example, an attribute with a data type specified as "NUMBER(2)" indicates that the attribute may contain a maximum of 2 digits ( for example , "11" or "5"), however, none of the digits are decimals. An attribute with a data type specified as "NUMBER(3,1)" may contain a maximum of 3 digits, however, the last digit is a fixed decimal ( for example , "4.0" or "12.7"). Likewise, "NUMBER(6,4)" would indicate that an attribute may contain a maximum of 6 digits, however, the last 4 digits are part of a fixed decimal ( for example , "18.7200"). Note: When needed, digits to the right of a fixed-decimal point are filled in with zero(s). |
| VARCHAR2(SIZE)     | A data type that contains alphanumeric data (numbers and/or characters) up to a specified maximum size. For example, an attribute with a data type specified as "VARCHAR2(8)" indicates that the attribute may contain a maximum of eight alphanumeric characters.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

Database Structure

Chapter 1 (revision: 12.2024)

## Chapter 2: Database Tables - Location Level

## Chapter Contents:

|   Section | Database table                  | Oracle table name   |
|-----------|---------------------------------|---------------------|
|       2.1 | Survey Table                    | SURVEY              |
|       2.2 | Project Table                   | PROJECT             |
|       2.3 | County Table                    | COUNTY              |
|       2.4 | Plot Table                      | PLOT                |
|       2.5 | Condition Table                 | COND                |
|       2.6 | Subplot Table                   | SUBPLOT             |
|       2.7 | Subplot Condition Table         | SUBP_COND           |
|       2.8 | Boundary Table                  | BOUNDARY            |
|       2.9 | Subplot Condition Change Matrix | SUBP_COND_CHNG_MTRX |