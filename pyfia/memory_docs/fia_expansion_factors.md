# Expansion Factors Documentation
**Extracted Pages:** 35-37 (3 pages)
**Source:** FIA Database Handbook v9.3
**Section:** Part of Chapter 1.2 - FIA Sampling and Estimation Procedures

---

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