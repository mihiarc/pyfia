# Section 5.8: Condition Down Woody Material Calculation Table
**Oracle Table Name:** COND_DWM_CALC
**Extracted Pages:** 393-402 (Chapter pages 5-57 to 5-66)
**Source:** FIA Database Handbook v9.3
**Chapter:** 5 - Database Tables - Down Woody Material

---

## 5.8 Condition Down Woody Material Calculation Table

## (Oracle table name: COND\_DWM\_CALC)

| Subsection   | Column name (attribute)   | Descriptive name                                                                              | Oracle data type   |
|--------------|---------------------------|-----------------------------------------------------------------------------------------------|--------------------|
| 5.8.1        | CN                        | Sequence number                                                                               | VARCHAR2(34)       |
| 5.8.2        | STATECD                   | State code                                                                                    | NUMBER(4)          |
| 5.8.3        | COUNTYCD                  | County code                                                                                   | NUMBER(3)          |
| 5.8.4        | PLOT                      | Plot number                                                                                   | NUMBER             |
| 5.8.5        | MEASYEAR                  | Measurement year                                                                              | NUMBER(4)          |
| 5.8.6        | INVYR                     | Inventory year                                                                                | NUMBER(4)          |
| 5.8.7        | CONDID                    | Condition class number                                                                        | NUMBER(1)          |
| 5.8.8        | EVALID                    | Evaluation identifier                                                                         | NUMBER(6)          |
| 5.8.9        | PLT_CN                    | Plot sequence number                                                                          | VARCHAR2(34)       |
| 5.8.10       | CND_CN                    | Condition sequence number                                                                     | VARCHAR2(34)       |
| 5.8.11       | STRATUM_CN                | Stratum sequence number                                                                       | VARCHAR2(34)       |
| 5.8.12       | PHASE                     | Phase                                                                                         | VARCHAR2(3)        |
| 5.8.13       | CONDPROP_CWD              | Proportion of coarse woody debris transects in the condition                                  | NUMBER(13,12)      |
| 5.8.14       | CONDPROP_FWD_SM           | Proportion of fine woody debris transects used to sample small-sized pieces in the condition  | NUMBER(13,12)      |
| 5.8.15       | CONDPROP_FWD_MD           | Proportion of fine woody debris transects used to sample medium-sized pieces in the condition | NUMBER(13,12)      |
| 5.8.16       | CONDPROP_FWD_LG           | Proportion of fine woody debris transects used to sample large-sized pieces in the condition  | NUMBER(13,12)      |
| 5.8.17       | CONDPROP_DUFF             | Proportion of sample points used to measure duff, litter, and fuelbed in the condition        | NUMBER(13,12)      |
| 5.8.18       | CWD_TL_COND               | Coarse woody debris transect length in the condition                                          | NUMBER(13,10)      |
| 5.8.19       | CWD_TL_UNADJ              | Coarse woody debris transect length, unadjusted                                               | NUMBER(13,10)      |
| 5.8.20       | CWD_TL_ADJ                | Coarse woody debris transect length, adjusted                                                 | NUMBER(13,10)      |
| 5.8.21       | CWD_LPA_COND              | Number of coarse woody debris logs (pieces) per acre in the condition                         | NUMBER             |
| 5.8.22       | CWD_LPA_UNADJ             | Number of coarse woody debris logs (pieces) per acre, unadjusted                              | NUMBER             |

| Subsection   | Column name (attribute)   | Descriptive name                                                               | Oracle data type   |
|--------------|---------------------------|--------------------------------------------------------------------------------|--------------------|
| 5.8.23       | CWD_LPA_ADJ               | Number of coarse woody debris logs (pieces) per acre, adjusted                 | NUMBER             |
| 5.8.24       | CWD_VOLCF_COND            | Coarse woody debris cubic-foot volume per acre in the condition                | NUMBER             |
| 5.8.25       | CWD_VOLCF_UNADJ           | Coarse woody debris cubic-foot volume per acre, unadjusted                     | NUMBER             |
| 5.8.26       | CWD_VOLCF_ADJ             | Coarse woody debris cubic-foot volume per acre, adjusted                       | NUMBER             |
| 5.8.27       | CWD_DRYBIO_COND           | Coarse woody debris biomass per acre in the condition                          | NUMBER             |
| 5.8.28       | CWD_DRYBIO_UNADJ          | Coarse woody debris biomass per acre, unadjusted                               | NUMBER             |
| 5.8.29       | CWD_DRYBIO_ADJ            | Coarse woody debris biomass per acre, adjusted                                 | NUMBER             |
| 5.8.30       | CWD_CARBON_COND           | Coarse woody debris carbon density in the condition                            | NUMBER             |
| 5.8.31       | CWD_CARBON_UNADJ          | Coarse woody debris carbon density, unadjusted                                 | NUMBER             |
| 5.8.32       | CWD_CARBON_ADJ            | Coarse woody debris carbon density, adjusted                                   | NUMBER             |
| 5.8.33       | FWD_SM_TL_COND            | Small-size class fine woody debris transect length in the condition            | NUMBER(13,10)      |
| 5.8.34       | FWD_SM_TL_UNADJ           | Small-size class fine woody debris transect length, unadjusted                 | NUMBER(13,10)      |
| 5.8.35       | FWD_SM_TL_ADJ             | Small-size class fine woody debris transect length, adjusted                   | NUMBER(13,10)      |
| 5.8.36       | FWD_SM_CNT_COND           | Small-size class fine woody debris pieces count in the condition               | NUMBER             |
| 5.8.37       | FWD_SM_VOLCF_COND         | Small-size class fine woody debris cubic-foot volume per acre in the condition | NUMBER             |
| 5.8.38       | FWD_SM_VOLCF_UNADJ        | Small-size class fine woody debris cubic-foot volume per acre, unadjusted      | NUMBER             |
| 5.8.39       | FWD_SM_VOLCF_ADJ          | Small-size class fine woody debris cubic-foot volume per acre, adjusted        | NUMBER             |
| 5.8.40       | FWD_SM_DRYBIO_COND        | Small-size class fine woody debris biomass per acre in the condition           | NUMBER             |
| 5.8.41       | FWD_SM_DRYBIO_UNADJ       | Small-size class fine woody debris biomass per acre, unadjusted                | NUMBER             |
| 5.8.42       | FWD_SM_DRYBIO_ADJ         | Small-size class fine woody debris biomass per acre, adjusted                  | NUMBER             |
| 5.8.43       | FWD_SM_CARBON_COND        | Small-size class fine woody debris carbon density in the condition             | NUMBER             |

| Subsection   | Column name (attribute)   | Descriptive name                                                                | Oracle data type   |
|--------------|---------------------------|---------------------------------------------------------------------------------|--------------------|
| 5.8.44       | FWD_SM_CARBON_UNADJ       | Small-size class fine woody debris carbon density, unadjusted                   | NUMBER             |
| 5.8.45       | FWD_SM_CARBON_ADJ         | Small-size class fine woody debris carbon density, adjusted                     | NUMBER             |
| 5.8.46       | FWD_MD_TL_COND            | Medium-size class fine woody debris transect length in the condition            | NUMBER(13,10)      |
| 5.8.47       | FWD_MD_TL_UNADJ           | Medium-size class fine woody debris transect length, unadjusted                 | NUMBER(13,10)      |
| 5.8.48       | FWD_MD_TL_ADJ             | Medium-size class fine woody debris transect length, adjusted                   | NUMBER(13,10)      |
| 5.8.49       | FWD_MD_CNT_COND           | Medium-size class fine woody debris pieces count in the condition               | NUMBER             |
| 5.8.50       | FWD_MD_VOLCF_COND         | Medium-size class fine woody debris cubic-foot volume per acre in the condition | NUMBER             |
| 5.8.51       | FWD_MD_VOLCF_UNADJ        | Medium-size class fine woody debris cubic-foot volume per acre, unadjusted      | NUMBER             |
| 5.8.52       | FWD_MD_VOLCF_ADJ          | Medium-size class fine woody debris cubic-foot volume per acre, adjusted        | NUMBER             |
| 5.8.53       | FWD_MD_DRYBIO_COND        | Medium-size class fine woody debris biomass per acre in the condition           | NUMBER             |
| 5.8.54       | FWD_MD_DRYBIO_UNADJ       | Medium-size class fine woody debris biomass per acre, unadjusted                | NUMBER             |
| 5.8.55       | FWD_MD_DRYBIO_ADJ         | Medium-size class fine woody debris biomass per acre, adjusted                  | NUMBER             |
| 5.8.56       | FWD_MD_CARBON_COND        | Medium-size class fine woody debris carbon density in the condition             | NUMBER             |
| 5.8.57       | FWD_MD_CARBON_UNADJ       | Medium-size class fine woody debris carbon density, unadjusted                  | NUMBER             |
| 5.8.58       | FWD_MD_CARBON_ADJ         | Medium-size class fine woody debris carbon density, adjusted                    | NUMBER             |
| 5.8.59       | FWD_LG_TL_COND            | Large-size class fine woody debris transect length in the condition             | NUMBER(13,10)      |
| 5.8.60       | FWD_LG_TL_UNADJ           | Large-size class fine woody debris transect length, unadjusted                  | NUMBER(13,10)      |
| 5.8.61       | FWD_LG_TL_ADJ             | Large-size class fine woody debris transect length, adjusted                    | NUMBER(13,10)      |
| 5.8.62       | FWD_LG_CNT_COND           | Large-size class fine woody debris pieces count in the condition                | NUMBER             |
| 5.8.63       | FWD_LG_VOLCF_COND         | Large-size class fine woody debris cubic-foot volume per acre in the condition  | NUMBER             |

| Subsection   | Column name (attribute)   | Descriptive name                                                          | Oracle data type   |
|--------------|---------------------------|---------------------------------------------------------------------------|--------------------|
| 5.8.64       | FWD_LG_VOLCF_UNADJ        | Large-size class fine woody debris cubic-foot volume per acre, unadjusted | NUMBER             |
| 5.8.65       | FWD_LG_VOLCF_ADJ          | Large-size class fine woody debris cubic-foot volume per acre, adjusted   | NUMBER             |
| 5.8.66       | FWD_LG_DRYBIO_COND        | Large-size class fine woody debris biomass per acre in the condition      | NUMBER             |
| 5.8.67       | FWD_LG_DRYBIO_UNADJ       | Large-size class fine woody debris biomass per acre, unadjusted           | NUMBER             |
| 5.8.68       | FWD_LG_DRYBIO_ADJ         | Large-size class fine woody debris biomass per acre, adjusted             | NUMBER             |
| 5.8.69       | FWD_LG_CARBON_COND        | Large-size class fine woody debris carbon density in the condition        | NUMBER             |
| 5.8.70       | FWD_LG_CARBON_UNADJ       | Large-size class fine woody debris carbon density, unadjusted             | NUMBER             |
| 5.8.71       | FWD_LG_CARBON_ADJ         | Large-size class fine woody debris carbon density, adjusted               | NUMBER             |
| 5.8.72       | PILE_SAMPLE_AREA_COND     | Condition area sampled for piles                                          | NUMBER(13,12)      |
| 5.8.73       | PILE_SAMPLE_AREA_UNADJ    | Plot area sampled for piles in all conditions, unadjusted                 | NUMBER(13,12)      |
| 5.8.74       | PILE_SAMPLE_AREA_ADJ      | Plot area sampled for piles in all conditions, adjusted                   | NUMBER(13,12)      |
| 5.8.75       | PILE_VOLCF_COND           | Cubic-foot volume per acre of piles in the condition                      | NUMBER             |
| 5.8.76       | PILE_VOLCF_UNADJ          | Cubic-foot volume per acre of piles, unadjusted                           | NUMBER             |
| 5.8.77       | PILE_VOLCF_ADJ            | Cubic-foot volume per acre of piles, adjusted                             | NUMBER             |
| 5.8.78       | PILE_DRYBIO_COND          | Biomass per acre of piles in the condition                                | NUMBER             |
| 5.8.79       | PILE_DRYBIO_UNADJ         | Biomass per acre of piles, unadjusted                                     | NUMBER             |
| 5.8.80       | PILE_DRYBIO_ADJ           | Biomass per acre of piles, adjusted                                       | NUMBER             |
| 5.8.81       | PILE_CARBON_COND          | Carbon density of piles in the condition                                  | NUMBER             |
| 5.8.82       | PILE_CARBON_UNADJ         | Carbon density of piles, unadjusted                                       | NUMBER             |
| 5.8.83       | PILE_CARBON_ADJ           | Carbon density of piles, adjusted                                         | NUMBER             |
| 5.8.84       | FUEL_DEPTH                | Average fuelbed depth in the condition                                    | NUMBER             |
| 5.8.85       | FUEL_BIOMASS              | Average fuelbed biomass per acre in the condition                         | NUMBER             |
| 5.8.86       | FUEL_CARBON               | Average fuelbed carbon density in the condition                           | NUMBER             |

| Subsection   | Column name (attribute)   | Descriptive name                                                                   | Oracle data type   |
|--------------|---------------------------|------------------------------------------------------------------------------------|--------------------|
| 5.8.87       | DUFF_DEPTH                | Average duff depth in the condition                                                | NUMBER             |
| 5.8.88       | DUFF_BIOMASS              | Average duff biomass per acre in the condition                                     | NUMBER             |
| 5.8.89       | DUFF_CARBON               | Average duff carbon density in the condition                                       | NUMBER             |
| 5.8.90       | LITTER_DEPTH              | Average litter depth in the condition                                              | NUMBER             |
| 5.8.91       | LITTER_BIOMASS            | Average litter biomass per acre in the condition                                   | NUMBER             |
| 5.8.92       | LITTER_CARBON             | Average litter carbon density in the condition                                     | NUMBER             |
| 5.8.93       | DUFF_TC_COND              | Number of duff, litter, and fuelbed sampling points in the condition               | NUMBER(14,12)      |
| 5.8.94       | DUFF_TC_UNADJ             | Number of duff, litter, and fuelbed sampling points on the entire plot, unadjusted | NUMBER(14,12)      |
| 5.8.95       | DUFF_TC_ADJ               | Number of duff, litter, and fuelbed sampling points on the entire plot, adjusted   | NUMBER(14,12)      |
| 5.8.96       | AVG_WOOD_DENSITY          | Average wood density                                                               | NUMBER(12,10)      |
| 5.8.97       | CREATED_BY                | Created by                                                                         | VARCHAR2(30)       |
| 5.8.98       | CREATED_DATE              | Created date                                                                       | DATE               |
| 5.8.99       | CREATED_IN_INSTANCE       | Created in instance                                                                | VARCHAR2(6)        |
| 5.8.100      | MODIFIED_BY               | Modified by                                                                        | VARCHAR2(30)       |
| 5.8.101      | MODIFIED_DATE             | Modified date                                                                      | DATE               |
| 5.8.102      | MODIFIED_IN_INSTANCE      | Modified in instance                                                               | VARCHAR2(6)        |
| 5.8.103      | CYCLE                     | Inventory cycle number                                                             | NUMBER(2)          |
| 5.8.104      | SUBCYCLE                  | Inventory subcycle number                                                          | NUMBER(2)          |
| 5.8.105      | UNITCD                    | Survey unit code                                                                   | NUMBER(2)          |
| 5.8.106      | RSCD                      | Region or station code                                                             | NUMBER(2)          |
| 5.8.107      | PILE_TL_COND              | Piles transect length in the condition                                             | NUMBER(13,10)      |
| 5.8.108      | PILE_TL_UNADJ             | Piles transect length, unadjusted                                                  | NUMBER(13,10)      |
| 5.8.109      | PILE_TL_ADJ               | Piles transect length, adjusted                                                    | NUMBER(13,10)      |
| 5.8.110      | CONDPROP_PILE             | Proportion of piles plot area or transect lengths in the condition                 | NUMBER(13,12)      |

| Key Type   | Column(s) order                                      | Tables to link   | Abbreviated notation   |
|------------|------------------------------------------------------|------------------|------------------------|
| Primary    | CN                                                   | N/A              | CDC_PK                 |
| Unique     | PLT_CN, CONDID, EVALID, RSCD                         | N/A              | CDC_UK                 |
| Unique     | STATECD, COUNTYCD, PLOT, INVYR, CONDID, EVALID, RSCD | N/A              | CDC_UK2                |

| Key Type   | Column(s) order                                                | Tables to link               | Abbreviated notation   |
|------------|----------------------------------------------------------------|------------------------------|------------------------|
| Unique     | STATECD, CYCLE, SUBCYCLE, COUNTYCD, PLOT, CONDID, EVALID, RSCD | N/A                          | CDC_UK3                |
| Foreign    | CND_CN                                                         | COND_DWM_CALC to COND        | CDC_CND_FK             |
| Foreign    | PLT_CN                                                         | COND_DWM_CALC to PLOT        | CDC_PLT_FK             |
| Foreign    | STRATUM_CN                                                     | COND_DWM_CALC to POP_STRATUM | CDC_PSM_FK             |

The size classes for fine woody debris (FWD) are as follows:

- · Small-size class - pieces must be 0.01- to 0.24-inch in diameter and located on a transect segment length on the plot specified in the sample design to measure small-size FWD.
- · Medium-size class - pieces must be 0.25- to 0.09-inch in diameter and located on a transect segment length on the plot specified in the sample design to measure medium-size FWD.
- · Large-size class - pieces must be 1.0- to 2.9-inches in diameter and located on a transect segment length on the plot specified in the sample design to measure large-size FWD.

## 5.8.1 CN

Sequence number. A unique sequence number used to identify a condition down woody material calculation record.

## 5.8.2 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 5.8.3 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 5.8.4 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combination of variables, PLOT may be used to uniquely identify a plot.

## 5.8.5 MEASYEAR

Measurement year. The year in which the plot was completed. MEASYEAR may differ from INVYR.

## 5.8.6 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 5.8.7 CONDID

Condition class number. The unique identifying number assigned to a condition on a plot. See COND.CONDID for details on the attributes which delineate a condition.

## 5.8.8 EVALID

Evaluation identifier. See POP\_EVAL.EVALID description for definition.

## 5.8.9 PLT\_CN

Plot sequence number. Foreign key linking the condition down woody material calculation record to the plot record.

## 5.8.10 CND\_CN

Condition sequence number. Foreign key linking the condition down woody material calculation record to the condition record for this location.

## 5.8.11 STRATUM\_CN

Stratum sequence number. Foreign key linking the condition down woody material calculation record to the population stratum record.

## 5.8.12 PHASE

Phase. A code indicating the plot design for DWM measurements. Only populated for certain FIA work units (SURVEY.RSCD = 26, 27).

## Codes: PHASE

| Code   | Description                                         |
|--------|-----------------------------------------------------|
| P2     | Phase 2 plot design.                                |
| P3     | Phase 3 plot design.                                |
| P23    | Phase 2 and phase 3 plot (both designs co-located). |

## 5.8.13 CONDPROP\_CWD

Proportion of coarse woody debris transects in the condition. A proportion developed by summing the CWD transect lengths in one condition and dividing that by the total unadjusted CWD transect length on the plot (CWD\_TL\_COND/CWD\_TL\_UNADJ).

## 5.8.14 CONDPROP\_FWD\_SM

Proportion of fine woody debris transects used to sample small-sized pieces in the condition. A proportion developed by summing the FWD transect lengths in one condition and dividing that by the total unadjusted FWD transect length on the plot (FWD\_SM\_TL\_COND/FWD\_SM\_TL\_UNADJ).

## 5.8.15 CONDPROP\_FWD\_MD

Proportion of fine woody debris transects used to sample medium-sized pieces in the condition. A proportion developed by summing the FWD transect lengths in one condition and dividing that by the total unadjusted FWD transect length on the plot (FWD\_MD\_TL\_COND/FWD\_MD\_TL\_UNADJ).

## 5.8.16 CONDPROP\_FWD\_LG

Proportion of fine woody debris transects used to sample large-sized pieces in the condition. A proportion developed by summing the FWD transect lengths in one condition and dividing that by the total unadjusted FWD transect length on the plot (FWD\_LG\_TL\_COND/FWD\_LG\_TL\_UNADJ).

## 5.8.17 CONDPROP\_DUFF

Proportion of sample points used to measure duff, litter, and fuelbed in the condition.

A proportion developed by summing the number of sample points in one condition and dividing that by the total number of points on the plot (DUFF\_TC\_COND/DUFF\_TC\_UNADJ).

## 5.8.18 CWD\_TL\_COND

Coarse woody debris transect length in the condition. The sum of all transect lengths, in feet, in one condition on a plot. This total length is used to calculate per-acre estimates of volume, biomass, carbon, and number of logs for CWD in the condition. CWD attribute columns that end with a '\_COND' suffix use this length in the estimation equation.

## 5.8.19 CWD\_TL\_UNADJ

Coarse woody debris transect length, unadjusted. The sum of all transect lengths, in feet, in all conditions on a plot, as specified by the sampling design. CWD\_TL\_UNADJ (target transect length) is the maximum length of transect line that would be installed for CWD on each subplot across all conditions (forest, nonforest, sampled, nonsampled) on the plot, before adjustment for partially nonsampled plots in the stratum. This attribute is used in equations to calculate the unadjusted per-acre attributes of CWD, which are columns that end with an '\_UNADJ' suffix.

## 5.8.20 CWD\_TL\_ADJ

Coarse woody debris transect length, adjusted. The sum of all transect lengths, in feet, in all conditions on a plot, as specified by the sampling design, CWD\_TL\_ADJ (adjusted target transect length) is the maximum length of transect line that would be installed on each subplot across all conditions (forest, nonforest, sampled, nonsampled) on the plot, after adjustment for partially nonsampled plots in the stratum. This attribute is used in equations to calculate the adjusted per-acre attributes of CWD, which are columns that end with an '\_ADJ' suffix.

## 5.8.21 CWD\_LPA\_COND

Number of coarse woody debris logs (pieces) per acre in the condition. The sum of logs per acre from all pieces tallied in one condition on a plot, based on transects installed in that condition. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual conditions within a plot.

Note: Because this attribute describes one condition on a plot, it is not used to develop population estimates and is never adjusted. When multiple conditions exist on a plot and one estimate is needed for the plot location (e.g., for a GIS analysis), the plot estimate must be based on the sum of transect lengths from all sampled conditions of interest. For example, an estimate for all forested conditions on the plot would require that CWD\_LPA\_COND be multiplied by CWD\_TL\_COND / (sum of CWD\_TL\_COND on forest conditions) and then summed to the plot level.

## 5.8.22 CWD\_LPA\_UNADJ

Number of coarse woody debris logs (pieces) per acre, unadjusted. The sum of logs per acre from all CWD pieces tallied in one condition on a plot, before adjustment for partially nonsampled plots in the stratum. It is based on the target transect length (CWD\_TL\_UNADJ), which is the total length of transect that could potentially be installed on the plot. This attribute is used to calculate population estimates and not to derive estimates for one condition or individual plot. It must be adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table and then expanded by the acres in POP\_STRATUM.EXPNS to produce population totals for number of CWD logs.

## 5.8.23 CWD\_LPA\_ADJ

Number of coarse woody debris logs (pieces) per acre, adjusted. The sum of logs per acre from all CWD pieces tallied in one condition on a plot, after adjustment for partially nonsampled plots in the stratum. It is based on the adjusted target transect length (CWD\_TL\_ADJ), which is the total length of transect that could potentially be installed on the plot. This attribute is used to calculate population estimates and not to derive estimates for one condition or individual plots. For ease of use, this attribute has been adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table. To expand per acre values to population totals for number of CWD logs, multiply by the acres in POP\_STRATUM.EXPNS.

## 5.8.24 CWD\_VOLCF\_COND

Coarse woody debris cubic-foot volume per acre in the condition. The sum of gross volume, in cubic feet per acre, from all CWD pieces tallied in one condition on a plot, based on transects installed in that condition. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual conditions within a plot.

Note: Because this attribute describes one condition on a plot, it is not used to develop population estimates and is never adjusted. When multiple conditions exist on a plot and one estimate is needed for the plot location (e.g., for a GIS analysis), the plot estimate must be based on the sum of transect lengths from all sampled conditions of interest. For example, an estimate for all forested conditions on the plot would require that CWD\_VOLCF\_COND be multiplied by CWD\_TL\_COND / (sum of CWD\_TL\_COND on forest conditions) and then summed to the plot level.

## 5.8.25 CWD\_VOLCF\_UNADJ

Coarse woody debris cubic-foot volume per acre, unadjusted. The sum of gross volume, in cubic feet per acre, from all CWD pieces tallied in one condition on a plot, before adjustment for partially nonsampled plots in the stratum. This attribute is based on the target transect length (CWD\_TL\_UNADJ), and is used to calculate population estimates and not used to derive estimates for one condition or individual plot. It must be adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table and then expanded by the acres in POP\_STRATUM.EXPNS to produce population totals for gross cubic volume of CWD.

## 5.8.26 CWD\_VOLCF\_ADJ

Coarse woody debris cubic-foot volume per acre, adjusted. The sum of gross volume on a plot, in cubic feet per acre, from all CWD pieces tallied in one condition, after adjustment for partially nonsampled plots in the stratum. This attribute is based on the adjusted target transect length (CWD\_TL\_ADJ), and is used to calculate population estimates and not to derive estimates for one condition or individual plot. For ease of use,

this attribute has been adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table. To expand per acre values to population totals for gross cubic volume of CWD, multiply by the acres in POP\_STRATUM.EXPNS.

## 5.8.27 CWD\_DRYBIO\_COND

Coarse woody debris biomass per acre in the condition. The sum of biomass, in oven-dry pounds per acre, from all CWD pieces tallied in one condition on a plot, based on transects installed in that condition. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual conditions within a plot.

Note: Because this attribute describes one condition on a plot, it is not used to develop population estimates and is never adjusted. When multiple conditions exist on a plot and one estimate is needed for the plot location (e.g., for a GIS analysis), the plot estimate must be based on the sum of transect lengths from all sampled conditions of interest. For example, an estimate for all forested conditions on the plot would require that CWD\_ DRYBIO \_COND be multiplied by CWD\_TL\_COND / (sum of CWD\_TL\_COND on forest conditions) and then summed to the plot level.

## 5.8.28 CWD\_DRYBIO\_UNADJ

Coarse woody debris biomass per acre, unadjusted. The sum of biomass, in oven-dry pounds per acre, from all CWD pieces tallied in one condition on a plot, before adjustment for partially nonsampled plots in the stratum. This attribute is based on the target transect length (CWD\_TL\_UNADJ), and is used to calculate population estimates and not used to derive estimates for one condition or individual plot. It must be adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table and then expanded by the acres in POP\_STRATUM.EXPNS to produce population totals for dry biomass of CWD.

## 5.8.29 CWD\_DRYBIO\_ADJ

Coarse woody debris biomass per acre, adjusted. The sum of biomass, in oven-dry pounds per acre, from all CWD pieces tallied in one condition on a plot, after adjustment for partially nonsampled plots in the stratum. This attribute is based on the adjusted target transect length (CWD\_TL\_ADJ), and is used to calculate population estimates and not used to derive estimates for one condition or individual plot. For ease of use, this attribute has been adjusted by the factor ADJ\_FACTOR\_CWD stored in the POP\_STRATUM table. To expand per acre values to population totals for dry biomass of CWD, multiply by the acres in POP\_STRATUM.EXPNS.

## 5.8.30 CWD\_CARBON\_COND

Coarse woody debris carbon density in the condition. The sum of carbon, in pounds per acre, from all CWD pieces tallied in one condition on a plot, based on transects installed in that condition. This attribute is useful for analysis projects that involve modeling, mapping, or classifying individual conditions within a plot.

Note: Because this attribute describes one condition on a plot, it is not used to develop population estimates and is never adjusted. When multiple conditions exist on a plot and one estimate is needed for the plot location (e.g., for a GIS analysis), the plot estimate must be based on the sum of transect lengths from all sampled conditions of interest. For example, an estimate for all forested conditions on the plot would require that CWD\_ CARBON \_COND be multiplied by CWD\_TL\_COND / (sum of CWD\_TL\_COND on forest conditions) and then summed to the plot level.