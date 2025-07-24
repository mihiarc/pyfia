# Section 4.2: Phase 2 Vegetation Subplot Species Table
**Oracle Table Name:** P2VEG_SUBPLOT_SPP
**Extracted Pages:** 325-330 (Chapter pages 4-7 to 4-12)
**Source:** FIA Database Handbook v9.3
**Chapter:** 4 - Database Tables - Invasive Species; Understory Vegetation

---

## 4.2 Phase 2 Vegetation Subplot Species Table (Oracle table name: P2VEG\_SUBPLOT\_SPP)

| Subsection   | Column name (attribute)   | Descriptive name                         | Oracle data type   |
|--------------|---------------------------|------------------------------------------|--------------------|
| 4.2.1        | CN                        | Sequence number                          | VARCHAR2(34)       |
| 4.2.2        | PLT_CN                    | Plot sequence number                     | VARCHAR2(34)       |
| 4.2.3        | INVYR                     | Inventory year                           | NUMBER(4)          |
| 4.2.4        | STATECD                   | State code                               | NUMBER(4)          |
| 4.2.5        | UNITCD                    | Survey unit code                         | NUMBER(2)          |
| 4.2.6        | COUNTYCD                  | County code                              | NUMBER(3)          |
| 4.2.7        | PLOT                      | Plot number                              | NUMBER             |
| 4.2.8        | SUBP                      | Subplot number                           | NUMBER             |
| 4.2.9        | CONDID                    | Condition class number                   | NUMBER(1)          |
| 4.2.10       | VEG_FLDSPCD               | Vegetation field species code            | VARCHAR2(10)       |
| 4.2.11       | UNIQUE_SP_NBR             | Unique species number                    | NUMBER(2)          |
| 4.2.12       | VEG_SPCD                  | Vegetation species code                  | VARCHAR2(10)       |
| 4.2.13       | GROWTH_HABIT_CD           | Growth habit code (species growth habit) | VARCHAR2(2)        |
| 4.2.14       | LAYER                     | Layer (species vegetation layer)         | NUMBER(1)          |
| 4.2.15       | COVER_PCT                 | Cover percent (species canopy cover)     | NUMBER(3)          |
| 4.2.16       | CREATED_BY                | Created by                               | VARCHAR2(30)       |
| 4.2.17       | CREATED_DATE              | Created date                             | DATE               |
| 4.2.18       | CREATED_IN_INSTANCE       | Created in instance                      | VARCHAR2(6)        |
| 4.2.19       | MODIFIED_BY               | Modified by                              | VARCHAR2(30)       |
| 4.2.20       | MODIFIED_DATE             | Modified date                            | DATE               |
| 4.2.21       | MODIFIED_IN_INSTANCE      | Modified in instance                     | VARCHAR2(6)        |
| 4.2.22       | CYCLE                     | Inventory cycle number                   | NUMBER(2)          |
| 4.2.23       | SUBCYCLE                  | Inventory subcycle number                | NUMBER(2)          |

| Key Type   | Column(s) order                                  | Tables to link                 | Abbreviated notation   |
|------------|--------------------------------------------------|--------------------------------|------------------------|
| Primary    | CN                                               | N/A                            | P2VSSP_PK              |
| Unique     | PLT_CN, VEG_FLDSPCD, UNIQUE_SP_NBR, SUBP, CONDID | N/A                            | P2VSSP_UK              |
| Foreign    | PLT_CN                                           | P2VEG_SUBPLOT_SPP to PLOT      | P2VSSP_PLT_FK          |
| Foreign    | PLT_CN, SUBP, CONDID                             | P2VEG_SUBPLOT_SPP to SUBP_COND | P2VSSP_SCD_FK          |

FIA identifies species and other taxonomic ranks for plants using symbols (SYMBOL) as assigned by NRCS (Natural Resources Conservation Service) for the PLANTS database (https://plants.usda.gov) on a periodic basis. The most recent NRCS download for the FIA program was September 15, 2017.

## 4.2.1 CN

Sequence number. A unique sequence number used to identify a Phase 2 (P2) vegetation subplot species record.

## 4.2.2 PLT\_CN

Plot sequence number. Foreign key linking the Phase 2 (P2) vegetation subplot species record to the plot record for this location.

## 4.2.3 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 4.2.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 4.2.5 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. Refer to appendix B for codes.

## 4.2.6 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 4.2.7 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 4.2.8 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot values of 1 through 4.

## Codes: SUBP

|   Code | Description        |
|--------|--------------------|
|      1 | Center subplot.    |
|      2 | North subplot.     |
|      3 | Southeast subplot. |
|      4 | Southwest subplot. |

## 4.2.9 CONDID

Condition class number. The unique identifying number assigned to a condition on which the vegetation species is located, and is defined in the COND table. See COND.CONDID for details on the attributes which delineate a condition.

## 4.2.10 VEG\_FLDSPCD

Vegetation field species code. The species code assigned by the field crew, conforming to the NRCS PLANTS database.

## 4.2.11 UNIQUE\_SP\_NBR

Unique species number. A unique number indicating each unidentified species encountered on the plot. This attribute identifies the number of species occurrences within each NRCS genus or unknown code. For example, two unidentifiable CAREX species would be entered as two separate records with differing unique species numbers to show that they are not the same species.

## 4.2.12 VEG\_SPCD

Vegetation species code. A code indicating each sampled vascular plant species found rooted in or overhanging the sampled condition of the subplot at any height. Species codes are the standardized codes in the NRCS PLANTS database.

## 4.2.13 GROWTH\_HABIT\_CD

Growth habit code (species growth habit). A code indicating the growth habit of the species. Tally tree species are always recorded as trees, even when they exhibited a shrub-like growth habit. If a species had more than one growth habit on the same accessible condition in a subplot, the most prevalent one was recorded; however, both tree habits (SD and LT) could be coded for the same species if PLOT.P2VEG\_SAMPLING\_LEVEL\_DETAIL\_CD = 3 and the species was found in both size classes. A species may be recorded with a different growth habit on a different subplot condition on the same plot. P2VEG\_SUBPLOT\_SPP.GROWTH\_HABIT\_CD is not to be confused with P2VEG\_SUBP\_STRUCTURE.GROWTH\_HABIT\_CD. The codes are similar, but not exactly the same.

## Codes: GROWTH\_HABIT\_CD

| Code   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SD     | Seedlings and Saplings: Small trees less than 5 inches d.b.h. or d.r.c., including tally and non-tally tree species. Seedlings of any length are included (i.e., no minimum). Up to four species are recorded if individual species total aerial canopy cover is at least 3% on the subplot and within the GROWTH_HABIT_CD.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| SH     | Shrubs/Subshrubs/Woody Vines: Woody, multiple-stemmed plants of any size, subshrubs (low-growing shrubs under 1.5 feet tall at maturity), and woody vines. Most cacti are included in this category. Subshrub species are usually included in this category. However, there are many species that can exhibit either subshrub or forb/herb growth habits. Each FIA region will develop a list of common species that can exhibit either growth habits (according to the NRCS PLANTS database) with regional guidance as to which growth habit the species should normally be assigned, while still allowing species assignments to different growth habits when the species is obviously present in a different growth habit. Up to four species are recorded if individual species total aerial canopy cover is at least 3% on the subplot and within the GROWTH_HABIT_CD. |

| Code   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| FB     | Forbs: Herbaceous, broad-leaved plants; includes non-woody-vines, ferns (does not include mosses and cryptobiotic crusts). Up to four species are recorded if individual species total aerial canopy cover is at least 3% on the subplot and within the GROWTH_HABIT_CD.                                                                                                                                                                                   |
| GR     | Graminoids: Grasses and grass-like plants (includes rushes and sedges). Up to four species are recorded if individual species total aerial canopy cover is at least 3% on the subplot and within the GROWTH_HABIT_CD.                                                                                                                                                                                                                                      |
| LT     | Large Trees: Large trees greater than or equal to 5 inches d.b.h. or d.r.c. For PLOT.P2VEG_SAMPLING_LEVEL_DETAIL_CD = 2, only non-tally tree species are included; for PLOT.P2VEG_SAMPLING_LEVEL_DETAIL_CD = 3, tally and non-tally tree species are included. Up to four species of large trees (d.b.h. or d.r.c. at least 5 inches) are recorded if individual species aerial canopy cover is at least 3% on the subplot and within the GROWTH_HABIT_CD. |

Codes: GROWTH\_HABIT\_CD (additional codes for PNWRS, SURVEY.RSCD = 26, 27) LAYER

| Code   | Description                                                                                                                                                                      |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ST     | Seedlings: Small trees <1 inch d.b.h. or d.r.c. Populated for PLOT.MANUAL <5.0.                                                                                                  |
| TR     | Trees - Alaska 2005: All trees, regardless of size. Populated for Alaska 2005 Wilderness data category. For more information, contact the PNWRS Analyst Contact (see table 1-1). |

## 4.2.14 LAYER

Layer (species vegetation layer). A code indicating the vertical layer in which the plant species was found. If a species occurs in more than one layer, the layer where most of the species canopy cover is recorded.

Codes: LAYER

|   Code | Description           |
|--------|-----------------------|
|      1 | 0 to 2.0 feet.        |
|      2 | 2.1 to 6.0 feet.      |
|      3 | 6.1 to 16.0 feet.     |
|      4 | Greater than 16 feet. |

## 4.2.15 COVER\_PCT

Cover percent (species canopy cover). For each species recorded, the canopy cover present on the subplot condition to the nearest 1 percent. Canopy cover is based on a vertically projected polygon described by the outline of the foliage, ignoring any normal spaces occurring between the leaves of plants (Daubenmire 1959), and ignoring overlap among multiple layers of a species. For each species, cover can never exceed 100 percent.

Note: Cover is always recorded as a percent of the full subplot area, even if the condition that was assessed did not cover the full subplot. Canopy cover for species is assigned to the dominant layer.

## 4.2.16 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 4.2.17 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 4.2.18 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 4.2.19 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 4.2.20 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 4.2.21 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 4.2.22 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 4.2.23 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

Phase 2 Vegetation Subplot Species Table

Chapter 4 (revision: 04.2024)