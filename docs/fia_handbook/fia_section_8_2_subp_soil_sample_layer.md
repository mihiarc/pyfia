# Section 8.2: Subplot Soil Sample Layer Table
**Oracle Table Name:** SUBP_SOIL_SAMPLE_LAYER
**Extracted Pages:** 457-464 (Chapter pages 8-9 to 8-16)
**Source:** FIA Database Handbook v9.3
**Chapter:** 8 - Database Tables - Soils, Pacific Northwest Research Station (PNWRS)

---

## 8.2 Subplot Soil Sample Layer Table (Oracle table name: SUBP\_SOIL\_SAMPLE\_LAYER)

| Subsection   | Column name (attribute)        | Descriptive name                             | Oracle data type   |
|--------------|--------------------------------|----------------------------------------------|--------------------|
| 8.2.1        | CN                             | Sequence number                              | VARCHAR2(34)       |
| 8.2.2        | SSSL_CN                        | Subplot soil sample location sequence number | VARCHAR2(34)       |
| 8.2.3        | PLT_CN                         | Plot sequence number                         | VARCHAR2(34)       |
| 8.2.4        | STATECD                        | State code                                   | NUMBER(4)          |
| 8.2.5        | COUNTYCD                       | County code                                  | NUMBER(3)          |
| 8.2.6        | PLOT                           | Plot number                                  | NUMBER(6)          |
| 8.2.7        | INVYR                          | Inventory year                               | NUMBER(4)          |
| 8.2.8        | INV_VST_NBR                    | Inventory visit number                       | NUMBER(2)          |
| 8.2.9        | CYCLE                          | Inventory cycle number                       | NUMBER(2)          |
| 8.2.10       | SUBCYCLE                       | Inventory subcycle number                    | NUMBER(2)          |
| 8.2.11       | SUBP                           | Subplot number                               | NUMBER(1)          |
| 8.2.12       | VSTNBR                         | Visit number                                 | NUMBER(1)          |
| 8.2.13       | SAMPLER_TYPE                   | Sampler type                                 | VARCHAR2(2)        |
| 8.2.14       | SAMPLE_DIA                     | Sample diameter                              | NUMBER(7,3)        |
| 8.2.15       | LAYER_TYPE                     | Layer type                                   | VARCHAR2(30)       |
| 8.2.16       | SOIL_SAMP_PER_AC               | Soil sample area expansion factor            | NUMBER(14,6)       |
| 8.2.17       | LAYER_THICKNESS                | Layer thickness                              | NUMBER(5,3)        |
| 8.2.18       | LAYER_COLLECTED_CD             | Layer collected code                         | NUMBER(1)          |
| 8.2.19       | WT_FIELD_MOIST                 | Field-moist soil weight                      | NUMBER(7,2)        |
| 8.2.20       | WT_AIR_DRY                     | Air-dry soil weight                          | NUMBER(7,2)        |
| 8.2.21       | WT_OVEN_DRY                    | Oven-dry soil weight                         | NUMBER(7,2)        |
| 8.2.22       | WT_ROCK                        | Rock particle weight                         | NUMBER(7,2)        |
| 8.2.23       | WATER_CONTENT_PCT_FIELD_MO IST | Field-moist water content percent            | NUMBER(6,2)        |
| 8.2.24       | WATER_CONTENT_PCT_RESIDUAL     | Residual water content percent               | NUMBER(6,2)        |
| 8.2.25       | WATER_CONTENT_PCT_TOTAL        | Total water content percent                  | NUMBER(6,2)        |
| 8.2.26       | BULK_DENSITY                   | Bulk density                                 | NUMBER(10,6)       |
| 8.2.27       | COARSE_FRACTION_PCT            | Coarse fraction percent                      | NUMBER(7,3)        |
| 8.2.28       | BULK_DENSITY_FINES             | Bulk density of fine soil fraction           | NUMBER(10,6)       |
| 8.2.29       | TEXTURE_CD                     | Texture code                                 | NUMBER(1)          |
| 8.2.30       | PH_H2O                         | pH in water                                  | NUMBER(7,3)        |
| 8.2.31       | PH_CACL2                       | pH in calcium chloride solution              | NUMBER(7,3)        |
| 8.2.32       | ECEC                           | Effective cation exchange capacity           | NUMBER(7,3)        |

| Subsection   | Column name (attribute)   | Descriptive name                       | Oracle data type   |
|--------------|---------------------------|----------------------------------------|--------------------|
| 8.2.33       | EXCHNG_AL                 | Exchangeable aluminum                  | NUMBER(7,3)        |
| 8.2.34       | EXCHNG_CA                 | Exchangeable calcium                   | NUMBER(8,3)        |
| 8.2.35       | EXCHNG_CD                 | Exchangeable cadmium                   | NUMBER(7,3)        |
| 8.2.36       | EXCHNG_CU                 | Exchangeable copper                    | NUMBER(7,3)        |
| 8.2.37       | EXCHNG_FE                 | Exchangeable iron                      | NUMBER(7,3)        |
| 8.2.38       | EXCHNG_K                  | Exchangeable potassium                 | NUMBER(7,3)        |
| 8.2.39       | EXCHNG_MG                 | Exchangeable magnesium                 | NUMBER(8,3)        |
| 8.2.40       | EXCHNG_MN                 | Exchangeable manganese                 | NUMBER(7,3)        |
| 8.2.41       | EXCHNG_NA                 | Exchangeable sodium                    | NUMBER(8,3)        |
| 8.2.42       | EXCHNG_NI                 | Exchangeable nickel                    | NUMBER(7,3)        |
| 8.2.43       | EXCHNG_PB                 | Exchangeable lead                      | NUMBER(7,3)        |
| 8.2.44       | EXCHNG_S                  | Exchangeable sulfur                    | NUMBER(8,3)        |
| 8.2.45       | EXCHNG_ZN                 | Exchangeable zinc                      | NUMBER(7,3)        |
| 8.2.46       | BRAY1_P                   | Bray 1 phosphorus                      | NUMBER(7,3)        |
| 8.2.47       | OLSEN_P                   | Olsen phosphorus                       | NUMBER(7,3)        |
| 8.2.48       | C_ORG_PCT                 | Organic carbon percent                 | NUMBER(7,3)        |
| 8.2.49       | C_INORG_PCT               | inorganic carbon percent               | NUMBER(7,3)        |
| 8.2.50       | C_TOTAL_PCT               | Total carbon percent                   | NUMBER(7,3)        |
| 8.2.51       | C_MG_AC                   | Carbon content per acre                | NUMBER(10,6)       |
| 8.2.52       | C_MIN3_MG_AC              | Carbon content 3-inch depth per acre   | NUMBER(10,6)       |
| 8.2.53       | N_TOTAL_PCT               | Total nitrogen percent                 | NUMBER(7,3)        |
| 8.2.54       | N_MG_AC                   | Nitrogen content per acre              | NUMBER(10,6)       |
| 8.2.55       | N_MIN3_MG_AC              | Nitrogen content 3-inch depth per acre | NUMBER(10,6)       |
| 8.2.56       | CREATED_BY                | Created by                             | VARCHAR2(30)       |
| 8.2.57       | CREATED_DATE              | Created date                           | DATE               |
| 8.2.58       | CREATED_IN_INSTANCE       | Created in instance                    | VARCHAR2(6)        |
| 8.2.59       | MODIFIED_BY               | Modified by                            | VARCHAR2(30)       |
| 8.2.60       | MODIFIED_DATE             | Modified date                          | DATE               |
| 8.2.61       | MODIFIED_IN_INSTANCE      | Modified in instance                   | VARCHAR2(6)        |

| Key Type   | Column(s) order   | Tables to link                                 | Abbreviated notation   |
|------------|-------------------|------------------------------------------------|------------------------|
| Primary    | CN                | N/A                                            | SSSLYR_PK              |
| Foreign    | PLT_CN            | SUBP_SOIL_SAMPLE_LAYER to PLOT                 | SSSLYR_FK              |
| Foreign    | SSSL_CN           | SUBP_SOIL_SAMPLE_LAYER to SUBP_SOIL_SAMPLE_LOC | SSSLYR_FK2             |

## 8.2.1 CN

Sequence number. A unique sequence number used to identify a subplot soil sample layer record.

## 8.2.2 SSSL\_CN

Subplot soil sample location sequence number. Foreign key linking the subplot soil sample layer record to the subplot soil sample location record.

## 8.2.3 PLT\_CN

Plot sequence number. Foreign key linking the subplot soil sample layer record to the plot record.

## 8.2.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 8.2.5 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 8.2.6 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 8.2.7 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 8.2.8 INV\_VST\_NBR

Inventory visit number. Visit number within a cycle. A plot is usually visited once per cycle, but may be visited again for quality assurance visits or other measurements.

## 8.2.9 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 8.2.10 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition..

## 8.2.11 SUBP

Subplot number. The number assigned to the subplot adjacent to the soil sampling site. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Soils protocols use only subplots 2-4.

Codes: SUBP

|   Code | Description        |
|--------|--------------------|
|      2 | North subplot.     |
|      3 | Southeast subplot. |
|      4 | Southwest subplot. |

## 8.2.12 VSTNBR

Visit number. The number of the soil sampling location at which the soil sample was collected. Values are 1-9.

Figure 8-2: Location of soil sampling site.

<!-- image -->

## 8.2.13 SAMPLER\_TYPE

Sampler type. A code indicating the type of soil sampler used.

Codes: SAMPLER\_TYPE

| Code   | Description   |
|--------|---------------|
| BD     | Bulk density. |
| SF     | Sample frame. |
| O      | Other.        |

## 8.2.14 SAMPLE\_DIA

Sample diameter. The diameter (inches) of the sample area.

## 8.2.15 LAYER\_TYPE

Layer type. A code indicating the soil layer type of the sample. Code values differ depending on the value of SUBP\_SOIL\_SAMPLE\_LOC.SOILS\_SAMPLE\_METHOD\_CD.

Codes: LAYER\_TYPE (when SOILS\_SAMPLE\_METHOD\_CD = 1) (P3 Soils)

| Code                | Description                                          |
|---------------------|------------------------------------------------------|
| LITTER              | Litter layer.                                        |
| FOREST FLOOR        | Forest floor.                                        |
| MINERAL_SOIL_0-4_IN | Mineral soil layer collected at 0-4 inches in depth. |
| MINERAL_SOIL_4-8_IN | Mineral soil layer collected at 4-8 inches in depth. |

Codes: LAYER\_TYPE (when SOILS\_SAMPLE\_METHOD\_CD = 2) (Interior Alaska pilot)

| Code          | Description           |
|---------------|-----------------------|
| IDENT_PARTS   | Identifiable parts.   |
| UNIDENT_PARTS | Unidentifiable parts. |
| MINERAL_SOIL  | Mineral soil.         |
| UNK_SOIL      | Unknown soil.         |

Codes: LAYER\_TYPE (when SOILS\_SAMPLE\_METHOD\_CD = 3) (Interior Alaska)

| Code              | Description                |
|-------------------|----------------------------|
| LITTER            | Litter layer.              |
| GREEN_MOSS_LICHEN | Green moss / lichen layer. |
| IDENT_PARTS       | Identifiable parts.        |
| UNIDENT_PARTS     | Unidentifiable parts.      |
| MINERAL_SOIL      | Mineral soil.              |
| UNK_SOIL          | Unknown soil.              |

Codes: LAYER\_TYPE (when SOILS\_SAMPLE\_METHOD\_CD = 4) (Hawaii)

| Code                   | Description                   |
|------------------------|-------------------------------|
| MINERAL_SOIL_0-20_CM   | 0-20 cm mineral soil layer.   |
| MINERAL_SOIL_20-40_CM  | 20-40 cm mineral soil layer.  |
| MINERAL_SOIL_40-60_CM  | 40-60 cm mineral soil layer.  |
| MINERAL_SOIL_60-80_CM  | 60-80 cm mineral soil layer.  |
| MINERAL_SOIL_80-100_CM | 80-100 cm mineral soil layer. |

## 8.2.16 SOIL\_SAMP\_PER\_AC

Soil sample area expansion factor. The expansion factor from the layer sampling area to an acre.

## 8.2.17 LAYER\_THICKNESS

Layer thickness. The layer thickness to the nearest 0.1 inch.

## 8.2.18 LAYER\_COLLECTED\_CD

Layer collected code. A code indicating whether or not a soil layer was sampled.

## Codes: TEXTURE\_CD

|   Code | Description                                      |
|--------|--------------------------------------------------|
|      1 | Sample collected for analysis.                   |
|      2 | Sample not collected for analysis; other reason. |

## 8.2.19 WT\_FIELD\_MOIST

Field-moist soil weight. The weight (g) of the soil sample as received from the field.

## 8.2.20 WT\_AIR\_DRY

Air-dry soil weight. The weight (g) of the soil sample after air-drying at ambient temperature.

## 8.2.21 WT\_OVEN\_DRY

Oven-dry soil weight. The calculated weight (g) of the soil sample based on an oven-dried subsample

## 8.2.22 WT\_ROCK

Rock particle weight. The weight (g) of mineral soil &gt;2 mm in size.

## 8.2.23 WATER\_CONTENT\_PCT\_FIELD\_MOIST

Field-moist water content percent. The field-moist to air-dry water content in percent.

## 8.2.24 WATER\_CONTENT\_PCT\_RESIDUAL

Residual water content percent. The air-dry to oven-dry water content in percent.

## 8.2.25 WATER\_CONTENT\_PCT\_TOTAL

Total water content percent. The field-moist to air-dry (WATER\_CONTENT\_PPCT\_FIELD\_MOIST) plus air-dry to oven-dry water (WATER\_CONTENT\_PCT\_RESIDUAL) contents in percent.

## 8.2.26 BULK\_DENSITY

Bulk density. The soil bulk density calculated as weight per unit volume of soil, g/cm 3 .

## 8.2.27 COARSE\_FRACTION\_PCT

Coarse fraction percent. The percentage of mineral soil &gt;2 mm in size.

## 8.2.28 BULK\_DENSITY\_FINES

Bulk density of fine soil fraction. The bulk density of mineral soil particles &lt;2 mm calculated as weight per unit volume of soil, g/cm 3 .

## 8.2.29 TEXTURE\_CD

Texture code. A code indicating the texture of the soil layer.

## Codes: TEXTURE\_CD for P3 (SOILS\_SAMPLE\_METHOD \_CD = 1)

|   Code | Description   |
|--------|---------------|
|      0 | Organic.      |
|      1 | Loamy.        |
|      2 | Clayey.       |
|      3 | Sandy.        |
|      4 | Coarse sand.  |
|      9 | Not measured. |

## Codes: TEXTURE\_CD for Interior Alaska (SOILS\_SAMPLE\_METHOD\_CD = 2, 3)

|   Code | Description   |
|--------|---------------|
|      1 | Loamy.        |
|      2 | Clayey.       |
|      3 | Sandy.        |
|      4 | Coarse sand.  |
|      9 | Not measured. |

## 8.2.30 PH\_H2O

pH in water. Soil pH measured in a 1:1 soil/water suspension.

## 8.2.31 PH\_CACL2

pH in calcium chloride solution. Soil pH measured in 0.01 M CaCl2 solution.

## 8.2.32 ECEC

Effective cation exchange capacity. Exchangeable sodium (Na) + potassium (K) + magnesium (Mg) + calcium (Ca) + aluminum (Al) in cmolc/kg.

## 8.2.33 EXCHNG\_AL

Exchangeable aluminum. Exchangeable aluminum (Al) in mg/kg.

## 8.2.34 EXCHNG\_CA

Exchangeable calcium. Exchangeable calcium (Ca) in mg/kg.

## 8.2.35 EXCHNG\_CD

Exchangeable cadmium. Exchangeable cadmium (Cd) in mg/kg.

## 8.2.36 EXCHNG\_CU

Exchangeable copper. Exchangeable copper (Cu) in mg/kg.

## 8.2.37 EXCHNG\_FE

Exchangeable iron. Exchangeable iron (Fe) in mg/kg.

## 8.2.38 EXCHNG\_K

Exchangeable potassium. Exchangeable potassium (K) in mg/kg.

## 8.2.39 EXCHNG\_MG

Exchangeable magnesium. Exchangeable magnesium (Mg) in mg/kg.

## 8.2.40 EXCHNG\_MN

Exchangeable manganese. Exchangeable manganese (Mn) in mg/kg.

## 8.2.41 EXCHNG\_NA

Exchangeable sodium. Exchangeable sodium (Na) in mg/kg.

## 8.2.42 EXCHNG\_NI

Exchangeable nickel. Exchangeable nickel (Ni) in mg/kg.

## 8.2.43 EXCHNG\_PB

Exchangeable lead. Exchangeable lead (Pb) in mg/kg.

## 8.2.44 EXCHNG\_S

Exchangeable sulfur. Exchangeable sulfur (S) in mg/kg.

## 8.2.45 EXCHNG\_ZN

Exchangeable zinc. Exchangeable zinc (Zn) in mg/kg.

## 8.2.46 BRAY1\_P

Bray 1 phosphorus.

Bray 1 extractable phosphorus in mg/kg.

## 8.2.47 OLSEN\_P

Olsen phosphorus. Olsen extractable phosphorus in mg/kg.

## 8.2.48 C\_ORG\_PCT

Organic carbon percent. Organic carbon in percent.

## 8.2.49 C\_INORG\_PCT

Inorganic carbon percent. Inorganic carbon (carbonates) in percent.

## 8.2.50 C\_TOTAL\_PCT

Total carbon percent. Total carbon (organic + inorganic) in percent.

## 8.2.51 C\_MG\_AC

Carbon content per acre. Carbon content (Mg) per acre.

## 8.2.52 C\_MIN3\_MG\_AC

Carbon content 3-inch depth per acre. Carbon content (Mg) per acre to a standard depth of three inches.

## 8.2.53 N\_TOTAL\_PCT

Total nitrogen percent. Total nitrogen (N) in percent.