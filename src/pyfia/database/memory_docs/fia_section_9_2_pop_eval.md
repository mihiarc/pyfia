# Section 9.2: Population Evaluation Table
**Oracle Table Name:** POP_EVAL
**Extracted Pages:** 473-476 (Chapter pages 9-7 to 9-10)
**Source:** FIA Database Handbook v9.3
**Chapter:** 9 - Database Tables - Population

---

## 9.2 Population Evaluation Table

## (Oracle table name: POP\_EVAL)

| Subsection   | Column name (attribute)   | Descriptive name                 | Oracle data type   |
|--------------|---------------------------|----------------------------------|--------------------|
| 9.2.1        | CN                        | Sequence number                  | VARCHAR2(34)       |
| 9.2.2        | EVAL_GRP_CN               | Evaluation group sequence number | VARCHAR2(34)       |
| 9.2.3        | RSCD                      | Region or station code           | NUMBER(2)          |
| 9.2.4        | EVALID                    | Evaluation identifier            | NUMBER(6)          |
| 9.2.5        | EVAL_DESCR                | Evaluation description           | VARCHAR2(255)      |
| 9.2.6        | STATECD                   | State code                       | NUMBER(4)          |
| 9.2.7        | LOCATION_NM               | Location name                    | VARCHAR2(255)      |
| 9.2.8        | REPORT_YEAR_NM            | Report year name                 | VARCHAR2(255)      |
| 9.2.9        | START_INVYR               | Start inventory year             | NUMBER(4)          |
| 9.2.10       | END_INVYR                 | End inventory year               | NUMBER(4)          |
| 9.2.11       | LAND_ONLY                 | Land only                        | VARCHAR2(1)        |
| 9.2.12       | TIMBERLAND_ONLY           | Timberland only                  | VARCHAR2(1)        |
| 9.2.13       | GROWTH_ACCT               | Growth accounting                | VARCHAR2(1)        |
| 9.2.14       | ESTN_METHOD               | Estimation method                | VARCHAR2(40)       |
| 9.2.15       | NOTES                     | Notes                            | VARCHAR2(2000)     |
| 9.2.16       | CREATED_BY                | Created by                       | VARCHAR2(30)       |
| 9.2.17       | CREATED_DATE              | Created date                     | DATE               |
| 9.2.18       | CREATED_IN_INSTANCE       | Created in instance              | VARCHAR2(6)        |
| 9.2.19       | MODIFIED_BY               | Modified by                      | VARCHAR2(30)       |
| 9.2.20       | MODIFIED_DATE             | Modified date                    | DATE               |
| 9.2.21       | MODIFIED_IN_INSTANCE      | Modified in instance             | VARCHAR2(6)        |

| Key Type   | Column(s) order   | Tables to link           | Abbreviated notation   |
|------------|-------------------|--------------------------|------------------------|
| Primary    | CN                | N/A                      | PEV_PK                 |
| Unique     | RSCD, EVALID      | N/A                      | PEV_UK                 |
| Foreign    | EVAL_GRP_CN       | POP_EVAL to POP_EVAL_GRP | PEV_PEG_FK             |

## 9.2.1 CN

Sequence number. A unique sequence number used to identify a population evaluation record.

## 9.2.2 EVAL\_GRP\_CN

Evaluation group sequence number. Foreign key linking the population evaluation record to the population evaluation group record.

## 9.2.3 RSCD

Region or Station code. See SURVEY.RSCD description for definition.

## 9.2.4 EVALID

Evaluation identifier. The EVALID is the unique identifier that represents the population used to produce a type of estimate. The EVALID is generally a concatenation of a 2-digit State code, a 2-digit year code, and a 2-digit evaluation type code (see REF\_POP\_EVAL\_TYP\_DESCR.EVAL\_TYP\_CD). For example, EVALID = 261600 represents the Michigan 2016 evaluation for all sampled and nonsampled plots.

If several types of evaluations are combined for an EVALID, the lowest evaluation type code number within the set is typically used for the last 2 digits of the EVALID. For example, the type code of 03 is used when the evaluation combines sampled plots for tree growth, removals, mortality, and area change estimates. However, the type code of 03 can also be used if the evaluation only combines sampled plots for tree growth and mortality.

## Example evaluation type code used for EVALID when evaluation types are combined:

|   Last 2 digits of EVALID | Evaluation type description                                                         |
|---------------------------|-------------------------------------------------------------------------------------|
|                        01 | Sampled plots used for current area and tree-level estimates.                       |
|                        03 | Sampled plots used for tree growth, removals, mortality, and area change estimates. |

## 9.2.5 EVAL\_DESCR

Evaluation description. A description of the area being evaluated (often a State), the time period of the evaluation, and the type of estimates that can be computed using the evaluation (e.g., area, volume, growth, removals, mortality). For example, 'MINNESOTA 2017: 2013-2017: CURRENT AREA, CURRENT VOLUME' is an evaluation description.

## 9.2.6 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 9.2.7 LOCATION\_NM

Location name. Geographic area as it would appear in the title of a report.

## 9.2.8 REPORT\_YEAR\_NM

Report year name. The data collection years that would appear in the title of a report.

## 9.2.9 START\_INVYR

Start inventory year. The starting year for the data included in the evaluation.

## 9.2.10 END\_INVYR

End inventory year. The ending year for the data included in the evaluation.

## 9.2.11 LAND\_ONLY

Land only. A code indicating area used in stratifying evaluations. See POP\_ESTN\_UNIT.AREA\_SOURCE for more information.

## Codes: LAND\_ONLY

| Code   | Description                                                    |
|--------|----------------------------------------------------------------|
| Y      | Only census land was used in the stratification process.       |
| N      | Census land and water were used in the stratification process. |

## 9.2.12 TIMBERLAND\_ONLY

Timberland only. A code indicting if the estimate can be made for timberland or for timberland and forest land. Timberland is a subset of forest land defined as nonreserved forest land capable of producing at least 20 cubic feet of wood volume per acre per year (COND.COND\_STATUS\_CD = 1, COND.RESERVCD = 0, COND.SITECLCD &lt;7).

## Codes: TIMBERLAND\_ONLY

| Code   | Description                                                                     |
|--------|---------------------------------------------------------------------------------|
| Y      | Only timberland attributes can be estimated for the evaluation.                 |
| N      | Both timberland and forest land attributes can be estimated for the evaluation. |

## 9.2.13 GROWTH\_ACCT

Growth accounting. A code indicating whether the evaluation can be used for growth accounting. This attribute is blank (null) when the POP\_EVAL\_TYP.EVAL\_TYP is not 'EXPGROW' evaluation type. See The Forest Inventory and Analysis Database: Population Estimation User Guide for examples of the growth accounting method.

## Codes: GROWTH\_ACCT

| Code   | Description                                          |
|--------|------------------------------------------------------|
| Y      | The evaluation can be used for growth accounting.    |
| N      | The evaluation cannot be used for growth accounting. |

## 9.2.14 ESTN\_METHOD

Estimation method. Describes the method of estimation. Post-stratification is used for most inventories where PLOT.MANUAL  1.0.

## Values

- · Simple random sampling
- · Stratified random sampling
- · Double sampling for stratification
- · Post-stratification
- · Subsampling units of unequal size

## 9.2.15 NOTES

Notes. Additional information related to the evaluation, such as notes pertaining to any special procedures that had to be implemented for the stratification method. This column may also include citation(s) for any publications that used the evaluation.

## 9.2.16 CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 9.2.17 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 9.2.18 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 9.2.19 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 9.2.20 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 9.2.21 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.