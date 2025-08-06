# Section 2.1: Survey Table
**Oracle Table Name:** SURVEY
**Extracted Pages:** 47-50 (Chapter pages 2-3 to 2-6)
**Source:** FIA Database Handbook v9.3
**Chapter:** 2 - Database Tables - Location Level

---

## 2.1 Survey Table

## (Oracle table name: SURVEY)

| Subsection   | Column name (attribute)   | Descriptive name          | Oracle data type   |
|--------------|---------------------------|---------------------------|--------------------|
| 2.1.1        | CN                        | Sequence number           | VARCHAR2(34)       |
| 2.1.2        | INVYR                     | Inventory year            | NUMBER(4)          |
| 2.1.3        | P3_OZONE_IND              | Phase 3 ozone indicator   | VARCHAR2(1)        |
| 2.1.4        | STATECD                   | State code                | NUMBER(4)          |
| 2.1.5        | STATEAB                   | State abbreviation        | VARCHAR2(2)        |
| 2.1.6        | STATENM                   | State name                | VARCHAR2(40)       |
| 2.1.7        | RSCD                      | Region or station code    | NUMBER(2)          |
| 2.1.8        | ANN_INVENTORY             | Annual inventory          | VARCHAR2(1)        |
| 2.1.9        | NOTES                     | Notes                     | VARCHAR2(2000)     |
| 2.1.10       | CREATED_BY                | Created by                | VARCHAR2(30)       |
| 2.1.11       | CREATED_DATE              | Created date              | DATE               |
| 2.1.12       | CREATED_IN_INSTANCE       | Created in instance       | VARCHAR2(6)        |
| 2.1.13       | MODIFIED_BY               | Modified by               | VARCHAR2(30)       |
| 2.1.14       | MODIFIED_DATE             | Modified date             | DATE               |
| 2.1.15       | MODIFIED_IN_INSTANCE      | Modified in instance      | VARCHAR2(6)        |
| 2.1.16       | CYCLE                     | Inventory cycle number    | NUMBER(2)          |
| 2.1.17       | SUBCYCLE                  | Inventory subcycle number | NUMBER(2)          |
| 2.1.18       | PRJ_CN                    | Project sequence number   | VARCHAR2(34)       |

| Key Type   | Column(s) order                                       | Tables to link    | Abbreviated notation   |
|------------|-------------------------------------------------------|-------------------|------------------------|
| Primary    | CN                                                    | N/A               | SRV_PK                 |
| Unique     | PRJ_CN, STATECD, INVYR, P3_OZONE_IND, CYCLE, SUBCYCLE | N/A               | SRV_UK                 |
| Foreign    | PRJ_CN                                                | SURVEY to PROJECT | SRV_PRJ_FK             |

## 2.1.1 CN

Sequence number. A unique sequence number used to identify a survey record.

## 2.1.2 INVYR

Inventory year. The year that best represents when the inventory data were collected. Under the annual inventory system, a group of plots is selected each year for sampling. The selection is based on a panel system. INVYR is the year in which the majority of plots in that group were collected (plots in the group have the same panel and, if applicable, subpanel). Under periodic inventory, a reporting inventory year was selected, usually based on the year in which the majority of the plots were collected or the mid-point of the

years over which the inventory spanned. For either annual or periodic inventory, INVYR is not necessarily the same as MEASYEAR.

## Exceptions:

INVYR = 9999. INVYR is set to 9999 to distinguish Phase 3 plots taken by the western FIA work units that are "off subpanel." This is due to differences in measurement intervals between Phase 3 (measurement interval = 5 years) and Phase 2 (measurement interval = 10 years) plots. Only users interested in performing certain Phase 3 data analyses should access plots with this anomalous value in INVYR.

## 2.1.3 P3\_OZONE\_IND

Phase 3 ozone indicator. A code indicating whether or not the survey is for a P3 ozone inventory.

Note: P3\_OZONE\_IND is part of the unique key because ozone data are stored as a separate inventory (survey); therefore, combinations of STATECD and INVYR may occur more than one time.

Codes: . P3\_OZONE\_IND

| Code   | Description                                     |
|--------|-------------------------------------------------|
| Y      | Yes, the survey is for a P3 ozone inventory.    |
| N      | No, the survey is not for a P3 ozone inventory. |

## 2.1.4 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 2.1.5 STATEAB

State abbreviation. The 2-character State abbreviation. Refer to appendix B.

## 2.1.6 STATENM

State name. Refer to appendix B.

## 2.1.7 RSCD

Region or Station code. Identification number of the Forest Service National Forest System Region or Station (FIA work unit) that provided the inventory data (see appendix B for more information).

## Codes: RSCD

|   Code | Description                                    |
|--------|------------------------------------------------|
|     22 | Rocky Mountain Research Station (RMRS).        |
|     23 | North Central Research Station (NCRS).         |
|     24 | Northeastern Research Station (NERS).          |
|     26 | Pacific Northwest Research Station (PNWRS).    |
|     27 | Pacific Northwest Research Station (PNWRS-AK). |
|     33 | Southern Research Station (SRS).               |

## 2.1.8 ANN\_INVENTORY

Annual inventory. A code indicating whether a particular inventory was collected as an annual inventory or as a periodic inventory.

## Codes: ANN\_INVENTORY

| Code   | Description                      |
|--------|----------------------------------|
| Y      | Yes, the inventory is annual.    |
| N      | No, the inventory is not annual. |

## 2.1.9 NOTES

Notes. An optional item where notes about the inventory may be stored.

## 2.1.10 CREATED\_BY

Created by. The employee who created the record. This attribute is intentionally left blank (null) in download files.

## 2.1.11 CREATED\_DATE

Created date. The date the record was created.

## 2.1.12 CREATED\_IN\_INSTANCE

Created in instance. The database instance in which the record was created. Each computer system has a unique database instance code and this attribute stores that information to determine on which computer the record was created.

## 2.1.13 MODIFIED\_BY

Modified by. The employee who modified the record. This field will be blank (null) if the data have not been modified since initial creation. This attribute is intentionally left blank in download files.

## 2.1.14 MODIFIED\_DATE

Modified date. The date the record was last modified. This field will be blank (null) if the data have not been modified since initial creation.

## 2.1.15 MODIFIED\_IN\_INSTANCE

Modified in instance. The database instance in which the record was modified. This field will be blank (null) if the data have not been modified since initial creation.

## 2.1.16 CYCLE

Inventory cycle number. A number assigned to a set of plots, measured over a particular period of time from which a State estimate using all possible plots is obtained. A cycle number &gt;1 does not necessarily mean that information for previous cycles resides in the database. A cycle is relevant for periodic and annual inventories.

## 2.1.17 SUBCYCLE

Inventory subcycle number. For an annual inventory that takes n years to measure all plots, subcycle shows in which of the n years of the cycle the data were measured. Subcycle is 0 for a periodic inventory. Subcycle 99 may be used for plots that are not included in the estimation process.

## 2.1.18 PRJ\_CN

Project sequence number. Foreign key linking the survey record to the project record.