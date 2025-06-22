# Section 3.1: Tree Table
**Oracle Table Name:** TREE
**Extracted Pages:** 161-232 (Chapter pages 3-3 to 3-74)
**Source:** FIA Database Handbook v9.3
**Chapter:** 3 - Database Tables - Tree Level

---

## 3.1 Tree Table

## (Oracle table name: TREE)

| Subsection   | Column name (attribute)   | Descriptive name                | Oracle data type   |
|--------------|---------------------------|---------------------------------|--------------------|
| 3.1.1        | CN                        | Sequence number                 | VARCHAR2(34)       |
| 3.1.2        | PLT_CN                    | Plot sequence number            | VARCHAR2(34)       |
| 3.1.3        | PREV_TRE_CN               | Previous tree sequence number   | VARCHAR2(34)       |
| 3.1.4        | INVYR                     | Inventory year                  | NUMBER(4)          |
| 3.1.5        | STATECD                   | State code                      | NUMBER(4)          |
| 3.1.6        | UNITCD                    | Survey unit code                | NUMBER(2)          |
| 3.1.7        | COUNTYCD                  | County code                     | NUMBER(3)          |
| 3.1.8        | PLOT                      | Plot number                     | NUMBER(5)          |
| 3.1.9        | SUBP                      | Subplot number                  | NUMBER(3)          |
| 3.1.10       | TREE                      | Tree number                     | NUMBER(9)          |
| 3.1.11       | CONDID                    | Condition class number          | NUMBER(1)          |
| 3.1.12       | AZIMUTH                   | Azimuth                         | NUMBER(3)          |
| 3.1.13       | DIST                      | Horizontal distance             | NUMBER(4,1)        |
| 3.1.14       | PREVCOND                  | Previous condition class number | NUMBER(1)          |
| 3.1.15       | STATUSCD                  | Status code                     | NUMBER(1)          |
| 3.1.16       | SPCD                      | Species code                    | NUMBER             |
| 3.1.17       | SPGRPCD                   | Species group code              | NUMBER(2)          |
| 3.1.18       | DIA                       | Current diameter                | NUMBER(5,2)        |
| 3.1.19       | DIAHTCD                   | Diameter height code            | NUMBER(1)          |
| 3.1.20       | HT                        | Total height                    | NUMBER(3)          |
| 3.1.21       | HTCD                      | Height method code              | NUMBER(2)          |
| 3.1.22       | ACTUALHT                  | Actual height                   | NUMBER(3)          |
| 3.1.23       | TREECLCD                  | Tree class code                 | NUMBER(2)          |
| 3.1.24       | CR                        | Compacted crown ratio           | NUMBER(3)          |
| 3.1.25       | CCLCD                     | Crown class code                | NUMBER(2)          |
| 3.1.26       | TREEGRCD                  | Tree grade code                 | NUMBER(2)          |
| 3.1.27       | AGENTCD                   | Cause of death (agent) code     | NUMBER(2)          |
| 3.1.28       | CULL                      | Rotten and missing cull         | NUMBER(3)          |
| 3.1.29       | DAMLOC1                   | Damage location 1               | NUMBER(2)          |
| 3.1.30       | DAMTYP1                   | Damage type 1                   | NUMBER(2)          |
| 3.1.31       | DAMSEV1                   | Damage severity 1               | NUMBER(1)          |
| 3.1.32       | DAMLOC2                   | Damage location 2               | NUMBER(2)          |
| 3.1.33       | DAMTYP2                   | Damage type 2                   | NUMBER(2)          |
| 3.1.34       | DAMSEV2                   | Damage severity 2               | NUMBER(1)          |

| Subsection   | Column name (attribute)   | Descriptive name                                                       | Oracle data type   |
|--------------|---------------------------|------------------------------------------------------------------------|--------------------|
| 3.1.35       | DECAYCD                   | Decay class code                                                       | NUMBER(2)          |
| 3.1.36       | STOCKING                  | Tree stocking                                                          | NUMBER(7,4)        |
| 3.1.37       | WDLDSTEM                  | Woodland tree species stem count                                       | NUMBER(3)          |
| 3.1.38       | VOLCFNET                  | Net cubic-foot stem wood volume                                        | NUMBER(11,6)       |
| 3.1.39       | VOLCFGRS                  | Gross cubic-foot stem wood volume                                      | NUMBER(11,6)       |
| 3.1.40       | VOLCSNET                  | Net cubic-foot wood volume in the sawlog portion of a sawtimber tree   | NUMBER(11,6)       |
| 3.1.41       | VOLCSGRS                  | Gross cubic-foot wood volume in the sawlog portion of a sawtimber tree | NUMBER(11,6)       |
| 3.1.42       | VOLBFNET                  | Net board-foot wood volume in the sawlog portion of a sawtimber tree   | NUMBER(11,6)       |
| 3.1.43       | VOLBFGRS                  | Gross board-foot wood volume in the sawlog portion of a sawtimber tree | NUMBER(11,6)       |
| 3.1.44       | VOLCFSND                  | Sound cubic-foot stem wood volume                                      | NUMBER(11,6)       |
| 3.1.45       | DIACHECK                  | Diameter check code                                                    | NUMBER(2)          |
| 3.1.46       | MORTYR                    | Mortality year                                                         | NUMBER(4)          |
| 3.1.47       | SALVCD                    | Salvable dead code                                                     | NUMBER(2)          |
| 3.1.48       | UNCRCD                    | Uncompacted live crown ratio                                           | NUMBER(3)          |
| 3.1.49       | CPOSCD                    | Crown position code                                                    | NUMBER(2)          |
| 3.1.50       | CLIGHTCD                  | Crown light exposure code                                              | NUMBER(2)          |
| 3.1.51       | CVIGORCD                  | Crown vigor code (sapling)                                             | NUMBER(2)          |
| 3.1.52       | CDENCD                    | Crown density code                                                     | NUMBER(3)          |
| 3.1.53       | CDIEBKCD                  | Crown dieback code                                                     | NUMBER(3)          |
| 3.1.54       | TRANSCD                   | Foliage transparency code                                              | NUMBER(3)          |
| 3.1.55       | TREEHISTCD                | Tree history code                                                      | NUMBER(3)          |
| 3.1.56       | BHAGE                     | Breast height age                                                      | NUMBER(4)          |
| 3.1.57       | TOTAGE                    | Total age                                                              | NUMBER(4)          |
| 3.1.58       | CULLDEAD                  | Dead cull                                                              | NUMBER(3)          |
| 3.1.59       | CULLFORM                  | Form cull                                                              | NUMBER(3)          |
| 3.1.60       | CULLMSTOP                 | Missing top cull                                                       | NUMBER(3)          |
| 3.1.61       | CULLBF                    | Board-foot cull                                                        | NUMBER(3)          |
| 3.1.62       | CULLCF                    | Cubic-foot cull                                                        | NUMBER(3)          |
| 3.1.63       | BFSND                     | Board-foot-cull soundness                                              | NUMBER(3)          |
| 3.1.64       | CFSND                     | Cubic-foot-cull soundness                                              | NUMBER(3)          |
| 3.1.65       | SAWHT                     | Sawlog height                                                          | NUMBER(2)          |
| 3.1.66       | BOLEHT                    | Bole height                                                            | NUMBER(3)          |
| 3.1.67       | FORMCL                    | Form class                                                             | NUMBER(1)          |
| 3.1.68       | HTCALC                    | Current height calculated                                              | NUMBER(3)          |

| Subsection   | Column name (attribute)   | Descriptive name                                       | Oracle data type   |
|--------------|---------------------------|--------------------------------------------------------|--------------------|
| 3.1.69       | HRDWD_CLUMP_CD            | Hardwood clump code                                    | NUMBER(1)          |
| 3.1.70       | SITREE                    | Calculated site index                                  | NUMBER(3)          |
| 3.1.71       | CREATED_BY                | Created by                                             | VARCHAR2(30)       |
| 3.1.72       | CREATED_DATE              | Created date                                           | DATE               |
| 3.1.73       | CREATED_IN_INSTANCE       | Created in instance                                    | VARCHAR2(6)        |
| 3.1.74       | MODIFIED_BY               | Modified by                                            | VARCHAR2(30)       |
| 3.1.75       | MODIFIED_DATE             | Modified date                                          | DATE               |
| 3.1.76       | MODIFIED_IN_INSTANCE      | Modified in instance                                   | VARCHAR2(6)        |
| 3.1.77       | MORTCD                    | Mortality code                                         | NUMBER(1)          |
| 3.1.78       | HTDMP                     | Height to diameter measurement point                   | NUMBER(3,1)        |
| 3.1.79       | ROUGHCULL                 | Rough cull                                             | NUMBER(2)          |
| 3.1.80       | MIST_CL_CD                | Mistletoe class code                                   | NUMBER(1)          |
| 3.1.81       | CULL_FLD                  | Rotten/missing cull, field recorded                    | NUMBER(2)          |
| 3.1.82       | RECONCILECD               | Reconcile code                                         | NUMBER(1)          |
| 3.1.83       | PREVDIA                   | Previous diameter                                      | NUMBER(5,2)        |
| 3.1.84       | P2A_GRM_FLG               | Periodic to annual growth, removal, and mortality flag | VARCHAR2(1)        |
| 3.1.85       | TREECLCD_NERS             | Tree class code, Northeastern Research Station         | NUMBER(2)          |
| 3.1.86       | TREECLCD_SRS              | Tree class code, Southern Research Station             | NUMBER(2)          |
| 3.1.87       | TREECLCD_NCRS             | Tree class code, North Central Research Station        | NUMBER(2)          |
| 3.1.88       | TREECLCD_RMRS             | Tree class code, Rocky Mountain Research Station       | NUMBER(2)          |
| 3.1.89       | STANDING_DEAD_CD          | Standing dead code                                     | NUMBER(2)          |
| 3.1.90       | PREV_STATUS_CD            | Previous tree status code                              | NUMBER(1)          |
| 3.1.91       | PREV_WDLDSTEM             | Previous woodland stem count                           | NUMBER(3)          |
| 3.1.92       | TPA_UNADJ                 | Trees per acre unadjusted                              | NUMBER(11,6)       |
| 3.1.93       | DRYBIO_BOLE               | Dry biomass of wood in the merchantable bole           | NUMBER(13,6)       |
| 3.1.94       | DRYBIO_STUMP              | Dry biomass of wood in the stump                       | NUMBER(13,6)       |
| 3.1.95       | DRYBIO_BG                 | Belowground dry biomass                                | NUMBER(13,6)       |
| 3.1.96       | CARBON_AG                 | Aboveground carbon of wood and bark                    | NUMBER(13,6)       |
| 3.1.97       | CARBON_BG                 | Belowground carbon                                     | NUMBER(13,6)       |
| 3.1.98       | CYCLE                     | Inventory cycle number                                 | NUMBER(2)          |
| 3.1.99       | SUBCYCLE                  | Inventory subcycle number                              | NUMBER(2)          |

| Subsection   | Column name (attribute)   | Descriptive name                                                      | Oracle data type   |
|--------------|---------------------------|-----------------------------------------------------------------------|--------------------|
| 3.1.100      | BORED_CD_PNWRS            | Tree bored code, Pacific Northwest Research Station                   | NUMBER(1)          |
| 3.1.101      | DAMLOC1_PNWRS             | Damage location 1, Pacific Northwest Research Station                 | NUMBER(2)          |
| 3.1.102      | DAMLOC2_PNWRS             | Damage location 2, Pacific Northwest Research Station                 | NUMBER(2)          |
| 3.1.103      | DIACHECK_PNWRS            | Diameter check, Pacific Northwest Research Station                    | NUMBER(1)          |
| 3.1.104      | DMG_AGENT1_CD_PNWRS       | Damage agent 1, Pacific Northwest Research Station                    | NUMBER(2)          |
| 3.1.105      | DMG_AGENT2_CD_PNWRS       | Damage agent 2, Pacific Northwest Research Station                    | NUMBER(2)          |
| 3.1.106      | DMG_AGENT3_CD_PNWRS       | Damage agent 3, Pacific Northwest Research Station                    | NUMBER(2)          |
| 3.1.107      | MIST_CL_CD_PNWRS          | Leafy mistletoe class code, Pacific Northwest Research Station        | NUMBER(1)          |
| 3.1.108      | SEVERITY1_CD_PNWRS        | Damage severity 1, Pacific Northwest Research Station                 | NUMBER(1)          |
| 3.1.109      | SEVERITY1A_CD_PNWRS       | Damage severity 1A, Pacific Northwest Research Station                | NUMBER(2)          |
| 3.1.110      | SEVERITY1B_CD_PNWRS       | Damage severity 1B, Pacific Northwest Research Station                | NUMBER(1)          |
| 3.1.111      | SEVERITY2_CD_PNWRS        | Damage severity 2, Pacific Northwest Research Station                 | NUMBER(1)          |
| 3.1.112      | SEVERITY2A_CD_PNWRS       | Damage severity 2A, Pacific Northwest Research Station                | NUMBER(2)          |
| 3.1.113      | SEVERITY2B_CD_PNWRS       | Damage severity 2B, Pacific Northwest Research Station                | NUMBER(1)          |
| 3.1.114      | SEVERITY3_CD_PNWRS        | Damage severity 3, Pacific Northwest Research Station                 | NUMBER(1)          |
| 3.1.115      | UNKNOWN_DAMTYP1_PNWRS     | Unknown damage type 1, Pacific Northwest Research Station             | NUMBER(1)          |
| 3.1.116      | UNKNOWN_DAMTYP2_PNWRS     | Unknown damage type 2, Pacific Northwest Research Station             | NUMBER(1)          |
| 3.1.117      | PREV_PNTN_SRS             | Previous periodic prism point, tree number, Southern Research Station | NUMBER(4)          |
| 3.1.118      | DISEASE_SRS               | Disease, Southern Research Station                                    | NUMBER(1)          |
| 3.1.119      | DIEBACK_SEVERITY_SRS      | Dieback severity, Southern Research Station                           | NUMBER(2)          |
| 3.1.120      | DAMAGE_AGENT_CD1          | Damage agent code 1                                                   | NUMBER(5)          |
| 3.1.121      | DAMAGE_AGENT_CD2          | Damage agent code 2                                                   | NUMBER(5)          |
| 3.1.122      | DAMAGE_AGENT_CD3          | Damage agent code 3                                                   | NUMBER(5)          |
| 3.1.123      | CENTROID_DIA              | Centroid diameter (Pacific Islands)                                   | NUMBER(4,1)        |

| Subsection   | Column name (attribute)   | Descriptive name                                                       | Oracle data type   |
|--------------|---------------------------|------------------------------------------------------------------------|--------------------|
| 3.1.124      | CENTROID_DIA_HT           | Calculated centroid diameter height (Pacific Islands)                  | NUMBER(4,1)        |
| 3.1.125      | CENTROID_DIA_HT_ACTUAL    | Actual centroid diameter height (Pacific Islands)                      | NUMBER(4,1)        |
| 3.1.126      | UPPER_DIA                 | Upper stem diameter (Pacific Islands)                                  | NUMBER(4,1)        |
| 3.1.127      | UPPER_DIA_HT              | Upper stem diameter height (Pacific Islands)                           | NUMBER(4,1)        |
| 3.1.128      | VOLCSSND                  | Sound cubic-foot wood volume in the sawlog portion of a sawtimber tree | NUMBER(11,6)       |
| 3.1.129      | DRYBIO_SAWLOG             | Dry biomass of wood in the sawlog portion of a sawtimber tree          | NUMBER(13,6)       |
| 3.1.130      | DAMAGE_AGENT_CD1_SRS      | Damage agent code 1 (Caribbean Islands), Southern Research Station     | NUMBER(5)          |
| 3.1.131      | DAMAGE_AGENT_CD2_SRS      | Damage agent code 2 (Caribbean Islands), Southern Research Station     | NUMBER(5)          |
| 3.1.132      | DAMAGE_AGENT_CD3_SRS      | Damage agent code 3 (Caribbean Islands), Southern Research Station     | NUMBER(5)          |
| 3.1.133      | DRYBIO_AG                 | Aboveground dry biomass of wood and bark                               | NUMBER(13,6)       |
| 3.1.134      | ACTUALHT_CALC             | Actual height, calculated                                              | NUMBER(3)          |
| 3.1.135      | ACTUALHT_CALC_CD          | Actual height, calculated, code                                        | NUMBER(1)          |
| 3.1.136      | CULL_BF_ROTTEN            | Rotten/missing board-foot cull of the sawlog                           | NUMBER(12,9)       |
| 3.1.137      | CULL_BF_ROTTEN_CD         | Rotten/missing board-foot cull of the sawlog code                      | NUMBER(2)          |
| 3.1.138      | CULL_BF_ROUGH             | Rough board-foot cull of the sawlog                                    | NUMBER(12,9)       |
| 3.1.139      | CULL_BF_ROUGH_CD          | Rough board-foot cull of the sawlog code                               | NUMBER(2)          |
| 3.1.140      | PREVDIA_FLD               | Previous diameter, field                                               | NUMBER             |
| 3.1.141      | TREECLCD_31_NCRS          | Tree class code (version 3.1), North Central Research Station          | NUMBER(1)          |
| 3.1.142      | TREE_GRADE_NCRS           | Tree grade, North Central Research Station                             | NUMBER(3)          |
| 3.1.143      | BOUGHS_AVAILABLE_NCRS     | Balsam fir boughs available, North Central Research Station            | NUMBER(1)          |
| 3.1.144      | BOUGHS_HRVST_NCRS         | Balsam fir boughs harvested, North Central Research Station            | NUMBER(1)          |
| 3.1.145      | TREECLCD_31_NERS          | Tree class code (version 3.1), Northeastern Research Station           | NUMBER(1)          |

| Subsection   | Column name (attribute)   | Descriptive name                                                                       | Oracle data type   |
|--------------|---------------------------|----------------------------------------------------------------------------------------|--------------------|
| 3.1.146      | AGENTCD_NERS              | General damage / cause of death (agent) code, Northeastern Research Station            | NUMBER(2)          |
| 3.1.147      | BFSNDCD_NERS              | Board-foot soundness code, Northeastern Research Station                               | NUMBER(1)          |
| 3.1.148      | AGECHKCD_RMRS             | Radial growth and tree age check code, Rocky Mountain Research Station                 | NUMBER(1)          |
| 3.1.149      | PREV_AGECHKCD_RMRS        | Previous radial growth and tree age check code, Rocky Mountain Research Station        | NUMBER(1)          |
| 3.1.150      | PREV_BHAGE_RMRS           | Previous breast height age, Rocky Mountain Research Station                            | NUMBER(4)          |
| 3.1.151      | PREV_TOTAGE_RMRS          | Previous total age, Rocky Mountain Research Station                                    | NUMBER(4)          |
| 3.1.152      | PREV_TREECLCD_RMRS        | Previous tree class code, Rocky Mountain Research Station                              | NUMBER(2)          |
| 3.1.153      | RADAGECD_RMRS             | Radial growth / age code, Rocky Mountain Research Station                              | NUMBER(1)          |
| 3.1.154      | RADGRW_RMRS               | Radial growth, Rocky Mountain Research Station                                         | NUMBER(2)          |
| 3.1.155      | VOLBSGRS                  | Gross board-foot wood volume in the sawlog portion of a sawtimber tree (Scribner Rule) | NUMBER(11,6)       |
| 3.1.156      | VOLBSNET                  | Net board-foot wood volume in the sawlog portion of a sawtimber tree (Scribner Rule)   | NUMBER(11,6)       |
| 3.1.157      | SAPLING_FUSIFORM_SRS      | Sapling fusiform, Southern Research Station                                            | NUMBER(1)          |
| 3.1.158      | EPIPHYTE_PNWRS            | Epiphyte loading (Pacific Islands), Pacific Northwest Research Station                 | NUMBER(1)          |
| 3.1.159      | ROOT_HT_PNWRS             | Rooting height (Pacific Islands), Pacific Northwest Research Station                   | NUMBER(2)          |
| 3.1.160      | CAVITY_USE_PNWRS          | Cavity presence, Pacific Northwest Research Station                                    | VARCHAR2(1)        |
| 3.1.161      | CORE_LENGTH_PNWRS         | Length of measured core, Pacific Northwest Research Station                            | NUMBER(4,1)        |
| 3.1.162      | CULTURALLY_KILLED_PNWRS   | Culturally killed code, Pacific Northwest Research Station                             | NUMBER(1)          |
| 3.1.163      | DIA_EST_PNWRS             | Standing dead estimated diameter, Pacific Northwest Research Station                   | NUMBER(4,1)        |
| 3.1.164      | GST_PNWRS                 | Growth sample tree, Pacific Northwest Research Station                                 | VARCHAR2(1)        |
| 3.1.165      | INC10YR_PNWRS             | 10-year increment, Pacific Northwest Research Station                                  | NUMBER(3)          |

| Subsection   | Column name (attribute)         | Descriptive name                                                       | Oracle data type   |
|--------------|---------------------------------|------------------------------------------------------------------------|--------------------|
| 3.1.166      | INC5YRHT_PNWRS                  | 5-year height growth, Pacific Northwest Research Station               | NUMBER(3,1)        |
| 3.1.167      | INC5YR_PNWRS                    | 5-year increment, Pacific Northwest Research Station                   | NUMBER(3)          |
| 3.1.168      | RING_COUNT_INNER_2INCHES_PN WRS | Number of rings in inner 2 inches, Pacific Northwest Research Station  | NUMBER(3)          |
| 3.1.169      | RING_COUNT_PNWRS                | Number of rings, Pacific Northwest Research Station                    | NUMBER(3)          |
| 3.1.170      | SNAG_DIS_CD_PNWRS               | Snag reason for disappearance code, Pacific Northwest Research Station | NUMBER(1)          |
| 3.1.171      | CONEPRESCD1                     | Cone presence code 1                                                   | NUMBER(1)          |
| 3.1.172      | CONEPRESCD2                     | Cone presence code 2                                                   | NUMBER(1)          |
| 3.1.173      | CONEPRESCD3                     | Cone presence code 3                                                   | NUMBER(1)          |
| 3.1.174      | MASTCD                          | Mast code                                                              | NUMBER(1)          |
| 3.1.175      | VOLTSGRS                        | Gross cubic-foot total-stem wood volume                                | NUMBER(13,6)       |
| 3.1.176      | VOLTSGRS_BARK                   | Gross cubic-foot total-stem bark volume                                | NUMBER(13,6)       |
| 3.1.177      | VOLTSSND                        | Sound cubic-foot total-stem wood volume                                | NUMBER(13,6)       |
| 3.1.178      | VOLTSSND_BARK                   | Sound cubic-foot total-stem bark volume                                | NUMBER(13,6)       |
| 3.1.179      | VOLCFGRS_STUMP                  | Gross cubic-foot stump wood volume                                     | NUMBER(13,6)       |
| 3.1.180      | VOLCFGRS_STUMP_BARK             | Gross cubic-foot stump bark volume                                     | NUMBER(13,6)       |
| 3.1.181      | VOLCFSND_STUMP                  | Sound cubic-foot stump wood volume                                     | NUMBER(13,6)       |
| 3.1.182      | VOLCFSND_STUMP_BARK             | Sound cubic-foot stump bark volume                                     | NUMBER(13,6)       |
| 3.1.183      | VOLCFGRS_BARK                   | Gross cubic-foot stem bark volume                                      | NUMBER(13,6)       |
| 3.1.184      | VOLCFGRS_TOP                    | Gross cubic-foot stem-top wood volume                                  | NUMBER(13,6)       |
| 3.1.185      | VOLCFGRS_TOP_BARK               | Gross cubic-foot stem-top bark volume                                  | NUMBER(13,6)       |
| 3.1.186      | VOLCFSND_BARK                   | Sound cubic-foot stem bark volume                                      | NUMBER(13,6)       |
| 3.1.187      | VOLCFSND_TOP                    | Sound cubic-foot stem-top wood volume                                  | NUMBER(13,6)       |
| 3.1.188      | VOLCFSND_TOP_BARK               | Sound cubic-foot stem-top bark volume                                  | NUMBER(13,6)       |
| 3.1.189      | VOLCFNET_BARK                   | Net cubic-foot stem bark volume                                        | NUMBER(13,6)       |
| 3.1.190      | VOLCSGRS_BARK                   | Gross cubic-foot bark volume in the sawlog portion of a sawtimber tree | NUMBER(13,6)       |

| Subsection   | Column name (attribute)   | Descriptive name                                                       | Oracle data type   |
|--------------|---------------------------|------------------------------------------------------------------------|--------------------|
| 3.1.191      | VOLCSSND_BARK             | Sound cubic-foot bark volume in the sawlog portion of a sawtimber tree | NUMBER(13,6)       |
| 3.1.192      | VOLCSNET_BARK             | Net cubic-foot bark volume in the sawlog portion of a sawtimber tree   | NUMBER(13,6)       |
| 3.1.193      | DRYBIO_STEM               | Dry biomass of wood in the total stem                                  | NUMBER(13,6)       |
| 3.1.194      | DRYBIO_STEM_BARK          | Dry biomass of bark in the total stem                                  | NUMBER(13,6)       |
| 3.1.195      | DRYBIO_STUMP_BARK         | Dry biomass of bark in the stump                                       | NUMBER(13,6)       |
| 3.1.196      | DRYBIO_BOLE_BARK          | Dry biomass of bark in the merchantable bole                           | NUMBER(13,6)       |
| 3.1.197      | DRYBIO_BRANCH             | Dry biomass of branches                                                | NUMBER(13,6)       |
| 3.1.198      | DRYBIO_FOLIAGE            | Dry biomass of foliage                                                 | NUMBER(13,6)       |
| 3.1.199      | DRYBIO_SAWLOG_BARK        | Dry biomass of bark in the sawlog portion of a sawtimber tree          | NUMBER(13,6)       |
| 3.1.200      | PREV_ACTUALHT_FLD         | Previous actual height                                                 | NUMBER(3)          |
| 3.1.201      | PREV_HT_FLD               | Previous total height                                                  | NUMBER(3)          |
| 3.1.202      | UTILCLCD                  | Utilization class code                                                 | NUMBER(1)          |

| Key Type   | Column(s) order                                    | Tables to link   | Abbreviated notation   |
|------------|----------------------------------------------------|------------------|------------------------|
| Primary    | CN                                                 | N/A              | TRE_PK                 |
| Unique     | PLT_CN, SUBP, TREE                                 | N/A              | TRE_UK                 |
| Natural    | STATECD, INVYR, UNITCD, COUNTYCD, PLOT, SUBP, TREE | N/A              | TRE_NAT_I              |
| Foreign    | PLT_CN                                             | TREE to PLOT     | TRE_PLT_FK             |

Prior to October 2006, there were two separate research stations in the North, the Northeastern Research Station (NERS) and the North Central Research Station (NCRS).

The NERS region included the following States: Connecticut, Delaware, Maine, Maryland, Massachusetts, New Hampshire, New Jersey, New York, Pennsylvania, Ohio, Rhode Island, Vermont, and West Virginia.

The NCRS region included the following States: Illinois, Indiana, Iowa, Kansas, Michigan, Minnesota, Missouri, Nebraska, North Dakota, South Dakota, and Wisconsin.

In October 2006, these two research stations were combined into one, the Northern Research Station (NRS). Following the database structure created prior to the merger, regional data collected by the NRS are currently split into NCRS and NERS columns determined by the State of data collection.

Since the merger starting at PLOT.MANUAL = 3.1, there has been only one regional field guide for all NRS States, the regional NRS field guide. In the database, however, there are attributes named MANUAL\_NERS and MANUAL\_NCRS. Only one of these attributes is populated; the other is blank (NULL), depending on the State of data collection.

## 3.1.1 CN

Sequence number. A unique sequence number used to identify a tree record.

## 3.1.2 PLT\_CN

Plot sequence number. Foreign key linking the tree record to the plot record.

## 3.1.3 PREV\_TRE\_CN

Previous tree sequence number. Foreign key linking the tree to the previous inventory's tree record for this tree. Only populated on trees remeasured from a previous annual inventory.

## 3.1.4 INVYR

Inventory year. See SURVEY.INVYR description for definition.

## 3.1.5 STATECD

State code. Bureau of the Census Federal Information Processing Standards (FIPS) 2-digit code for each State. Refer to appendix B.

## 3.1.6 UNITCD

Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. For periodic inventories, survey units may be made up of lands of particular owners. Refer to appendix B for codes.

## 3.1.7 COUNTYCD

County code. The identification number for a county, parish, watershed, borough, or similar governmental unit in a State. FIPS codes from the Bureau of the Census are used. Refer to appendix B for codes.

Note: Summarizing data by county is not recommended for Alaska datasets. For assistance with analyses for Alaska, please consult the PNWRS analyst contact listed in table 1-1.

## 3.1.8 PLOT

Plot number. An identifier for a plot. Along with STATECD, INVYR, UNITCD, COUNTYCD and/or some other combinations of variables, PLOT may be used to uniquely identify a plot.

## 3.1.9 SUBP

Subplot number. The number assigned to the subplot. The national plot design (PLOT.DESIGNCD = 1) has subplot number values of 1 through 4. Other plot designs have various subplot number values. See PLOT.DESIGNCD and appendix G for information about plot designs. For more explanation about SUBP, contact the appropriate FIA work unit (table 1-1).

## 3.1.10 TREE

Tree number. A number used to uniquely identify a tree on a subplot. Tree numbers can be used to track trees when PLOT.DESIGNCD is the same between inventories.

## 3.1.11 CONDID

Condition class number. The unique identifying number assigned to a condition on which the tree is located, and is defined in the COND table. See COND.CONDID for details on the attributes which delineate a condition.

## 3.1.12 AZIMUTH

Azimuth. This attribute now available from the FIA Spatial Data Services (SDS) group by following the instructions provided at the following web address: https://www.fs.usda.gov/research/programs/fia/sds.

## 3.1.13 DIST

Horizontal distance. This attribute now available from the FIA Spatial Data Services (SDS) group by following the instructions provided at the following web address: https://www.fs.usd.gov/research/programs/fia/sds.

## 3.1.14 PREVCOND

Previous condition class number. This Identifies the condition within the plot on which the tree occurred at the previous inventory.

## 3.1.15 STATUSCD

Status code. A code indicating whether the sample tree is live, cut, or dead at the time of measurement. Includes dead and cut trees, which are required to estimate aboveground biomass and net annual volume for growth, mortality, and removals. This code is not used when querying data for change estimates.

Note: New and replacement plots use only codes 1 and 2.

## Codes: STATUSCD

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                           |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | No status - Tree is not presently in the sample (remeasurement plots only). Tree was incorrectly tallied at the previous inventory, currently not tallied due to definition or procedural change, or is not tallied because it is located on a nonsampled condition (e.g., hazardous or denied). RECONCILECD = 5-9 required for remeasured annual inventory data but not for periodic inventory data. |
|      1 | Live tree.                                                                                                                                                                                                                                                                                                                                                                                            |
|      2 | Dead tree.                                                                                                                                                                                                                                                                                                                                                                                            |
|      3 | Retired code - Starting with PLOT.MANUAL = 9.3, this code is only valid for some periodic-to-periodic, periodic-to-annual, and modeled GRM estimates. Only populated by certain FIA work units (SURVEY.RSCD = 23, 24). Removed - Cut and removed by direct human activity related to harvesting, silviculture, or land clearing. This tree is assumed to be utilized.                                 |

## 3.1.16 SPCD

Species code. An FIA tree species code. Refer to appendix F for codes.

## 3.1.17 SPGRPCD

Species group code. A code assigned to each tree species to group them for reporting purposes. Codes and their associated names (see REF\_SPECIES\_GROUP.NAME) are

shown in appendix E. Refer to appendix F for individual tree species and corresponding species group codes.

## 3.1.18 DIA

Current diameter. The current diameter, in inches, of the sample tree at the point of diameter measurement. For timber species, diameter is measured at breast height (d.b.h.), which is usually measured at 4.5 feet above the ground line on the uphill side of the tree. For woodland species, which are often multi-stemmed, diameter is measured at the ground line or at the stem root collar (d.r.c.), whichever is higher. DIA for woodland species (DRC) is computed using the following formula:

DRC = SQRT [SUM (stem diameter 2 )]

For additional information about where the tree diameter is measured, see DIAHTCD or HTDMP. DIA for live trees contains the measured value. DIA for cut and dead trees presents problems associated with uncertainty of when the tree was cut or died as well as structural deterioration of dead trees. Consult individual FIA work units (table 1-1) for explanations of how DIA is collected for dead and cut trees.

## 3.1.19 DIAHTCD

Diameter height code. A code indicating the location at which diameter was measured. For trees with code 1 (d.b.h.), the actual measurement point may be found in HTDMP.

## Codes: DIAHTCD

|   Code | Description             |
|--------|-------------------------|
|      1 | Breast height (d.b.h.). |
|      2 | Root collar (d.r.c.).   |

## 3.1.20 HT

Total height. (All live and standing dead tally trees  1.0 inch d.b.h./d.r.c.) The total length (height) of a tree, in feet, from the ground to the tip of the apical meristem beginning in PLOT.MANUAL = 1.1. The total length of a tree is not always its actual length. If the main stem is broken, the actual length is measured or estimated and the missing piece is added to the actual length to estimate total length. The amount added is determined by measuring the broken piece if it can be located on the ground; otherwise it is estimated. The minimum height for timber species is 5 feet and for woodland species is 1 foot. Starting with PLOT.MANUAL = 7.0, the core minimum diameter to qualify for a standing dead tree was changed from 5.0 inches to 1.0 inch. For multi-stemmed woodland species, this attribute is based on the length of the longest stem present.

Note: Prior to PLOT.MANUAL = 7.0, this attribute was tallied as follows:

- · Core Phase 2:  5.0-inch d.b.h./d.r.c. live trees.
- · Core optional Phase 2: 1.0-4.9-inch d.b.h./d.r.c. live trees and  5.0-inch d.b.h./d.r.c. standing dead trees.
- · Core Phase 3:  1.0-inch d.b.h./d.r.c. live trees.
- · Core optional Phase 3:  5.0-inch d.b.h./d.r.c. standing dead trees.

## 3.1.21 HTCD

Height method code. (All live and standing dead tally trees  1.0 inch d.b.h./d.r.c.) A code indicating how length (height) was determined beginning in PLOT.MANUAL = 1.1. Starting with PLOT.MANUAL = 7.0, the core minimum diameter to qualify for a standing dead tree was changed from 5.0 inches to 1.0 inch.

Note: Prior to PLOT.MANUAL = 7.0, this attribute was tallied as follows:

- · Core Phase 2:  5.0-inch d.b.h./d.r.c. live trees.
- · Core optional Phase 2: 1.0-4.9-inch d.b.h./d.r.c. live trees and  5.0-inch d.b.h./d.r.c. standing dead trees.
- · Core Phase 3:  1.0-inch d.b.h./d.r.c. live trees.
- · Core optional Phase 3:  5.0-inch d.b.h./d.r.c. standing dead trees.

## Codes: HTCD

|   Code | Description                                                           |
|--------|-----------------------------------------------------------------------|
|      1 | Field measured (total and actual length).                             |
|      2 | Total length visually estimated in the field, actual length measured. |
|      3 | Total and actual lengths are visually estimated.                      |
|      4 | Estimated with a model.                                               |

## 3.1.22 ACTUALHT

Actual height. (All live and standing dead tally trees  1.0 inch d.b.h./d.r.c.) The length (height) of the tree to the nearest foot from ground level to the highest remaining portion of the tree still present and attached to the bole. If ACTUALHT = HT, then the tree does not have a broken top. If ACTUALHT &lt;HT, then the tree does have a broken or missing top. The minimum height for timber species is 5 feet and for woodland species is 1 foot. Starting with PLOT.MANUAL = 7.0, the core minimum diameter to qualify for a standing dead tree was changed from 5.0 inches to 1.0 inch.

Note: Prior to PLOT.MANUAL = 7.0, this attribute was tallied as follows:

- · Core Phase 2: live and standing dead trees with broken tops,  5.0 inches d.b.h./d.r.c.
- · Core optional Phase 2: live trees 1.0-4.9 inches d.b.h./d.r.c. with broken or missing tops.
- · Core Phase 3: live trees  1.0 inch d.b.h./d.r.c. (with broken or missing tops) and standing dead trees  5.0 inches d.b.h./d.r.c. (with broken or missing tops).

## 3.1.23 TREECLCD

Tree class code. A code indicating the general quality of the tree. In annual inventory, this is the tree class for both live and dead trees at the time of current measurement. In periodic inventory, for cut and dead trees, this is the tree class of the tree at the time it died or was cut. Therefore, cut and dead trees collected in periodic inventory can be coded as growing-stock trees.

## Codes: TREECLCD

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      2 | Growing stock - All live trees of commercial species that meet minimum merchantability standards. In general, these trees have at least one solid 8-foot section, are reasonably free of form defect on the merchantable bole, and at least 34 percent or more of the volume is merchantable. For the California, Oregon, Washington, and Alaska inventories, a 26 percent or more merchantable volume standard is applied, rather than 34 percent or more. Excludes rough or rotten cull trees.                                                                                                                                                                                                                                                                                                                            |
|      3 | Rough cull - All live trees that do not now, or prospectively, have at least one solid 8-foot section, reasonably free of form defect on the merchantable bole, or have 67 percent or more of the merchantable volume cull; and more than half of this cull is due to sound dead wood cubic-foot loss or severe form defect volume loss. For the California, Oregon, Washington, and Alaska inventories, 75 percent or more cull, rather than 67 percent or more cull, applies. This class also contains all trees of noncommercial species, or those species where SPGRPCD equals 23 (woodland softwoods), 43 (eastern noncommercial hardwoods), or 48 (woodland hardwoods). Refer to appendix F to find the species that have these SPGRPCD codes. For dead trees, this code indicates that the tree is salvable (sound). |
|      4 | Rotten cull - All live trees with 67 percent or more of the merchantable volume cull, and more than half of this cull is due to rotten or missing cubic-foot volume loss. California, Oregon, Washington, and Alaska inventories use a 75 percent cutoff. For dead trees, this code indicates that the tree is nonsalvable (not sound).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

## 3.1.24 CR

Compacted crown ratio. The percent of the tree bole supporting live, healthy foliage (the crown is ocularly compacted to fill in gaps) when compared to actual length (ACTUALHT). When PLOT.MANUAL &lt;1.0 the variable may have been a code, which was converted to the midpoint of the ranges represented by the codes, and is stored as a percentage. May not be populated for periodic inventories.

## 3.1.25 CCLCD

Crown class code. A code indicating the amount of sunlight received and the crown position within the canopy.

## Codes: CCLCD

|   Code | Description                                                                                                                                                                                                                                                                                      |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Open grown - Trees with crowns that have received full light from above and from all sides throughout all or most of their life, particularly during early development.                                                                                                                          |
|      2 | Dominant - Trees with crowns extending above the general level of the canopy and receiving full light from above and partly from the sides; larger than the average trees in the stand, and with crowns well developed, but possibly somewhat crowded on the sides.                              |
|      3 | Codominant - Trees with crowns forming part of the general level of the canopy cover and receiving full light from above, but comparatively little from the side. Usually with medium crowns more or less crowded on the sides.                                                                  |
|      4 | Intermediate - Trees shorter than those in the preceding two classes, with crowns either below or extending into the canopy formed by the dominant and codominant trees, receiving little direct light from above, and none from the sides; usually with small crowns very crowded on the sides. |
|      5 | Overtopped - Trees with crowns entirely below the general canopy level and receiving no direct light either from above or the sides.                                                                                                                                                             |

## 3.1.26 TREEGRCD

Tree grade code. A code indicating the quality of sawtimber trees. This attribute is populated for live, growing-stock, sawtimber trees on subplots 1-4 where PLOT.MANUAL  1.0 for plots that are in a forest condition class. This attribute may be populated for other tree records that do not meet the above criteria. For example, it may be populated with the previous tree grade on dead and cut trees. Standards for tree grading are specific to species and differ slightly by research station. Only populated by certain FIA work units (SURVEY.RSCD = 23, 24, 33). Tree grade codes  along with explanations of the codes and when they are used can be found in the Northern regional field guide (SURVEY.RSCD = 23, 24) and Southern regional field guide (SURVEY.RSCD = 33).

## 3.1.27 AGENTCD

Cause of death (agent) code. (core: all remeasured plots when the tree was alive at the previous visit and at revisit is dead or removed OR the tree is standing dead in the current inventory and the tree is ingrowth, through growth, or a missed live tree; core optional: all initial plot visits when tree qualifies as a mortality tree) When PLOT.MANUAL  1.0, this attribute was collected on only dead and cut trees. When PLOT.MANUAL &lt;1.0, this attribute was collected on all trees (live, dead, and cut). Cause of damage was recorded for live trees if the presence of damage or pathogen activity was serious enough to reduce the quality or vigor of the tree. When a tree was damaged by more than one agent, the most severe damage was coded. When no damage was observed on a live tree, 00 was recorded. Damage recorded for dead trees was the cause of death. Each FIA program records specific codes that may differ from one State to the next. These codes fall within the ranges listed below. For the specific codes used in a particular State, contact the FIA work unit responsible for that State (table 1-1).

## Codes: AGENTCD

|   Code | Description                                                                                                                                                              |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     00 | No agent recorded (only allowed on live trees in data prior to 1999).                                                                                                    |
|     10 | Insect.                                                                                                                                                                  |
|     20 | Disease.                                                                                                                                                                 |
|     30 | Fire.                                                                                                                                                                    |
|     40 | Animal.                                                                                                                                                                  |
|     50 | Weather.                                                                                                                                                                 |
|     60 | Vegetation (e.g., suppression, competition, vines/kudzu).                                                                                                                |
|     70 | Unknown / not sure / other - includes death from human activity not related to silvicultural or landclearing activity (accidental, random, etc.).                        |
|     80 | Silvicultural or landclearing activity (death caused by harvesting or other silvicultural activity, including girdling, chaining, etc., or other landclearing activity). |

## 3.1.28 CULL

Rotten and missing cull. The percent of the cubic-foot volume in a live or dead tally tree that is rotten or missing. This is a calculated value that is derived from the field-recorded cull estimate (see CULL\_FLD) or a modeled cull estimate.

## 3.1.29 DAMLOC1

Damage location 1. (core where PLOT.MANUAL = 1.0 through 1.6; core optional beginning with PLOT.MANUAL = 1.7) A code indicating where damage (meeting or exceeding a severity threshold, as defined in the field guide) is present on the tree.

## Codes: DAMLOC1

|   Code | Description                                                                       |
|--------|-----------------------------------------------------------------------------------|
|      0 | No damage.                                                                        |
|      1 | Roots (exposed) and stump (up to 12 inches from ground level).                    |
|      2 | Roots, stump, and lower bole.                                                     |
|      3 | Lower bole (lower half of bole between stump and base of live crown).             |
|      4 | Lower and upper bole.                                                             |
|      5 | Upper bole (upper half of bole between stump and base of live crown).             |
|      6 | Crownstem (main stem within the live crown).                                      |
|      7 | Branches (>1 inch diameter at junction with main stem and within the live crown). |
|      8 | Buds and shoots of current year.                                                  |
|      9 | Foliage.                                                                          |

## 3.1.30 DAMTYP1

Damage type 1. (core where PLOT.MANUAL = 1.0 through 1.6; core optional beginning with PLOT.MANUAL = 1.7) A code indicating the kind of damage (meeting or exceeding a severity threshold, as defined in the field guide) present. If DAMLOC1 = 0, then DAMTYP1 = blank (null).

## Codes: DAMTYP1

|   Code | Description                                          |
|--------|------------------------------------------------------|
|     01 | Canker, gall.                                        |
|     02 | Conk, fruiting body, or sign of advanced decay.      |
|     03 | Open wound.                                          |
|     04 | Resinosis or gumosis.                                |
|     05 | Crack or seam.                                       |
|     11 | Broken bole or broken root within 3 feet of bole.    |
|     12 | Broom on root or bole.                               |
|     13 | Broken or dead root further than 3 feet from bole.   |
|     20 | Vines in the crown.                                  |
|     21 | Loss of apical dominance, dead terminal.             |
|     22 | Broken or dead branches.                             |
|     23 | Excessive branching or brooms within the live crown. |
|     24 | Damaged shoots, buds, or foliage.                    |
|     25 | Discoloration of foliage.                            |
|     31 | Other.                                               |

## 3.1.31 DAMSEV1

Damage severity 1. (core where PLOT.MANUAL = 1.0 through 1.6; core optional beginning with PLOT.MANUAL = 1.7) A code indicating how much of the tree is affected. Valid severity codes vary by damage type and damage location and must exceed a threshold value, as defined in the field guide. If DAMLOC1 = 0, then DAMSEV1 = blank (null).

## Codes: DAMSEV1

|   Code | Description                     |
|--------|---------------------------------|
|      0 | 01 to 09% of location affected. |
|      1 | 10 to 19% of location affected. |
|      2 | 20 to 29% of location affected. |
|      3 | 30 to 39% of location affected. |
|      4 | 40 to 49% of location affected. |
|      5 | 50 to 59% of location affected. |
|      6 | 60 to 69% of location affected. |
|      7 | 70 to 79% of location affected. |
|      8 | 80 to 89% of location affected. |
|      9 | 90 to 99% of location affected. |

## 3.1.32 DAMLOC2

Damage location 2. (core where PLOT.MANUAL = 1.0 through 1.6; core optional beginning with PLOT.MANUAL = 1.7) A code indicating where secondary damage (meeting or exceeding a severity threshold, as defined in the field guide) is present. Uses same codes as DAMLOC1. If DAMLOC1 = 0, then DAMLOC2 = blank (null) or 0.

## 3.1.33 DAMTYP2

Damage type 2. (core where PLOT.MANUAL = 1.0 through 1.6; core optional beginning with PLOT.MANUAL = 1.7) A code indicating the kind of secondary damage (meeting or exceeding a severity threshold, as defined in the field guide) present. Uses same codes as DAMTYP1. If DAMLOC1 = 0, then DAMTYP2 = blank (null).

## 3.1.34 DAMSEV2

Damage severity 2. (core where PLOT.MANUAL = 1.0 through 1.6; core optional beginning with PLOT.MANUAL = 1.7) A code indicating how much of the tree is affected by the secondary damage. Valid severity codes vary by damage type and damage location and must exceed a threshold value, as defined in the field guide. Uses same codes as DAMSEV1. If DAMLOC1 = 0, then DAMSEV2 = blank (null).

## 3.1.35 DECAYCD

Decay class code. A code indicating the stage of decay in a standing dead tree (STANDING\_DEAD\_CD = 1). Not populated for standing dead saplings (1.0-4.9 inches d.b.h./d.r.c.) when PLOT.MANUAL &lt;7.0.

## Codes: DECAYCD

|   Code | Description                                                                                                                                                                                                                          |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | All limbs and branches are present; the top of the crown is still present; all bark remains; sapwood is intact, with minimal decay; heartwood is sound and hard.                                                                     |
|      2 | There are few limbs and no fine branches; the top may be broken; a variable amount of bark remains; sapwood is sloughing with advanced decay; heartwood is sound at base but beginning to decay in the outer part of the upper bole. |
|      3 | Only limb stubs exist; the top is broken; a variable amount of bark remains; sapwood is sloughing; heartwood has advanced decay in upper bole and is beginning at the base.                                                          |
|      4 | Few or no limb stubs remain; the top is broken; a variable amount of bark remains; sapwood is sloughing; heartwood has advanced decay at the base and is sloughing in the upper bole.                                                |
|      5 | No evidence of branches remains; the top is broken; <20 percent of the bark remains; sapwood is gone; heartwood is sloughing throughout.                                                                                             |

## 3.1.36 STOCKING

Tree stocking. The stocking value, in percent, computed for each live tree. Stocking values are computed using several specific species equations that were developed from normal yield tables and stocking charts. Resultant values are a function of diameter. The stocking of individual trees is used to calculate COND.GSSTK, COND.GSSTKCD, COND.ALSTK, and COND.ALSTKCD on the condition record.

## 3.1.37 WDLDSTEM

Woodland tree species stem count. The number of live and dead stems used to calculate diameter on a woodland tree. Woodland species are identified by REF\_SPECIES.WOODLAND =  'Y' in the REF\_SPECIES table.SFTWD\_HRDWD These tree species have diameter measured at the root collar. For a stem to be counted, it must have a minimum stem size of 1 inch in diameter and 1 foot in length.

## 3.1.38 VOLCFNET

Net cubic-foot stem wood volume. The net cubic-foot volume of wood in the central stem of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h., from a 1-foot stump to a minimum 4-inch top diameter, or to where the central stem breaks into limbs all of which are &lt;4.0 inches in diameter. Calculated for live and standing dead trees. Does not include rotten, missing, and form cull (volume loss due to rotten, missing, and form cull defect has been deducted). This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for more information on FIA volume, biomass, and carbon estimation.

Figure 3-1: Illustration of timber species net cubic-foot stem wood volume (VOLCFNET) in black. Gray trees and gray parts are excluded. See VOLCFNET for a full description of this attribute.

<!-- image -->

## 3.1.39 VOLCFGRS

Gross cubic-foot stem wood volume. The total cubic-foot volume of wood in the central stem of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h., from a 1-foot stump to a minimum 4-inch top diameter, or to where the central stem breaks into limbs all of which are &lt;4.0 inches in diameter. Calculated for live and standing dead trees. Includes rotten, missing, and form cull (volume loss due to rotten, missing, and form cull defect has not been deducted). This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

Figure 3-2: Illustration of timber species gross cubic-foot wood volume (VOLCFGRS) in black. Gray trees and gray parts are excluded. See VOLCFGRS for a full description of this attribute.

<!-- image -->

## 3.1.40 VOLCSNET

Net cubic-foot wood volume in the sawlog portion of a sawtimber tree. The net cubic-foot volume of wood in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods), from a 1-foot stump to a minimum top diameter, (7.0 inches for softwoods, 9.0 inches for hardwoods) or to where the central stem breaks into limbs, all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber-size trees have entries in this field if they are growing-stock trees (TREECLCD = 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null) in this field. Form cull and rotten/missing cull are excluded. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.41 VOLCSGRS

Gross cubic-foot wood volume in the sawlog portion of a sawtimber tree. The total cubic-foot volume of wood in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods), or to where the central stem breaks into limbs, all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber-size trees have entries in this field if they are growing-stock trees (TREECLCD = 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null)

in this field. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.42 VOLBFNET

Net board-foot wood volume in the sawlog portion of a sawtimber tree. The net board-foot (International ¼-inch Rule) volume of wood in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods), or to where the central stem breaks into limbs all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per unit area information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber-size trees have entries in this field if they are growing-stock trees (TREECLCD = 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null) in this field. Form cull and rotten/missing cull are excluded.

Figure 3-3: Illustration of timber species net board-foot wood volume in the sawlog porition of a sawtimber tree (VOLBFNET) in black. Gray trees and gray parts are excluded. See VOLBFNET for a full description of this attribute.

<!-- image -->

## 3.1.43 VOLBFGRS

Gross board-foot wood volume in the sawlog portion of a sawtimber tree. The total board-foot (International ¼-inch Rule) volume of wood in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods), or to where the central stem breaks into limbs all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per unit area information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber-size trees have entries in this field if they are growing-stock trees (TREECLCD

= 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null) in this field.

## 3.1.44 VOLCFSND

Sound cubic-foot stem wood volume. The sound cubic-foot volume of wood in the central stem of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h., from a 1-foot stump to a minimum 4-inch top diameter, or to where the central stem breaks into limbs all of which are &lt;4.0 inches in diameter. Calculated for live and standing dead trees. Does not include rotten and missing cull (volume loss due to rotten and missing cull defect has been deducted). This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.45 DIACHECK

Diameter check code. A code indicating the reliability of the diameter measurement.

Note:

If both codes 1 and 2 apply, code 2 is used.

## Codes: DIACHECK

|   Code | Description                                                                                   |
|--------|-----------------------------------------------------------------------------------------------|
|      0 | Diameter accurately measured.                                                                 |
|      1 | Diameter estimated.                                                                           |
|      2 | Diameter measured at different location than previous measurement (remeasurement trees only). |
|      5 | Diameter modeled in the office (used with periodic inventories).                              |

## 3.1.46 MORTYR

Mortality year. (core optional) The estimated year in which a remeasured tree died or was cut. Populated where PLOT.MANUAL  1.0 and populated by some FIA work units where PLOT.MANUAL &lt;1.0.

## 3.1.47 SALVCD

Salvable dead code. A code indicating whether or not a standing or down dead tree is salvable based on regional standards. Contact the appropriate FIA work unit for information on how this code is assigned for a particular State (table 1-1).

## Codes: SALVCD

|   Code | Description        |
|--------|--------------------|
|      0 | Dead not salvable. |
|      1 | Dead salvable.     |

## 3.1.48 UNCRCD

Uncompacted live crown ratio. (core optional Phase 2:  5.0-inch live trees; core Phase 3:  1.0-inch live trees) Percentage determined by dividing the live crown length by the actual tree length. When PLOT.MANUAL &lt;3.0 the variable was a code, which was converted to the midpoint of the ranges represented by the codes, and is stored as a percentage.

## 3.1.49 CPOSCD

Crown position code. (core on Phase 3 plots only) The relative position of each tree in relation to the overstory canopy.

## Codes: CPOSCD

|   Code | Description   |
|--------|---------------|
|      1 | Superstory.   |
|      2 | Overstory.    |
|      3 | Understory.   |
|      4 | Open canopy.  |

## 3.1.50 CLIGHTCD

Crown light exposure code. (core optional on Phase 2 plots; core on Phase 3 plots only) A code indicating the amount of light being received by the tree crown. Collected for all live trees at least 5 inches d.b.h./d.r.c. Trees with UNCRCD &lt;35 have a maximum CLIGHTCD of 1.

## Codes: CLIGHTCD

|   Code | Description                                                                                      |
|--------|--------------------------------------------------------------------------------------------------|
|      0 | The tree receives no direct sunlight because it is shaded by adjacent trees or other vegetation. |
|      1 | Receives full light from the top or 1 side.                                                      |
|      2 | Receives full light from the top and 1 side (or 2 sides without the top).                        |
|      3 | Receives full light from the top and 2 sides (or 3 sides without the top).                       |
|      4 | Receives full light from the top and 3 sides.                                                    |
|      5 | Receives full light from the top and 4 sides.                                                    |

## 3.1.51 CVIGORCD

Crown vigor code (sapling). (core optional on Phase 2 plots; core on Phase 3 plots only) A code indicating the vigor of sapling crowns. Collected for live trees 1.0-4.9 inches d.b.h./d.r.c.

## Codes: CVIGORCD

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                         |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Saplings must have an uncompacted live crown ratio of 35 or higher, have <5 percent dieback (deer/rabbit browse is not considered as dieback but is considered missing foliage) and 80 percent or more of the foliage present is normal or at least 50 percent of each leaf is not damaged or missing. Twigs and branches that are dead because of normal shading are not included. |

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                   |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      2 | Saplings do not meet class 1 or 3 criteria. They may have any uncompacted live crown ratio, may or may not have dieback and may have between 21 and 100 percent of the foliage classified as normal.                                                                                                                                                                                                          |
|      3 | Saplings may have any uncompacted live crown ratio and have 1 to 20 percent normal foliage or the percent of foliage missing combined with the percent of leaves that are over 50 percent damaged or missing should equal 80 percent or more of the live crown. Twigs and branches that are dead because of normal shading are not included. Code is also used for saplings that have no crown by definition. |

## 3.1.52 CDENCD

Crown density code. (core optional on Phase 2 plots; core on Phase 3 plots only) A code indicating how dense the tree crown is, estimated in percent classes. Collected for all live trees  5.0 inches d.b.h./d.r.c. Crown density is the amount of crown branches, foliage, and reproductive structures that blocks light visibility through the crown.

## Codes: CDENCD

|   Code | Description   |
|--------|---------------|
|     00 | 0%            |
|     05 | 1-5%          |
|     10 | 6-10%         |
|     15 | 11-15%        |
|     20 | 16-20%        |
|     25 | 21-25%        |
|     30 | 26-30%        |
|     35 | 31-35%        |
|     40 | 36-40%        |
|     45 | 41-45%        |
|     50 | 46-50%        |
|     55 | 51-55%        |
|     60 | 56-60%        |
|     65 | 61-65%        |
|     70 | 66-70%        |
|     75 | 71-75%        |
|     80 | 76-80%        |
|     85 | 81-85%        |
|     90 | 86-90%        |
|     95 | 91-95%        |
|     99 | 96-100%       |

## 3.1.53 CDIEBKCD

Crown dieback code. (core optional on Phase 2 plots; core on Phase 3 plots only) A code indicating the amount of recent dead material in the upper and outer portion of the

crown, estimated in percent classes. Collected for all live trees  5.0 inches d.b.h./d.r.c. See CDENCD for codes.

## 3.1.54 TRANSCD

Foliage transparency code. (core optional on Phase 2 plots; core on Phase 3 plots only) A code indicating the amount of light penetrating the foliated portion of the crown, estimated in percent classes. Collected for all live trees  5.0 inches d.b.h./d.r.c. See CDENCD for codes.

## 3.1.55 TREEHISTCD

Tree history code. Identifies the tree with detailed information as to whether the tree is live, dead, cut, removed due to land use change, etc. Contact the appropriate FIA work unit for the definitions (table 1-1). Only populated by certain FIA work units (SURVEY.RSCD = 23, 24, 33).

## 3.1.56 BHAGE

Breast height age. The age of a live tree derived from counting tree rings from an increment core sample extracted at a height of 4.5 feet above ground. Breast height age is collected for a subset of trees and only for trees when the diameter is measured at breast height (d.b.h.). This data item is used to calculate classification attributes such as stand age. For PNWRS, one tree is sampled for BHAGE for each species, within each crown class, and for each condition class present on a plot. Age of saplings (&lt;5.0 inches d.b.h.) may be aged by counting branch whorls above 4.5 feet. No timber hardwood species other than red alder are bored for age. For RMRS, one tree is sampled for each species and broad diameter class present on a plot. Only populated by certain FIA work units (SURVEY.RSCD = 22, 26) and is left blank (null) when it is not collected.

## 3.1.57 TOTAGE

Total age. The age of a live tree derived either from counting tree rings from an increment core sample extracted at the base of a tree where diameter is measured at root collar (d.r.c.), or for small saplings (1.0-2.9 inches d.b.h.) by counting all branch whorls, or by adding a species-dependent number of years to breast height age. Total age is collected for a subset of trees and is used to calculate classification attributes such as stand age. Only populated by certain FIA work units (SURVEY.RSCD = 22, 26) and is left blank (null) when it is not collected.

## 3.1.58 CULLDEAD

Dead cull. The percentage of cubic-foot volume in the merchantable bole that is cull due to sound dead material. Recorded for trees  5.0 inches d.b.h./d.r.c. The merchantable bole is from a 1-foot stump to a 4-inch top diameter outside bark (DOB). For woodland species (REF\_SPECIES. WOODLAND = 'Y'), the merchantable portion is between the point of d.r.c. measurement to a 1.5-inch top DOB. For trees with broken tops, cull above the actual height (ACTUALHT) is not included. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 3.1.59 CULLFORM

Form cull. The percentage of cubic-foot volume in the sawlog portion that is cull due to form defect. Recorded for live timber species of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods). The sawlog portion is from a 1-foot stump to a 7.0-inch top diameter outside bark (DOB) for softwoods and a

9.0-inch top DOB for hardwoods. For trees with broken tops, cull above the actual height (ACTUALHT) is not included. This attribute is blank (null) for dead trees and woodland species (REF\_SPECIES.WOODLAND = 'Y'). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 3.1.60 CULLMSTOP

Missing top cull. The percentage of cubic-foot volume that is cull due to a missing (broken) merchantable top. Recorded for trees  5.0 inches d.b.h./d.r.c. This  estimate does not include any portion of the missing top that is &lt;4.0 inches diameter outside bark (DOB). Some broken-top trees have 0 percent missing top cull because no merchantable volume was lost. For woodland species (REF\_SPECIES.WOODLAND = 'Y') with multiple stems, CULLMSTOP = 0 is recorded. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 3.1.61 CULLBF

Board-foot cull. The percent of the gross board-foot volume that is cull due to rot or form. Only populated by certain FIA work units (SURVEY.RSCD = 24).

## 3.1.62 CULLCF

Cubic-foot cull. The percent of the gross cubic-foot volume that is cull due to rot or form. Only populated by certain FIA work units (SURVEY.RSCD = 24).

## 3.1.63 BFSND

Board-foot-cull soundness. The percent of the board-foot cull that is sound (due to form). Only populated by certain FIA work units (SURVEY.RSCD = 24).

## 3.1.64 CFSND

Cubic-foot-cull soundness. The percent of the cubic-foot cull that is sound (due to form). Only populated by certain FIA work units (SURVEY.RSCD = 24).

## 3.1.65 SAWHT

Sawlog height. The length (height) of a tree, recorded to a 7-inch top (9-inch for hardwoods), where at least one 8-foot log, merchantable or not, is present. On broken topped trees, sawlog length is recorded to the point of the break. Only populated by certain FIA work units (SURVEY.RSCD = 24).

## 3.1.66 BOLEHT

Bole height. The length between the 1-foot stump and the 4.0-inch top diameter of outside bark (DOB), where at least one 4-foot section is present. In periodic inventories, this attribute was measured in the field. For annual inventories, this attribute is a calculated, modeled value. Only populated by certain FIA work units (SURVEY.RSCD = 24).

## 3.1.67 FORMCL

Form class. A code used in calculating merchantable bole net volume. Recorded for all live hardwood trees tallied that are  5.0 inch d.b.h./d.r.c. Also recorded for conifers  5.0 inch d.b.h. in Region 5 National Forests (only collected when INVYR = 2001-2009). Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Tree Table

## Codes: FORMCL

|   Code | Description                                                                                                              |
|--------|--------------------------------------------------------------------------------------------------------------------------|
|      1 | First 8 feet above stump is straight.                                                                                    |
|      2 | First 8 feet above stump is NOT straight or forked; but there is at least one straight 8-foot log elsewhere in the tree. |
|      3 | No 8-foot logs anywhere in the tree now or in the future due to form.                                                    |

## 3.1.68 HTCALC

Current height calculated. If the height is unmeasurable (e.g., the tree is cut or dead), the height is calculated, in feet, and stored in this attribute. Only populated by certain FIA work units (SURVEY.RSCD = 33).

## 3.1.69 HRDWD\_CLUMP\_CD

Hardwood clump code. A code sequentially assigned to each hardwood clump within each species as they are found on a subplot. Up to 9 hardwood clumps can be identified and coded within each species on each subplot. A clump is defined as having 3 or more live stems originating from a common point on the root system. Woodland hardwood species are not evaluated for clump code. Clump code data are used to adjust stocking estimates since trees growing in clumps contribute less to stocking than do individual trees. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.70 SITREE

Calculated site index. Computed for every tree. The site index represents the average total length, in feet, that dominant and co-dominant trees in fully-stocked, even-aged stands (of the same species as this tree) will obtain at key ages (usually 25 or 50 years). Only computed by certain FIA work units (SURVEY.RSCD = 23).

3.1.71

CREATED\_BY

Created by. See SURVEY.CREATED\_BY description for definition.

## 3.1.72 CREATED\_DATE

Created date. See SURVEY.CREATED\_DATE description for definition.

## 3.1.73 CREATED\_IN\_INSTANCE

Created in instance. See SURVEY.CREATED\_IN\_INSTANCE description for definition.

## 3.1.74 MODIFIED\_BY

Modified by. See SURVEY.MODIFIED\_BY description for definition.

## 3.1.75 MODIFIED\_DATE

Modified date. See SURVEY.MODIFIED\_DATE description for definition.

## 3.1.76 MODIFIED\_IN\_INSTANCE

Modified in instance. See SURVEY.MODIFIED\_IN\_INSTANCE description for definition.

## 3.1.77 MORTCD

Mortality code. (core optional) Used for a tree that was alive within past 5 years, but has died. Not populated for standing dead trees &lt;5.0 inches d.b.h./d.r.c. when PLOT.MANUAL &lt;7.0.

## Codes: MORTCD

|   Code | Description                         |
|--------|-------------------------------------|
|      0 | Tree does not qualify as mortality. |
|      1 | Tree does qualify as mortality.     |

## 3.1.78 HTDMP

Height to diameter measurement point. This value is equal to the actual length, to the nearest 0.1 foot, from ground to the point of diameter measurement. This attribute is populated for trees  1.0 inch d.b.h., which were not measured for diameter directly at breast height (due to an abnormal swelling, branches, damage, or other at 4.5 feet above ground). This item is blank (null) for trees  1.0 inch d.b.h., where diameter was measured directly at breast height (4.5 feet above ground) and for woodland species where diameter was measured at root collar. Core optional when PLOT.MANUAL &lt;8.0; core when PLOT.MANUAL  8.0. 

## 3.1.79 ROUGHCULL

Rough cull. (core optional) The percentage of cubic-foot volume in the merchantable bole that is cull due to sound dead material or tree form. Recorded for live trees  5.0 inches d.b.h./d.r.c. The merchantable bole is from a 1-foot stump to a 4-inch top diameter outside bark (DOB). For woodland species (REF\_SPECIES.WOODLAND = 'Y'), the merchantable portion is between the point of d.r.c. measurement to a 1.5-inch top DOB, and ROUGHCULL only includes sound dead material.

## Note:

- · For PLOT.MANUAL &lt;2.0, ROUGHCULL only included sound dead material (i.e., it did not include cull due to tree form).
- · When SURVEY.RSCD = 26,27, only populated for live conifers, red alder, and bigleaf maple.

## 3.1.80 MIST\_CL\_CD

Mistletoe class code. (core optional) A rating of dwarf mistletoe infection. Recorded on all live conifer species except juniper. Using the Hawksworth (1979) six-class rating system, the live crown is divided into thirds, and each third is rated using the following scale: 0 is for no visible infection, 1 for &lt;50 percent of branches infected, 2 for &gt;50 percent of branches infected. The ratings for each third are summed together to yield the Hawksworth rating.

## Codes: MIST\_CL\_CD

|   Code | Description                                       |
|--------|---------------------------------------------------|
|      0 | Hawksworth tree DMR rating of 0, no infection.    |
|      1 | Hawksworth tree DMR rating of 1, light infection. |
|      2 | Hawksworth tree DMR rating of 2, light infection. |

## Tree Table

|   Code | Description                                        |
|--------|----------------------------------------------------|
|      3 | Hawksworth tree DMR rating of 3, medium infection. |
|      4 | Hawksworth tree DMR rating of 4, medium infection. |
|      5 | Hawksworth tree DMR rating of 5, heavy infection.  |
|      6 | Hawksworth tree DMR rating of 6, heavy infection.  |

## 3.1.81 CULL\_FLD

Rotten/missing cull, field recorded. (core:  5.0-inch live trees; core optional:  5.0-inch standing dead trees) The percentage rotten or missing cubic-foot cull volume, estimated to the nearest 1 percent. This estimate does not include any cull estimate above actual length; therefore volume lost from a broken top is not included (see CULL for percent cull including cull from broken top). When field crews estimate volume loss (tree cull), they only consider the cull on the merchantable bole of the tree, from a 1-foot stump to a 4-inch top diameter outside bark (DOB). For woodland species, the merchantable portion is between the point of d.r.c. measurement to a 1.5-inch top DOB.

## 3.1.82 RECONCILECD

Reconcile code. A code indicating the reason a tree either enters or is no longer a part of the inventory. Only recorded for remeasurement locations.

## Notes:

- · Starting with PLOT.MANUAL = 9.0, codes 1-2 are only valid for new trees (STATUSCD = 1, 2) on the plot and exclude trees associated with a change in procedures/definitions or previous cruiser error, as such trees are accounted for with RECONCILECD  = 7 or 8. Codes 6-9 are valid for both new tally trees and remeasured trees that no linger qualify as tally.
- · When PLOT.MANUAL = 7.0 through 8.0, standing dead saplings that were not included in the previous inventory were assigned RECONCILECD = 4.

## Codes: RECONCILECD

|   Code | Description                                                                                                                                                                                                                                                                     |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Ingrowth - Either (a) a new tally tree not qualifying as through growth, or (b) a new tree on land that was formerly nonforest and now qualifies as forest land unrelated to cruiser error or procedural/definition change.                                                     |
|      2 | Through growth - New tally tree 5 inches d.b.h./d.r.c. and larger, within the microplot, which was not missed at the previous inventory (i.e., grew from seedling to at least 5.0 inches d.b.h. between plot inventory cycles - such trees were never tallied on a microplot).  |
|      3 | Retired code - Starting with PLOT.MANUAL = 9.0, this code is no longer used; it is still valid for PLOT.MANUAL <9.0. Missed live - a live tree missed at previous inventory and that is live or dead now. Includes currently tallied trees on previously nonsampled conditions. |
|      4 | Retired code - Starting with PLOT.MANUAL = 9.0, this code is no longer used; it is still valid for PLOT.MANUAL <9.0. Missed dead - a dead tree missed at previous inventory and that is dead now. Includes currently tallied trees on previously nonsampled conditions.         |
|      5 | Shrank - Live tree that shrunk below threshold diameter on microplot/subplot/macroplot. Must currently be alive. Only valid for remeasured trees that no longer qualify as tally (STATUSCD = 0).                                                                                |

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      6 | Physical movement - Either (a) tree was correctly tallied in previous inventory, but has now moved beyond the radius of the plot due to natural causes (e.g., small earth movement, hurricane), or (b) tree was outside the radius of the plot previously, but has now moved within the plot due to natural causes. Tree must be either live before and still alive now or dead before and dead now. If tree was live before and now dead, this is a mortality tree and should have STATUSCD = 2 (not 0). |
|      7 | Cruiser error - Either (a) tree was erroneously tallied (added tree), or (b) tree was erroneously not tallied (missed tree) at the previous inventory.                                                                                                                                                                                                                                                                                                                                                    |
|      8 | Procedural change - Either (a) tree was tallied at the previous inventory, but is no longer tallied due to a definition or procedural change, or (b) tree was not tallied at the previous inventory, but is now tallied due to a definition or procedural change, regardless of d.b.h/d.r.c. at the time of the previous inventory.                                                                                                                                                                       |
|      9 | Nonsampled area - Either (a) tree was located in a sampled condition at the previous inventory, but now is in a nonsampled condition, or (b) the area where the tree is located was previously not sampled, but now is sampled. All trees located in a nonsampled area (either now or previously) have RECONCILECD = 9.                                                                                                                                                                                   |

## 3.1.83 PREVDIA

Previous diameter. The previous diameter, in inches, of the sample tree at the point of diameter measurement. Populated for remeasured trees.

## 3.1.84 P2A\_GRM\_FLG

Periodic to annual growth, removal, and mortality flag. A code indicating if this tree is part of a periodic inventory that is only included for the purposes of computing growth, removals and/or mortality estimates. The flag is set to 'Y' for those trees that are needed for estimation and otherwise is left blank (null).

## 3.1.85 TREECLCD\_NERS

Tree class code, Northeastern Research Station. In annual inventory, this code represents a classification of the overall quality of a tree that is  5.0 inches d.b.h. It classifies the quality of a sawtimber tree based on the present condition, or it classifies the quality of a poletimber tree as a prospective determination (i.e., a forecast of potential quality when and if the tree becomes sawtimber size). For more detailed description, see the regional field guide located at the NRS Data Collection web page (https://research.fs.usda.gov/nrs/programs/fia). Only populated by certain FIA work units (SURVEY.RSCD = 24).

## Codes: TREECLCD\_NERS

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Preferred - Live tree that would be favored in cultural operations. Mature tree, that is older than the rest of the stand; has less than 20 percent total board-foot cull; is expected to live for 5 more years; and is a low risk tree. In general, the tree has the following qualifications: • must be free from "general" damage (i.e., damages that would now or prospectively cause a reduction of tree class, significantly deter growth, or prevent it from producing marketable products in the next 5 years). • should have no more than 10 percent board-foot cull due to form defect. • should have good vigor, usually indicated by a crown ratio of 30 percent or more and dominant or co-dominant. • usually has a grade 1 butt log. |
|      2 | Acceptable - This class includes: • live sawtimber tree that does not qualify as a preferred tree, but is not a cull tree (see Rough and Rotten Cull). • live poletimber tree that prospectively will not qualify as a preferred tree, but is not now or prospectively a cull tree (see Rough and Rotten Cull).                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|      3 | Rough Cull - This class includes: • live sawtimber tree that currently has 67 percent or more predominantly sound board-foot cull; or does not contain one merchantable 12-foot sawlog or two non-contiguous merchantable 8-foot sawlogs. • live poletimber tree that currently has 67 percent or more predominantly sound cubic-foot cull; or prospectively will have 67 percent or more predominantly sound board-foot cull; or will not contain one merchantable 12-foot sawlog or two noncontiguous merchantable 8-foot sawlogs.                                                                                                                                                                                                                |
|      4 | Rotten Cull - This class includes: • live sawtimber tree that currently has 67 percent or more predominantly unsound board-foot cull. • live poletimber tree that currently has 67 percent or more predominantly unsound cubic-foot cull; or prospectively will have 67 percent or more predominantly unsound board-foot cull.                                                                                                                                                                                                                                                                                                                                                                                                                      |
|      5 | Dead - Tree that has recently died (within the last several years); but still retains many branches (including some small branches and possibly some fine twigs); and has bark that is generally tight and hard to remove from the tree.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|      6 | Snag - Dead tree, or what remains of a dead tree, that is at least 4.5 feet tall and is missing most of its bark. This category includes a tree covered with bark that is very loose. This bark can usually be removed, often times in big strips, with very little effort. A snag is not a recently dead tree. Most often, it has been dead for several years - sometimes, for more than a decade.                                                                                                                                                                                                                                                                                                                                                 |

## 3.1.86 TREECLCD\_SRS

Tree class code, Southern Research Station. A code indicating the general quality of the tree. Prior to the merger of the Southern and Southeastern Research Stations (INVYR  1997), a growing-stock classification (code 2) was only assigned to species that were considered to have commercial value. Since the merger (INVYR &gt;1997), code 2 has been applied to all tree species meeting the growing-stock form, grade, size and soundness requirements, regardless of commercial value. Only populated by certain FIA work units (SURVEY.RSCD = 33).

## Codes: TREECLCD\_SRS

|   Code | Description                                                                                                                                                                                                                                           |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      2 | Growing-stock - All trees that have at least one 12-foot log or two 8-foot logs that meet grade and size requirements and at least 1/3 of the total board foot volume is merchantable. Poletimber-sized trees are evaluated based on their potential. |
|      3 | Rough cull - Trees that do not contain at least one 12-foot log or two 8-foot logs, or more than 1/3 of the total board foot volume is not merchantable, primarily due to roughness or poor form.                                                     |
|      4 | Rotten cull: Trees that do not contain at least one 12-foot log or two 8-foot logs, or more than 1/3 of the total board foot volume is not merchantable, primarily due to rotten, unsound wood.                                                       |

## 3.1.87 TREECLCD\_NCRS

Tree class code, North Central Research Station. In annual inventory, a code indicating tree suitability for timber products, or the extent of decay in the butt section of down-dead trees. It is recorded on live standing, standing-dead, and down dead trees that are  1.0 inch d.b.h. Tree class is basically a check for the straightness and soundness of the sawlog portion on a sawtimber tree or the potential sawlog portion on a poletimber tree or sapling. "Sawlog portion" is defined as the length between the 1-foot stump and the 9.0-inch top diameter of outside bark, DOB, for hardwoods, or the 7.0-inch top DOB for softwoods. For more detailed description, see the regional field guide located at the NRS Data Collection web page (https://research.fs.usda.gov/nrs/programs/fia). Only populated by certain FIA work units (SURVEY.RSCD = 23).

## Codes: TREECLCD\_NCRS

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     20 | Growing stock - Any live tree of commercial species that is sawtimber size and has at least one merchantable 12-foot sawlog or two merchantable 8-foot sawlogs meeting minimum log-grade requirements. At least one-third of the gross board-foot volume of the sawlog portion must be merchantable material. A merchantable sawlog must be at least 50 percent sound at any point. Any pole timber size tree that has the potential to meet the above specifications.                                                                                                                                 |
|     30 | Rough Cull, Salvable, and Salvable-down - Includes any tree of noncommercial species, or any tree that is sawtimber size and has no merchantable sawlog. Over one-half of the volume in the sawlog portion does not meet minimum log-grade specifications due to roughness, excessive sweep or crook, splits, cracks, limbs, or forks. Rough cull pole-size trees do not have the potential to meet the specifications for growing-stock because of forks, limb stoppers, or excessive sweep or crook. A down-dead tree  5.0-inch d.b.h. that meets these standards is given a tree/decay code of 30. |

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     31 | Short-log Cull - Any live sawtimber-size tree of commercial species that has at least one 8-foot sawlog, but < a 12-foot sawlog, meeting minimum log-grade specifications. Any live sawtimber-size tree of commercial species that has less than one-third of the volume of the sawlog portion in merchantable logs, but has at least one 8-foot or longer sawlog meeting minimum log-grade specifications. A short sawlog must be 50 percent sound at any point. Pole-size trees never receive a tree class code 31.                                                                                                                                                                                                                                                                                     |
|     40 | Rotten Cull - Any live tree of commercial species that is sawtimber size and has no merchantable sawlog. Over one-half of the volume in the sawlog portion does not meet minimum log-grade specifications primarily because of rot, missing sections, or deadwood. Classify any pole-size tree that does not have the potential to meet the specifications for growing-stock because of rot as rotten cull. Assume that all live trees will eventually attain sawlog size at d.b.h. Predicted death, tree vigor, and plot site index are not considered in determining tree class. A standing-dead tree without an 8-foot or longer section that is at least 50 percent sound has a tree class of 40. On remeasurement of a sapling, if it has died and is still standing it is given a tree class of 40. |

## 3.1.88 TREECLCD\_RMRS

Tree class code, Rocky Mountain Research Station. A code indicating the general quality of the tree. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: TREECLCD\_RMRS

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                              |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      1 | Sound-live timber species - All live timber trees (species with diameter measured at breast height) that meet minimum merchantability standards. In general, these trees have at least one solid 8-foot section, are reasonably free of form defect on the merchantable bole, and at least 34 percent or more of the volume is merchantable. Excludes rough or rotten cull timber trees. |
|      2 | All live woodland species - All live woodland trees (woodland species can be identified by REF_SPECIES.WOODLAND = 'Y'). All trees assigned to species groups 23 and 48 belong in this category (see appendix E).                                                                                                                                                                         |
|      3 | Rough-live timber species - All live trees that do not now, or prospectively, have at least one solid 8-foot section, reasonably free of form defect on the merchantable bole, or have 67 percent or more of the merchantable volume cull; and more than half of this cull is due to sound dead wood cubic-foot loss or severe form defect volume loss.                                  |
|      4 | Rotten-live timber species - All live trees with 67 percent or more of the merchantable volume cull, and more than half of this cull is due to rotten or missing cubic-foot volume loss.                                                                                                                                                                                                 |
|      5 | Hard (salvable) dead - dead trees that have less than 67 percent of the volume cull due to rotten or missing cubic-foot volume loss.                                                                                                                                                                                                                                                     |
|      6 | Soft (nonsalvable) dead - dead trees that have 67 percent or more of the volume cull due to rotten or missing cubic-foot volume loss.                                                                                                                                                                                                                                                    |

## 3.1.89 STANDING\_DEAD\_CD

Standing dead code. A code indicating if a tree qualifies as standing dead. To qualify as a standing dead tally tree, the dead tree must be  1.0 inch d.b.h., have a bole that has an unbroken actual length (ACTUALHT)  4.5 feet, and lean &lt;45 degrees from vertical as measured from the base of the tree to 4.5 feet. For woodland species with multiple stems,

a tree is considered down if more than 2/3 of the volume is no longer attached or upright; cut and removed volume is not considered. For woodland species with single stems to qualify as a standing dead tally tree, dead trees must be  1.0 inch d.r.c., be  1.0 foot in unbroken actual length (ACTUALHT), and lean &lt;45 degrees from vertical.

Populated where PLOT.MANUAL  2.0; may be populated using information collected on dead trees in earlier inventories for dead trees.

Note: Starting with PLOT.MANUAL = 7.0, the core minimum diameter to qualify for a standing dead tree was changed from 5.0 inches to 1.0 inch.

## Codes: STANDING\_DEAD\_CD

|   Code | Description                                  |
|--------|----------------------------------------------|
|      0 | No - tree does not qualify as standing dead. |
|      1 | Yes - tree does qualify as standing dead.    |

## 3.1.90 PREV\_STATUS\_CD

Previous tree status code. Tree status that was recorded at the previous inventory on all tally trees  1.0 inch d.b.h./d.r.c. Includes all new standing dead trees (STATUSCD = 2, STANDING\_DEAD\_CD = 1, RECONCILECD &gt;0).

## Codes: PREV\_STATUS\_CD

|   Code | Description                                          |
|--------|------------------------------------------------------|
|      1 | Live tree - live tree at the previous inventory.     |
|      2 | Dead tree - standing dead at the previous inventory. |

## 3.1.91 PREV\_WDLDSTEM

Previous woodland stem count. Woodland tree species stem count that was recorded at the previous inventory.

## 3.1.92 TPA\_UNADJ

Trees per acre unadjusted. The number of trees per acre that the sample tree theoretically represents based on the sample design. For fixed-radius plots taken with the mapped plot design (PLOT.DESIGNCD = 1), TPA\_UNADJ is set to a constant derived from the plot size and equals 6.018046 for trees sampled on subplots, 74.965282 for trees sampled on microplots, and 0.999188 for trees sampled on macroplots. Variable-radius plots were often used in earlier inventories, so the value in TPA\_UNADJ decreases as the tree diameter increases. Based on the procedures described in Bechtold and Patterson (2005), this attribute must be adjusted using factors stored in the POP\_STRATUM table to derive population estimates. Examples of estimating population totals are shown in The Forest Inventory and Analysis Database: Population Estimation User Guide.

## 3.1.93 DRYBIO\_BOLE

Dry biomass of wood in the merchantable bole. The oven-dry biomass, in pounds, of wood in the merchantable bole of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h., from a 1-foot stump to a minimum 4-inch top diameter. Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for

timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

Figure 3-4: Illustration of timber species dry biomass of wood in the merchantable bole biomass (DRYBIO\_BOLE) in black. Gray trees and gray parts are excluded. Wood biomass is proportionally reduced to account for cull; the cull wood, represented by the cross-hatched area in the figure, has lost some of its structural integrity and therefore its mass. See DRYBIO\_BOLE for a full description of this attribute.

<!-- image -->

## 3.1.94 DRYBIO\_STUMP

Dry biomass of wood in the stump. The oven-dry biomass, in pounds, of wood in the stump of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The stump is that portion of the tree from the ground line to the bottom of the merchantable bole (i.e., below 1 foot). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.95 DRYBIO\_BG

Belowground dry biomass. The oven-dry biomass, in pounds, of the belowground portion of a tree, including coarse roots with a root diameter  0.1 inch. This is a modeled estimate, calculated for live and standing dead trees  1.0 inch d.b.h./d.r.c. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

Note: Not populated for standing dead saplings (1.0-4.9 inches d.b.h./d.r.c.) when PLOT.MANUAL &lt;7.0.

## 3.1.96 CARBON\_AG

Aboveground carbon of wood and bark. The carbon, in pounds, of wood and bark in the aboveground portion, excluding foliage, of live and standing dead trees  1.0 inch

d.b.h./d.r.c. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. The amount of carbon is calculated based on applying species-specific carbon fractions to the aboveground biomass:

CARBON\_AG = REF\_SPECIES.CARBON\_RATIO\_LIVE * DRYBIO\_AG

This attribute is populated for all tree species tallied in the continental U.S. as well as both the Caribbean and Pacific Islands, including Hawaii (refer to the FIA Master Tree Species List, available at the following web address:

https://www.fia.fs.usda.gov/library/field-guides-methods-proc/). Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

Note: Not populated for standing dead saplings (1.0-4.9 inches d.b.h./d.r.c.) when PLOT.MANUAL &lt;7.0.

## 3.1.97 CARBON\_BG

Belowground carbon. The carbon, in pounds, of the belowground portion of a tree, including coarse roots with a root diameter  0.1 inch.  Calculated for live and standing dead trees  1.0 inch d.b.h./d.r.c. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. The amount of carbon is calculated based on applying species-specific carbon fractions to the belowground biomass:

CARBON\_BG = REF\_SPECIES.CARBON\_RATIO\_LIVE * DRYBIO\_BG

Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

Note: Not populated for standing dead saplings (1.0-4.9 inches d.b.h./d.r.c.) when PLOT.MANUAL &lt;7.0.

## 3.1.98 CYCLE

Inventory cycle number. See SURVEY.CYCLE description for definition.

## 3.1.99 SUBCYCLE

Inventory subcycle number. See SURVEY.SUBCYCLE description for definition.

## 3.1.100 BORED\_CD\_PNWRS

Tree bored code, Pacific Northwest Research Station. Used in conjunction with tree age (BHAGE and TOTAGE). Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: BORED\_CD\_PNWRS

|   Code | Description                                              |
|--------|----------------------------------------------------------|
|      1 | Trees bored or 'whorl counted' at the current inventory. |
|      2 | Tree age derived from a previous inventory.              |
|      3 | Tree age was extrapolated.                               |

## 3.1.101 DAMLOC1\_PNWRS

Damage location 1, Pacific Northwest Research Station. The location on the tree where Damage Agent 1 is found. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: DAMLOC1\_PNWRS

|   Code | Location   | Definition                                                                                                                                                                                                                                                                                                                                                                                                    |
|--------|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | -          | No damage found.                                                                                                                                                                                                                                                                                                                                                                                              |
|      1 | Roots      | Above ground up to 12 inches on bole.                                                                                                                                                                                                                                                                                                                                                                         |
|      2 | Bole       | Main stem(s) starting at 12 inches above the ground, including forks up to a 4 inch top. (A fork is at least equal to 1/3 diameter of the bole, and occurs at an angle <45 degrees in relation to the bole.) This is not a valid location code for woodland species; use only locations 1, 3, and 4. For saplings, bole includes the main stem starting at ground level and extends to the top of the leader. |
|      3 | Branch     | All other woody material. Primary branch(es) occur at an angle  45 degrees in relation to the bole. For saplings, a branch is all branches (not any part of the bole).                                                                                                                                                                                                                                       |
|      4 | Foliage    | All leaves, buds, and shoots.                                                                                                                                                                                                                                                                                                                                                                                 |

## 3.1.102 DAMLOC2\_PNWRS

Damage location 2, Pacific Northwest Research Station. See DAMLOC1\_PNWRS. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.103 DIACHECK\_PNWRS

Diameter check, Pacific Northwest Research Station. A separate estimate of the diameter without the obstruction if the diameter was estimated because of moss/vine/obstruction, etc. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: DIACHECK\_PNWRS

|   Code | Description                                |
|--------|--------------------------------------------|
|      5 | Diameter estimated because of moss.        |
|      6 | Diameter estimated because of vines.       |
|      7 | Diameter estimated (double nail diameter). |

## 3.1.104 DMG\_AGENT1\_CD\_PNWRS

Damage agent 1, Pacific Northwest Research Station. Primary damage agent code in PNWRS. Up to three damaging agents can be coded in PNWRS as DMG\_AGENT1\_CD\_PNWRS, DMG\_AGENT2\_CD\_PNWRS, and DMG\_AGENT3\_CD\_PNWRS. A 2-digit code (with values from 01 to 99) indicating the tree damaging agent that is considered to be of greatest importance to predict tree growth, survival, and forest composition and structure. Additionally, there are two classes of damaging agents. Class I damage agents are considered more important than class II agents and are thus coded as a primary agent before the class II agents. For more information, see appendix I. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.105 DMG\_AGENT2\_CD\_PNWRS

Damage agent 2, Pacific Northwest Research Station. See DMG\_AGENT1\_CD\_PNWRS. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.106 DMG\_AGENT3\_CD\_PNWRS

Damage agent 3, Pacific Northwest Research Station. See DMG\_AGENT1\_CD\_PNWRS. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.107 MIST\_CL\_CD\_PNWRS

Leafy mistletoe class code, Pacific Northwest Research Station. All juniper species, incense cedars, white fir (CA only) and oak trees are rated for leafy mistletoe infection. This item is used to describe the extent and severity of leafy mistletoe infection (see MIST\_CL\_CD for dwarf mistletoe information). Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: MIST\_CL\_CD\_PNWRS

|   Code | Description                                                   |
|--------|---------------------------------------------------------------|
|      0 | None.                                                         |
|      7 | <50 percent of crown infected.                                |
|      8 |  50 percent of crown infected or any occurrence on the bole. |

## 3.1.108 SEVERITY1\_CD\_PNWRS

Damage severity 1, Pacific Northwest Research Station. Damage severity depends on the damage agent coded (see appendix I for codes). This is a 2-digit code that indicates either percent of location damaged (01-99), or the appropriate class of damage (values vary from 0-9 depending on the specific Damage Agent). Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.109 SEVERITY1A\_CD\_PNWRS

Damage severity 1A, Pacific Northwest Research Station. Damage severity depends on the damage agent coded (see appendix I for codes). This is a 2-digit code indicating either percent of location damaged (01-99), or the appropriate class of damage (values vary from 0-4 depending on the specific Damage Agent). Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.110 SEVERITY1B\_CD\_PNWRS

Damage severity 1B, Pacific Northwest Research Station. Damage severity B is only coded when the Damage Agent is white pine blister rust (36). Only populated by certain FIA work units (SURVEY.RSCD= 26).

## Codes: SEVERITY1B\_CD\_PNWRS

|   Code | Description                                                                           |
|--------|---------------------------------------------------------------------------------------|
|      1 | Branch infections located more than 2.0 feet from tree bole.                          |
|      2 | Branch infections located 0.5 to 2.0 feet from tree bole.                             |
|      3 | Branch infection located within 0.5 feet of tree bole OR tree bole infection present. |

## 3.1.111 SEVERITY2\_CD\_PNWRS

Damage severity 2, Pacific Northwest Research Station. Damage severity depends on the damage agent coded (see appendix I for codes). This is a 2-digit code indicating either percent of location damaged (01-99), or the appropriate class of damage (values vary

from 0-9 depending on the specific Damage Agent). Only populated by certain FIA work units (SURVEY.RSCD= 26).

## 3.1.112 SEVERITY2A\_CD\_PNWRS

Damage severity 2A, Pacific Northwest Research Station. See SEVERITY1A\_CD\_PNWRS. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.113 SEVERITY2B\_CD\_PNWRS

Damage severity 2B, Pacific Northwest Research Station. See SEVERITY1B\_CD\_PNWRS. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.114 SEVERITY3\_CD\_PNWRS

Damage severity 3, Pacific Northwest Research Station. Damage severity depends on the damage agent coded (see appendix I for codes). This is a 2-digit code indicating either percent of location damaged (01-99), or the appropriate class of damage (values vary from 0-9 depending on the specific Damage Agent). Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.115 UNKNOWN\_DAMTYP1\_PNWRS

Unknown damage type 1, Pacific Northwest Research Station. A code indicating the sign or symptom recorded when UNKNOWN damage code 90 is used. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Codes: UNKNOWN\_DAMTYP1\_PNWRS

|   Code | Description                    |
|--------|--------------------------------|
|      1 | Canker/gall.                   |
|      2 | Open wound.                    |
|      3 | Resinosis.                     |
|      4 | Broken.                        |
|      5 | Damaged or discolored foliage. |
|      6 | Other.                         |

## 3.1.116 UNKNOWN\_DAMTYP2\_PNWRS

Unknown damage type 2, Pacific Northwest Research Station. See UNKNOWN\_DAMTYP1\_PNWRS. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.117 PREV\_PNTN\_SRS

Previous periodic prism point, tree number, Southern Research Station. In some older Southeast Experiment Station States, the prism point, tree number (PNTN) of the current cycle did not match the previous cycle's prism point, tree number. PREV\_PNTN\_SRS is used to join the current and the previous prism plot trees. Not populated for the Caribbean Islands.

## 3.1.118 DISEASE\_SRS

Disease, Southern Research Station. A code indicating the incidence of fusiform, comandra rust or dieback. Dieback is only recorded for live hardwood trees where DIA  5.0 inches with at least 10 percent dieback. Fusiform and comandra rust are only

recorded for live pine trees  5.0 inches d.b.h. with the following species codes: 110, 111, 121, 126, 128, or 131. Populated for all forested plots using the National Field Guide protocols (PLOT.MANUAL = 1.6-5.1). Only populated by certain FIA work units (SURVEY.RSCD= 33).

## Codes: DISEASE\_SRS

|   Code | Description                                                                                                                               |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | None.                                                                                                                                     |
|      1 | Fusiform/Comandra rust on species codes 110, 111, 121, 126, 128, and 131, based on any incidence of cankers within 12 inches of the stem. |
|      2 | Hardwood dieback of 10% or more of the crown area. Not recorded on overtopped trees.                                                      |

## 3.1.119 DIEBACK\_SEVERITY\_SRS

Dieback severity, Southern Research Station. A code indicating the severity of hardwood crown dieback. Populated when DISEASE\_SRS = 2. Populated for all forested plots using the National Field Guide protocols (PLOT.MANUAL = 1.6-5.1). Only populated by certain FIA work units (SURVEY.RSCD = 33).

## Codes: DIEBACK\_SEVERITY\_SRS

|   Code | Description               |
|--------|---------------------------|
|      1 | 10-19% of crown affected. |
|      2 | 20-29% of crown affected. |
|      3 | 30-39% of crown affected. |
|      4 | 40-49% of crown affected. |
|      5 | 50-59% of crown affected. |
|      6 | 60-69% of crown affected. |
|      7 | 70-79% of crown affected. |
|      8 | 80-89% of crown affected. |
|      9 | 90-99% of crown affected. |

## 3.1.120 DAMAGE\_AGENT\_CD1

Damage agent code 1. (core: all live tally trees  5.0 inches d.b.h./d.r.c; core optional: all live tally trees  1.0 inch d.b.h./d.r.c.) A code indicating the first damage agent recorded by the field crew when inspecting the tree from bottom to top (roots, bole, branches, foliage). Up to three damage agents can be recorded per tree (DAMAGE\_AGENT\_CD1, DAMAGE\_AGENT\_CD2, and DAMAGE\_AGENT\_CD3). Damage agents are not necessarily recorded in order of severity.The codes used for damage agents come from the January 2012 Pest Trend Impact Plot System (PTIPS) list from the Forest Health Assessment and Applied Sciences Team (FHAAST) that has been modified to meet FIA's needs. The list is modified by each region to meet the specific needs of that region. The general agent codes are listed here. See appendix H for the complete list of codes.

## Codes: DAMAGE\_AGENT\_CD1

|   Code | General Agent                                                  | Damage Threshold*                                                                                                                                                                                                                                                                                                                | Descriptions                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------|----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | -                                                              | No damage.                                                                                                                                                                                                                                                                                                                       | -                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|  10000 | General insects.                                               | Any damage to the terminal leader; damage  20% of the roots or boles with >20% of the circumference affected; damage >20% of the multiple-stems (on multi-stemmed woodland species) with >20% of the circumference affected; >20% of the branches affected; damage  20% of the foliage with  50% of the leaf/needle affected. | Insect damage that cannot be placed in any of the following insect categories.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|  11000 | Bark beetles.                                                  | Any evidence of a successful attack (successful attacks generally exhibit boring dust, many pitch tubes and/or fading crowns).                                                                                                                                                                                                   | Bark beetles ( Dendroctonus , Ips , and other genera) are phloem-feeding insects that bore through the bark and create extensive galleries between the bark and the wood. Symptoms of beetle damage include fading or discolored tree crown (yellow or red), pitch tubes or pitch streaks on the bark, extensive egg galleries in the phloem, boring dust in the bark crevices or at the base of the tree. Bark chipping by woodpeckers may be conspicuous. They inflict damage or destroy all parts of trees at all stages of growth by boring in the bark, inner bark, and phloem. Visible signs of attack include pitch tubes or large pitch masses on the tree, dust and frass on the bark and ground, and resin streaming. Internal tunneling has various patterns. Most have tunnels of uniform width with smaller galleries of variable width radiating from them. Galleries may or may not be packed with fine boring dust. |
|  12000 | Defoliators.                                                   | Any damage to the terminal leader; damage  20% of the foliage with  50% of the leaf/needle affected.                                                                                                                                                                                                                           | These are foliage-feeding insects that may reduce growth and weaken the tree causing it to be more susceptible to other damaging agents. General symptoms of defoliation damage include large amounts of missing foliage, browning foliage, extensive branch mortality, or dead tree tops.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|  13000 | Chewing insects. Note: This is only collected by RMRS and SRS. | Any damage to the terminal leader; damage  20% of the foliage with  50% of the leaf/needle affected.                                                                                                                                                                                                                           | Insects, like grasshoppers and cicadas that chew on trees (those insects not covered by defoliators in code 12000).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |

|   Code | General Agent     | Damage Threshold*                                                                                                                                                                                                                                                                                                                | Descriptions                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|--------|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  14000 | Sucking insects.  | Any damage to the terminal leader; damage  20% of the foliage with  50% of the leaf/needle affected.                                                                                                                                                                                                                           | Adelgids, scales and aphids feed on all parts of the tree. Often they cause galling on branches and trunks. Some appear benign but enable fungi to invade where they otherwise could not (e.g., beech bark disease). The most important ones become conspicuous because of the mass of white, cottony wax that conceals eggs and young nymphs.                                                                                                                                                                                                                                                                                                                                                                               |
|  15000 | Boring insects.   | Any damage to the terminal leader; damage  20% of the roots, stems, or branches.                                                                                                                                                                                                                                                | Most wood boring insects attack only severely declining and dead trees. Certain wood boring insects cause significant damage to trees, especially the exotic Asian longhorn beetle, emerald ash borer, and Sirex wood wasp. Bark beetles have both larval and adult galleries in the phloem and adjacent surface of the wood. Wood borers have galleries caused only by larval feeding. Some, such as the genus Agrilus (including the emerald ash borer) have galleries only in the phloem and surface of the wood. Other wood borers, such as Asian longhorn beetle bore directly into the phloem and wood. Sirex adults oviposit their eggs through the bark, and developing larvae bore directly into the wood of pines. |
|  19000 | General diseases. | Any damage to the terminal leader; damage  20% of the roots or boles with >20% of the circumference affected; damage >20% of the multiple-stems (on multi-stemmed woodland species) with >20% of the circumference affected; >20% of the branches affected; damage  20% of the foliage with  50% of the leaf/needle affected. | Diseases that cannot be placed in any of the following disease categories.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

|   Code | General Agent       | Damage Threshold*   | Descriptions                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------|---------------------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  21000 | Root/butt diseases. | Any occurrence.     | Root disease kills all or a portion of a tree's roots. Quite often, the pathogenic fungus girdles the tree at the root collar. Tree damage includes mortality (often occurring in groups or "centers"), reduced tree growth, and increased susceptibility to other agents (especially bark beetles). General symptoms include resin at the root collar, thin, chlorotic (faded) foliage, and decay of roots. A rot is a wood decay caused by fungi. Rots are characterized by a progression of symptoms in the affected wood. First, the wood stains and discolors, then it begins to lose its structural strength, and finally the wood starts to break down, forming cavities in the stem. Even early stages of wood decay can cause cull due to losses in wood strength and staining of the wood. Rot can lead to mortality, cull, an increased susceptibility to other agents (such as insects), wind throw, and stem breakage. |

|   Code | General Agent                     | Damage Threshold*                                                                                  | Descriptions                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|--------|-----------------------------------|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  22000 | Cankers (non-rust).               | Any occurrence.                                                                                    | A canker -- a sunken lesion on the stem caused by the death of cambium -- may cause tree breakage or kill the portion of the tree above the canker. Cankers may be caused by various agents but are most often caused by fungi. A necrotic lesion begins in the bark of branches, trunk or roots, and progresses inward killing the cambium and underlying cells. The causal agent may or may not penetrate the wood. This results in areas of dead tissue that become deeper and wider. There are two types of cankers, annual and perennial. Annual cankers enlarge only once and do so within an interval briefer than the growth cycle of the tree, usually less than one year. Little or no callus is associated with annual cankers, and they may be difficult to distinguish from mechanical injuries. Perennial cankers are usually the more serious of the two, and grow from year to year with callus forming each year on the canker margin, often resulting in a target shape. The most serious non-rust cankers occur on hardwoods, although branch |
|  22500 | Stem decays.                      | Any visual evidence (conks; fruiting bodies; rotten wood).                                         | Rot occurring in the bole/stems of trees above the roots and stump.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|  23000 | Parasitic / Epiphytic plants.     | Dwarf mistletoes with Hawksworth rating of  3; true mistletoes and vines covering  50% of crown. | Parasitic and epiphytic plants can cause damage to trees in a variety of ways. The most serious ones are dwarf mistletoes, which reduce growth and can cause severe deformities. Vines may damage trees by strangulation, shading, or physical damage. Benign epiphytes, such as lichens or mosses, are not considered damaging agents.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|  24000 | Decline Complexes/ Dieback/Wilts. | Damage  20% dieback of crown area.                                                                | Tree disease which results not from a single causal agent but from an interacting set of factors. Terms that denote the symptom syndrome, such as dieback and wilt, are commonly used to identify these diseases.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|  25000 | Foliage diseases.                 | Damage  20% of the foliage with  50% of the leaf/needle affected.                                | Foliage diseases are caused by fungi and result in needle shed, growth loss, and, potentially, tree mortality. This category includes needle casts, blights, and needle rusts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

|   Code | General Agent   | Damage Threshold*                                                                                                                                  | Descriptions                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|--------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  26000 | Stem rusts.     | Any occurrence on the bole or stems (on multi-stemmed woodland species), or on branches  1 foot from boles or stems; damage to  20% of branches. | A stem rust is a disease caused by fungi that kill or deform all or a portion of the stem or branches of a tree. Stem rusts are obligate parasites and host specialization is very common. They infect and develop on fast-growing tissues and cause accelerated growth of infected tissues resulting in galls or cankers. Heavy resinosis is usually associated with infections. Sometimes yellow or reddish-orange spores are present giving a "rusty" appearance. Damage occurs when the disease attacks the cambium of the host, girdling and eventually killing the stem above the attack. Symptoms of rusts include galls (an abnormal and pronounced swelling or deformation of plant tissue that forms on branches or stems) and cankers (a sunken lesion on the stem caused by death of the cambium which often results in the death of tree tops and branches). |
|  27000 | Broom rusts.    |  50% of crown area affected.                                                                                                                      | Broom rust is a disease caused by fungi that kill or deform all or a portion of the branches of a tree. Broom rusts are obligate parasites and host specialization is very common. They infect and develop on fast-growing tissues and cause accelerated growth of infected tissues resulting in galls. Symptoms of rusts include galls, an abnormal and pronounced swelling or deformation of plant tissue that forms on branches or stems.                                                                                                                                                                                                                                                                                                                                                                                                                              |
|  30000 | Fire.           | Damage  20% of bole circumference; >20% of stems on multi-stemmed woodland species affected;  20% of crown affected.                             | Fire damage may be temporary, such as scorched foliage, or may be permanent, such as in cases where cambium is killed around some portion of the bole. The location and amount of fire damage will determine how the damage may affect the growth and survival of the tree. Fire often causes physiological stress, which may predispose the tree to attack by insects of other damaging agents.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

|   Code | General Agent     | Damage Threshold*                                                                                                                                                                                                                                                                                                                | Descriptions                                                                                                                                                                                                   |
|--------|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  41000 | Wild animals.     | Any damage to the terminal leader; damage  20% of the roots or boles with>20% of the circumference affected; damage >20% of the multiple-stems (on multi-stemmed woodland species) with >20% of the circumference affected; >20% of the branches affected; damage  20% of the foliage with  50% of the leaf/needle affected.  | Wild animals from birds to large mammals cause open wounds. Some common types of damage include: sapsucker bird peck, deer rub, bear clawing, porcupine feeding, and beaver gnawing.                           |
|  42000 | Domestic animals. | Any damage to the terminal leader; damage  20% of the roots or boles with>20% of the circumference affected; damage >20% of the multiple-stems (on multi-stemmed woodland species) with >20% of the circumference affected; >20% of the branches affected; damage  20% of the foliage with  50% of the leaf/needle affected.  | Open wounds caused by cattle and horses occur on the roots and lower trunk. Soil compaction from the long term presence of these animals in a woodlot can also cause indirect damage.                          |
|  50000 | Abiotic.          | Any damage to the terminal leader; damage  20% of the roots or boles with>20% of the circumference affected; damage >20% of the multiple-stems (on multi-stemmed woodland species) with >20% of the circumference affected; >20% of the branches affected; damage  20% of the foliage with  50% of the leaf/needle affected.  | Abiotic damages are those that are not caused by other organisms. In some cases, the type and severity of damage may be similar for different types of agents (e.g., broken branches from wind, snow, or ice). |
|  60000 | Competition.      | Overtopped shade-intolerant trees that are not expected to survive for 5 years or saplings not expected to reach tree size (5.0 inches d.b.h./d.r.c.).                                                                                                                                                                           | Suppression of overtopped shade-intolerant species. Trees that are not expected to survive for 5 years or saplings not expected to reach tree size (5.0 inches d.b.h./d.r.c.).                                 |
|  70000 | Human activities. | Any damage to the terminal leader; damage  20% of the roots or boles with >20% of the circumference affected; damage >20% of the multiple-stems (on multi-stemmed woodland species) with >20% of the circumference affected; >20% of the branches affected; damage  20% of the foliage with  50% of the leaf/needle affected. | People can injure trees in a variety of ways, from poor pruning, to vandalism, to logging injury. Signs include open wounds or foreign embedded objects.                                                       |

|   Code | General Agent   | Damage Threshold*                                                                                                                                                                                                                                                                                                                | Descriptions                                                                                 |
|--------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
|  71000 | Harvest.        | Removal of  10% of cubic volume.                                                                                                                                                                                                                                                                                                | Only recorded for woodland species trees that have partial cutting.                          |
|  90000 | Other damage.   | Any damage to the terminal leader; damage  20% of the roots or boles with >20% of the circumference affected; damage >20% of the multiple-stems (on multi-stemmed woodland species) with >20% of the circumference affected; >20% of the branches affected; damage  20% of the foliage with  50% of the leaf/needle affected. | -                                                                                            |
|  99000 | Unknown damage. | Any damage to the terminal leader; damage  20% of the roots or boles with >20% of the circumference affected; damage >20% of the multiple-stems (on multi-stemmed woodland species) with >20% of the circumference affected; >20% of the branches affected; damage  20% of the foliage with  50% of the leaf/needle affected. | Use this code only when observed damage cannot be attributed to a general or specific agent. |

- * Some Regional specific damage agents within a category may have differing damage thresholds.

## 3.1.121 DAMAGE\_AGENT\_CD2

Damage agent code 2. (core: all live tally trees  5.0 inches d.b.h./d.r.c; core optional: all live tally trees  1.0 inch d.b.h./d.r.c.) See DAMAGE\_AGENT\_CD1.

## 3.1.122 DAMAGE\_AGENT\_CD3

Damage agent code 3. (core: all live tally trees  5.0 inches d.b.h./d.r.c; core optional: all live tally trees  1.0 inch d.b.h./d.r.c.) See DAMAGE\_AGENT\_CD1.

## 3.1.123 CENTROID\_DIA

Centroid diameter ( Pacific Islands ). The outside bark diameter, in inches, measured at CENTROID\_DIA\_HT\_ACTUAL. For tree ferns, diameter is measured where the fronds emerge from the trunk. Only populated by certain FIA work units (SURVEY.RSCD = 26) for the Pacific Islands. This diameter is part of the upper stem diameter protocol that began with remeasurement, except for Hawaii, where the protocol was implemented in the first measurement.

## 3.1.124 CENTROID\_DIA\_HT

Calculated centroid diameter height ( Pacific Islands ). The height, in feet, to stem centroid. The stem centroid is located at 30 percent of the total length (HT) of the stem. Only populated by certain FIA work units (SURVEY.RSCD = 26) for the Pacific Islands. This height is part of the upper stem diameter protocol that began with the first remeasurement, except for Hawaii, where the protocol was implemented in the first measurement.

## 3.1.125 CENTROID\_DIA\_HT\_ACTUAL

Actual centroid diameter height ( Pacific Islands ). The height, in feet, to where stem centroid diameter was actually measured. It may differ from CENTROID\_DIA\_HT if abnormalities in the stem prevented a normal diameter measurement. Only populated by certain FIA work units (SURVEY.RSCD = 26) for the Pacific Islands. This height is part of the upper stem diameter protocol that began with the first remeasurement, except for Hawaii, where the protocol was implemented in the first measurement.

## 3.1.126 UPPER\_DIA

Upper stem diameter ( Pacific Islands ). The outside bark upper stem diameter, in inches, measured at least 3 feet above the point where DIA was taken. For larger trees, UPPER\_DIA was recorded at the point where the main stem was at least 4.0 inches in diameter. This diameter is used in the calculation of stem taper, needed to improve the estimation of stem volume. Only populated by certain FIA work units (SURVEY.RSCD = 26) for the Pacific Islands. This is the legacy upper stem diameter protocol and will not be collected after the first remeasurement.

## 3.1.127 UPPER\_DIA\_HT

Upper stem diameter height ( Pacific Islands ). The height, in feet, to where upper stem diameter (UPPER\_DIA) was measured. Only populated by certain FIA work units (SURVEY.RSCD= 26) for the Pacific Islands. This is the legacy upper stem diameter protocol and will not be collected after the first remeasurement.

## 3.1.128 VOLCSSND

Sound cubic-foot wood volume in the sawlog portion of a sawtimber tree. The sound cubic-foot volume of wood in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches minimum d.b.h. for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods) or to where the central stem breaks into limbs, all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber-size trees have entries in this field if they are growing-stock trees (TREECLCD = 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null) in this field. Does not include rotten and missing cull (volume loss due to rotten and missing cull defect has been deducted). Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.129 DRYBIO\_SAWLOG

Dry biomass of wood in the sawlog portion of a sawtimber tree. The oven-dry biomass, in pounds, of wood in the sawlog portion of timber species trees of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches minimum d.b.h. for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods) or to where the central stem breaks into limbs, all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods) and standing dead trees. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.130 DAMAGE\_AGENT\_CD1\_SRS

Damage agent code 1 ( Caribbean Islands ), Southern Research Station. A code indicating the first damage agent observed when inspecting the tree from bottom to top (roots, bole, branches, foliage). Up to three damage agents can be recorded (DAMAGE\_AGENT\_CD1\_SRS, DAMAGE\_AGENT\_CD2\_SRS, DAMAGE\_AGENT\_CD3\_SRS). If more than one agent is observed, the most threatening one is listed first where agents threatening survival are listed first and agents threatening wood quality second. The codes used for damage agents come from the January 2012 Pest Trend Impact Plot System (PTIPS) list from the Forest Health Assessment and Applied Sciences Team (FHAAST) that has been modified to meet FIA's needs. See appendix H for the complete list of codes. Only populated by certain FIA work units (SURVEY.RSCD = 33) for the Caribbean Islands.

## 3.1.131 DAMAGE\_AGENT\_CD2\_SRS

Damage agent code 2 ( Caribbean Islands ), Southern Research Station. See DAMAGE\_AGENT\_CD1\_SRS.

## 3.1.132 DAMAGE\_AGENT\_CD3\_SRS

Damage agent code 3 ( Caribbean Islands ), Southern Research Station. See DAMAGE\_AGENT\_CD1\_SRS.

## 3.1.133 DRYBIO\_AG

Aboveground dry biomass of wood and bark. The oven-dry biomass, in pounds, of wood and bark in the aboveground portion, excluding foliage, of live and standing dead trees  1.0 inch d.b.h./d.r.c. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. Refer to appendix K on FIA volume, biomass, and carbon estimation.

Note: Not populated for standing dead saplings (1.0-4.9 inches d.b.h./d.r.c.) when PLOT.MANUAL &lt;7.0.

Figure 3-5: Illustration of aboveground dry biomass (DRYBIO\_AG) in black. Roots, foliage, and missing wood are excluded. Wood biomass is proportionally reduced to amount for cull. See DRYBIO\_AG for a full description of this attribute.

<!-- image -->

## 3.1.134 ACTUALHT\_CALC

Actual height, calculated. The calculated length (height) of the tree to the nearest foot from ground level to the highest remaining portion of the tree still present and attached to the bole. The calculations are made using regional methods.

## 3.1.135 ACTUALHT\_CALC\_CD

Actual height, calculated, code. A code identifying the method used to calculate the ACTUALHT\_CALC value. Only populated by certain FIA work units (SURVEY.RSCD = 24). For more information about regional methods, contact the appropriate FIA work unit (table 1-1).

## Codes: ACTUALHT\_CALC\_CD

|   Code | Meaning      | Description                                              |
|--------|--------------|----------------------------------------------------------|
|      1 | NERS method. | Value calculated for NERS States using regional methods. |

## 3.1.136 CULL\_BF\_ROTTEN

Rotten/missing board-foot cull of the sawlog. The percent of volume within the recorded sawlog length (SAWHT) that cannot be used to produce boards, because of rot or missing sections of the bole. Does not include cull due to sweep, crook, excessive branches (e.g., whorls), large limbs and other defects. Only populated by certain FIA work units (SURVEY.RSCD= 24). Not collected for all years or States.

## 3.1.137 CULL\_BF\_ROTTEN\_CD

Rotten/missing board-foot cull of the sawlog code. A code indicating if the CULL\_BF\_ROTTEN attribute is not null. Only populated by certain FIA work units (SURVEY.RSCD = 24). Not collected for all years or States.

## Codes: CULL\_BF\_ROTTEN\_CD

|   Code | Description                                                        |
|--------|--------------------------------------------------------------------|
|      1 | Rotten board-foot cull (CULL_BF_ROTTEN) of the sawlog is not null. |

## 3.1.138 CULL\_BF\_ROUGH

Rough board-foot cull of the sawlog. The percent of volume within the recorded sawlog length (SAWHT) that cannot be used to produce boards, because of sweep, crook, excessive branches (e.g., whorls), large limbs and other defects. Does not include cull due to rot. Only populated by certain FIA work units (SURVEY.RSCD = 24). Not collected for all years or States.

## 3.1.139 CULL\_BF\_ROUGH\_CD

Rough board-foot cull of the sawlog code. A code indicating if the CULL\_BF\_ROUGH attribute is not null. Only populated by certain FIA work units (SURVEY.RSCD = 24). Not collected for all years or States.

## Codes: CULL\_BF\_ROUGH\_CD

|   Code | Description                                                      |
|--------|------------------------------------------------------------------|
|      1 | Rough board-foot cull (CULL_BF_ROUGH) of the sawlog is not null. |

## 3.1.140 PREVDIA\_FLD

Previous diameter, field. The previous diameter, in inches, of the sample tree at the point of diameter measurement, if the value was updated by the current field crew.

Note: PREVDIA differs from PREVDIA\_FLD when the field crew updates the downloaded value in the data collection program.

- · PREVDIA - This value is the downloaded diameter from the previous inventory record.
- · PREVDIA\_FLD - This value is the editable field.

## 3.1.141 TREECLCD\_31\_NCRS

Tree class code (version 3.1), North Central Research Station. A classification of the general quality of a tree that is  5.0 inches d.b.h. It classifies the quality of a live sawtimber tree based on the present condition. It also forecasts the potential quality of a live poletimber tree when it becomes sawtimber size. For standing dead trees, it identifies those trees that could be salvaged for wood fiber (e.g., chips) if a salvage operation was imminent.

Collected on all live and dead trees  5.0 inches d.b.h. for States in the NCRS region (SURVEY.RSCD = 23) for PLOT.MANUAL\_NCRS  3.0.

## Codes: TREECLCD\_31\_NCRS

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      2 | Growing Stock - A live sawtimber-size tree with one-third or more of the gross board-foot volume in the entire sawlog length meeting grade, soundness, and size requirements; or the potential to do so for poletimber-size trees. It must contain one merchantable 12-foot log or two non-contiguous merchantable 8-foot logs, now (sawtimber) or prospectively (poletimber).                                                                                                                                                      |
|      3 | Rough Cull - A live tree that does not contain at least one 12-foot sawlog or two noncontiguous 8-foot logs now (sawtimber) or prospectively (poletimber), primarily because of roughness or poor form within the sawlog length. Or sawtimber and prospectively poletimber with two-thirds or more of its gross board-foot volume that does not meet size, soundness, and grade requirements; and 50% or more of the assigned total board-foot cull within the sawlog length is rough cull.                                         |
|      4 | Rotten Cull - A live tree that does not contain at least one 12-foot sawlog or two noncontiguous 8-foot logs now (sawtimber) or prospectively (poletimber) and/or do not meet grade specifications for percent sound primarily because of rot within the sawlog length. Or sawtimber and prospectively poletimber with two-thirds or more of its gross board-foot volume that does not meet size, soundness, and grade requirements; and 50% or more of the assigned total board-foot cull within the sawlog length is rotten cull. |
|      5 | Salvable Dead - A standing dead tree with at least one-third merchantable sound volume. ROTTEN/MISSING CUBIC-FOOT CULL does not exceed 67%. Note: ROUGH CUBIC-FOOT CULL is not a criterion for determining salvable dead.                                                                                                                                                                                                                                                                                                           |
|      6 | Nonsalvable Dead - A standing dead tree that does not qualify as salvable.                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

## 3.1.142 TREE\_GRADE\_NCRS

Tree grade, North Central Research Station. A 3-digit code indicating the quality of sawtimber-sized trees that have a TREECLCD\_NCRS of 20 or 31. The first digit indicates the grade and the second and third digits represent the limiting factor for hardwood grades.

Minimum sawlog length for tree grades is 12 feet and for log grades is 8 feet. Sawlog lengths do not extend above large forks, have excessive limbs or other defects, or have a section of the tree bole that does not meet minimum log grade specification. Limitations or "stoppers" for all softwoods and for hardwood grades 1, 2 and 3 include: any limb (live or dead) having a collar diameter exceeding the stem DOB at that point; or any group of 2.0-inch collar diameter or larger limbs (live or dead), within a 1-foot span, having a combined sum of diameters greater than the stem DOB of that section. Limitations for grade 4 hardwoods include: any limb or group of limbs, within a 1 foot span, with a collar diameter or sum of collar diameters greater than 1/3 of the stem DOB of that section.

Data collected for States in the NCRS region (SURVEY.RSCD = 23) in inventory years 1999-2009, PLOT.MANUAL\_NCRS = 1.0-4.0. Refer to PLOT.MANUAL\_NCRS for additional information (such as tables on tree grades and standard specifications for logs).

## First digit:

- · For a hardwood sawtimber tree (TREECLCD\_NCRS = 20), the grade of the sawlog portion of the tree is based on "Hardwood Tree Grades for Factory Lumber" (Hanks 1976).

- · For a softwood sawtimber tree (TREECLCD\_NCRS = 20), the grade is based on the portion of the log that gives the best grade. For a softwood, where TREECLCD\_NCRS = 31, the grade is based on the log that is present.

Codes: TREE\_GRADE\_NCRS (1st digit)

|   Grade | Valid species                  |
|---------|--------------------------------|
|       1 | Hardwoods and softwoods.       |
|       2 | Hardwoods and softwoods.       |
|       3 | Hardwoods and softwoods.       |
|       4 | Hardwoods only and white pine. |
|       5 | Hardwoods only.                |

## Second and third digits:

- · For hardwoods with a grade 2, 3, 4, or 5, the second and third digit indicate the limiting quality factor that is keeping the log from moving into a better quality grade. For hardwood logs with a grade 5, the second digit is a 2 or 7 when an 8-foot log is present. If a 12-foot upper log is present, the second digit is 6.
- · For softwoods , the second and third digits are always '00'.

Codes: TREE\_GRADE\_NCRS (2nd and 3rd digits)

|   Code | Limiting factor                                   |
|--------|---------------------------------------------------|
|     00 | Not applicable, already a grade 1, all softwoods. |
|     10 | Diameter.                                         |
|     20 | Length.                                           |
|     30 | Clear cuttings.                                   |
|     40 | Sweep and crook.                                  |
|     50 | Cull.                                             |
|     60 | Position in tree.                                 |
|     70 | Multiple factors.                                 |
|     80 | Diameter and clear cutting.                       |

## Codes: TREE\_GRADE\_NCRS (Possible code combinations)

|   Code | Tree type            |
|--------|----------------------|
|    000 | Hardwoods/softwoods. |
|    100 | Hardwoods/softwoods. |
|    200 | Softwoods.           |
|    210 | Hardwoods.           |
|    230 | Hardwoods.           |
|    240 | Hardwoods.           |
|    250 | Hardwoods.           |
|    270 | Hardwoods.           |

|   Code | Tree type                    |
|--------|------------------------------|
|    280 | Hardwoods.                   |
|    300 | Softwoods.                   |
|    310 | Hardwoods.                   |
|    330 | Hardwoods.                   |
|    340 | Hardwoods.                   |
|    350 | Hardwoods.                   |
|    370 | Hardwoods.                   |
|    380 | Hardwoods.                   |
|    400 | Softwoods - white pine only. |
|    430 | Hardwoods.                   |
|    520 | Hardwoods.                   |
|    560 | Hardwoods.                   |
|    570 | Hardwoods.                   |

## 3.1.143 BOUGHS\_AVAILABLE\_NCRS

Balsam fir boughs available, North Central Research Station. A code indicating if harvestable balsam fir boughs are present on trees  1.0 inch d.b.h. Boughs are harvestable if they occur in the bottom 7.5 feet of the tree and there is at least one branch no larger in diameter than a pencil where clipped and they are at least 18 inches in length with live needles.

Data populated for States in the NCRS region (SURVEY.RSCD = 23) for PLOT.MANUAL\_NCRS = 2.0-3.0 (INVYR = 2004-2006). Only populated in Minnesota for

PLOT.MANUAL\_NCRS = 3.1-4.0 (INVYR in 2007, 2008).

## Codes: BOUGHS\_AVAILABLE\_NCRS

|   Code | Description          |
|--------|----------------------|
|      0 | No boughs available. |
|      1 | Boughs available.    |

## 3.1.144 BOUGHS\_HRVST\_NCRS

Balsam fir boughs harvested, North Central Research Station. A code indicating whether or not balsam fir boughs were harvested on trees  1.0 inch d.b.h.

Data populated for States in the NCRS region (SURVEY.RSCD = 24) for PLOT.MANUAL\_NCRS = 2.0-3.0 (INVYR = 2004-2006). Only populated in Minnesota for PLOT.MANUAL\_NERS = 3.1-4.0 (INVYR in 2007, 2008).

## Codes: BOUGHS\_HRVST\_NCRS

|   Code | Description                     |
|--------|---------------------------------|
|      0 | Boughs have not been harvested. |
|      1 | Boughs have been harvested.     |

## 3.1.145 TREECLCD\_31\_NERS

Tree class code (version 3.1), Northeastern Research Station. A classification of the general quality of a tree that is  5.0 inches d.b.h. It classifies the quality of a live sawtimber tree based on the present condition. It also forecasts the potential quality of a live poletimber tree when it becomes sawtimber size. For standing dead trees, it identifies trees that could be salvaged for wood fiber (e.g., chips) if a salvage operation was imminent. Implemented beginning with PLOT.MANUAL\_NERS = 3.1 (inventory year 2007) of the field guide.

Data collected as follows (SURVEY.RSCD = 24):

- · All trees  5.0 inches d.b.h./d.r.c. when STATUSCD = 1 or 2 and STANDING\_DEAD\_CD\_NERS = 1.
- · Annual data inventory years 2007 to present.

## Codes: TREECLCD\_31\_NERS

|   Code | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      2 | Growing Stock - A live sawtimber-size tree with one-third or more of the gross board-foot volume in the entire sawlog length meeting grade, soundness, and size requirements; or the potential to do so for poletimber-size trees. It must contain one merchantable 12-foot log or two non-contiguous merchantable 8-foot logs, now (sawtimber) or prospectively (poletimber).                                                                                                                                                      |
|      3 | Rough Cull - A live tree that does not contain at least one 12-foot sawlog or two noncontiguous 8-foot logs now (sawtimber) or prospectively (poletimber), primarily because of roughness or poor form within the sawlog length. Or sawtimber and prospectively poletimber with two-thirds or more of its gross board-foot volume that does not meet size, soundness, and grade requirements; and 50% or more of the assigned total board-foot cull within the sawlog length is rough cull.                                         |
|      4 | Rotten Cull - A live tree that does not contain at least one 12-foot sawlog or two noncontiguous 8-foot logs now (sawtimber) or prospectively (poletimber) and/or do not meet grade specifications for percent sound primarily because of rot within the sawlog length. Or sawtimber and prospectively poletimber with two-thirds or more of its gross board-foot volume that does not meet size, soundness, and grade requirements; and 50% or more of the assigned total board-foot cull within the sawlog length is rotten cull. |
|      5 | Salvable Dead - A standing dead tree with at least one-third merchantable sound volume. Rotten/missing cubic-foot cull does not exceed 67%. Note: Rough cubic-foot cull is not a criterion for determining salvable dead.                                                                                                                                                                                                                                                                                                           |
|      6 | Nonsalvable Dead - A standing dead tree that does not qualify as salvable.                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

## 3.1.146 AGENTCD\_NERS

General damage / cause of death (agent) code, Northeastern Research Station. The cause of death for all trees since the previous survey. Also used as a damage indicator for periodic surveys until 2000.

Data collected as follows (SURVEY.RSCD = 24):

- · Annual data through inventory year 2006, except Ohio (39).
- · Last periodic for CT, DE, MD, MA, NH, NJ, NY, RI, VT and WV:

1993: New York (36)

1997: New Hampshire (33), Vermont (50)

1998: Connecticut (9), Massachusetts (25), Rhode Island (44)

1999: Delaware (10), Maryland (24), New Jersey (34)

2000: West Virginia (54)

## Codes: AGENTCD\_NERS (periodic inventories in CT, DE, MD, MA, NH, NJ, RI, VT, WV)

|   Code | Description        |
|--------|--------------------|
|      0 | None.              |
|      1 | Insect.            |
|      2 | Disease.           |
|      3 | Fire.              |
|      4 | Animal.            |
|      5 | Weather.           |
|      6 | Suppression.       |
|      7 | Unknown and other. |
|      8 | Harvest-related.   |

## Codes: AGENTCD\_NERS (periodic inventory in NY, 1993)

|   Code | Description             |
|--------|-------------------------|
|     00 | None.                   |
|     10 | Insect.                 |
|     20 | Disease.                |
|     30 | Fire.                   |
|     40 | Animal.                 |
|     41 | Animal browse: 1-10%.   |
|     42 | Animal browse: 11-40%.  |
|     43 | Animal browse: 41-100%. |
|     50 | Weather.                |
|     60 | Suppression.            |
|     70 | Harvest-related.        |
|     80 | Other human.            |
|     90 | Unknown or not listed.  |
|     99 | Dead sapling.           |

## Codes: AGENTCD\_NERS (annual data: 2000-2003)

|   Code | Description     |
|--------|-----------------|
|     10 | Insect damage.  |
|     20 | Disease damage. |
|     30 | Fire damage.    |
|     40 | Animal damage.  |
|     50 | Weather damage. |

## Tree Table

|   Code | Description                                                       |
|--------|-------------------------------------------------------------------|
|     60 | Vegetation (suppression, competition, vines/kudzu).               |
|     70 | Unknown / not sure / other (include notes).                       |
|     80 | Human-caused damage (cultural, logging, accidental damage, etc.). |
|     90 | Physical (hit by a falling tree).                                 |

## Codes: AGENTCD\_NERS (annual data: 2004-2006)

|   Code | Description                                                                                                                                                             |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|     10 | Insect.                                                                                                                                                                 |
|     20 | Disease.                                                                                                                                                                |
|     30 | Fire.                                                                                                                                                                   |
|     40 | Animal.                                                                                                                                                                 |
|     50 | Weather.                                                                                                                                                                |
|     60 | Vegetation (suppression, competition, vines/kudzu).                                                                                                                     |
|     70 | Unknown / not sure / other - includes death from human activity not related to silvicultural or land clearing activity (accidental, random, etc.).                      |
|     80 | Silvicultural or land clearing activity (death caused by harvesting or other silvicultural activity, including girdling, chaining, etc., or to land clearing activity). |

## 3.1.147 BFSNDCD\_NERS

Board-foot soundness code, Northeastern Research Station. A code based on percentage of board-foot cull that is sound cull. Sound cull is caused by form defects: sweep, crook, limbs and forks.

Data collected as follows (SURVEY.RSCD = 24):

- · Live and dead trees,  9.0 inches d.b.h. if softwood, and  11.0 inches d.b.h. if hardwood.
- · Annual data through inventory year 2006, except Ohio (39).
- · Last periodic for CT, DE, MD, MA, NH, NJ, NY, RI, VT and WV:

1993: New York (36)

1997: New Hampshire (33), Vermont (50)

1998: Connecticut (9), Massachusetts (25), Rhode Island (44)

1999: Delaware (10), Maryland (24), New Jersey (34)

2000: West Virginia (54)

## Codes: BFSNDCD\_NERS

|   Code | Description   |
|--------|---------------|
|      0 | 00-09%        |
|      1 | 10-19%        |
|      2 | 20-29%        |
|      3 | 30-39%        |
|      4 | 40-49%        |
|      5 | 50-59%        |
|      6 | 60-69%        |
|      7 | 70-79%        |
|      8 | 80-89%        |
|      9 | 90-100%       |

## 3.1.148 AGECHKCD\_RMRS

Radial growth and tree age check code, Rocky Mountain Research Station. A code indicating the method used to obtain radial growth and tree age. Only populated by certain FIA work units (SURVEY.RSCD = 22).

Note: Code 3 was added starting with PLOT.MANUAL = 6.0.

## Codes: AGECHKCD\_RMRS

|   Code | Description                                                                                                                                                                                                                  |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | • Age/radial growth measured directly from core. • Age/radial growth calculated from remeasurement data (same tree).                                                                                                         |
|      1 | • Age/radial growth was estimated due to rot. • Age/radial growth was estimated because rings were difficult to count (old suppressed trees). • Age was estimated because the increment bore could not reach to tree center. |
|      2 | • Age/radial growth was calculated from a similar remeasure tree (same species and diameter class). • Age/radial growth was based on a similar tree off the subplot.                                                         |
|      3 | • Age measured from a collected tree core (for cores collected and sent into the office for aging).                                                                                                                          |

## 3.1.149 PREV\_AGECHKCD\_RMRS

Previous radial growth and tree age check code, Rocky Mountain Research Station. A code indicating the method used to obtain radial growth and tree age assigned at the previous inventory (from annual or periodic data). Populated for PLOT.MANUAL  4.0. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: PREV\_AGECHKCD\_RMRS

|   Code | Description                                                                                                                                                                                                                  |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | • Age/radial growth measured directly from core. • Age/radial growth calculated from remeasurement data (same tree).                                                                                                         |
|      1 | • Age/radial growth was estimated due to rot. • Age/radial growth was estimated because rings were difficult to count (old suppressed trees). • Age was estimated because the increment bore could not reach to tree center. |
|      2 | • Age/radial growth was calculated from a similar remeasure tree (same species and diameter class). • Age/radial growth was based on a similar tree off the subplot.                                                         |
|      3 | • Age measured from a collected tree core (for cores collected and sent into the office for aging).                                                                                                                          |

Note: Code 3 was added starting with PLOT.MANUAL = 6.0.

## 3.1.150 PREV\_BHAGE\_RMRS

Previous breast height age, Rocky Mountain Research Station. The breast height age (BHAGE) assigned to a tree at the previous inventory (from annual or periodic data). Populated for PLOT.MANUAL  4.0.

BHAGE is the age of a live tree derived from counting tree rings from an increment core sample extracted at a height of 4.5 feet above ground. Breast height age is collected for a subset of trees and only for trees where the diameter is measured at breast height (d.b.h.). This data item is used to calculate classification attributes such as stand age. It is left blank (null) when it is not collected.

For RMRS (SURVEY.RSCD = 22), one tree is sampled for each species and broad diameter class present on a plot.

## 3.1.151 PREV\_TOTAGE\_RMRS

Previous total age, Rocky Mountain Research Station. The total age assigned to a tree at the previous inventory (from annual or periodic data). Populated for PLOT.MANUAL  4.0. The age for live trees is derived by counting tree rings from an increment core sample extracted at the base of a tree where diameter is measured at root collar (d.r.c.), or for small saplings (1.0-2.9 inches d.b.h.) by counting all branch whorls, or by adding a species-dependent number of years to breast height age. Total age is collected for a subset of trees and is used to calculate classification attributes such as stand age. It is left blank (null) when it is not collected. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 3.1.152 PREV\_TREECLCD\_RMRS

Previous tree class code, Rocky Mountain Research Station. The tree class (TREECLCD\_RMRS) assigned at the previous inventory (from annual or periodic data). This attribute is downloaded from the previous inventory and is editable by the current field crew. If the past tree class is obviously wrong (e.g., the previous code was recorded as 6 [soft dead] and the tree is still alive), an updated PREV\_TREECLCD\_RMRS is recorded. This attribute is also recorded for new mortality trees. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 3.1.153 RADAGECD\_RMRS

Radial growth / age code, Rocky Mountain Research Station. A code indicating if growth and/or age information is required for the tree.

Tree age and radial growth information are collected for specified tally trees and timber species site trees. In addition, age information is collected for timber species seedling counts. General guidelines for radial growth and age tree selection are as follows:

## Radial growth and age tree selection guidelines:

## · Timber species -

-  Radial growth information is required for a minimum of two trees in each diameter class (starting with the 4-inch class) for each species.
-  Age information is required for a minimum of one tree in each diameter class and species, and for one timber species seedling count per species (i.e., one count for each species group for the entire plot, not condition class).
-  For both radial growth and age, if rough or rotten trees are bored, select additional sound trees if tallied.

## · Woodland species -

-  For each woodland genus group tallied across the subplots, select one representative live tally tree within each stand-size class tallied. Core the largest stem near the base to obtain the age and radial.

## Codes: RADAGECD\_RMRS

|   Code | Description                                                                                                                                                                 |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | No. Do NOT collect radial growth or age information. This is not a site tree nor an age and/or growth tree.                                                                 |
|      1 | Yes. Collect only radial growth; this is a timber species growth tree only.                                                                                                 |
|      2 | Yes. Collect both radial growth and age information; this tree is either a site tree or an age/growth tree. Also use this code for 2-inch class saplings that get age only. |
|      3 | Yes. Collect radial growth, age will be determined from the core. Use this code where it is required to collect tree cores (cannot be used for site trees).                 |
|      4 | Yes. Use Past/Current Diameters for growth (replaces just radial).                                                                                                          |
|      5 | Yes. Collect age information. Use Past/Current Diameters for growth (replace radial).                                                                                       |

Diameter size-class ranges for timber species are as follows:

| Stand-Size Class   | Softwoods                                    | Hardwoods                 |
|--------------------|----------------------------------------------|---------------------------|
| -                  | Size-class range (d.b.h.)                    | Size-class range (d.b.h.) |
| 1                  | 0-0.9 inches (count whorls/scars): age only. | 0-0.9 inches.             |
| 1                  | 1.0-2.9 inches (age at base): age only.      | 1.0-2.9 inches.           |
| 1                  | 3.0-4.9 inches (age at BH): age and radial.  | 3.0-4.9 inches.           |
| 2                  | 5.0-8.9 inches.                              | 5.0-8.9 inches.           |
| 2                  | -                                            | 9.0-10.9 inches.          |
| 3                  | 9.0-12.9 inches.                             | 11.0-12.9 inches.         |

| Stand-Size Class   | Softwoods                 | Hardwoods                 |
|--------------------|---------------------------|---------------------------|
| -                  | Size-class range (d.b.h.) | Size-class range (d.b.h.) |
| 3                  | 13.0-16.9 inches.         | 13.0-16.9 inches.         |
| 3                  | 17.0-20.9 inches.         | 17.0-20.9 inches.         |
| 3                  | etc.                      | etc.                      |

## 3.1.154 RADGRW\_RMRS

Radial growth, Rocky Mountain Research Station. A 2-digit number indicating the length of a 10-year radial increment for trees that require radial growth information to be collected (see RADAGECD\_RMRS for radial growth and age tree selection guidelines).

Radial growth measurement is taken to the nearest 1/20th inch for the last 10 years of radial growth from an increment core taken immediately below the point of diameter measurement and at a right angle to the bole. Using a ruler with a 1/20th-inch scale, the length on the core is measured from the inner edge of the last (most recent) complete summer wood ring to the inner edge of the summer wood ring 10 years previous (for example, 6/20 inches is recorded as 06 and 23/20 inches is recorded as 23). Only populated by certain FIA work units (SURVEY.RSCD = 22).

## 3.1.155 VOLBSGRS

Gross board-foot wood volume in the sawlog portion of a sawtimber tree (Scribner Rule). The total board-foot (Scribner Rule) volume of wood in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods), or to where the central stem breaks into limbs all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per unit area information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber-size trees have entries in this field if they are growing-stock trees (TREECLCD = 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null) in this field. Only populated by certain FIA work units (SURVEY.RSCD = 22, 26, 27).

## 3.1.156 VOLBSNET

Net board-foot wood volume in the sawlog portion of a sawtimber tree (Scribner Rule). The net board-foot (Scribner Rule) volume of wood in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods), or to where the central stem breaks into limbs all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per unit area information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber-size trees have entries in this field if they are growing-stock trees (TREECLCD = 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null) in this field. Form cull and rotten/missing cull are excluded. Only populated by certain FIA work units (SURVEY.RSCD = 22, 26, 27).

Figure 3-6: Illustration of timber species net board-foot wood volume in the sawlog portion of a sawtimber tree (VOLBSNET) in black. Gray trees and gray parts are excluded. See VOLBSNET for a full description of this attribute.

<!-- image -->

## 3.1.157 SAPLING\_FUSIFORM\_SRS

Sapling fusiform, Southern Research Station. A code indicating the incidence of fusiform occurring on the main stem or on a live branch within 12 inches of the main stem of longleaf, slash, and loblolly pine saplings (STATUSCD = 1 and SPCD = 11, 121, or 131 and 1  DIA &lt;5). Only populated by certain FIA work units (SURVEY.RSCD = 33). Not populated for the Caribbean Islands.

## Codes: SAPLING\_FUSIFORM\_SRS

|   Code | Description       |
|--------|-------------------|
|      0 | None.             |
|      1 | Fusiform present. |

## 3.1.158 EPIPHYTE\_PNWRS

Epiphyte loading ( Pacific Islands ), Pacific Northwest Research Station. A rating indicating the extent of epiphyte loading. Epiphytes are defined as plants that use the tree for support, however, they do not draw nourishment from it. The rating is based on the Hawksworth (1979) six-class rating system. Using this rating system, the live crown is divided into thirds, and each third is rated using the following scale: 0 is for no visible epiphytes, 1 for &lt;50 percent of the branches or bole loaded with epiphytes, 2 for &gt;50 percent of the branches or bole loaded with epiphytes. The ratings for each third are summed together to yield the Hawksworth rating. This rating is collected for all live trees  1.0 inch d.b.h. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## Tree Table

## Codes: EPIPHYTE\_PNWRS

|   Code | Description                                           |
|--------|-------------------------------------------------------|
|      0 | Hawksworth tree rating of 0, none.                    |
|      1 | Hawksworth tree rating of 1, light epiphyte loading.  |
|      2 | Hawksworth tree rating of 2, light epiphyte loading.  |
|      3 | Hawksworth tree rating of 3, medium epiphyte loading. |
|      4 | Hawksworth tree rating of 4, medium epiphyte loading. |
|      5 | Hawksworth tree rating of 5, heavy epiphyte loading.  |
|      6 | Hawksworth tree rating of 6, heavy epiphyte loading.  |

## 3.1.159 ROOT\_HT\_PNWRS

Rooting height ( Pacific Islands ), Pacific Northwest Research Station. The height of the stilted or buttressed root system from the ground level to the highest point where the stilts or buttresses protrude from the bole of the tree. Measured to the nearest foot. Only populated by certain FIA work units (SURVEY.RSCD = 26).

## 3.1.160 CAVITY\_USE\_PNWRS

Cavity presence, Pacific Northwest Research Station. A code indicating the largest cavity present in a live tree that is utilized by wildlife. Only populated by certain FIA work units (SURVEY.RSCD = 26). Not populated for the Pacific Islands.

## Codes: CAVITY\_USE\_PNWRS

|   Code | Description                              |
|--------|------------------------------------------|
|      0 | No cavity or den present.                |
|      1 | Cavity or den present <6.0 inches wide.  |
|      2 | Cavity or den present  6.0 inches wide. |

## 3.1.161 CORE\_LENGTH\_PNWRS

Length of measured core, Pacific Northwest Research Station. The total length, in inches, of the extracted core used when the tree age is extrapolated. Only populated by certain FIA work units (SURVEY.RSCD = 26). Not populated for the Pacific Islands.

## 3.1.162 CULTURALLY\_KILLED\_PNWRS

Culturally killed code, Pacific Northwest Research Station. A code indicating if a cut tree was killed by direct human intervention, but not utilized (removed from plot). Only populated by certain FIA work units (SURVEY.RSCD = 26, 27). Not populated for the Pacific Islands.

## Codes: CULTURALLY\_KILLED\_PNWRS

|   Code | Description                                                                                                                                                                     |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | Any tree that does not meet the criteria listed in code 1.                                                                                                                      |
|      1 | Any tree that was killed by direct human cause (girdled, cut, knocked over, sprayed with herbicide, etc.), which has not been removed from plot (a treatment must be recorded). |

## 3.1.163 DIA\_EST\_PNWRS

Standing dead estimated diameter, Pacific Northwest Research Station. An estimate of the diameter at breast height for a standing dead tree when it was alive. Only populated by certain FIA work units (SURVEY.RSCD = 26, 27). Not populated for the Pacific Islands.

## 3.1.164 GST\_PNWRS

Growth sample tree, Pacific Northwest Research Station. A code indicating whether or not a tree is to be measured for total length and actual length and used as a growth sample tree. Only populated by certain FIA work units (SURVEY.RSCD = 26, 27). Not populated for the Pacific Islands.

## Codes: GST\_PNWRS

| Code   | Description                       |
|--------|-----------------------------------|
| N      | Tree is not a growth sample tree. |
| Y      | Tree is a growth sample tree.     |

## 3.1.165 INC10YR\_PNWRS

10-year increment, Pacific Northwest Research Station. The radial increment for the most recent ten years of full growth for all conifers and red alder. This measurement is taken to the nearest 1/20th inch using an increment borer at the current inventory. Only populated by certain FIA work units (SURVEY.RSCD = 26, 27). Not populated for the Pacific Islands.

## 3.1.166 INC5YRHT\_PNWRS

5-year height growth, Pacific Northwest Research Station. The height to the nearest 1.0 foot, for the most recent five years of growth for pine, spruce, Douglas-fir, and true firs. Only populated by certain FIA work units (SURVEY.RSCD = 26). Not populated for the Pacific Islands.

Note: This measurement is only populated for USFS Region 5 and Region 6 administered lands; it is used for growth and yield models.

## 3.1.167 INC5YR\_PNWRS

5-year increment, Pacific Northwest Research Station. The radial increment for the most recent five years of full growth for all conifers and red alder. This measurement is taken to the nearest 1/20th inch using an increment borer at the current inventory. Only populated by certain FIA work units (SURVEY.RSCD = 26). Not populated for the Pacific Islands.

## 3.1.168 RING\_COUNT\_INNER\_2INCHES\_PNWRS

Number of rings in inner 2 inches, Pacific Northwest Research Station. The number of tree rings in the inner two inches of the core closest to the center of the tree. Only populated by certain FIA work units (SURVEY.RSCD = 26). Not populated for the Pacific Islands.

## 3.1.169 RING\_COUNT\_PNWRS

Number of rings, Pacific Northwest Research Station. The total number of tree rings counted when the tree age is extrapolated. Only populated by certain FIA work units (SURVEY.RSCD = 26). Not populated for the Pacific Islands.

## 3.1.170 SNAG\_DIS\_CD\_PNWRS

Snag reason for disappearance code, Pacific Northwest Research Station. A code indicating the reason why a standing dead tree recorded during a previous inventory visit is no longer tallied. Only populated by certain FIA work units (SURVEY.RSCD = 26, 27). Not populated for the Pacific Islands.

## Codes: SNAG\_DIS\_CD\_PNWRS

|   Code | Description                                                                                                                             |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------|
|      2 | Fell over "naturally" (wind, decay, etc.) or no longer self-supported; still present.                                                   |
|      3 | Fell over "naturally"; removed from the site, or not discernible by crew.                                                               |
|      4 | Cut down or pushed over; still present.                                                                                                 |
|      5 | Cut down or pushed over; removed from the site, or not discernible by crew.                                                             |
|      6 | Diameter (d.b.h./d.r.c.) and/or height no longer meet minimum for tally (snag "shrank" to <5.0 inches d.b.h./d.r.c. or <4.5 feet tall). |

## 3.1.171 CONEPRESCD1

Cone presence code 1. A code indicating the type of cone presence on a live pinyon pine species  5.0 inches d.r.c. Up to three codes may be recorded per tree (CONEPRESCD1, CONEPRESCD2, CONEPRESCD3) if more than one code describes cone presence. When multiple codes apply, the cone type that describes the most abundant presence is recorded for CONEPRESCD1. The cone type describing the second most abundant presence is recorded for CONEPRESCD2. The cone type describing the third most abundant presence is recorded for CONEPRESCD3. Code 0 is recorded for CONEPRESCD1 to indicate that there are no cones present. Code 0 is recorded for CONEPRESCD2 or CONEPRESCD3 to indicate that there are no additional codes describing cone presence for the tree. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: CONEPRESCD1

|   Code | Description                                                                                                                                           |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | No cones present on tree.                                                                                                                             |
|      1 | Non-viable cone - Small  inch d.r.c), closed brown conelets with no yellow/green coloration.                                                      |
|      2 | Last year's old cones - Open, empty (dark brown or gray) cone(s) (  2 inches d.r.c.) with no sap or seeds present (previous years' cones).           |
|      3 | Current year viable cones - Small  inch d.r.c) and/or large (  2 inches d.r.c.) green cone(s) with sap and/or seeds present (this year's cones). |

## 3.1.172 CONEPRESCD2

Cone presence code 2. See CONEPRESCD1 description for definition.

## 3.1.173 CONEPRESCD3

Cone presence code 3. See CONEPRESCD1 description for definition.

## 3.1.174 MASTCD

Mast code. A rating indicating the amount of masting for a live pinyon pine species  5.0 inches d.r.c. Mast refers to the production of a seed (in this case, cone) crop. Using a

rating system, the live crown of a tree is divided into thirds horizontally, and each third is evaluated for the current-year cone production using the following scale: 0 indicates no cones present, 1 indicates that the section has &lt;50 percent of its branches producing cones, and 2 indicates that the section has  50 percent of its branches producing cones. The ratings for each third are summed together to obtain a MASTCD rating (0-6) for the tree. Only populated by certain FIA work units (SURVEY.RSCD = 22).

## Codes: MASTCD

|   Code | Description                                                 |
|--------|-------------------------------------------------------------|
|      0 | Mast rating of 0, none.                                     |
|      1 | Mast rating of 1, low level of branches producing cones.    |
|      2 | Mast rating of 2, low level of branches producing cones.    |
|      3 | Mast rating of 3, medium level of branches producing cones. |
|      4 | Mast rating of 4, medium level of branches producing cones. |
|      5 | Mast rating of 5, high level of branches producing cones.   |
|      6 | Mast rating of 6, high level of branches producing cones.   |

## 3.1.175 VOLTSGRS

Gross cubic-foot total-stem wood volume. For timber species (trees where the diameter is measured at breast height [d.b.h.])  1.0 inch d.b.h., this is the total cubic-foot volume of wood in the central stem from ground line to the tree tip. For woodland species (trees where the diameter is measured at root collar [d.r.c.]; identified by REF\_SPECIES.WOODLAND = 'Y')  1.5 inches d.r.c., this is the total cubic-foot volume of wood and bark from the d.r.c. measurement point(s) to a 1.5-inch top diameter, including branches that are at least 1.5 inches in diameter along the length of the branch. Calculated for live and standing dead trees. Includes rotten, missing, and form cull (volume loss due to rotten, missing, and form cull defect has not been deducted). This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for woodland species with DIA &lt;1.5 inches d.r.c. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

Figure 3-7: Illustration of timber species gross cubic-foot total-stem wood volume (VOLTSGRS) in black. Gray tree parts are excluded. See VOLTSGRS for a full description of this attribute.

<!-- image -->

## 3.1.176 VOLTSGRS\_BARK

Gross cubic-foot total-stem bark volume. The total cubic-foot volume of bark in the central stem of timber species (trees where diameter is measured at breast height [d.b.h.])  1.0 inch d.b.h., from ground line to the tree tip. Calculated for live and standing dead trees. Includes rotten, missing, and form cull (volume loss due to rotten, missing, and form cull defect has not been deducted). This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.177 VOLTSSND

Sound cubic-foot total-stem wood volume. For timber species (trees where the diameter is measured at breast height [d.b.h.])  1.0 inch d.b.h., this is the total sound cubic-foot volume of wood in the central stem from ground line to the tree tip. For woodland species (trees where the diameter is measured at root collar [d.r.c.]; identified by REF\_SPECIES.WOODLAND = 'Y')  1.5 inches d.r.c., this is the total sound cubic-foot volume of wood and bark from the d.r.c. measurement point(s) to a 1.5-inch top diameter, including branches that are at least 1.5 inches in diameter along the length of the branch. Calculated for live and standing dead trees. Does not include rotten and missing cull (volume loss due to rotten and missing cull defect has been deducted). This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for woodland species with DIA &lt;1.5 inches d.r.c. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.178 VOLTSSND\_BARK

Sound cubic-foot total-stem bark volume. The total sound cubic-foot volume of bark in the central stem of timber species (trees where diameter is measured at breast height [d.b.h.])  1.0 inch d.b.h., from ground line to the tree tip. Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.179 VOLCFGRS\_STUMP

Gross cubic-foot stump wood volume. The total cubic-foot volume of wood in the stump of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The stump is that portion of the tree from the ground line to the bottom of the merchantable bole (i.e., below 1 foot). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.180 VOLCFGRS\_STUMP\_BARK

Gross cubic-foot stump bark volume. The total cubic-foot volume of bark in the stump of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The stump is that portion of the tree from the ground line to the bottom of the merchantable bole (i.e., below 1 foot). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.181 VOLCFSND\_STUMP

Sound cubic-foot stump wood volume. The sound cubic-foot volume of wood in the stump of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The stump is that portion of the tree from the ground line to the bottom of the merchantable bole (i.e., below 1 foot). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.182 VOLCFSND\_STUMP\_BARK

Sound cubic-foot stump bark volume. The sound cubic-foot volume of bark in the stump of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The stump is that portion of the tree from the ground line to the bottom of the merchantable bole (i.e., below 1 foot). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.183 VOLCFGRS\_BARK

Gross cubic-foot stem bark volume. The total cubic-foot volume of bark in the central stem of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h., from a 1-foot stump to a minimum 4-inch top diameter, or to where the central stem breaks into limbs all of which are &lt;4.0 inches in diameter. Calculated for live and standing dead trees. Includes rotten, missing, and form cull (volume loss due to rotten, missing, and form cull defect has not been deducted). This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.184 VOLCFGRS\_TOP

Gross cubic-foot stem-top wood volume. The total cubic-foot volume of wood in the non-merchantable top of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The top is the portion of the stem above the merchantable bole (i.e., above the 4-inch top diameter). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.185 VOLCFGRS\_TOP\_BARK

Gross cubic-foot stem-top bark volume. The total cubic-foot volume of bark in the non-merchantable top of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The top is the portion of the stem above the merchantable bole (i.e., above the 4-inch top diameter). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.186 VOLCFSND\_BARK

Sound cubic-foot stem bark volume. The sound cubic-foot volume of bark in the central stem of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h., from a 1-foot stump to a minimum 4-inch top diameter, or to where the central stem breaks into limbs all of which are &lt;4.0 inches in diameter. Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.187 VOLCFSND\_TOP

Sound cubic-foot stem-top wood volume. The sound cubic-foot volume of wood in the non-merchantable top of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The top is the portion of the stem above the merchantable bole (i.e., above the 4-inch top diameter). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.188 VOLCFSND\_TOP\_BARK

Sound cubic-foot stem-top bark volume. The sound cubic-foot volume of bark in the non-merchantable top of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The top is the portion of the stem above the merchantable bole (i.e., above the 4-inch top diameter). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.189 VOLCFNET\_BARK

Net cubic-foot stem bark volume. The net cubic-foot volume of bark in the central stem of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h., from a 1-foot stump to a minimum 4-inch top diameter, or to where the central stem breaks into limbs all of which are &lt;4.0 inches in diameter. Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.190 VOLCSGRS\_BARK

Gross cubic-foot bark volume in the sawlog portion of a sawtimber tree. The total cubic-foot volume of bark in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods), or to where the central stem breaks into limbs, all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber-size trees have entries in this field if they are growing-stock trees (TREECLCD = 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null) in this field. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.191 VOLCSSND\_BARK

Sound cubic-foot bark volume in the sawlog portion of a sawtimber tree. The sound cubic-foot volume of bark in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods), or to where the central stem breaks into limbs, all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber- size trees have entries in this field if they are growing-stock trees (TREECLCD = 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null) in this field. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.192 VOLCSNET\_BARK

Net cubic-foot bark volume in the sawlog portion of a sawtimber tree. The net cubic-foot volume of bark in the central stem of a timber species tree of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches d.b.h. minimum for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods), or to where the central stem breaks into limbs, all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods). All sawtimber- size trees have entries in this field if they are growing-stock trees (TREECLCD = 2 and STATUSCD = 1). All rough and rotten trees (TREECLCD = 3 or 4) and dead and cut trees (STATUSCD = 2 or 3) are blank (null) in this field. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.193 DRYBIO\_STEM

Dry biomass of wood in the total stem. The oven-dry biomass, in pounds, of wood in the total stem of timber species (trees where diameter is measured at breast height [d.b.h.])  1.0 inch d.b.h., from ground line to the tree tip. Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.194 DRYBIO\_STEM\_BARK

Dry biomass of bark in the total stem. The oven-dry biomass, in pounds, of bark in the total stem of timber species (trees where diameter is measured at breast height [d.b.h.])  1.0 inch d.b.h., from ground line to the tree tip. Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.195 DRYBIO\_STUMP\_BARK

Dry biomass of bark in the stump. The oven-dry biomass, in pounds, of bark in the stump of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h. The stump is that portion of the tree from the ground line to the bottom of the merchantable bole (i.e., below 1 foot). Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.196 DRYBIO\_BOLE\_BARK

Dry biomass of bark in the merchantable bole. The oven-dry biomass, in pounds, of bark in the merchantable bole of timber species (trees where diameter is measured at breast height [d.b.h.])  5.0 inches d.b.h., from a 1-foot stump to a minimum 4-inch top diameter. Calculated for live and standing dead trees. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for timber species with DIA &lt;5.0 inches and for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.197 DRYBIO\_BRANCH

Dry biomass of branches. The oven-dry biomass, in pounds, of wood and bark in the branches/limbs of timber species (trees where diameter is measured at breast height [d.b.h.])  1.0 inch d.b.h. DRYBIO\_BRANCH is only branches; it does not include any portion of the total stem. Calculated for live and standing dead trees. For live trees, this value is reduced for broken tops. For standing dead trees, this value is reduced for broken tops as well as DECAYCD. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for woodland species. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.198 DRYBIO\_FOLIAGE

Dry biomass of foliage. The oven-dry biomass, in pounds, of foliage for live trees  1.0 inch d.b.h./d.r.c. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.199 DRYBIO\_SAWLOG\_BARK

Dry biomass of bark in the sawlog portion of a sawtimber tree. The oven-dry biomass, in pounds, of bark in the sawlog portion of timber species trees of sawtimber size (9.0 inches d.b.h. minimum for softwoods, 11.0 inches minimum d.b.h. for hardwoods), from a 1-foot stump to a minimum top diameter (7.0 inches for softwoods, 9.0 inches for hardwoods) or to where the central stem breaks into limbs, all of which are less than the minimum top diameter. This is a per tree value and must be multiplied by TPA\_UNADJ to obtain per acre information. This attribute is blank (null) for softwood trees with DIA &lt;9.0 inches (&lt;11.0 inches for hardwoods) and standing dead trees. Refer to appendix K for information on FIA volume, biomass, and carbon estimation.

## 3.1.200 PREV\_ACTUALHT\_FLD

Previous actual height. (All live and standing dead tally trees  1.0 inch d.b.h./d.r.c.) The actual height measured in the field from the previous inventory. See ACTUALHT for details.

## 3.1.201 PREV\_HT\_FLD

Previous total height. (All live and standing dead tally trees  1.0 inch d.b.h./d.r.c.) The total height from the previous inventory (HTCD = 1, 2, or 3). See HT for details.

## 3.1.202 UTILCLCD

Utilization class code. A code indicating the utilization class of trees that are dead (STATUSCD = 2) and no longer standing (STANDING\_DEAD\_CD = 0).

## Codes: UTILCLCD

|   Code | Description                                                                                                                                                                                                                  |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      0 | Not utilized - tree bole is presumed to not have been utilized for any purpose (e.g., piled and burned trees, piles of trees that have been cut or knocked over).                                                            |
|      1 | Commercial utilization - some portion of the tree removed for commercial purposes. Commercial uses include sawlogs, pulpwood, veneer logs, poles, and other products such as firewood cut by commercial firewood operations. |

## Tree Table

|   Code | Description                                                                                                                                                                                                                                                                 |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|      2 | Noncommercial utilization - some portion of the tree removed for noncommercial purposes. Noncommercial uses may include private landowner domestic firewood use, barn poles, fence posts, domestic landscaping, rough slabs, etc.                                           |
|      4 | Undifferentiated utilization (unknown) - some portion of the tree was removed and utilized, but it is not known if this was for commercial or non-commercial purposes. Note: For the NFI, this code is for office use only (e.g., code assigned when back-populating data). |