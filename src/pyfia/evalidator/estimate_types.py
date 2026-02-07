"""
EVALIDator SNUM Values - Forest Inventory Estimate Types

This module contains all 752 estimate types (snum values) available from the
USDA Forest Service FIA EVALIDator API.

Source: https://apps.fs.usda.gov/fiadb-api/fullreport/parameters/snum
"""

from __future__ import annotations

from enum import IntEnum


class EstimateType(IntEnum):
    """
    EVALIDator estimate types (snum values).

    These values are used with the FIADB API fullreport endpoint to specify
    what type of forest inventory estimate to retrieve.

    Categories:
    - AREA: Land area estimates (3)
    - AREA_CHANGE: Area change over time (10)
    - TREE_COUNT: Number of trees/seedlings (10)
    - BASAL_AREA: Basal area measurements (4)
    - VOLUME: Wood volume estimates (311)
    - BIOMASS: Biomass in dry/green tons (327)
    - CARBON: Carbon storage estimates (38)
    - DOWN_WOODY: Down woody materials (1)
    - TREE_DYNAMICS: Mortality, removals, growth in trees (48)

    Total: 752 estimate types
    """

    # --- AREA (3 estimates) ---
    SNUM_2 = 2
    AREA_FOREST = 2  # alias
    SNUM_3 = 3
    AREA_TIMBERLAND = 3  # alias
    SNUM_79 = 79
    AREA_SAMPLED = 79  # alias

    # --- AREA CHANGE (10 estimates) ---
    SNUM_126 = 126
    SNUM_127 = 127
    SNUM_128 = 128
    SNUM_129 = 129
    SNUM_130 = 130
    SNUM_135 = 135
    SNUM_136 = 136
    AREA_CHANGE_ANNUAL_FOREST_BOTH = 136  # alias
    SNUM_137 = 137
    AREA_CHANGE_ANNUAL_FOREST_EITHER = 137  # alias
    SNUM_138 = 138
    SNUM_139 = 139

    # --- TREE COUNT (10 estimates) ---
    SNUM_4 = 4
    TREE_COUNT_1INCH_FOREST = 4  # alias
    SNUM_5 = 5
    TREE_COUNT_5INCH_FOREST = 5  # alias
    SNUM_6 = 6
    SNUM_7 = 7
    TREE_COUNT_1INCH_TIMBER = 7  # alias
    SNUM_8 = 8
    TREE_COUNT_5INCH_TIMBER = 8  # alias
    SNUM_9 = 9
    SNUM_45 = 45
    SNUM_46 = 46
    SNUM_11264 = 11264
    SNUM_11265 = 11265

    # --- BASAL AREA (4 estimates) ---
    SNUM_1004 = 1004
    SNUM_1005 = 1005
    SNUM_1007 = 1007
    SNUM_1008 = 1008

    # --- VOLUME (311 estimates) ---
    SNUM_15 = 15
    VOLUME_NET_GROWINGSTOCK = 15  # alias
    SNUM_16 = 16
    SNUM_18 = 18
    SNUM_19 = 19
    SNUM_20 = 20
    VOLUME_SAWLOG_INTERNATIONAL = 20  # alias
    SNUM_21 = 21
    SNUM_22 = 22
    SNUM_104 = 104
    SNUM_107 = 107
    SNUM_110 = 110
    SNUM_114 = 114
    SNUM_117 = 117
    SNUM_120 = 120
    SNUM_123 = 123
    SNUM_131 = 131
    SNUM_132 = 132
    SNUM_202 = 202
    GROWTH_NET_VOLUME = 202  # alias
    SNUM_203 = 203
    SNUM_204 = 204
    SNUM_205 = 205
    SNUM_206 = 206
    SNUM_208 = 208
    SNUM_209 = 209
    SNUM_210 = 210
    SNUM_211 = 211
    SNUM_212 = 212
    SNUM_214 = 214
    MORTALITY_VOLUME = 214  # alias
    SNUM_215 = 215
    SNUM_216 = 216
    SNUM_217 = 217
    SNUM_218 = 218
    SNUM_220 = 220
    SNUM_221 = 221
    SNUM_222 = 222
    SNUM_223 = 223
    SNUM_224 = 224
    SNUM_226 = 226
    REMOVALS_VOLUME = 226  # alias
    SNUM_227 = 227
    SNUM_228 = 228
    SNUM_229 = 229
    SNUM_230 = 230
    SNUM_232 = 232
    SNUM_233 = 233
    SNUM_234 = 234
    SNUM_235 = 235
    SNUM_236 = 236
    SNUM_238 = 238
    SNUM_239 = 239
    SNUM_240 = 240
    SNUM_241 = 241
    SNUM_242 = 242
    SNUM_244 = 244
    SNUM_245 = 245
    SNUM_246 = 246
    SNUM_247 = 247
    SNUM_248 = 248
    SNUM_250 = 250
    SNUM_251 = 251
    SNUM_252 = 252
    SNUM_253 = 253
    SNUM_254 = 254
    SNUM_256 = 256
    SNUM_257 = 257
    SNUM_258 = 258
    SNUM_259 = 259
    SNUM_260 = 260
    SNUM_953 = 953
    SNUM_956 = 956
    SNUM_1020 = 1020
    SNUM_1021 = 1021
    SNUM_1022 = 1022
    SNUM_1023 = 1023
    SNUM_1202 = 1202
    SNUM_1203 = 1203
    SNUM_1204 = 1204
    SNUM_1205 = 1205
    SNUM_1206 = 1206
    SNUM_1208 = 1208
    SNUM_1209 = 1209
    SNUM_1210 = 1210
    SNUM_1211 = 1211
    SNUM_1212 = 1212
    SNUM_2202 = 2202
    SNUM_2203 = 2203
    SNUM_2204 = 2204
    SNUM_2205 = 2205
    SNUM_2206 = 2206
    SNUM_2208 = 2208
    SNUM_2209 = 2209
    SNUM_2210 = 2210
    SNUM_2211 = 2211
    SNUM_2212 = 2212
    SNUM_11001 = 11001
    SNUM_11002 = 11002
    SNUM_11003 = 11003
    SNUM_11004 = 11004
    SNUM_11005 = 11005
    SNUM_11006 = 11006
    SNUM_11007 = 11007
    SNUM_11008 = 11008
    SNUM_11009 = 11009
    SNUM_11010 = 11010
    SNUM_11011 = 11011
    SNUM_11012 = 11012
    SNUM_11013 = 11013
    SNUM_11014 = 11014
    SNUM_11015 = 11015
    SNUM_11024 = 11024
    SNUM_11025 = 11025
    SNUM_11026 = 11026
    SNUM_11027 = 11027
    SNUM_11028 = 11028
    SNUM_11029 = 11029
    SNUM_11030 = 11030
    SNUM_11031 = 11031
    SNUM_11033 = 11033
    SNUM_11034 = 11034
    SNUM_11035 = 11035
    SNUM_11036 = 11036
    SNUM_11037 = 11037
    SNUM_11038 = 11038
    SNUM_11039 = 11039
    SNUM_11040 = 11040
    SNUM_11041 = 11041
    SNUM_11042 = 11042
    SNUM_11043 = 11043
    SNUM_11044 = 11044
    SNUM_11045 = 11045
    SNUM_11046 = 11046
    SNUM_11047 = 11047
    SNUM_11056 = 11056
    SNUM_11057 = 11057
    SNUM_11058 = 11058
    SNUM_11059 = 11059
    SNUM_11060 = 11060
    SNUM_11061 = 11061
    SNUM_11062 = 11062
    SNUM_11063 = 11063
    SNUM_11065 = 11065
    SNUM_11066 = 11066
    SNUM_11067 = 11067
    SNUM_11068 = 11068
    SNUM_11069 = 11069
    SNUM_11070 = 11070
    SNUM_11071 = 11071
    SNUM_11072 = 11072
    SNUM_11087 = 11087
    SNUM_11088 = 11088
    SNUM_11089 = 11089
    SNUM_11090 = 11090
    SNUM_11091 = 11091
    SNUM_11092 = 11092
    SNUM_11093 = 11093
    SNUM_11094 = 11094
    SNUM_11095 = 11095
    SNUM_11096 = 11096
    SNUM_11097 = 11097
    SNUM_11098 = 11098
    SNUM_11099 = 11099
    SNUM_11100 = 11100
    SNUM_11101 = 11101
    SNUM_11102 = 11102
    SNUM_11103 = 11103
    SNUM_11104 = 11104
    SNUM_11105 = 11105
    SNUM_11106 = 11106
    SNUM_11107 = 11107
    SNUM_11108 = 11108
    SNUM_11109 = 11109
    SNUM_11110 = 11110
    SNUM_11111 = 11111
    SNUM_11112 = 11112
    SNUM_11113 = 11113
    SNUM_11114 = 11114
    SNUM_11143 = 11143
    SNUM_11144 = 11144
    SNUM_11145 = 11145
    SNUM_11146 = 11146
    SNUM_11147 = 11147
    SNUM_11148 = 11148
    SNUM_11149 = 11149
    SNUM_11150 = 11150
    SNUM_11151 = 11151
    SNUM_11152 = 11152
    SNUM_11153 = 11153
    SNUM_11154 = 11154
    SNUM_11155 = 11155
    SNUM_11156 = 11156
    SNUM_11157 = 11157
    SNUM_11158 = 11158
    SNUM_11159 = 11159
    SNUM_11160 = 11160
    SNUM_11161 = 11161
    SNUM_11162 = 11162
    SNUM_11163 = 11163
    SNUM_11164 = 11164
    SNUM_11165 = 11165
    SNUM_11166 = 11166
    SNUM_11167 = 11167
    SNUM_11168 = 11168
    SNUM_11169 = 11169
    SNUM_11170 = 11170
    SNUM_11185 = 11185
    SNUM_11186 = 11186
    SNUM_11187 = 11187
    SNUM_11188 = 11188
    SNUM_11189 = 11189
    SNUM_11190 = 11190
    SNUM_11191 = 11191
    SNUM_11192 = 11192
    SNUM_11193 = 11193
    SNUM_11194 = 11194
    SNUM_11195 = 11195
    SNUM_11196 = 11196
    SNUM_11197 = 11197
    SNUM_11198 = 11198
    SNUM_11215 = 11215
    SNUM_11216 = 11216
    SNUM_11245 = 11245
    SNUM_11246 = 11246
    SNUM_11247 = 11247
    SNUM_11248 = 11248
    SNUM_11252 = 11252
    SNUM_11253 = 11253
    SNUM_11254 = 11254
    SNUM_11255 = 11255
    SNUM_11256 = 11256
    SNUM_11257 = 11257
    SNUM_11258 = 11258
    SNUM_11259 = 11259
    SNUM_11260 = 11260
    SNUM_11261 = 11261
    SNUM_11262 = 11262
    SNUM_11263 = 11263
    SNUM_11270 = 11270
    SNUM_11271 = 11271
    SNUM_11272 = 11272
    SNUM_11273 = 11273
    SNUM_11274 = 11274
    SNUM_11275 = 11275
    SNUM_11276 = 11276
    SNUM_11277 = 11277
    SNUM_11278 = 11278
    SNUM_11279 = 11279
    SNUM_11280 = 11280
    SNUM_11281 = 11281
    SNUM_11282 = 11282
    SNUM_11283 = 11283
    SNUM_11284 = 11284
    SNUM_11285 = 11285
    SNUM_11286 = 11286
    SNUM_11287 = 11287
    SNUM_11288 = 11288
    SNUM_11289 = 11289
    SNUM_11290 = 11290
    SNUM_11291 = 11291
    SNUM_11292 = 11292
    SNUM_11293 = 11293
    SNUM_11294 = 11294
    SNUM_11295 = 11295
    SNUM_11296 = 11296
    SNUM_11297 = 11297
    SNUM_11298 = 11298
    SNUM_11299 = 11299
    SNUM_11300 = 11300
    SNUM_11303 = 11303
    SNUM_11304 = 11304
    SNUM_11305 = 11305
    SNUM_11306 = 11306
    SNUM_11307 = 11307
    SNUM_11308 = 11308
    SNUM_11309 = 11309
    SNUM_11310 = 11310
    SNUM_11311 = 11311
    SNUM_11312 = 11312
    SNUM_574155 = 574155
    SNUM_574156 = 574156
    SNUM_574157 = 574157
    SNUM_574158 = 574158
    SNUM_574159 = 574159
    SNUM_574160 = 574160
    SNUM_574161 = 574161
    SNUM_574162 = 574162
    SNUM_574163 = 574163
    SNUM_574164 = 574164
    SNUM_574165 = 574165
    SNUM_574166 = 574166
    SNUM_574167 = 574167
    SNUM_574168 = 574168
    SNUM_574171 = 574171
    SNUM_574172 = 574172
    SNUM_574173 = 574173
    SNUM_574174 = 574174
    SNUM_574175 = 574175
    SNUM_574200 = 574200
    SNUM_574201 = 574201
    SNUM_574202 = 574202
    SNUM_574203 = 574203
    SNUM_574204 = 574204
    SNUM_574205 = 574205
    SNUM_574206 = 574206
    SNUM_574207 = 574207
    SNUM_574208 = 574208
    SNUM_574209 = 574209
    SNUM_574210 = 574210
    SNUM_574211 = 574211
    SNUM_574212 = 574212
    SNUM_574213 = 574213
    SNUM_574214 = 574214
    SNUM_574215 = 574215
    SNUM_574216 = 574216

    # --- BIOMASS (327 estimates) ---
    SNUM_10 = 10
    BIOMASS_AG_LIVE = 10  # alias
    SNUM_13 = 13
    SNUM_59 = 59
    BIOMASS_BG_LIVE = 59  # alias
    SNUM_73 = 73
    SNUM_96 = 96
    SNUM_105 = 105
    SNUM_108 = 108
    SNUM_111 = 111
    SNUM_115 = 115
    SNUM_118 = 118
    SNUM_121 = 121
    SNUM_124 = 124
    SNUM_311 = 311
    SNUM_312 = 312
    SNUM_313 = 313
    SNUM_314 = 314
    SNUM_315 = 315
    SNUM_316 = 316
    SNUM_317 = 317
    SNUM_318 = 318
    SNUM_319 = 319
    SNUM_320 = 320
    SNUM_321 = 321
    SNUM_322 = 322
    SNUM_335 = 335
    SNUM_336 = 336
    SNUM_337 = 337
    SNUM_338 = 338
    SNUM_339 = 339
    SNUM_340 = 340
    SNUM_341 = 341
    SNUM_342 = 342
    SNUM_343 = 343
    SNUM_344 = 344
    SNUM_345 = 345
    SNUM_346 = 346
    SNUM_369 = 369
    SNUM_370 = 370
    SNUM_371 = 371
    SNUM_372 = 372
    SNUM_373 = 373
    SNUM_374 = 374
    SNUM_375 = 375
    SNUM_376 = 376
    SNUM_377 = 377
    SNUM_378 = 378
    SNUM_379 = 379
    SNUM_380 = 380
    SNUM_403 = 403
    SNUM_404 = 404
    SNUM_405 = 405
    SNUM_406 = 406
    SNUM_407 = 407
    SNUM_408 = 408
    SNUM_409 = 409
    SNUM_410 = 410
    SNUM_411 = 411
    SNUM_412 = 412
    SNUM_413 = 413
    SNUM_414 = 414
    SNUM_437 = 437
    SNUM_438 = 438
    SNUM_439 = 439
    SNUM_440 = 440
    SNUM_441 = 441
    SNUM_442 = 442
    SNUM_443 = 443
    SNUM_444 = 444
    SNUM_445 = 445
    SNUM_446 = 446
    SNUM_447 = 447
    SNUM_448 = 448
    SNUM_1311 = 1311
    SNUM_1312 = 1312
    SNUM_1313 = 1313
    SNUM_1314 = 1314
    SNUM_1315 = 1315
    SNUM_1316 = 1316
    SNUM_1317 = 1317
    SNUM_1318 = 1318
    SNUM_1319 = 1319
    SNUM_1320 = 1320
    SNUM_1321 = 1321
    SNUM_1322 = 1322
    SNUM_2311 = 2311
    SNUM_2312 = 2312
    SNUM_2313 = 2313
    SNUM_2314 = 2314
    SNUM_2315 = 2315
    SNUM_2316 = 2316
    SNUM_2317 = 2317
    SNUM_2318 = 2318
    SNUM_2319 = 2319
    SNUM_2320 = 2320
    SNUM_2321 = 2321
    SNUM_2322 = 2322
    SNUM_2635 = 2635
    SNUM_2636 = 2636
    SNUM_2637 = 2637
    SNUM_2638 = 2638
    SNUM_2639 = 2639
    SNUM_2640 = 2640
    SNUM_2641 = 2641
    SNUM_2642 = 2642
    SNUM_2649 = 2649
    SNUM_2650 = 2650
    SNUM_2651 = 2651
    SNUM_2652 = 2652
    SNUM_2653 = 2653
    SNUM_2654 = 2654
    SNUM_2661 = 2661
    SNUM_2662 = 2662
    SNUM_2665 = 2665
    SNUM_2667 = 2667
    SNUM_2668 = 2668
    SNUM_2669 = 2669
    SNUM_2674 = 2674
    SNUM_2675 = 2675
    SNUM_2676 = 2676
    SNUM_2677 = 2677
    SNUM_2680 = 2680
    SNUM_2681 = 2681
    SNUM_2682 = 2682
    SNUM_2683 = 2683
    SNUM_11000 = 11000
    SNUM_11016 = 11016
    SNUM_11017 = 11017
    SNUM_11018 = 11018
    SNUM_11019 = 11019
    SNUM_11020 = 11020
    SNUM_11021 = 11021
    SNUM_11022 = 11022
    SNUM_11023 = 11023
    SNUM_11032 = 11032
    SNUM_11048 = 11048
    SNUM_11049 = 11049
    SNUM_11050 = 11050
    SNUM_11051 = 11051
    SNUM_11052 = 11052
    SNUM_11053 = 11053
    SNUM_11054 = 11054
    SNUM_11055 = 11055
    SNUM_11064 = 11064
    SNUM_11213 = 11213
    SNUM_11214 = 11214
    SNUM_11249 = 11249
    SNUM_11250 = 11250
    SNUM_11251 = 11251
    SNUM_11266 = 11266
    SNUM_11267 = 11267
    SNUM_12000 = 12000
    SNUM_56000 = 56000
    SNUM_57000 = 57000
    SNUM_58000 = 58000
    SNUM_60000 = 60000
    SNUM_70000 = 70000
    SNUM_71000 = 71000
    SNUM_72000 = 72000
    SNUM_74000 = 74000
    SNUM_133000 = 133000
    SNUM_134000 = 134000
    SNUM_511000 = 511000
    SNUM_512000 = 512000
    SNUM_533000 = 533000
    SNUM_534000 = 534000
    SNUM_556000 = 556000
    SNUM_557000 = 557000
    SNUM_558000 = 558000
    SNUM_560000 = 560000
    SNUM_570000 = 570000
    SNUM_571000 = 571000
    SNUM_572000 = 572000
    SNUM_574000 = 574000
    SNUM_574001 = 574001
    SNUM_574002 = 574002
    SNUM_574003 = 574003
    SNUM_574004 = 574004
    SNUM_574005 = 574005
    SNUM_574006 = 574006
    SNUM_574007 = 574007
    SNUM_574008 = 574008
    SNUM_574009 = 574009
    SNUM_574010 = 574010
    SNUM_574011 = 574011
    SNUM_574012 = 574012
    SNUM_574013 = 574013
    SNUM_574014 = 574014
    SNUM_574015 = 574015
    SNUM_574016 = 574016
    SNUM_574017 = 574017
    SNUM_574018 = 574018
    SNUM_574019 = 574019
    SNUM_574020 = 574020
    SNUM_574021 = 574021
    SNUM_574022 = 574022
    SNUM_574023 = 574023
    SNUM_574024 = 574024
    SNUM_574025 = 574025
    SNUM_574026 = 574026
    SNUM_574027 = 574027
    SNUM_574028 = 574028
    SNUM_574029 = 574029
    SNUM_574030 = 574030
    SNUM_574031 = 574031
    SNUM_574032 = 574032
    SNUM_574033 = 574033
    SNUM_574034 = 574034
    SNUM_574035 = 574035
    SNUM_574036 = 574036
    SNUM_574037 = 574037
    SNUM_574038 = 574038
    SNUM_574039 = 574039
    SNUM_574040 = 574040
    SNUM_574041 = 574041
    SNUM_574042 = 574042
    SNUM_574043 = 574043
    SNUM_574044 = 574044
    SNUM_574045 = 574045
    SNUM_574046 = 574046
    SNUM_574047 = 574047
    SNUM_574048 = 574048
    SNUM_574049 = 574049
    SNUM_574050 = 574050
    SNUM_574051 = 574051
    SNUM_574052 = 574052
    SNUM_574053 = 574053
    SNUM_574054 = 574054
    SNUM_574055 = 574055
    SNUM_574056 = 574056
    SNUM_574057 = 574057
    SNUM_574058 = 574058
    SNUM_574059 = 574059
    SNUM_574060 = 574060
    SNUM_574061 = 574061
    SNUM_574062 = 574062
    SNUM_574063 = 574063
    SNUM_574064 = 574064
    SNUM_574065 = 574065
    SNUM_574066 = 574066
    SNUM_574067 = 574067
    SNUM_574068 = 574068
    SNUM_574069 = 574069
    SNUM_574070 = 574070
    SNUM_574071 = 574071
    SNUM_574072 = 574072
    SNUM_574073 = 574073
    SNUM_574074 = 574074
    SNUM_574075 = 574075
    SNUM_574076 = 574076
    SNUM_574077 = 574077
    SNUM_574078 = 574078
    SNUM_574079 = 574079
    SNUM_574080 = 574080
    SNUM_574081 = 574081
    SNUM_574082 = 574082
    SNUM_574083 = 574083
    SNUM_574084 = 574084
    SNUM_574085 = 574085
    SNUM_574086 = 574086
    SNUM_574087 = 574087
    SNUM_574088 = 574088
    SNUM_574089 = 574089
    SNUM_574090 = 574090
    SNUM_574091 = 574091
    SNUM_574092 = 574092
    SNUM_574093 = 574093
    SNUM_574094 = 574094
    SNUM_574095 = 574095
    SNUM_574096 = 574096
    SNUM_574097 = 574097
    SNUM_574098 = 574098
    SNUM_574099 = 574099
    SNUM_574100 = 574100
    SNUM_574101 = 574101
    SNUM_574102 = 574102
    SNUM_574103 = 574103
    SNUM_574104 = 574104
    SNUM_574105 = 574105
    SNUM_574106 = 574106
    SNUM_574107 = 574107
    SNUM_574108 = 574108
    SNUM_574109 = 574109
    SNUM_574110 = 574110
    SNUM_574111 = 574111
    SNUM_574112 = 574112
    SNUM_574113 = 574113
    SNUM_574114 = 574114
    SNUM_574115 = 574115
    SNUM_574116 = 574116
    SNUM_574117 = 574117
    SNUM_574118 = 574118
    SNUM_574119 = 574119
    SNUM_574120 = 574120
    SNUM_574121 = 574121
    SNUM_574122 = 574122
    SNUM_574123 = 574123
    SNUM_574124 = 574124
    SNUM_574125 = 574125
    SNUM_574126 = 574126
    SNUM_574127 = 574127
    SNUM_574128 = 574128
    SNUM_574129 = 574129
    SNUM_574130 = 574130
    SNUM_574131 = 574131
    SNUM_574132 = 574132
    SNUM_574133 = 574133
    SNUM_574134 = 574134
    SNUM_574135 = 574135
    SNUM_574136 = 574136
    SNUM_574137 = 574137
    SNUM_574138 = 574138
    SNUM_574139 = 574139
    SNUM_574140 = 574140
    SNUM_574141 = 574141
    SNUM_574142 = 574142
    SNUM_574143 = 574143
    SNUM_574144 = 574144
    SNUM_574145 = 574145
    SNUM_574146 = 574146
    SNUM_574147 = 574147
    SNUM_574148 = 574148
    SNUM_574149 = 574149
    SNUM_574150 = 574150
    SNUM_574151 = 574151
    SNUM_574152 = 574152
    SNUM_574153 = 574153
    SNUM_574154 = 574154

    # --- CARBON (38 estimates) ---
    SNUM_48 = 48
    SNUM_49 = 49
    SNUM_50 = 50
    SNUM_51 = 51
    SNUM_52 = 52
    SNUM_62 = 62
    SNUM_63 = 63
    SNUM_64 = 64
    SNUM_65 = 65
    SNUM_66 = 66
    SNUM_97 = 97
    SNUM_98 = 98
    SNUM_99 = 99
    SNUM_100 = 100
    SNUM_101 = 101
    SNUM_102 = 102
    SNUM_103 = 103
    CARBON_POOL_TOTAL = 103  # alias
    SNUM_106 = 106
    SNUM_109 = 109
    SNUM_112 = 112
    SNUM_116 = 116
    SNUM_119 = 119
    SNUM_122 = 122
    SNUM_125 = 125
    SNUM_11268 = 11268
    SNUM_11269 = 11269
    SNUM_11301 = 11301
    SNUM_11302 = 11302
    SNUM_47000 = 47000
    SNUM_47001 = 47001
    SNUM_53000 = 53000
    CARBON_AG_LIVE = 53000  # alias
    SNUM_54000 = 54000
    SNUM_55000 = 55000
    CARBON_TOTAL_LIVE = 55000  # alias
    SNUM_61000 = 61000
    SNUM_61001 = 61001
    SNUM_67000 = 67000
    SNUM_68000 = 68000
    SNUM_69000 = 69000

    # --- DOWN WOODY (1 estimates) ---
    SNUM_113 = 113

    # --- TREE DYNAMICS (48 estimates) ---
    SNUM_901 = 901
    SNUM_902 = 902
    SNUM_903 = 903
    SNUM_904 = 904
    SNUM_905 = 905
    SNUM_906 = 906
    SNUM_907 = 907
    SNUM_908 = 908
    SNUM_909 = 909
    SNUM_910 = 910
    SNUM_911 = 911
    SNUM_912 = 912
    SNUM_913 = 913
    SNUM_914 = 914
    SNUM_915 = 915
    SNUM_916 = 916
    SNUM_917 = 917
    SNUM_918 = 918
    SNUM_919 = 919
    SNUM_920 = 920
    SNUM_921 = 921
    SNUM_922 = 922
    SNUM_923 = 923
    SNUM_924 = 924
    SNUM_3000 = 3000
    SNUM_3001 = 3001
    SNUM_3002 = 3002
    SNUM_3003 = 3003
    SNUM_3004 = 3004
    SNUM_3005 = 3005
    SNUM_3006 = 3006
    SNUM_3007 = 3007
    SNUM_3008 = 3008
    SNUM_3009 = 3009
    SNUM_3010 = 3010
    SNUM_3011 = 3011
    SNUM_3012 = 3012
    SNUM_3013 = 3013
    SNUM_3014 = 3014
    SNUM_3015 = 3015
    SNUM_3016 = 3016
    SNUM_3017 = 3017
    SNUM_3018 = 3018
    SNUM_3019 = 3019
    SNUM_3020 = 3020
    SNUM_3021 = 3021
    SNUM_3022 = 3022
    SNUM_3023 = 3023


# Complete mapping of SNUM values to their descriptions
SNUM_DESCRIPTIONS: dict[int, str] = {
    2: "Area of forest land, in acres",
    3: "Area of timberland, in acres",
    4: "Number of live trees (at least 1 inch d.b.h./d.r.c.), in trees, on forest land",
    5: "Number of growing-stock trees (at least 5 inches d.b.h.), in trees, on forest land",
    6: "Number of standing dead trees (at least 5 inches d.b.h./d.r.c.), in trees, on forest land",
    7: "Number of live trees (at least 1 inch d.b.h./d.r.c.), in trees, on timberland",
    8: "Number of growing-stock trees (at least 5 inches d.b.h.), in trees, on timberland",
    9: "Number of standing dead trees (at least 5 inches d.b.h./d.r.c.), in trees, on timberland",
    10: "Aboveground biomass of live trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    13: "Aboveground biomass of live trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    15: "Net merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land",
    16: "Net sawlog wood volume of sawtimber trees, in cubic feet, on forest land",
    18: "Net merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on timberland",
    19: "Net sawlog wood volume of sawtimber trees, in cubic feet, on timberland",
    20: "Net sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    21: "Net sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on timberland",
    22: "Gross sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    45: "Number of live seedlings (less than 1 inch d.b.h./d.r.c.), in seedlings, on forest land",
    46: "Number of live seedlings (less than 1 inch d.b.h./d.r.c.), in seedlings, on timberland",
    48: "Aboveground carbon in live seedlings, shrubs, and bushes, in short tons, on forest land",
    49: "Belowground carbon in live seedlings, shrubs, and bushes, in short tons, on forest land",
    50: "Carbon in stumps, coarse roots, and coarse woody debris, in short tons, on forest land",
    51: "Carbon in litter, in short tons, on forest land",
    52: "Carbon in organic soil, in short tons, on forest land",
    59: "Belowground biomass of live trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    62: "Aboveground carbon in live seedlings, shrubs, and bushes, in short tons, on timberland",
    63: "Belowground carbon in live seedlings, shrubs, and bushes, in short tons, on timberland",
    64: "Carbon in stumps, coarse roots, and coarse woody debris, in short tons, on timberland",
    65: "Carbon in litter, in short tons, on timberland",
    66: "Carbon in organic soil, in short tons, on timberland",
    73: "Belowground biomass of live trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    79: "Area of sampled land and water, in acres",
    96: "Aboveground biomass of standing dead trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    97: "Total carbon, in short tons, on forest land",
    98: "Forest carbon pool 1: live aboveground, in metric tonnes, on forest land",
    99: "Forest carbon pool 2: live belowground, in metric tonnes, on forest land",
    100: "Forest carbon pool 3: dead wood, in metric tonnes, on forest land",
    101: "Forest carbon pool 4: litter, in metric tonnes, on forest land",
    102: "Forest carbon pool 5: soil organic, in metric tonnes, on forest land",
    103: "Forest carbon total: all 5 pools, in metric tonnes, on forest land",
    104: "Total volume of FWD (small) pieces, in cubic feet, on forest land",
    105: "Biomass of FWD (small) pieces, in dry short tons, on forest land",
    106: "Carbon in FWD (small) pieces, in short tons, on forest land",
    107: "Total volume of FWD (medium) pieces, in cubic feet, on forest land",
    108: "Biomass of FWD (medium) pieces, in dry short tons, on forest land",
    109: "Carbon in FWD (medium) pieces, in short tons, on forest land",
    110: "Total volume of FWD (large) pieces, in cubic feet, on forest land",
    111: "Biomass of FWD (large) pieces, in dry short tons, on forest land",
    112: "Carbon in FWD (large) pieces, in short tons, on forest land",
    113: "Number of CWD pieces, in pieces, on forest land",
    114: "Total volume of CWD, in cubic feet, on forest land",
    115: "Biomass of CWD, in dry short tons, on forest land",
    116: "Carbon in CWD, in short tons, on forest land",
    117: "Total volume of DWM piles, in cubic feet, on forest land",
    118: "Biomass of DWM piles, in dry short tons, on forest land",
    119: "Carbon in DWM piles, in short tons, on forest land",
    120: "Total volume of FWD (all sizes) pieces, in cubic feet, on forest land",
    121: "Biomass of FWD (all sizes) pieces, in dry short tons, on forest land",
    122: "Carbon in FWD (all sizes) pieces, in short tons, on forest land",
    123: "Total volume of DWM (FWD, CWD and piles) in cubic feet, on forest land",
    124: "Total biomass of DWM (FWD, CWD, and piles), in dry short tons, on forest land",
    125: "Total carbon in DWM (FWD, CWD and piles) in short tons, on forest land",
    126: "Area change of sampled land and water, in acres, on all remeasured conditions",
    127: "Area change of forest land, in acres, on remeasured conditions where both measurements are forest land",
    128: "Area change of forest land, in acres, on remeasured conditions where either measurement is forest land",
    129: "Area change of timberland, in acres, on remeasured conditions where both measurements are timberland",
    130: "Area change of timberland, in acres, on remeasured conditions where either measurement is timberland",
    131: "Sound sawlog wood volume of sawtimber trees, in cubic feet, on forest land",
    132: "Sound sawlog wood volume of sawtimber trees, in cubic feet, on timberland",
    135: "Average annual area change of sampled land and water, in acres, on all remeasured conditions",
    136: "Average annual area change of forest land, in acres, on remeasured conditions where both measurements are forest land",
    137: "Average annual area change of forest land, in acres, on remeasured conditions where either measurement is forest land",
    138: "Average annual area change of timberland, in acres, on remeasured conditions where both measurements are timberland",
    139: "Average annual area change of timberland, in acres, on remeasured conditions where either measurement is timberland",
    202: "Average annual net growth of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land",
    203: "Average annual net growth of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    204: "Average annual net growth of sawlog wood volume of sawtimber trees, in cubic feet, on forest land",
    205: "Average annual net growth of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on forest land",
    206: "Average annual net growth of merchantable bole wood volume of sawtimber trees, in cubic feet, on forest land",
    208: "Average annual net growth of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on timberland",
    209: "Average annual net growth of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on timberland",
    210: "Average annual net growth of sawlog wood volume of sawtimber trees, in cubic feet, on timberland",
    211: "Average annual net growth of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on timberland",
    212: "Average annual net growth of merchantable bole wood volume of sawtimber trees, in cubic feet, on timberland",
    214: "Average annual mortality of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land",
    215: "Average annual mortality of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    216: "Average annual mortality of sawlog wood volume of sawtimber trees, in cubic feet, on forest land",
    217: "Average annual mortality of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on forest land",
    218: "Average annual mortality of merchantable bole wood volume of sawtimber trees, in cubic feet, on forest land",
    220: "Average annual mortality of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on timberland",
    221: "Average annual mortality of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on timberland",
    222: "Average annual mortality of sawlog wood volume of sawtimber trees, in cubic feet, on timberland",
    223: "Average annual mortality of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on timberland",
    224: "Average annual mortality of merchantable bole wood volume of sawtimber trees, in cubic feet, on timberland",
    226: "Average annual removals of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land",
    227: "Average annual removals of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    228: "Average annual removals of sawlog wood volume of sawtimber trees, in cubic feet, on forest land",
    229: "Average annual removals of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on forest land",
    230: "Average annual removals of merchantable bole wood volume of sawtimber trees, in cubic feet, on forest land",
    232: "Average annual removals of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on timberland",
    233: "Average annual removals of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on timberland",
    234: "Average annual removals of sawlog wood volume of sawtimber trees, in cubic feet, on timberland",
    235: "Average annual removals of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on timberland",
    236: "Average annual removals of merchantable bole wood volume of sawtimber trees, in cubic feet, on timberland",
    238: "Average annual harvest removals of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land",
    239: "Average annual harvest removals of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    240: "Average annual harvest removals of sawlog wood volume of sawtimber trees, in cubic feet, on forest land",
    241: "Average annual harvest removals of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on forest land",
    242: "Average annual harvest removals of merchantable bole wood volume of sawtimber trees, in cubic feet, on forest land",
    244: "Average annual harvest removals of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on timberland",
    245: "Average annual harvest removals of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on timberland",
    246: "Average annual harvest removals of sawlog wood volume of sawtimber trees, in cubic feet, on timberland",
    247: "Average annual harvest removals of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on timberland",
    248: "Average annual harvest removals of merchantable bole wood volume of sawtimber trees, in cubic feet, on timberland",
    250: "Average annual other removals of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land",
    251: "Average annual other removals of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    252: "Average annual other removals of sawlog wood volume of sawtimber trees, in cubic feet, on forest land",
    253: "Average annual other removals of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on forest land",
    254: "Average annual other removals of merchantable bole wood volume of sawtimber trees, in cubic feet, on forest land",
    256: "Average annual other removals of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on timberland",
    257: "Average annual other removals of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on timberland",
    258: "Average annual other removals of sawlog wood volume of sawtimber trees, in cubic feet, on timberland",
    259: "Average annual other removals of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on timberland",
    260: "Average annual other removals of merchantable bole wood volume of sawtimber trees, in cubic feet, on timberland",
    311: "Average annual net growth of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    312: "Average annual net growth of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    313: "Average annual net growth of aboveground biomass of sawtimber trees, in dry short tons, on forest land",
    314: "Average annual net growth of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    315: "Average annual net growth of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    316: "Average annual net growth of aboveground biomass of sawtimber trees, in dry short tons, on timberland",
    317: "Average annual net growth of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    318: "Average annual net growth of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    319: "Average annual net growth of belowground biomass of sawtimber trees, in dry short tons, on forest land",
    320: "Average annual net growth of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    321: "Average annual net growth of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    322: "Average annual net growth of belowground biomass of sawtimber trees, in dry short tons, on timberland",
    335: "Average annual mortality of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    336: "Average annual mortality of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    337: "Average annual mortality of aboveground biomass of sawtimber trees, in dry short tons, on forest land",
    338: "Average annual mortality of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    339: "Average annual mortality of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    340: "Average annual mortality of aboveground biomass of sawtimber trees, in dry short tons, on timberland",
    341: "Average annual mortality of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    342: "Average annual mortality of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    343: "Average annual mortality of belowground biomass of sawtimber trees, in dry short tons, on forest land",
    344: "Average annual mortality of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    345: "Average annual mortality of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    346: "Average annual mortality of belowground biomass of sawtimber trees, in dry short tons, on timberland",
    369: "Average annual removals of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    370: "Average annual removals of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    371: "Average annual removals of aboveground biomass of sawtimber trees, in dry short tons, on forest land",
    372: "Average annual removals of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    373: "Average annual removals of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    374: "Average annual removals of aboveground biomass of sawtimber trees, in dry short tons, on timberland",
    375: "Average annual removals of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    376: "Average annual removals of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    377: "Average annual removals of belowground biomass of sawtimber trees, in dry short tons, on forest land",
    378: "Average annual removals of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    379: "Average annual removals of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    380: "Average annual removals of belowground biomass of sawtimber trees, in dry short tons, on timberland",
    403: "Average annual harvest removals of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    404: "Average annual harvest removals of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    405: "Average annual harvest removals of aboveground biomass of sawtimber trees, in dry short tons, on forest land",
    406: "Average annual harvest removals of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    407: "Average annual harvest removals of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    408: "Average annual harvest removals of aboveground biomass of sawtimber trees, in dry short tons, on timberland",
    409: "Average annual harvest removals of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    410: "Average annual harvest removals of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    411: "Average annual harvest removals of belowground biomass of sawtimber trees, in dry short tons, on forest land",
    412: "Average annual harvest removals of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    413: "Average annual harvest removals of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    414: "Average annual harvest removals of belowground biomass of sawtimber trees, in dry short tons, on timberland",
    437: "Average annual other removals of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    438: "Average annual other removals of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    439: "Average annual other removals of aboveground biomass of sawtimber trees, in dry short tons, on forest land",
    440: "Average annual other removals of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    441: "Average annual other removals of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    442: "Average annual other removals of aboveground biomass of sawtimber trees, in dry short tons, on timberland",
    443: "Average annual other removals of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    444: "Average annual other removals of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    445: "Average annual other removals of belowground biomass of sawtimber trees, in dry short tons, on forest land",
    446: "Average annual other removals of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    447: "Average annual other removals of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    448: "Average annual other removals of belowground biomass of sawtimber trees, in dry short tons, on timberland",
    901: "Average annual mortality of trees (at least 5 inches d.b.h./d.r.c.), in trees, on forest land",
    902: "Average annual mortality of growing-stock trees (at least 5 inches d.b.h.), in trees, on forest land",
    903: "Average annual mortality of sawtimber trees, in trees, on forest land",
    904: "Average annual mortality of trees (at least 5 inches d.b.h./d.r.c.), in trees, on timberland",
    905: "Average annual mortality of growing-stock trees (at least 5 inches d.b.h.), in trees, on timberland",
    906: "Average annual mortality of sawtimber trees, in trees, on timberland",
    907: "Average annual removals of trees (at least 5 inches d.b.h./d.r.c.), in trees, on forest land",
    908: "Average annual removals of growing-stock trees (at least 5 inches d.b.h.), in trees, on forest land",
    909: "Average annual removals of sawtimber trees, in trees, on forest land",
    910: "Average annual removals of trees (at least 5 inches d.b.h./d.r.c.), in trees, on timberland",
    911: "Average annual removals of growing-stock trees (at least 5 inches d.b.h.), in trees, on timberland",
    912: "Average annual removals of sawtimber trees, in trees, on timberland",
    913: "Average annual harvest removals of trees (at least 5 inches d.b.h./d.r.c.), in trees, on forest land",
    914: "Average annual harvest removals of growing-stock trees (at least 5 inches d.b.h.), in trees, on forest land",
    915: "Average annual harvest removals of sawtimber trees, in trees, on forest land",
    916: "Average annual harvest removals of trees (at least 5 inches d.b.h./d.r.c.), in trees, on timberland",
    917: "Average annual harvest removals of growing-stock trees (at least 5 inches d.b.h.), in trees, on timberland",
    918: "Average annual harvest removals of sawtimber trees, in trees, on timberland",
    919: "Average annual other removals of trees (at least 5 inches d.b.h./d.r.c.), in trees, on forest land",
    920: "Average annual other removals of growing-stock trees (at least 5 inches d.b.h.), in trees, on forest land",
    921: "Average annual other removals of sawtimber trees, in trees, on forest land",
    922: "Average annual other removals of trees (at least 5 inches d.b.h./d.r.c.), in trees, on timberland",
    923: "Average annual other removals of growing-stock trees (at least 5 inches d.b.h.), in trees, on timberland",
    924: "Average annual other removals of sawtimber trees, in trees, on timberland",
    953: "Average annual net growth of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    956: "Average annual net growth of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on timberland",
    1004: "Basal area of live trees (at least 1 inch d.b.h./d.r.c.), in square feet, on forest land",
    1005: "Basal area of growing-stock trees (at least 5 inches d.b.h.), in square feet, on forest land",
    1007: "Basal area of live trees (at least 1 inch d.b.h./d.r.c.), in square feet, on timberland",
    1008: "Basal area of growing-stock trees (at least 5 inches d.b.h.), in square feet, on timberland",
    1020: "Net sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on forest land",
    1021: "Net sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on timberland",
    1022: "Average annual net growth of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on forest land",
    1023: "Average annual net growth of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on timberland",
    1202: "Average annual gross growth of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land",
    1203: "Average annual gross growth of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    1204: "Average annual gross growth of sawlog wood volume of sawtimber trees, in cubic feet, on forest land",
    1205: "Average annual gross growth of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on forest land",
    1206: "Average annual gross growth of merchantable bole wood volume of sawtimber trees, in cubic feet, on forest land",
    1208: "Average annual gross growth of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on timberland",
    1209: "Average annual gross growth of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on timberland",
    1210: "Average annual gross growth of sawlog wood volume of sawtimber trees, in cubic feet, on timberland",
    1211: "Average annual gross growth of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on timberland",
    1212: "Average annual gross growth of merchantable bole wood volume of sawtimber trees, in cubic feet, on timberland",
    1311: "Average annual gross growth of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    1312: "Average annual gross growth of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    1313: "Average annual gross growth of aboveground biomass of sawtimber trees, in dry short tons, on forest land",
    1314: "Average annual gross growth of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    1315: "Average annual gross growth of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    1316: "Average annual gross growth of aboveground biomass of sawtimber trees, in dry short tons, on timberland",
    1317: "Average annual gross growth of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    1318: "Average annual gross growth of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    1319: "Average annual gross growth of belowground biomass of sawtimber trees, in dry short tons, on forest land",
    1320: "Average annual gross growth of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    1321: "Average annual gross growth of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    1322: "Average annual gross growth of belowground biomass of sawtimber trees, in dry short tons, on timberland",
    2202: "Average annual net change of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land",
    2203: "Average annual net change of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on forest land",
    2204: "Average annual net change of sawlog wood volume of sawtimber trees, in cubic feet, on forest land",
    2205: "Average annual net change of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on forest land",
    2206: "Average annual net change of merchantable bole wood volume of sawtimber trees, in cubic feet, on forest land",
    2208: "Average annual net change of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on timberland",
    2209: "Average annual net change of sawlog wood volume of sawtimber trees, in board feet (International 1/4-inch rule), on timberland",
    2210: "Average annual net change of sawlog wood volume of sawtimber trees, in cubic feet, on timberland",
    2211: "Average annual net change of merchantable bole wood volume above the sawlog of sawtimber trees, in cubic feet, on timberland",
    2212: "Average annual net change of merchantable bole wood volume of sawtimber trees, in cubic feet, on timberland",
    2311: "Average annual net change of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    2312: "Average annual net change of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    2313: "Average annual net change of aboveground biomass of sawtimber trees, in dry short tons, on forest land",
    2314: "Average annual net change of aboveground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    2315: "Average annual net change of aboveground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    2316: "Average annual net change of aboveground biomass of sawtimber trees, in dry short tons, on timberland",
    2317: "Average annual net change of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    2318: "Average annual net change of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    2319: "Average annual net change of belowground biomass of sawtimber trees, in dry short tons, on forest land",
    2320: "Average annual net change of belowground biomass of trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    2321: "Average annual net change of belowground biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    2322: "Average annual net change of belowground biomass of sawtimber trees, in dry short tons, on timberland",
    2635: "Average annual net growth of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2636: "Average annual net growth of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2637: "Average annual mortality of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2638: "Average annual mortality of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2639: "Average annual net growth of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2640: "Average annual net growth of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2641: "Average annual mortality of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2642: "Average annual mortality of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2649: "Average annual harvest removals of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2650: "Average annual harvest removals of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2651: "Average annual harvest removals of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2652: "Average annual gross growth of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2653: "Average annual gross growth of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2654: "Average annual harvest removals of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2661: "Average annual gross growth of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2662: "Average annual gross growth of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2665: "Average annual other removals of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2667: "Average annual other removals of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2668: "Average annual other removals of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2669: "Average annual other removals of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2674: "Average annual removals of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2675: "Average annual removals of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2676: "Average annual removals of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2677: "Average annual removals of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2680: "Average annual net change of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2681: "Average annual net change of aboveground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    2682: "Average annual net change of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    2683: "Average annual net change of belowground biomass of trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    3000: "Average annual ingrowth of trees (at least 5 inches d.b.h./d.r.c.), in trees, on forest land",
    3001: "Average annual ingrowth of growing-stock trees (at least 5 inches d.b.h.), in trees, on forest land",
    3002: "Average annual ingrowth of sawtimber trees, in trees, on forest land",
    3003: "Average annual ingrowth of trees (at least 5 inches d.b.h./d.r.c.), in trees, on timberland",
    3004: "Average annual ingrowth of growing-stock trees (at least 5 inches d.b.h.), in trees, on timberland",
    3005: "Average annual ingrowth of sawtimber trees, in trees, on timberland",
    3006: "Average annual diversion of trees (at least 5 inches d.b.h./d.r.c.), in trees, on forest land",
    3007: "Average annual diversion of growing-stock trees (at least 5 inches d.b.h.), in trees, on forest land",
    3008: "Average annual diversion of sawtimber trees, in trees, on forest land",
    3009: "Average annual diversion of trees (at least 5 inches d.b.h./d.r.c.), in trees, on timberland",
    3010: "Average annual diversion of growing-stock trees (at least 5 inches d.b.h.), in trees, on timberland",
    3011: "Average annual diversion of sawtimber trees, in trees, on timberland",
    3012: "Average annual reversion of trees (at least 5 inches d.b.h./d.r.c.), in trees, on forest land",
    3013: "Average annual reversion of growing-stock trees (at least 5 inches d.b.h.), in trees, on forest land",
    3014: "Average annual reversion of sawtimber trees, in trees, on forest land",
    3015: "Average annual reversion of trees (at least 5 inches d.b.h./d.r.c.), in trees, on timberland",
    3016: "Average annual reversion of growing-stock trees (at least 5 inches d.b.h.), in trees, on timberland",
    3017: "Average annual reversion of sawtimber trees, in trees, on timberland",
    3018: "Average annual survival of trees (at least 5 inches d.b.h./d.r.c.), in trees, on forest land",
    3019: "Average annual survival of growing-stock trees (at least 5 inches d.b.h.), in trees, on forest land",
    3020: "Average annual survival of sawtimber trees, in trees, on forest land",
    3021: "Average annual survival of trees (at least 5 inches d.b.h./d.r.c.), in trees, on timberland",
    3022: "Average annual survival of growing-stock trees (at least 5 inches d.b.h.), in trees, on timberland",
    3023: "Average annual survival of sawtimber trees, in trees, on timberland",
    11000: "Merchantable bole bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    11001: "Gross total-stem wood volume of live trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11002: "Gross total-stem bark volume of live trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11003: "Sound total-stem wood volume of live trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11004: "Sound total-stem bark volume of live trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11005: "Gross stump wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11006: "Gross stump bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11007: "Sound stump wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11008: "Sound stump bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11009: "Gross bole bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11010: "Gross stem-top (above 4-inch top diameter) wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11011: "Gross stem-top (above 4-inch top diameter) bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11012: "Sound bole bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11013: "Sound stem-top (above 4-inch top diameter) wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11014: "Sound stem-top (above 4-inch top diameter) bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11015: "Net bole bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11016: "Total-stem (from ground line to tree tip) wood biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    11017: "Total-stem (from ground line to tree tip) bark biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    11018: "Stump bark biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    11019: "Merchantable bole bark biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    11020: "Foliage biomass of live trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    11021: "Foliage biomass of live trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on forest land",
    11022: "Foliage biomass of live trees (woodland species at least 1 inch d.r.c.), in dry short tons, on forest land",
    11023: "Branch (excluding any part of the stem) biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    11024: "Gross total-stem wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11025: "Gross total-stem bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11026: "Sound total-stem wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11027: "Sound total-stem bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11028: "Gross total-stem wood volume of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in cubic feet, on forest land",
    11029: "Gross total-stem bark volume of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in cubic feet, on forest land",
    11030: "Sound total-stem wood volume of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in cubic feet, on forest land",
    11031: "Sound total-stem bark volume of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in cubic feet, on forest land",
    11032: "Branch (excluding any part of the stem) biomass of live trees (timber species at least 1 inch d.b.h.), in dry short tons, on forest land",
    11033: "Gross total-stem wood volume of live trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11034: "Gross total-stem bark volume of live trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11035: "Sound total-stem wood volume of live trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11036: "Sound total-stem bark volume of live trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11037: "Gross stump wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11038: "Gross stump bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11039: "Sound stump wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11040: "Sound stump bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11041: "Gross bole bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11042: "Gross stem-top (above 4-inch top diameter) wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11043: "Gross stem-top (above 4-inch top diameter) bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11044: "Sound bole bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11045: "Sound stem-top (above 4-inch top diameter) wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11046: "Sound stem-top (above 4-inch top diameter) bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11047: "Net bole bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11048: "Total-stem (from ground line to tree tip) wood biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    11049: "Total-stem (from ground line to tree tip) bark biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    11050: "Stump bark biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    11051: "Merchantable bole bark biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    11052: "Foliage biomass of live trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    11053: "Foliage biomass of live trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    11054: "Foliage biomass of live trees (woodland species at least 1 inch d.r.c.), in dry short tons, on timberland",
    11055: "Branch (excluding any part of the stem) biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    11056: "Gross total-stem wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11057: "Gross total-stem bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11058: "Sound total-stem wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11059: "Sound total-stem bark volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11060: "Gross total-stem wood volume of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in cubic feet, on timberland",
    11061: "Gross total-stem bark volume of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in cubic feet, on timberland",
    11062: "Sound total-stem wood volume of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in cubic feet, on timberland",
    11063: "Sound total-stem bark volume of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in cubic feet, on timberland",
    11064: "Branch (excluding any part of the stem) biomass of live trees (timber species at least 1 inch d.b.h.), in dry short tons, on timberland",
    11065: "Gross total-stem bark and wood volume of live trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11066: "Sound total-stem bark and wood volume of live trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11067: "Gross total-stem bark and wood volume of live trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11068: "Sound total-stem bark and wood volume of live trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11069: "Gross total-stem bark and wood volume of live trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11070: "Sound total-stem bark and wood volume of live trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11071: "Gross total-stem bark and wood volume of live trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11072: "Sound total-stem bark and wood volume of live trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11087: "Average annual net growth of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11088: "Average annual net growth of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11089: "Average annual mortality of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11090: "Average annual mortality of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11091: "Average annual removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11092: "Average annual removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11093: "Average annual harvest removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11094: "Average annual harvest removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11095: "Average annual other removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11096: "Average annual other removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11097: "Average annual gross growth of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11098: "Average annual gross growth of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11099: "Average annual net change of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11100: "Average annual net change of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h. and woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11101: "Average annual net growth of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11102: "Average annual net growth of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11103: "Average annual mortality of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11104: "Average annual mortality of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11105: "Average annual removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11106: "Average annual removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11107: "Average annual harvest removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11108: "Average annual harvest removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11109: "Average annual other removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11110: "Average annual other removals of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11111: "Average annual gross growth of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11112: "Average annual gross growth of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11113: "Average annual net change of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11114: "Average annual net change of sound total-stem bark and wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11143: "Average annual net growth of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11144: "Average annual net growth of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11145: "Average annual mortality of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11146: "Average annual mortality of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11147: "Average annual removals of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11148: "Average annual removals of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11149: "Average annual harvest removals of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11150: "Average annual harvest removals of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11151: "Average annual other removals of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11152: "Average annual other removals of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11153: "Average annual gross growth of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11154: "Average annual gross growth of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11155: "Average annual net change of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11156: "Average annual net change of sound total-stem bark and wood volume of trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11157: "Average annual net growth of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11158: "Average annual net growth of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11159: "Average annual mortality of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11160: "Average annual mortality of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11161: "Average annual removals of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11162: "Average annual removals of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11163: "Average annual harvest removals of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11164: "Average annual harvest removals of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11165: "Average annual other removals of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11166: "Average annual other removals of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11167: "Average annual gross growth of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11168: "Average annual gross growth of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11169: "Average annual net change of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11170: "Average annual net change of sound total-stem wood volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11185: "Average annual net growth of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11186: "Average annual net growth of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11187: "Average annual mortality of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11188: "Average annual mortality of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11189: "Average annual removals of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11190: "Average annual removals of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11191: "Average annual harvest removals of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11192: "Average annual harvest removals of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11193: "Average annual other removals of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11194: "Average annual other removals of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11195: "Average annual gross growth of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11196: "Average annual gross growth of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11197: "Average annual net change of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11198: "Average annual net change of sound total-stem bark volume of trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11213: "Foliage biomass of live trees (timber species at least 1 inch d.b.h.), in dry short tons, on forest land",
    11214: "Foliage biomass of live trees (timber species at least 1 inch d.b.h.), in dry short tons, on timberland",
    11215: "Average annual net growth of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11216: "Average annual net growth of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on timberland",
    11245: "Average annual net growth of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11246: "Average annual net growth of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land",
    11247: "Average annual net growth of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11248: "Average annual net growth of merchantable bole wood volume of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on timberland",
    11249: "Aboveground biomass of live trees (at least 1 inch d.b.h./d.r.c.), in green short tons, on forest land",
    11250: "Aboveground biomass of live trees (at least 1 inch d.b.h./d.r.c.), in green short tons, on timberland",
    11251: "Aboveground biomass of standing dead trees (at least 5 inches d.b.h./d.r.c.), in dry short tons, on timberland",
    11252: "Net merchantable bole wood volume of standing dead trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11253: "Net merchantable bole wood volume of standing dead trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    11254: "Average annual mortality of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on forest land",
    11255: "Average annual mortality of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on timberland",
    11256: "Average annual removals of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on forest land",
    11257: "Average annual removals of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on timberland",
    11258: "Average annual harvest removals of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on forest land",
    11259: "Average annual harvest removals of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on timberland",
    11260: "Average annual other removals of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on forest land",
    11261: "Average annual other removals of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on timberland",
    11262: "Average annual net change of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on forest land",
    11263: "Average annual net change of sawlog wood volume of sawtimber trees, in board feet (Doyle rule), on timberland",
    11264: "Number of standing dead trees (at least 1 inch d.b.h./d.r.c.), in trees, on forest land",
    11265: "Number of standing dead trees (at least 1 inch d.b.h./d.r.c.), in trees, on timberland",
    11266: "Aboveground biomass of standing dead trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on forest land",
    11267: "Aboveground biomass of standing dead trees (at least 1 inch d.b.h./d.r.c.), in dry short tons, on timberland",
    11268: "Aboveground carbon in standing dead trees (at least 1 inch d.b.h./d.r.c.), in short tons, on forest land",
    11269: "Aboveground carbon in standing dead trees (at least 1 inch d.b.h./d.r.c.), in short tons, on timberland",
    11270: "Sound total-stem wood volume of standing dead trees (timber species at least 1 inch d.b.h.), in cubic feet, on forest land",
    11271: "Sound total-stem wood volume of standing dead trees (timber species at least 1 inch d.b.h.), in cubic feet, on timberland",
    11272: "Sound total-stem bark and wood volume of standing dead trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on forest land",
    11273: "Sound total-stem bark and wood volume of standing dead trees (woodland species at least 1.5 inches d.r.c.), in cubic feet, on timberland",
    11274: "Average annual mortality of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11275: "Average annual mortality of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on timberland",
    11276: "Average annual removals of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11277: "Average annual removals of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on timberland",
    11278: "Average annual harvest removals of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11279: "Average annual harvest removals of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on timberland",
    11280: "Average annual other removals of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11281: "Average annual other removals of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on timberland",
    11282: "Gross total-stem bark and wood volume of live trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11283: "Sound total-stem bark and wood volume of live trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11284: "Gross total-stem bark and wood volume of live trees (woodland species at least 5 inches d.r.c.), in cubic feet, on timberland",
    11285: "Sound total-stem bark and wood volume of live trees (woodland species at least 5 inches d.r.c.), in cubic feet, on timberland",
    11286: "Gross total-stem bark and wood volume of live trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11287: "Sound total-stem bark and wood volume of live trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11288: "Gross total-stem bark and wood volume of live trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on timberland",
    11289: "Sound total-stem bark and wood volume of live trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on timberland",
    11290: "Sound total-stem bark and wood volume of live trees (woodland species at least 5 inches d.r.c.) and sound bole wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11291: "Average annual mortality of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.) and sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11292: "Average annual removals of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.) and sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11293: "Average annual harvest removals of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.) and sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11294: "Average annual other removals of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.) and sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11295: "Average annual net growth of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.) and sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11296: "Average annual gross growth of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.) and sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11297: "Average annual net change of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.) and sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11298: "Average annual net growth of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11299: "Average annual gross growth of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11300: "Average annual net change of sound total-stem bark and wood volume of trees (woodland species at least 5 inches d.r.c.), in cubic feet, on forest land",
    11301: "Aboveground carbon in standing dead trees (at least 5 inches d.b.h./d.r.c.), in short tons, on forest land",
    11302: "Aboveground carbon in standing dead trees (at least 5 inches d.b.h./d.r.c.), in short tons, on timberland",
    11303: "Average annual net growth of sound total-stem bark and wood volume of trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11304: "Average annual mortality of sound total-stem bark and wood volume of trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11305: "Average annual removals of sound total-stem bark and wood volume of trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11306: "Average annual harvest removals of sound total-stem bark and wood volume of trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11307: "Average annual other removals of sound total-stem bark and wood volume of trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11308: "Average annual gross growth of sound total-stem bark and wood volume of trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11309: "Average annual net change of sound total-stem bark and wood volume of trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11310: "Sound total-stem bark and wood volume of standing dead trees (at least 5 inches d.b.h./d.r.c.), in cubic feet, on forest land",
    11311: "Sound bole wood volume of standing dead trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    11312: "Sound bole wood volume of standing dead trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    12000: "Merchantable bole bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    47000: "Aboveground and belowground carbon in standing dead trees (at least 1 inch d.b.h./d.r.c.), in short tons, on forest land",
    47001: "Aboveground and belowground carbon in standing dead trees (at least 5 inches d.b.h./d.r.c.), in short tons, on forest land",
    53000: "Aboveground carbon in live trees (at least 1 inch d.b.h./d.r.c.), in short tons, on forest land",
    54000: "Belowground carbon in live trees (at least 1 inch d.b.h./d.r.c.), in short tons, on forest land",
    55000: "Aboveground and belowground carbon in live trees (at least 1 inch d.b.h./d.r.c.), in short tons, on forest land",
    56000: "Top and limb bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    57000: "Aboveground biomass of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in dry short tons, on forest land",
    58000: "Stump bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    60000: "Aboveground biomass of live trees (woodland species at least 1 inch d.r.c.), in dry short tons, on forest land",
    61000: "Aboveground and belowground carbon in standing dead trees (at least 1 inch d.b.h./d.r.c.), in short tons, on timberland",
    61001: "Aboveground and belowground carbon in standing dead trees (at least 5 inches d.b.h./d.r.c.), in short tons, on timberland",
    67000: "Aboveground carbon in live trees (at least 1 inch d.b.h./d.r.c.), in short tons, on timberland",
    68000: "Belowground carbon in live trees (at least 1 inch d.b.h./d.r.c.), in short tons, on timberland",
    69000: "Aboveground and belowground carbon in live trees (at least 1 inch d.b.h./d.r.c.), in short tons, on timberland",
    70000: "Top and limb bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    71000: "Aboveground biomass of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in dry short tons, on timberland",
    72000: "Stump bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    74000: "Aboveground biomass of live trees (woodland species at least 1 inch d.r.c.), in dry short tons, on timberland",
    133000: "Sawlog bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    134000: "Sawlog bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    511000: "Merchantable bole bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in green short tons, on forest land",
    512000: "Merchantable bole bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in green short tons, on timberland",
    533000: "Sawlog bark and wood biomass of sawtimber trees, in green short tons, on forest land",
    534000: "Sawlog bark and wood biomass of sawtimber trees, in green short tons, on timberland",
    556000: "Top and limb bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in green short tons, on forest land",
    557000: "Aboveground biomass of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in green short tons, on forest land",
    558000: "Stump bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in green short tons, on forest land",
    560000: "Aboveground biomass of live trees (woodland species at least 1 inch d.r.c.), in green short tons, on forest land",
    570000: "Top and limb bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in green short tons, on timberland",
    571000: "Aboveground biomass of live saplings (timber species at least 1 and less than 5 inches d.b.h.), in green short tons, on timberland",
    572000: "Stump bark and wood biomass of live trees (timber species at least 5 inches d.b.h.), in green short tons, on timberland",
    574000: "Aboveground biomass of live trees (woodland species at least 1 inch d.r.c.), in green short tons, on timberland",
    574001: "Average annual net growth of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574002: "Average annual net growth of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574003: "Average annual net growth of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574004: "Average annual net growth of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on forest land",
    574005: "Average annual net growth of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574006: "Average annual net growth of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574007: "Average annual net growth of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574008: "Average annual net growth of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574009: "Average annual net growth of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on timberland",
    574010: "Average annual net growth of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574011: "Average annual net growth of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574012: "Average annual net growth of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574013: "Average annual net growth of stump bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574014: "Average annual net growth of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574015: "Average annual net growth of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574016: "Average annual net growth of stump bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574017: "Average annual net growth of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574018: "Average annual net growth of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574019: "Average annual net growth of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574020: "Average annual net growth of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574021: "Average annual net growth of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574022: "Average annual net growth of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574023: "Average annual mortality of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574024: "Average annual mortality of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574025: "Average annual mortality of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574026: "Average annual mortality of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574027: "Average annual mortality of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574028: "Average annual mortality of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574029: "Average annual mortality of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574030: "Average annual mortality of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574031: "Average annual mortality of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on forest land",
    574032: "Average annual mortality of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on timberland",
    574033: "Average annual mortality of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574034: "Average annual mortality of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574035: "Average annual mortality of stump bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574036: "Average annual mortality of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574037: "Average annual mortality of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574038: "Average annual mortality of stump bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574039: "Average annual mortality of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574040: "Average annual mortality of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574041: "Average annual mortality of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574042: "Average annual mortality of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574043: "Average annual mortality of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574044: "Average annual mortality of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574045: "Average annual removals of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574046: "Average annual removals of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574047: "Average annual removals of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574048: "Average annual removals of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574049: "Average annual removals of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574050: "Average annual removals of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574051: "Average annual removals of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574052: "Average annual removals of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574053: "Average annual removals of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on forest land",
    574054: "Average annual removals of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on timberland",
    574055: "Average annual removals of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574056: "Average annual removals of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574057: "Average annual removals of stump bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574058: "Average annual removals of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574059: "Average annual removals of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574060: "Average annual removals of stump bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574061: "Average annual removals of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574062: "Average annual removals of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574063: "Average annual removals of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574064: "Average annual removals of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574065: "Average annual removals of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574066: "Average annual removals of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574067: "Average annual harvest removals of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574068: "Average annual harvest removals of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574069: "Average annual harvest removals of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574070: "Average annual harvest removals of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574071: "Average annual harvest removals of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574072: "Average annual harvest removals of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574073: "Average annual harvest removals of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574074: "Average annual harvest removals of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574075: "Average annual harvest removals of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on forest land",
    574076: "Average annual harvest removals of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on timberland",
    574077: "Average annual harvest removals of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574078: "Average annual harvest removals of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574079: "Average annual harvest removals of stump bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574080: "Average annual harvest removals of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574081: "Average annual harvest removals of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574082: "Average annual harvest removals of stump bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574083: "Average annual harvest removals of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574084: "Average annual harvest removals of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574085: "Average annual harvest removals of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574086: "Average annual harvest removals of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574087: "Average annual harvest removals of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574088: "Average annual harvest removals of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574089: "Average annual other removals of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574090: "Average annual other removals of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574091: "Average annual other removals of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574092: "Average annual other removals of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574093: "Average annual other removals of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574094: "Average annual other removals of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574095: "Average annual other removals of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574096: "Average annual other removals of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574097: "Average annual other removals of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on forest land",
    574098: "Average annual other removals of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on timberland",
    574099: "Average annual other removals of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574100: "Average annual other removals of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574101: "Average annual other removals of stump bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574102: "Average annual other removals of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574103: "Average annual other removals of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574104: "Average annual other removals of stump bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574105: "Average annual other removals of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574106: "Average annual other removals of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574107: "Average annual other removals of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574108: "Average annual other removals of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574109: "Average annual other removals of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574110: "Average annual other removals of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574111: "Average annual gross growth of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574112: "Average annual gross growth of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574113: "Average annual gross growth of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574114: "Average annual gross growth of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on forest land",
    574115: "Average annual gross growth of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574116: "Average annual gross growth of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574117: "Average annual gross growth of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574118: "Average annual gross growth of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574119: "Average annual gross growth of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on timberland",
    574120: "Average annual gross growth of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574121: "Average annual gross growth of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574122: "Average annual gross growth of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574123: "Average annual gross growth of stump bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574124: "Average annual gross growth of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574125: "Average annual gross growth of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574126: "Average annual gross growth of stump bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574127: "Average annual gross growth of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574128: "Average annual gross growth of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574129: "Average annual gross growth of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574130: "Average annual gross growth of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574131: "Average annual gross growth of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574132: "Average annual gross growth of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574133: "Average annual net change of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574134: "Average annual net change of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574135: "Average annual net change of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574136: "Average annual net change of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on forest land",
    574137: "Average annual net change of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574138: "Average annual net change of merchantable bole bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574139: "Average annual net change of merchantable bole bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574140: "Average annual net change of sawlog bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574141: "Average annual net change of merchantable bole bark and wood biomass above the sawlog of sawtimber trees, in dry short tons, on timberland",
    574142: "Average annual net change of merchantable bole bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574143: "Average annual net change of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574144: "Average annual net change of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574145: "Average annual net change of stump bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574146: "Average annual net change of stump bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574147: "Average annual net change of stump bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574148: "Average annual net change of stump bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574149: "Average annual net change of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on forest land",
    574150: "Average annual net change of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on forest land",
    574151: "Average annual net change of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on forest land",
    574152: "Average annual net change of top and limb bark and wood biomass of trees (timber species at least 5 inches d.b.h.), in dry short tons, on timberland",
    574153: "Average annual net change of top and limb bark and wood biomass of growing-stock trees (at least 5 inches d.b.h.), in dry short tons, on timberland",
    574154: "Average annual net change of top and limb bark and wood biomass of sawtimber trees, in dry short tons, on timberland",
    574155: "Average annual net growth of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574156: "Average annual net growth of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    574157: "Average annual mortality of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574158: "Average annual mortality of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    574159: "Average annual removals of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574160: "Average annual removals of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    574161: "Average annual harvest removals of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574162: "Average annual harvest removals of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    574163: "Average annual other removals of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574164: "Average annual other removals of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    574165: "Average annual gross growth of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574166: "Average annual gross growth of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    574167: "Average annual net change of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574168: "Average annual net change of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    574171: "Net merchantable bole wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574172: "Net merchantable bole wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    574173: "Gross bole wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574174: "Sound bole wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land",
    574175: "Sound bole wood volume of live trees (timber species at least 5 inches d.b.h.), in cubic feet, on timberland",
    574200: "Net sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on forest land",
    574201: "Net sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on timberland",
    574202: "Gross sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on forest land",
    574203: "Average annual net growth of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on forest land",
    574204: "Average annual net growth of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on timberland",
    574205: "Average annual mortality of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on forest land",
    574206: "Average annual mortality of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on timberland",
    574207: "Average annual removals of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on forest land",
    574208: "Average annual removals of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on timberland",
    574209: "Average annual harvest removals of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on forest land",
    574210: "Average annual harvest removals of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on timberland",
    574211: "Average annual other removals of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on forest land",
    574212: "Average annual other removals of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on timberland",
    574213: "Average annual gross growth of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on forest land",
    574214: "Average annual gross growth of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on timberland",
    574215: "Average annual net change of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on forest land",
    574216: "Average annual net change of sawlog wood volume of sawtimber trees, in board feet (Scribner rule), on timberland",
}


# Common estimate types for convenience
class CommonEstimates:
    """Commonly used estimate types for quick access."""

    # Area
    FOREST_LAND_AREA = 2
    TIMBERLAND_AREA = 3

    # Tree counts
    LIVE_TREES_FOREST = 4
    LIVE_TREES_TIMBERLAND = 7
    GROWING_STOCK_TREES_FOREST = 5
    GROWING_STOCK_TREES_TIMBERLAND = 8
    STANDING_DEAD_TREES_FOREST = 6
    STANDING_DEAD_TREES_TIMBERLAND = 9
    SEEDLINGS_FOREST = 45
    SEEDLINGS_TIMBERLAND = 46

    # Basal area
    BASAL_AREA_LIVE_FOREST = 1004
    BASAL_AREA_LIVE_TIMBERLAND = 1007
    BASAL_AREA_GROWING_STOCK_FOREST = 1005
    BASAL_AREA_GROWING_STOCK_TIMBERLAND = 1008

    # Volume
    NET_VOLUME_GROWING_STOCK_FOREST = 15
    NET_VOLUME_GROWING_STOCK_TIMBERLAND = 18
    NET_SAWLOG_VOLUME_FOREST = 16
    NET_SAWLOG_VOLUME_TIMBERLAND = 19
    NET_SAWLOG_BOARD_FEET_FOREST = 20
    NET_SAWLOG_BOARD_FEET_TIMBERLAND = 21

    # Biomass
    ABOVEGROUND_BIOMASS_FOREST = 10
    ABOVEGROUND_BIOMASS_TIMBERLAND = 13
    BELOWGROUND_BIOMASS_FOREST = 59
    BELOWGROUND_BIOMASS_TIMBERLAND = 73

    # Carbon
    TOTAL_CARBON_FOREST = 97
    CARBON_POOL_ABOVEGROUND = 98
    CARBON_POOL_BELOWGROUND = 99
    CARBON_POOL_DEAD_WOOD = 100
    CARBON_POOL_LITTER = 101
    CARBON_POOL_SOIL = 102
    CARBON_POOL_TOTAL = 103


def get_description(snum: int) -> str:
    """Get the description for an SNUM value."""
    return SNUM_DESCRIPTIONS.get(snum, f"Unknown SNUM: {snum}")


def get_category(snum: int) -> str:
    """Get the category for an SNUM value."""
    desc = SNUM_DESCRIPTIONS.get(snum, "").lower()

    if "area" in desc and "change" in desc:
        return "AREA_CHANGE"
    elif "basal area" in desc:
        return "BASAL_AREA"
    elif "area" in desc:
        return "AREA"
    elif "number of" in desc and ("tree" in desc or "seedling" in desc):
        return "TREE_COUNT"
    elif "volume" in desc:
        return "VOLUME"
    elif "biomass" in desc or "weight" in desc:
        return "BIOMASS"
    elif "carbon" in desc:
        return "CARBON"
    elif any(kw in desc for kw in ["cwd", "fwd", "dwm", "litter", "duff"]):
        return "DOWN_WOODY"
    else:
        return "TREE_DYNAMICS"


# =============================================================================
# BACKWARD COMPATIBILITY ALIASES
# =============================================================================
# These module-level aliases maintain compatibility with existing code that
# imports estimate types using descriptive names.
#
# Usage:
#     from pyfia.evalidator.estimate_types import AREA_FOREST, VOLUME_NET_GROWINGSTOCK

# Area estimates
AREA_FOREST = 2
AREA_TIMBERLAND = 3
AREA_SAMPLED = 79

# Area change estimates
AREA_CHANGE_SAMPLED = 126
AREA_CHANGE_FOREST_REMEASURED = 127
AREA_CHANGE_FOREST_EITHER = 128
AREA_CHANGE_TIMBERLAND_REMEASURED = 129
AREA_CHANGE_TIMBERLAND_EITHER = 130
AREA_CHANGE_ANNUAL_SAMPLED = 135
AREA_CHANGE_ANNUAL_FOREST_BOTH = 136
AREA_CHANGE_ANNUAL_FOREST_EITHER = 137
AREA_CHANGE_ANNUAL_TIMBERLAND_BOTH = 138
AREA_CHANGE_ANNUAL_TIMBERLAND_EITHER = 139

# Tree counts
TREE_COUNT_1INCH_FOREST = 4
TREE_COUNT_5INCH_FOREST = 5
TREE_COUNT_1INCH_TIMBER = 7
TREE_COUNT_5INCH_TIMBER = 8
TREE_COUNT_1INCH = 4  # Legacy alias
TREE_COUNT_5INCH = 5  # Legacy alias

# Basal area
BASAL_AREA_1INCH = 1004
BASAL_AREA_5INCH = 1007

# Volume
VOLUME_NET_GROWINGSTOCK = 15
VOLUME_NET_ALLSPECIES = 18
VOLUME_SAWLOG_DOYLE = 19
VOLUME_SAWLOG_INTERNATIONAL = 20
VOLUME_SAWLOG_SCRIBNER = 21

# Biomass
BIOMASS_AG_LIVE = 10
BIOMASS_AG_LIVE_5INCH = 13
BIOMASS_BG_LIVE = 59
BIOMASS_BG_LIVE_5INCH = 73

# Carbon
CARBON_AG_LIVE = 53000
CARBON_TOTAL_LIVE = 55000
CARBON_POOL_AG = 98
CARBON_POOL_BG = 99
CARBON_POOL_DEADWOOD = 100
CARBON_POOL_LITTER = 101
CARBON_POOL_SOIL = 102
CARBON_POOL_TOTAL = 103

# Growth
GROWTH_NET_VOLUME = 202
GROWTH_NET_BIOMASS = 311

# Mortality
MORTALITY_VOLUME = 214
MORTALITY_BIOMASS = 336

# Removals
REMOVALS_VOLUME = 226
REMOVALS_BIOMASS = 369
