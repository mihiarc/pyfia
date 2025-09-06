#!/usr/bin/env python
"""Debug the adjustment factor application"""

import duckdb
import polars as pl

conn = duckdb.connect('nfi_south.duckdb')

# Check a sample of the adjustment factors
query = """
SELECT 
    COND.PROP_BASIS,
    COUNT(*) as count,
    AVG(POP_STRATUM.ADJ_FACTOR_MACR) as avg_macr,
    AVG(POP_STRATUM.ADJ_FACTOR_SUBP) as avg_subp
FROM POP_STRATUM 
JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN) 
JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN) 
JOIN COND ON (COND.PLT_CN = PLOT.CN) 
WHERE COND.COND_STATUS_CD = 1 
    AND pop_stratum.evalid = 402301
GROUP BY COND.PROP_BASIS
"""

result = conn.execute(query).fetchdf()
print("Adjustment factors by PROP_BASIS:")
print(result)

# Check total area without adjustment
query2 = """
SELECT 
    SUM(COND.CONDPROP_UNADJ * pop_stratum.expns) as unadjusted_total
FROM POP_STRATUM 
JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN) 
JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN) 
JOIN COND ON (COND.PLT_CN = PLOT.CN) 
WHERE COND.COND_STATUS_CD = 1 
    AND pop_stratum.evalid = 402301
"""

result2 = conn.execute(query2).fetchone()
print(f"\nUnadjusted total: {result2[0]:,.0f} acres")

# Now with adjustment
query3 = """
SELECT 
    SUM(CAST(COND.CONDPROP_UNADJ AS DOUBLE) * 
        CASE COND.PROP_BASIS 
            WHEN 'MACR' THEN CAST(POP_STRATUM.ADJ_FACTOR_MACR AS DOUBLE)
            ELSE CAST(POP_STRATUM.ADJ_FACTOR_SUBP AS DOUBLE)
        END * CAST(pop_stratum.expns AS DOUBLE)
    ) as adjusted_total
FROM POP_STRATUM 
JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN) 
JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN) 
JOIN COND ON (COND.PLT_CN = PLOT.CN) 
WHERE COND.COND_STATUS_CD = 1 
    AND pop_stratum.evalid = 402301
"""

result3 = conn.execute(query3).fetchone()
print(f"Adjusted total: {result3[0]:,.0f} acres")

adjustment_factor = result3[0] / result2[0]
print(f"\nOverall adjustment factor: {adjustment_factor:.4f}")