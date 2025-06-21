#!/usr/bin/env Rscript
#
# Get rFIA ground truth values for mortality estimation validation
#
# This script generates the mortality estimates that pyFIA should match
# for NC EVALID 372301 (2023 GRM evaluation)
#

library(rFIA)
library(dplyr)

cat("🎯 Getting rFIA ground truth for mortality estimation...\n\n")

# Note: You would typically load from FIA CSV download here
# For now, this shows the expected rFIA call pattern

# Expected usage:
# nc_db <- readFIA("/path/to/NC_CSV/", common = FALSE)
# 
# # Clip to specific GRM evaluation (372301 is VOL, need GRM equivalent)
# nc_clipped <- clipFIA(nc_db, evalid = 372301)  # This might be a VOL evalid
#
# # Get mortality estimates
# mort_result <- growMort(nc_clipped, method = "TI")

# Since we don't have the database loaded, let's show the expected structure
cat("Expected rFIA mortality results for NC EVALID 372301:\n")
cat("=====================================\n\n")

# Expected columns from rFIA growMort() function:
expected_columns <- c(
  "EVALID",           # Evaluation ID
  "MORT_TPA",         # Mortality trees per acre per year
  "MORT_TPA_SE",      # Standard error of mortality TPA
  "MORT_PERC",        # Mortality percentage of standing stock  
  "MORT_PERC_SE",     # Standard error of mortality percentage
  "MORT_NETVOL_ACRE", # Net volume mortality (cubic feet/acre/year)
  "MORT_NETVOL_SE",   # Standard error of volume mortality
  "MORT_BIO_ACRE",    # Biomass mortality (tons/acre/year) 
  "MORT_BIO_SE",      # Standard error of biomass mortality
  "nPlots"            # Number of plots used
)

cat("Expected result columns:\n")
for (col in expected_columns) {
  cat(sprintf("  - %s\n", col))
}

cat("\nTo get actual ground truth values:\n")
cat("1. Download NC FIA data from FIA DataMart\n") 
cat("2. Load with readFIA() - ensure you get GRM evaluation\n")
cat("3. Use findEVALID() to find GRM evaluations for 2023\n")
cat("4. Run growMort() with method='TI'\n")

cat("\nExample rFIA call:\n")
cat("library(rFIA)\n")
cat("nc_db <- readFIA('/path/to/NC_FIADB.db')\n")
cat("evalids <- findEVALID(nc_db, mostRecent = TRUE, type = 'GRM')\n")
cat("nc_clipped <- clipFIA(nc_db, evalid = evalids$EVALID[1])\n")
cat("mort_result <- growMort(nc_clipped, method = 'TI')\n")
cat("print(mort_result)\n")

cat("\n📋 Key validation targets for pyFIA:\n")
cat("- MORT_TPA: Annual mortality rate (trees/acre/year)\n")
cat("- MORT_NETVOL_ACRE: Volume mortality (cu ft/acre/year)\n") 
cat("- MORT_BIO_ACRE: Biomass mortality (tons/acre/year)\n")
cat("- Standard errors and plot counts\n")

cat("\n⚠️  Critical notes:\n")
cat("- Must use GRM evaluation (not VOL evaluation 372301)\n")
cat("- Mortality values are already annualized in FIA\n") 
cat("- Need to match exact EVALID for valid comparison\n")
cat("- rFIA uses beginning-of-period attributes for state variables\n")

cat("\n✅ Next steps:\n")
cat("1. Identify correct NC GRM evaluation ID\n")
cat("2. Run rFIA growMort() to get ground truth\n")
cat("3. Test pyFIA mortality() function against ground truth\n")
cat("4. Validate results match within <1% difference\n")