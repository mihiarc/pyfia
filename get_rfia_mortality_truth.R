#!/usr/bin/env Rscript
#
# Get actual rFIA mortality ground truth for validation
#
# This script attempts to run rFIA growMort() to get the authoritative
# mortality estimates that pyFIA must match exactly.
#

library(rFIA)

cat("🎯 Getting rFIA ground truth for mortality validation...\n")
cat("rFIA is the source of truth - pyFIA must match these results exactly.\n\n")

# Try different approaches to get rFIA working

# Method 1: Check if we can read the SQLite database directly
cat("Method 1: Attempting to read SQLite database with rFIA...\n")
db_path <- "./SQLite_FIADB_NC.db"

if (file.exists(db_path)) {
  cat("✅ Found database:", db_path, "\n")
  
  tryCatch({
    # Try reading SQLite directly (may not work)
    cat("Attempting: readFIA() on SQLite database...\n")
    nc_data <- readFIA(db_path)
    
    cat("✅ Successfully loaded with readFIA!\n")
    
    # Find available evaluations
    cat("Finding available evaluations...\n")
    evalids <- findEVALID(nc_data)
    print(evalids)
    
    # Look for our target evaluation or similar GRM evaluation
    if (372303 %in% evalids$EVALID) {
      target_evalid <- 372303
      cat("✅ Found target EVALID 372303\n")
    } else {
      # Find the most recent GRM evaluation
      grm_evals <- evalids[grepl("GRM|MORT", evalids$EVAL_DESCR, ignore.case = TRUE), ]
      if (nrow(grm_evals) > 0) {
        target_evalid <- grm_evals$EVALID[nrow(grm_evals)]  # Most recent
        cat("Using most recent GRM evaluation:", target_evalid, "\n")
      } else {
        cat("❌ No GRM evaluations found\n")
        quit(status = 1)
      }
    }
    
    # Clip to target evaluation
    cat("Clipping to EVALID", target_evalid, "...\n")
    nc_clipped <- clipFIA(nc_data, evalid = target_evalid)
    
    # Run growMort to get mortality estimates
    cat("Running rFIA growMort()...\n")
    rfia_mortality <- growMort(nc_clipped, method = "TI")
    
    cat("✅ rFIA mortality results:\n")
    print(rfia_mortality)
    
    # Extract key values for comparison
    if (nrow(rfia_mortality) > 0) {
      mort_tpa <- rfia_mortality$MORT_TPA[1]
      mort_tpa_se <- rfia_mortality$MORT_TPA_SE[1]
      mort_vol <- rfia_mortality$MORT_NETVOL_ACRE[1] %||% NA
      mort_bio <- rfia_mortality$MORT_BIO_ACRE[1] %||% NA
      n_plots <- rfia_mortality$nPlots[1]
      
      cat("\n🎯 rFIA Ground Truth Values:\n")
      cat("=============================\n")
      cat(sprintf("EVALID: %d\n", target_evalid))
      cat(sprintf("Annual Mortality: %.6f trees/acre/year\n", mort_tpa))
      cat(sprintf("Mortality SE: %.2f%%\n", mort_tpa_se))
      if (!is.na(mort_vol)) cat(sprintf("Volume Mortality: %.6f cu ft/acre/year\n", mort_vol))
      if (!is.na(mort_bio)) cat(sprintf("Biomass Mortality: %.6f tons/acre/year\n", mort_bio))
      cat(sprintf("Number of Plots: %d\n", n_plots))
      
      # Compare with our pyFIA results
      cat("\n📊 Comparison with pyFIA:\n")
      cat("=========================\n")
      
      # pyFIA results (update these with actual values)
      pyfia_mort_tpa <- 0.080127
      pyfia_mort_vol <- 0.091
      pyfia_mort_bio <- 0.0029
      pyfia_n_plots <- 5673
      
      cat("Metric                  | rFIA        | pyFIA       | Difference\n")
      cat("------------------------|-------------|-------------|------------\n")
      cat(sprintf("Mortality TPA          | %.6f    | %.6f    | %.2f%%\n", 
                  mort_tpa, pyfia_mort_tpa, ((pyfia_mort_tpa - mort_tpa) / mort_tpa) * 100))
      
      if (!is.na(mort_vol)) {
        cat(sprintf("Volume Mortality       | %.6f    | %.6f    | %.2f%%\n", 
                    mort_vol, pyfia_mort_vol, ((pyfia_mort_vol - mort_vol) / mort_vol) * 100))
      }
      
      if (!is.na(mort_bio)) {
        cat(sprintf("Biomass Mortality      | %.6f    | %.6f    | %.2f%%\n", 
                    mort_bio, pyfia_mort_bio, ((pyfia_mort_bio - mort_bio) / mort_bio) * 100))
      }
      
      cat(sprintf("Number of Plots        | %d        | %d        | %s\n", 
                  n_plots, pyfia_n_plots, ifelse(n_plots == pyfia_n_plots, "✅ Match", "❌ Differ")))
      
      # Validation assessment
      tpa_diff <- abs((pyfia_mort_tpa - mort_tpa) / mort_tpa) * 100
      
      cat("\n🎯 Validation Assessment:\n")
      cat("=========================\n")
      
      if (tpa_diff < 1.0) {
        cat("✅ EXCELLENT: <1% difference - pyFIA matches rFIA\n")
      } else if (tpa_diff < 5.0) {
        cat("⚠️  GOOD: <5% difference - minor discrepancy\n")
      } else {
        cat("❌ NEEDS WORK: >5% difference - significant discrepancy\n")
      }
      
      cat(sprintf("TPA difference: %.2f%%\n", tpa_diff))
      
      if (n_plots != pyfia_n_plots) {
        cat("⚠️  Plot counts differ - check evaluation filtering\n")
      }
    }
    
  }, error = function(e) {
    cat("❌ Method 1 failed:", e$message, "\n\n")
  })
} else {
  cat("❌ Database file not found\n\n")
}

# Method 2: Instructions for CSV download approach
cat("Method 2: CSV Download Approach (if SQLite doesn't work)\n")
cat("========================================================\n\n")

cat("If the SQLite approach above failed, use this method:\n\n")

cat("1. Download NC FIA data in CSV format:\n")
cat("   - Visit: https://apps.fs.usda.gov/fia/datamart/CSV/\n")
cat("   - Download all NC tables as CSV files\n")
cat("   - Extract to 'NC_CSV/' directory\n\n")

cat("2. Run this R code:\n")
cat("```r\n")
cat("library(rFIA)\n")
cat("nc_data <- readFIA('NC_CSV/', common = FALSE)\n")
cat("evalids <- findEVALID(nc_data, type = 'GRM')\n")
cat("print(evalids)  # Find correct GRM evaluation\n")
cat("nc_clipped <- clipFIA(nc_data, evalid = 372303)  # Or correct GRM EVALID\n")
cat("rfia_mortality <- growMort(nc_clipped, method = 'TI')\n")
cat("print(rfia_mortality)\n")
cat("```\n\n")

cat("3. The rFIA result is the GROUND TRUTH that pyFIA must match.\n")
cat("   Acceptable difference: <1% for production ready status\n\n")

cat("🎯 Remember: rFIA is the authoritative source of truth.\n")
cat("   Literature values are useful for sanity checks only.\n")
cat("   pyFIA validation requires exact comparison with rFIA results.\n")