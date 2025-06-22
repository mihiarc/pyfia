# FIA Query Best Practices

This guide provides comprehensive guidelines for developing effective, accurate, and maintainable FIA database queries.

## 📝 Core Query Standards

### 1. **Always use EVALID filtering** for statistical estimates
- EVALIDs represent specific statistical evaluations with consistent methodology
- Never mix different EVALIDs in the same analysis
- Verify EVALID values match your analysis time period and scope

### 2. **Include appropriate status codes**
- `STATUSCD = 1` for live trees
- `COND_STATUS_CD = 1` for forest conditions
- Use appropriate filters for your analysis scope

### 3. **Handle NULL values** with explicit checks
- Use `IS NOT NULL` checks for critical fields
- Consider `COALESCE()` for default values
- NULL values can significantly skew calculations

### 4. **Use proper expansion factors**
- `TPA_UNADJ` for trees per acre (unadjusted)
- `EXPNS` for area expansion to population estimates
- Raw counts without expansion factors are not meaningful

### 5. **Join through POP_PLOT_STRATUM_ASSGN** for EVALID-based queries
- This table links plots to population strata for proper statistical weighting
- Essential for EVALIDator-compatible queries
- Ensures proper statistical methodology

### 6. **Include plot counts** for context
- Provides sample size information for reliability assessment
- Helps validate query results
- Important for understanding statistical confidence

### 7. **Order results meaningfully**
- Usually by the main metric DESC for rankings
- Consider logical groupings (species, diameter classes, etc.)
- Make results easy to interpret and validate

## 🔥 Critical Analysis Distinctions

### 8. **🚨 CRITICAL: Understand Forest Type vs Species Analysis**

This is one of the most important distinctions in FIA analysis:

**Forest Type Analysis (FORTYPCD)**:
- Analyzes all species within specific forest types
- Example: "TPA in loblolly pine forests" = all trees in forests dominated by loblolly pine
- Result: ~770 TPA (includes all species in loblolly-dominated stands)

**Species Analysis (SPCD)**:
- Analyzes specific species across all forest types
- Example: "Loblolly pine TPA" = only loblolly pine trees across all forest types
- Result: ~150 TPA (only loblolly pine trees, regardless of forest type)

**Results can differ by 5x or more - always clarify which interpretation is needed!**

## 🌲 Growth, Removal, and Mortality (GRM) Queries

### 9. **For GRM Queries: Use exact Oracle EVALIDator structure**

GRM queries are among the most complex in FIA analysis:

- **Complex tree joins** through previous plot connections
- Use `TREE_GRM_MIDPT.VOLCFNET` for volume calculations
- **Plot-level aggregation** before final species grouping
- **Proper SUBPTYP_GRM adjustment factor mapping**:
  - 1 = SUBP (subplot)
  - 2 = MICR (microplot)  
  - 3 = MACR (macroplot)

**For mortality**: Use `TPAMORT_UNADJ` with `MORTALITY%` components
**For harvest removals**: Use `TPAREMV_UNADJ` with `CUT%` components

**Important**: Don't add restrictive filters or mix components
- Original Oracle EVALIDator queries include all tree sizes and land types
- Adding `DIA ≥5"` or timberland-only filters can change results significantly
- Don't mix mortality (`MORTALITY%`) and harvest (`CUT%`) components in same query

## 🌿 Biomass and Carbon Queries

### 10. **For Biomass Queries: Use species-specific calculations**

Biomass queries require the most complex calculations:

- **Include complex biomass formulas** with wood and bark specific gravity
- **Apply moisture content adjustments** (`MC_PCT_GREEN_WOOD`, `MC_PCT_GREEN_BARK`)
- **Use bark volume percentage** (`BARK_VOL_PCT`) for accurate wood/bark ratios
- **Convert DRYBIO_AG** from pounds to tons (/2000)
- **Apply diameter-based adjustment factors** with nested CASE/LEAST logic
- **Handle special species group mappings** (e.g., eastern redcedar)

**Don't simplify these calculations** - they are scientifically validated equations.

## 🎯 Query Optimization and Simplification

### 11. **Query Simplification: Prefer readable, maintainable approaches**

Modern best practice emphasizes code quality alongside statistical accuracy:

- **Test simplified versions** that produce identical results to complex Oracle translations
- **Use direct field grouping** (e.g., `SPGRPCD`) instead of complex string formatting when possible
- **Validate simplified queries** against original Oracle EVALIDator methodology
- **Document verification** that simplified approaches match official results
- **Balance code readability with statistical accuracy** (both are achievable)

**Example**: The Colorado biomass query was simplified from 120+ lines to ~90 lines while maintaining identical results to Oracle EVALIDator.

## 🔍 Validation and Testing

### 12. **Always validate against known results**
- Compare with published FIA reports when available
- Test against Oracle EVALIDator results
- Verify plot counts and forest area calculations
- Check for reasonable orders of magnitude

### 13. **Document methodology and assumptions**
- Include expected results in query comments
- Note any deviations from standard EVALIDator methodology
- Explain complex calculations and their sources
- Provide validation notes for future reference

### 14. **Use consistent naming conventions**
- Follow FIA field naming standards
- Use descriptive aliases for calculated fields
- Maintain consistency across related queries
- Make code self-documenting where possible

## 🚀 Performance Considerations

### 15. **Optimize for large datasets**
- FIA databases contain millions of records
- Use appropriate indexes on join fields
- Consider query execution plans for complex calculations
- Test performance with full datasets, not samples

### 16. **Leverage modern SQL features**
- Use CTEs (Common Table Expressions) for complex logic
- Apply window functions for advanced analytics
- Consider materialized views for frequently-used calculations
- Take advantage of database-specific optimizations

## 📚 Documentation Standards

### 17. **Comprehensive query documentation**
- Include purpose, methodology, and expected results
- Document all assumptions and limitations
- Provide example output and interpretation
- Reference source materials (EVALIDator, FIA manuals, etc.)

### 18. **Version control and change tracking**
- Track query evolution and improvements
- Document validation of simplified approaches
- Maintain links to test results and comparisons
- Note when queries are superseded by better versions

This comprehensive approach ensures that FIA queries are not only statistically accurate but also maintainable, understandable, and reliable for long-term use. 