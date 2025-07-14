# FIA Query Library

This directory contains a comprehensive, organized collection of tested and validated SQL queries for the FIA database. All queries have been verified to work with the current database structure and follow EVALIDator methodology.

## 📁 Query Categories

### 🌳 [Basic Tree Queries](./basic_tree/)
- **Oregon Total Live Trees** - EVALIDator-style tree counting with proper adjustment factors
- Core tree enumeration and basic forest inventory queries

### 🌲 [Growth, Removal, and Mortality (GRM)](./growth_mortality/)
- **Colorado Merchantable Volume Mortality** - Annual mortality of growing-stock timber volume
- **North Carolina Tree Mortality Rate** - Simple mortality rate in trees per acre per year
- Advanced queries using TREE_GRM_* tables with proper component filtering

### 🌿 [Biomass and Carbon](./biomass_carbon/)
- **Colorado Above-Ground Biomass** - Species-specific biomass calculations with wood/bark properties
- Complex biomass equations with moisture content and specific gravity adjustments

### 🌳 [Tree Count and Density](./tree_density/)
- **North Carolina Live Trees by Species** - Species-level tree enumeration
- **Minnesota Forest Area by Type Group** - Forest type group area calculations
- Tree per acre (TPA) calculations and species distribution analysis

### 🏛️ [Ratio Estimation](./ratio_estimation/)
- **Alabama Trees Per Acre in Forest Types** - Ratio-based TPA calculations
- **Loblolly Pine Distribution Analysis** - Forest type vs species analysis examples
- Advanced statistical estimation techniques

### 📊 [Volume Analysis](./volume/)
- **California Volume by Diameter Class** - Merchantable timber volume analysis
- Diameter-based volume distribution and timber assessment

### 🗺️ [Forest Area](./forest_area/)
- **Alabama Land Area by Condition** - Land use classification and area estimates
- Forest vs non-forest land analysis

### 🔄 [Forest Change](./forest_change/)
- **Missouri Forest Type Change** - Forest area changes by type group using remeasurement data
- Temporal analysis using SUBP_COND_CHNG_MTRX and change evaluations

## 🎯 Key Features

- **✅ EVALIDator Compatible**: All queries match Oracle EVALIDator methodology
- **🧪 Tested & Validated**: Each query includes expected results and validation notes
- **📖 Well Documented**: Comprehensive methodology notes and insights
- **🔧 Maintainable**: Simplified approaches where possible without sacrificing accuracy
- **🤖 AI-Agent Friendly**: Modular structure for easy navigation and reference

## 📝 Query Standards

All queries in this library follow these standards:

1. **EVALID-based filtering** for proper statistical estimates
2. **Appropriate status codes** (STATUSCD=1 for live trees, COND_STATUS_CD=1 for forest)
3. **Proper expansion factors** (TPA_UNADJ, EXPNS) for population estimates
4. **NULL value handling** with explicit checks
5. **Meaningful result ordering** (usually by primary metric DESC)
6. **Comprehensive documentation** with methodology notes and key insights

## ⚠️ Critical Guidelines

- **Forest Type vs Species Analysis**: Understand the difference between analyzing species within forest types vs forest types containing specific species
- **GRM Methodology**: Use exact Oracle EVALIDator structure for Growth, Removal, Mortality queries
- **Biomass Calculations**: Include all species-specific properties and adjustments
- **Query Simplification**: Prefer readable approaches that maintain statistical accuracy

## 🚀 Getting Started

1. Browse categories above to find relevant query types
2. Each category contains detailed README with query descriptions
3. Individual query files include full documentation and expected results
4. Test queries against your FIA database before production use

## 📚 Additional Resources

- **[EVALIDator Quick Reference](./evaluator_quick_reference.md)** - Essential patterns and templates for EVALIDator translation
- **[EVALIDator Methodology Guide](./evaluator_methodology.md)** - Comprehensive guide for Oracle to DuckDB translation