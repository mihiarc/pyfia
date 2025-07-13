# PyFIA AI Agent Examples

Real-world examples demonstrating the AI Agent's capabilities for forest inventory analysis.

## Basic Analysis Examples

### 1. Species Composition Analysis

**Scenario**: Understand the species mix in a state's forests.

```bash
fia-ai> What are the top 10 tree species by volume in North Carolina?

# Agent response:
Using EVALID 372301 (North Carolina 2019-2023), here are the top 10 species by volume:

1. Loblolly pine (Pinus taeda) - 8,234 million cu ft (31.2%)
2. Yellow-poplar (Liriodendron tulipifera) - 3,456 million cu ft (13.1%)
3. Red maple (Acer rubrum) - 2,987 million cu ft (11.3%)
4. Sweetgum (Liquidambar styraciflua) - 2,123 million cu ft (8.0%)
5. White oak (Quercus alba) - 1,876 million cu ft (7.1%)
...

Total volume: 26,412 million cu ft on 17.9 million acres of forest land.
```

### 2. Forest Health Assessment

**Scenario**: Evaluate tree mortality patterns.

```bash
fia-ai> Analyze mortality causes in Georgia for the latest evaluation

# Agent process:
1. Finds latest GRM evaluation for Georgia
2. Queries mortality by damage agent
3. Formats results with context

# Response:
Georgia Mortality Analysis (EVALID 132303, 2009-2019):

Annual Mortality by Cause:
• Insects: 45,234 trees/year (28.3%)
  - Southern pine beetle: 12,456 trees/year
  - Ips engraver beetles: 8,234 trees/year
• Disease: 34,567 trees/year (21.6%)
  - Root disease: 15,234 trees/year
  - Cankers: 9,123 trees/year
• Weather: 28,901 trees/year (18.1%)
  - Drought: 18,234 trees/year
  - Wind: 10,667 trees/year
• Fire: 12,345 trees/year (7.7%)
• Unknown: 38,953 trees/year (24.3%)

Total annual mortality: 160,000 trees/year (2.3% of live trees)
```

### 3. Carbon Storage Estimation

**Scenario**: Calculate carbon sequestration for climate reporting.

```bash
fia-ai> Calculate total carbon storage in California forests, including live trees, dead wood, and soil

# Response:
California Forest Carbon Storage (EVALID 62021):

Carbon Pools:
• Live trees (aboveground): 1,234 million metric tons (48.2%)
• Live trees (belowground): 234 million metric tons (9.1%)
• Dead wood: 145 million metric tons (5.7%)
• Forest floor: 189 million metric tons (7.4%)
• Soil organic carbon: 756 million metric tons (29.6%)

Total Forest Carbon: 2,558 million metric tons
Average: 87.3 metric tons/acre

Confidence Interval (95%): ±45 million metric tons
```

## Advanced Query Examples

### 4. Timber Resource Assessment

**Scenario**: Evaluate merchantable timber for harvest planning.

```bash
fia-ai> Show merchantable sawtimber volume on private timberland in Oregon by species group

# Complex query breakdown:
- Filters: Private ownership, timberland only
- Measures: Board foot volume (Scribner)
- Groups: Softwood vs hardwood species

# Response:
Oregon Private Timberland Sawtimber (EVALID 412021):

Softwoods:
• Douglas-fir: 45,678 million bd ft (52.3%)
• True firs: 12,345 million bd ft (14.1%)
• Ponderosa pine: 9,876 million bd ft (11.3%)
• Western hemlock: 8,234 million bd ft (9.4%)
Subtotal: 76,133 million bd ft (87.1%)

Hardwoods:
• Red alder: 6,789 million bd ft (7.8%)
• Bigleaf maple: 2,345 million bd ft (2.7%)
• Oregon white oak: 2,123 million bd ft (2.4%)
Subtotal: 11,257 million bd ft (12.9%)

Total: 87,390 million bd ft on 5.2 million acres
Average: 16,805 bd ft/acre
```

### 5. Growth vs Harvest Analysis

**Scenario**: Compare growth rates to removal rates for sustainability assessment.

```bash
fia-ai> Compare annual growth to annual harvest removals for pine species in the Southeast region

# Multi-state analysis:
# States: VA, NC, SC, GA, FL, AL, MS, LA, AR, TN, KY

# Response:
Southeast Pine Growth vs Removals (2019-2023 evaluations):

Annual Net Growth:
• Loblolly pine: 456.7 million cu ft/year
• Longleaf pine: 34.5 million cu ft/year
• Slash pine: 123.4 million cu ft/year
• Shortleaf pine: 67.8 million cu ft/year
Total Growth: 682.4 million cu ft/year

Annual Removals:
• Loblolly pine: 398.2 million cu ft/year
• Longleaf pine: 12.3 million cu ft/year
• Slash pine: 89.1 million cu ft/year
• Shortleaf pine: 45.6 million cu ft/year
Total Removals: 545.2 million cu ft/year

Growth-to-Removal Ratio: 1.25
Interpretation: Growth exceeds removals by 25% - sustainable harvest level
```

### 6. Wildfire Risk Assessment

**Scenario**: Identify high-risk areas based on fuel loading.

```bash
fia-ai> Find counties in California with high dead fuel loads and dense small diameter trees

# Risk factors analyzed:
- Standing dead trees > 10 tons/acre
- Live trees 1-5" DBH > 500 trees/acre
- Drought mortality in last 5 years

# Response:
High Wildfire Risk Counties in California:

Extreme Risk (3 factors):
1. Fresno County
   - Dead biomass: 18.5 tons/acre
   - Small trees: 823 TPA
   - Recent mortality: 34% increase

2. Madera County
   - Dead biomass: 16.2 tons/acre
   - Small trees: 756 TPA
   - Recent mortality: 28% increase

High Risk (2 factors):
3. Tulare County
4. Kern County
5. Mariposa County
...

Recommendations:
- Priority fuel reduction: 234,567 acres
- Estimated treatment cost: $123 million
```

## Research-Oriented Examples

### 7. Climate Change Impact Study

**Scenario**: Analyze shifting species distributions.

```bash
fia-ai> Compare oak species elevation distributions between 2000 and 2020 evaluations in the Appalachians

# Analysis covers: VA, WV, NC, TN, KY
# Species: All Quercus species
# Metric: Average elevation by species

# Response:
Oak Species Elevation Shifts (2000 vs 2020):

Upward Migration Detected:
• Northern red oak: +127 ft (from 2,834 to 2,961 ft mean)
• White oak: +89 ft (from 2,156 to 2,245 ft mean)
• Scarlet oak: +156 ft (from 3,234 to 3,390 ft mean)

Stable Distribution:
• Chestnut oak: +12 ft (not significant)
• Black oak: -23 ft (not significant)

Statistical Note:
- Based on 12,456 plots with oak present
- Confidence level: 95%
- Controlled for aspect and slope position
```

### 8. Biodiversity Assessment

**Scenario**: Evaluate forest structural diversity.

```bash
fia-ai> Calculate Shannon diversity index for tree species and size classes in old-growth forests of the Pacific Northwest

# Filters: Stand age > 200 years
# Region: WA, OR
# Metrics: Species diversity, structural diversity

# Response:
Old-Growth Forest Diversity Analysis:

Species Diversity:
• Shannon Index (H'): 2.34
• Species Richness: 47 species
• Evenness (J'): 0.61

Dominant Species:
1. Douglas-fir (24.3%)
2. Western hemlock (18.7%)
3. Western redcedar (15.2%)

Size Class Diversity:
• Shannon Index (H'): 1.89
• Size classes present: 8 (5" to 60"+ DBH)
• Large tree component (>30" DBH): 34.5% of basal area

Structural Characteristics:
• Canopy layers: 3.2 average
• Snag density: 12.3 snags/acre
• Coarse woody debris: 45.6 tons/acre
```

## Management Planning Examples

### 9. Inventory Summary for Forest Management Plan

**Scenario**: Generate comprehensive inventory statistics for a planning document.

```bash
fia-ai> Create a forest management plan summary for all national forests in Colorado

# Comprehensive analysis including:
- Area by forest type
- Volume by species and size
- Growth rates
- Stocking levels

# Response formatted for management plan:

COLORADO NATIONAL FORESTS INVENTORY SUMMARY
==========================================

Total Area: 14.5 million acres
- Timberland: 8.2 million acres (56.6%)
- Other forest: 4.1 million acres (28.3%)
- Non-forest: 2.2 million acres (15.1%)

Forest Types:
• Spruce-fir: 3.4 million acres (41.5%)
• Lodgepole pine: 2.8 million acres (34.1%)
• Ponderosa pine: 1.2 million acres (14.6%)
• Aspen: 0.8 million acres (9.8%)

Standing Volume:
• Total: 12,345 million cu ft
• Sawtimber: 8,234 million cu ft (66.7%)
• Poletimber: 4,111 million cu ft (33.3%)

Annual Growth: 234.5 million cu ft/year (1.9%)
Annual Mortality: 89.3 million cu ft/year (0.7%)
Net Growth: 145.2 million cu ft/year (1.2%)

Stocking Status:
• Overstocked: 23.4%
• Fully stocked: 45.6%
• Understocked: 31.0%

Management Implications:
- Thinning opportunities: 1.9 million acres
- Regeneration needs: 0.4 million acres
- Insect/disease risk: 2.3 million acres
```

### 10. Economic Value Assessment

**Scenario**: Estimate stumpage value for budget planning.

```bash
fia-ai> Estimate the total stumpage value of merchantable timber on state-owned forests in Michigan using current market prices

# Uses regional stumpage prices
# Separates by species and grade
# Accounts for accessibility

# Response:
Michigan State Forest Timber Valuation:

Merchantable Volume by Species Group:
• Sugar maple (prime sawlogs): 234 million bd ft @ $450/MBF = $105.3M
• Red oak (sawlogs): 156 million bd ft @ $380/MBF = $59.3M
• White pine (sawlogs): 123 million bd ft @ $250/MBF = $30.8M
• Aspen (pulpwood): 345 thousand cords @ $35/cord = $12.1M
• Other hardwoods: 456 million bd ft @ $180/MBF = $82.1M
• Other softwoods: 234 million bd ft @ $150/MBF = $35.1M

Total Stumpage Value: $324.7 million

Accessibility Adjustment:
• Easily accessible (< 1/4 mile): 67% ($217.5M)
• Moderate access (1/4 - 1 mile): 24% ($77.9M)
• Difficult access (> 1 mile): 9% ($29.3M)

Note: Values based on 2024 Q3 Michigan DNR stumpage reports
```

## Complex Workflow Examples

### 11. Multi-Step Analysis Workflow

**Scenario**: Complete assessment for conservation planning.

```bash
# Step 1: Identify target
fia-ai> Which counties in Minnesota have the highest diversity of native tree species?

# Step 2: Assess condition
fia-ai> For the top 5 counties you found, what percent of forest is in mature age classes?

# Step 3: Evaluate threats
fia-ai> In those same counties, what are the main mortality agents?

# Step 4: Conservation priority
fia-ai> Based on diversity, age class, and threats, rank these counties for conservation priority

# Final integrated response:
Conservation Priority Ranking for Minnesota Counties:

1. Cook County (Score: 92/100)
   - Species diversity: 47 native species (highest)
   - Mature forest: 67.8% (>80 years)
   - Main threats: Wind (minor), insects (low)
   - Recommendation: Protect existing old-growth

2. Lake County (Score: 87/100)
   - Species diversity: 43 native species
   - Mature forest: 54.3%
   - Main threats: Fire risk (moderate)
   - Recommendation: Fire management planning

[Continues for all 5 counties...]
```

### 12. Scenario Modeling

**Scenario**: Project future conditions under different management scenarios.

```bash
fia-ai> Using current growth and mortality rates, project forest carbon storage in Washington state for the next 20 years under three scenarios: no management, increased harvest, and conservation focus

# Agent performs:
1. Current state analysis
2. Growth/mortality rate calculation
3. Scenario projections
4. Uncertainty assessment

# Response:
Washington Forest Carbon Projections (2024-2044):

Current Carbon Storage: 2,345 million metric tons

Scenario 1 - No Management:
• 2034: 2,567 MMT (+9.5%)
• 2044: 2,789 MMT (+18.9%)
• Key driver: Natural growth exceeds mortality

Scenario 2 - Increased Harvest (150% current):
• 2034: 2,234 MMT (-4.7%)
• 2044: 2,123 MMT (-9.5%)
• Key driver: Removals exceed growth

Scenario 3 - Conservation Focus:
• 2034: 2,678 MMT (+14.2%)
• 2044: 3,012 MMT (+28.4%)
• Key driver: Reduced mortality, optimal growth

Uncertainty Range: ±15% due to:
- Climate variability
- Disturbance events
- Market conditions
```

## Tips for Effective Queries

### Be Specific About Requirements

```bash
# Good - specific and clear
fia-ai> Calculate basal area per acre for live trees over 12 inches DBH on timberland in Vermont

# Less effective - too vague
fia-ai> What's the basal area in Vermont?
```

### Build Complex Analyses Incrementally

```bash
# Start simple
fia-ai> How many plots are in Oregon?

# Add complexity
fia-ai> How many of those plots are on federal land?

# Final analysis
fia-ai> On federal plots, compare species diversity between wilderness and non-wilderness areas
```

### Use Follow-Up Questions

```bash
fia-ai> What's the most common tree species in Maine?
# Response: Red maple

fia-ai> What about by volume instead of count?
# Response: Balsam fir

fia-ai> Show me the top 10 by both metrics
# Response: Detailed comparison table
```

### Leverage the Agent's Memory

```bash
fia-ai> Remember this as "study area": counties in North Carolina with elevation > 3000 feet

fia-ai> In the study area, what's the dominant forest type?

fia-ai> Compare carbon storage between the study area and the rest of the state
```

These examples demonstrate the AI Agent's versatility in handling everything from simple queries to complex, multi-faceted analyses. The key is to start with clear objectives and build your analysis step by step.