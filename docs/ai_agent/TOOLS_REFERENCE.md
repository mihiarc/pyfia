# AI Agent Tools Reference

This reference documents all tools available to the PyFIA AI Agent, including their parameters, return values, and usage examples.

## Core Query Tools

### execute_fia_query

Execute SQL queries against the FIA database with safety checks and result formatting.

```python
def execute_fia_query(
    sql_query: str,
    limit: Optional[int] = 100
) -> str
```

**Parameters:**
- `sql_query`: Valid SQL SELECT query
- `limit`: Maximum rows to return (default: 100, max: 10000)

**Returns:**
- Formatted query results as a string with headers and data

**Usage Examples:**
```sql
-- Count live trees by species
SELECT SPCD, COUNT(*) as tree_count
FROM TREE
WHERE STATUSCD = 1
GROUP BY SPCD
ORDER BY tree_count DESC

-- Get forest area by ownership
SELECT OWNGRPCD, SUM(CONDPROP_UNADJ) as area
FROM COND
WHERE COND_STATUS_CD = 1
GROUP BY OWNGRPCD
```

**Safety Features:**
- Read-only queries only (no INSERT, UPDATE, DELETE)
- Automatic result limiting
- Query timeout protection
- SQL injection prevention

### count_trees_by_criteria

Optimized tool for counting trees with various filter criteria.

```python
def count_trees_by_criteria(
    evalid: Optional[int] = None,
    species_code: Optional[int] = None,
    status: Optional[str] = None,
    min_diameter: Optional[float] = None,
    max_diameter: Optional[float] = None,
    state_code: Optional[int] = None
) -> str
```

**Parameters:**
- `evalid`: Evaluation ID for statistical validity
- `species_code`: FIA species code
- `status`: Tree status ("live", "dead", "all")
- `min_diameter`: Minimum DBH in inches
- `max_diameter`: Maximum DBH in inches
- `state_code`: State FIPS code

**Returns:**
- Formatted tree count with metadata

**Example:**
```python
# Count large live oak trees in California
count_trees_by_criteria(
    species_code=802,  # White oak
    status="live",
    min_diameter=20.0,
    state_code=6
)
```

## Schema and Metadata Tools

### get_database_schema

Retrieve schema information for FIA database tables.

```python
def get_database_schema(
    table_name: Optional[str] = None,
    include_columns: bool = True,
    include_indexes: bool = False
) -> str
```

**Parameters:**
- `table_name`: Specific table name or None for all tables
- `include_columns`: Include column details
- `include_indexes`: Include index information

**Returns:**
- Formatted schema information

**Common Tables:**
| Table | Description | Key Columns |
|-------|-------------|-------------|
| PLOT | Plot locations and metadata | PLT_CN, EVALID, LAT, LON |
| TREE | Individual tree measurements | TRE_CN, PLT_CN, SPCD, DIA, HT |
| COND | Forest conditions | COND_CN, PLT_CN, FORESTCD |
| POP_EVAL | Evaluation definitions | EVALID, STATECD, INVYR |
| REF_SPECIES | Species reference | SPCD, COMMON_NAME, GENUS |

### get_evalid_info

Get detailed information about evaluation IDs.

```python
def get_evalid_info(
    evalid: Optional[int] = None,
    state_code: Optional[int] = None,
    most_recent: bool = False
) -> str
```

**Parameters:**
- `evalid`: Specific evaluation ID
- `state_code`: Filter by state
- `most_recent`: Return only the most recent evaluation

**Returns:**
- Evaluation metadata including:
  - State and year information
  - Evaluation type (VOL, GRM, CHNG)
  - Plot counts
  - Temporal boundaries

**Example Output:**
```
EVALID: 372301
State: North Carolina
Years: 2019-2023
Type: Volume (VOL)
Plots: 3,521
Area: 21.9 million acres
```

## Species and Location Tools

### find_species_codes

Find FIA species codes by common or scientific names.

```python
def find_species_codes(
    species_name: str,
    search_type: str = "both",
    limit: int = 10
) -> str
```

**Parameters:**
- `species_name`: Common or scientific name to search
- `search_type`: "common", "scientific", or "both"
- `limit`: Maximum results to return

**Returns:**
- List of matching species with codes and names

**Examples:**
```python
# Search by common name
find_species_codes("oak")
# Returns: Multiple oak species

# Search by genus
find_species_codes("Quercus", search_type="scientific")
# Returns: All oak species

# Specific species
find_species_codes("loblolly pine")
# Returns: SPCD 131 - Pinus taeda
```

**Common Species Codes:**
| Code | Common Name | Scientific Name |
|------|-------------|-----------------|
| 131 | loblolly pine | Pinus taeda |
| 202 | Douglas-fir | Pseudotsuga menziesii |
| 316 | red maple | Acer rubrum |
| 802 | white oak | Quercus alba |
| 833 | northern red oak | Quercus rubra |

### get_state_codes

Look up state FIPS codes and names.

```python
def get_state_codes(
    state_name: Optional[str] = None,
    region: Optional[str] = None
) -> str
```

**Parameters:**
- `state_name`: State name or abbreviation
- `region`: Filter by FIA region

**Returns:**
- State codes and regional information

**FIA Regions:**
| Region | States |
|--------|--------|
| Northern | ME, NH, VT, MA, RI, CT, NY, NJ, PA, DE, MD, OH, IN, IL, MI, WI, WV, MO, IA, MN, ND, SD, NE, KS |
| Southern | VA, NC, SC, GA, FL, KY, TN, AL, MS, AR, LA, OK, TX |
| Rocky Mountain | MT, ID, WY, NV, UT, CO, AZ, NM |
| Pacific Northwest | WA, OR, CA, AK, HI |

## Advanced Query Tools

### analyze_temporal_change

Analyze changes between two time periods.

```python
def analyze_temporal_change(
    evalid1: int,
    evalid2: int,
    metric: str = "volume",
    group_by: Optional[str] = None
) -> str
```

**Parameters:**
- `evalid1`: First evaluation ID (earlier)
- `evalid2`: Second evaluation ID (later)
- `metric`: What to measure ("volume", "area", "trees", "biomass")
- `group_by`: Grouping variable ("species", "owner", "forest_type")

**Returns:**
- Change analysis with statistics

### explain_domain_filters

Explain the domain filtering applied to a query.

```python
def explain_domain_filters(
    tree_type: str = "all",
    land_type: str = "forest",
    custom_filters: Optional[Dict] = None
) -> str
```

**Parameters:**
- `tree_type`: Type of trees included
- `land_type`: Type of land included
- `custom_filters`: Additional filter criteria

**Returns:**
- Human-readable explanation of filters

## Tool Usage Patterns

### Basic Query Pattern

```python
# 1. Get schema information
schema = get_database_schema("TREE")

# 2. Find species codes
species = find_species_codes("pine")

# 3. Execute query
query = """
SELECT COUNT(*) as pine_count
FROM TREE
WHERE SPCD IN (131, 121, 111)
  AND STATUSCD = 1
"""
results = execute_fia_query(query)
```

### Statistical Analysis Pattern

```python
# 1. Find appropriate EVALID
evalid_info = get_evalid_info(state_code=37, most_recent=True)

# 2. Use EVALID for valid estimates
count = count_trees_by_criteria(
    evalid=372301,
    status="live"
)
```

### Comparative Analysis Pattern

```python
# 1. Get multiple EVALIDs
eval1 = get_evalid_info(state_code=37, year=2015)
eval2 = get_evalid_info(state_code=37, year=2023)

# 2. Compare metrics
change = analyze_temporal_change(eval1, eval2, metric="volume")
```

## Best Practices

### 1. Always Use EVALIDs

For statistically valid estimates:
```python
# Good - uses EVALID
count_trees_by_criteria(evalid=372301)

# Bad - no statistical validity
execute_fia_query("SELECT COUNT(*) FROM TREE")
```

### 2. Check Schema First

Before complex queries:
```python
# Check available columns
schema = get_database_schema("TREE")

# Then build query
query = "SELECT ... FROM TREE WHERE ..."
```

### 3. Use Specific Tools

Prefer specialized tools over raw SQL:
```python
# Good - optimized tool
count_trees_by_criteria(species_code=131)

# Less optimal - raw SQL
execute_fia_query("SELECT COUNT(*) FROM TREE WHERE SPCD=131")
```

### 4. Handle Large Results

Use limits and filtering:
```python
# Add reasonable limits
execute_fia_query(query, limit=1000)

# Filter at query level
query = """
SELECT TOP 100 *
FROM TREE
WHERE DIA > 20
ORDER BY DIA DESC
"""
```

## Error Handling

Common error messages and solutions:

| Error | Cause | Solution |
|-------|-------|----------|
| "Query validation failed" | Invalid SQL syntax | Check query structure |
| "No EVALID specified" | Missing evaluation ID | Use get_evalid_info first |
| "Species not found" | Invalid species name | Try partial match or genus |
| "Result limit exceeded" | Too many rows | Add LIMIT clause or use filters |
| "Read-only access" | Write operation attempted | Use SELECT queries only |

## Performance Tips

1. **Use Indexes**: Filter on indexed columns (PLT_CN, EVALID, SPCD)
2. **Limit Early**: Apply WHERE clauses before JOINs
3. **Aggregate Smart**: Use GROUP BY to reduce result size
4. **Cache Results**: Agent caches recent queries automatically

## Integration Examples

### With Domain Knowledge

```python
# Get domain expertise
concepts = get_fia_concepts()
units = get_measurement_units()

# Use in queries
query = f"""
SELECT 
    SPCD,
    AVG(DIA) as avg_dbh_inches,
    AVG({biomass_equation}) as avg_biomass_tons
FROM TREE
WHERE STATUSCD = 1
GROUP BY SPCD
"""
```

### With Result Formatting

Results are automatically formatted using the result formatter:
- Statistical precision (SE, CI)
- Reliability ratings
- Proper units
- Export options

## Tool Development

To add new tools, see the [Developer Guide](DEVELOPER_GUIDE.md). Tools should:
- Have clear, descriptive names
- Include comprehensive docstrings
- Return formatted strings
- Handle errors gracefully
- Be stateless and independent