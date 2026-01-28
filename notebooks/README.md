# pyFIA Tutorial Notebooks

Interactive Jupyter notebooks for learning pyFIA, a Python library for analyzing USDA Forest Inventory and Analysis (FIA) data.

## Open in Google Colab

Run these notebooks directly in your browser - no installation required!

| Notebook | Description | Colab Link |
|----------|-------------|------------|
| 01 - Getting Started | Introduction to FIA data and pyFIA basics | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mihiarc/pyfia/blob/main/notebooks/01_getting_started.ipynb) |
| 02 - Core Estimators | Area, volume, biomass, and TPA estimation | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mihiarc/pyfia/blob/main/notebooks/02_core_estimators.ipynb) |
| 03 - Filtering & Grouping | Domain expressions for custom analyses | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mihiarc/pyfia/blob/main/notebooks/03_filtering_grouping.ipynb) |
| 04 - Change Analysis | Growth, mortality, and removals (GRM) | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mihiarc/pyfia/blob/main/notebooks/04_change_analysis.ipynb) |
| 05 - Validation & Statistics | Statistical methodology and EVALIDator | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/mihiarc/pyfia/blob/main/notebooks/05_validation_statistics.ipynb) |

> **Note**: When running in Colab, the first cell will automatically install pyFIA and download sample data.

## Getting Started

### Prerequisites

1. Install pyFIA with development dependencies:
   ```bash
   uv pip install -e .[dev]
   ```

2. Install Jupyter:
   ```bash
   uv pip install jupyter
   ```

3. Launch Jupyter:
   ```bash
   uv run jupyter notebook
   ```

### Sample Data

These notebooks use **Rhode Island** as the primary dataset because:
- It's the smallest U.S. state, making downloads fast (~1-2 minutes)
- Contains representative FIA data structures
- Small enough to run examples quickly

Data is downloaded automatically when you run the first notebook.

## Notebooks

### Learning Path

```
01_getting_started.ipynb
        │
        ▼
02_core_estimators.ipynb ──► 03_filtering_grouping.ipynb
        │                              │
        ▼                              ▼
04_change_analysis.ipynb ◄─────────────┘
        │
        ▼
05_validation_statistics.ipynb
```

### Quick Start Path
New users should follow: **Notebook 1 → Notebook 2**

### Full Curriculum
For comprehensive learning: **Notebook 1 → 2 → 3 → 4 → 5**

---

## Notebook Descriptions

### 1. Getting Started with pyFIA (`01_getting_started.ipynb`)

**Duration:** ~30 minutes

Entry point for new users. Learn the basics of FIA data and pyFIA.

**Topics:**
- What is FIA data and why it matters
- Installing and setting up pyFIA
- Downloading data from FIA DataMart
- Connecting to databases with `FIA()`
- Understanding EVALIDs (evaluation identifiers)
- Your first estimate: forest area
- Interpreting results and standard errors

---

### 2. Core Estimators (`02_core_estimators.ipynb`)

**Duration:** ~45 minutes

Master the main estimation functions for forest inventory analysis.

**Topics:**
- `area()` - Forest and timberland area
- `volume()` - Net, gross, and sawlog volume
- `biomass()` - Aboveground, belowground, and carbon
- `tpa()` - Trees per acre and basal area
- Grouping results with `grp_by`
- Adding reference names with `join_species_names()`
- Enabling variance estimates

---

### 3. Domain Filtering and Grouping (`03_filtering_grouping.ipynb`)

**Duration:** ~40 minutes

Learn to filter data for custom analyses.

**Topics:**
- Land type shortcuts (`forest`, `timber`, `all`)
- Tree type shortcuts (`live`, `dead`, `gs`)
- `tree_domain` expressions (species, diameter)
- `area_domain` expressions (ownership, forest type)
- `plot_domain` expressions (county, geography)
- Combining multiple filters
- Real-world analysis patterns

---

### 4. Change Analysis (`04_change_analysis.ipynb`)

**Duration:** ~45 minutes

Analyze forest change using GRM (Growth-Removal-Mortality) methodology.

**Topics:**
- Introduction to GRM estimation
- `mortality()` - Annual mortality rates
- `growth()` - Net growth and accretion
- `removals()` - Harvest analysis
- Net change calculations
- Measure options (volume, biomass, TPA)
- Forest sustainability assessment

---

### 5. Validation and Statistics (`05_validation_statistics.ipynb`)

**Duration:** ~40 minutes

Understand the statistical methodology and validate results.

**Topics:**
- Statistical foundation (Bechtold & Patterson 2005)
- Variance and confidence intervals
- EVALIDator (official USFS tool)
- Running validation comparisons
- Interpreting differences
- Best practices and common pitfalls

---

## Exercises

Each notebook includes 1-2 exercises with solutions in collapsed cells. Try solving them yourself before revealing the answers!

## Shared Utilities

The `helpers.py` module provides:

- `ensure_ri_data()` - Download Rhode Island data if needed
- `display_estimate()` - Pretty-print results with Rich
- `plot_by_category()` - Create bar charts for categorical data
- `plot_time_series()` - Create time series plots

## Troubleshooting

### "No module named pyfia"
Ensure pyFIA is installed in development mode:
```bash
uv pip install -e .
```

### Download fails
Check your internet connection. FIA DataMart may occasionally be slow or unavailable.

### Jupyter kernel not found
Create a kernel for your virtual environment:
```bash
uv run python -m ipykernel install --user --name=pyfia
```
Then select "pyfia" as your kernel in Jupyter.

## Additional Resources

- [pyFIA Documentation](../docs/)
- [FIA DataMart](https://apps.fs.usda.gov/fia/datamart/datamart.html)
- [EVALIDator](https://apps.fs.usda.gov/fiadb-api/evalidator)
- [Bechtold & Patterson (2005)](https://www.fs.usda.gov/research/treesearch/20121) - Statistical methodology reference
