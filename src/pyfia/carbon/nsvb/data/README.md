# NSVB Coefficient Tables (vendored)

This directory contains coefficient and carbon-fraction tables from the National Scale
Volume and Biomass (NSVB) framework documented in:

> Westfall, J.A. et al. (2023). *A National-Scale Tree Volume, Biomass, and Carbon
> Modeling System for the United States.* Gen. Tech. Rep. WO-104. Washington, DC:
> U.S. Department of Agriculture, Forest Service, Washington Office.
> DOI: 10.2737/WO-GTR-104
> Supplementary archive: 10.2737/WO-GTR-104-Supp1

These CSVs are loaded by `pyfia.carbon.nsvb.coefficients` and
`pyfia.carbon.nsvb.carbon_fractions` via `importlib.resources` and ship inside the wheel.

## File map

| File | Source table (Supp1) | Rows | Purpose |
|---|---|---|---|
| `volib_spcd.csv` | S1a | 406 | Stem inside-bark wood volume coefficients by SPCD/DIVISION/STDORGCD |
| `volib_jenkins.csv` | S1b | 9 | Same, Jenkins-group fallback |
| `volbk_spcd.csv` | S2a | 339 | Stem bark volume coefficients |
| `volbk_jenkins.csv` | S2b | 9 | Jenkins fallback |
| `bark_biomass_spcd.csv` | S6a | 206 | Stem bark dry biomass |
| `bark_biomass_jenkins.csv` | S6b | 9 | Jenkins fallback |
| `branch_biomass_spcd.csv` | S7a | 175 | Branch dry biomass |
| `branch_biomass_jenkins.csv` | S7b | 9 | Jenkins fallback |
| `total_biomass_spcd.csv` | S8a | 173 | Total above-ground biomass (used in harmonization) |
| `total_biomass_jenkins.csv` | S8b | 9 | Jenkins fallback |
| `carbon_fraction_live.csv` | S10a (trimmed) | 2676 | Live tree carbon fractions by SPCD |
| `carbon_fraction_dead.csv` | S10b | 10 | Dead tree carbon fractions by hw/sw × DECAYCD |
| `dead_decay_proportions.csv` | REF_TREE_DECAY_PROP / Table 1 | 10 | Standing dead density/bark/branch loss proportions by hw/sw × DECAYCD |

## Coefficient table schema (S1a–S8b)

Columns: `SPCD, DIVISION, STDORGCD, model, a, a1, b, b1, c, c1`

- `SPCD` — FIA species code
- `DIVISION` — Bailey ecoprovince division code (e.g., `M240`, `210`). May be empty for the
  species-level fallback row.
- `STDORGCD` — Stand origin code. Usually empty.
- `model` — Which NSVB model form to use (1, 2, 4, or 5). See `equations.py`.
- `a, a1, b, b1, c, c1` — Model parameters. Which are populated depends on `model`.

**Lookup precedence** (per NSVB worked example, `gtr_wo104_westfall2023.md:684`):
1. SPCD + DIVISION + STDORGCD exact match
2. SPCD + DIVISION (STDORGCD null)
3. SPCD only (DIVISION and STDORGCD both null) — the species-level fallback
4. JENKINS_SPGRPCD fallback (Model 5 with WDSG multiplication)

The CSVs already include species-level fallback rows; the loader does not need to
synthesize them.

## Carbon fraction tables

### `carbon_fraction_live.csv` (trimmed from S10a)

Columns: `SPCD, hw_sw, fia_wood_c`

- `SPCD` — FIA species code
- `hw_sw` — `"hardwood"` or `"softwood"`. Renamed from S10a's `division` column
  (which uses `angiosperm`/`gymnosperm` — botanically equivalent but semantically
  collides with the ecological `DIVISION` column in S1a–S8b). The values were also
  remapped: `angiosperm` → `hardwood`, `gymnosperm` → `softwood`.
- `fia_wood_c` — Carbon as percent (e.g., `48.04`). The loader divides by 100 at
  load time to produce a fraction in `[0.40, 0.55]`.

### `carbon_fraction_dead.csv` (S10b)

Columns: `Decay code, S/H, C fraction`

10 rows: hardwood/softwood × decay class 1–5. Loaded by `carbon_fractions.py` and
used by the Phase 2 standing dead estimator for the biomass → carbon conversion.

### `dead_decay_proportions.csv` (REF_TREE_DECAY_PROP / GTR-WO-104 Table 1)

Columns: `hw_sw, DECAYCD, DENSITY_PROP, BARK_LOSS_PROP, BRANCH_LOSS_PROP`

10 rows: hardwood/softwood × decay class 1–5. Mirrors the FIADB
`REF_TREE_DECAY_PROP` table and the consolidated NSVB hardwood/softwood × DECAYCD
values from GTR-WO-104 Table 1 (Westfall et al. 2023). Loaded by `carbon_fractions.py`
and used by `equations.compute_nsvb_dead_biomass` to apply decay reductions to gross
NSVB component biomass (stem wood × DENSITY_PROP, bark × BARK_LOSS_PROP, branch ×
BRANCH_LOSS_PROP). Despite the `LOSS` suffix in the FIADB column names, the values
are the *remaining* proportions (not the *lost* proportions).

## Vendoring procedure (for maintainers)

When WO-104 is revised, re-vendor as follows:

1. Fetch the supplementary archive from <https://doi.org/10.2737/WO-GTR-104-Supp1>.
2. Copy `Table S{1a,1b,2a,2b,6a,6b,7a,7b,8a,8b,10a,10b}*.csv` from the archive into
   this directory, renaming each to its short form (see file map above).
3. Trim `carbon_fraction_live.csv` to keep only `SPCD, hw_sw, fia_wood_c`, and
   remap `angiosperm`/`gymnosperm` → `hardwood`/`softwood`. Reference one-liner:

   ```python
   import csv
   mapping = {'angiosperm': 'hardwood', 'gymnosperm': 'softwood'}
   with open('Table S10a_fia_wood_c_frac_live.csv.csv') as fin, \
        open('carbon_fraction_live.csv', 'w', newline='') as fout:
       reader = csv.DictReader(fin)
       writer = csv.DictWriter(fout, fieldnames=['SPCD', 'hw_sw', 'fia_wood_c'])
       writer.writeheader()
       for row in reader:
           writer.writerow({
               'SPCD': row['SPCD'],
               'hw_sw': mapping.get(row['division'].strip(), row['division'].strip()),
               'fia_wood_c': row['fia.wood.c'],
           })
   ```

4. Run `uv run pytest tests/unit/test_nsvb_*.py tests/unit/test_carbon_fractions.py`
   to verify no schema regressions.
5. Bump the patch version in `pyproject.toml` and commit with a message referencing
   the Supp1 revision date.

## Known issues in source data

- **SPCD 10 (`fir spp.`)** is labeled `angiosperm` in S10a but should be `gymnosperm`
  (Abies is a softwood genus). This is a data error in the source CSV; we preserve
  it as-is rather than silently fixing it. SPCD 11 (Pacific silver fir, Abies amabilis)
  is correctly labeled `gymnosperm`.
- The CSV coefficients are stored at ~9 significant figures. The WO-104 worked
  examples cite coefficients to 12+ significant figures. As a result, end-to-end
  pipeline tests using CSV-loaded coefficients are accurate to ~6 decimal places,
  not the 9+ decimal places shown in the prose. The `tests/unit/test_nsvb_equations.py`
  regression tests use hand-coded high-precision coefficients from the worked
  example prose, separate from the CSV path, so the equation math itself is verified
  to 9 decimal places.
