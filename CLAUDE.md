# CLAUDE.md

**pyFIA** is a high-performance Python library for analyzing USDA Forest Inventory and Analysis (FIA) data. This repo is public, and it's published to PyPI.

## Statistical rigor

- Design-based estimation following **Bechtold & Patterson (2005)**.
- Results must match **EVALIDator** (the official USFS tool).
- Always include uncertainty estimates (SE, confidence intervals). Never compromise accuracy for convenience.

## Shape of the code

This library favors direct module-level functions over class hierarchies — `volume(db)`, not a factory. Match that shape, keep the tree shallow, and choose fast implementations over elegant abstractions.

## Project policies

- **Breaking changes are acceptable, but only in a MINOR or MAJOR release, never a patch.** Don't carry deprecated APIs forward.
- **FIA table and column definitions are centralized constants** in `src/pyfia/constants/` (`tables.py`, `columns.py`) — reference them rather than hard-coding FIA name strings.
- **`mortality()` is the documentation gold standard**: match its docstring quality. See `src/pyfia/estimation/estimators/mortality.py`.
- **This repo is public.** Never name private repositories, clients, or internal datasets in code, docs, commit messages, issues or PR comments. Cite public artifacts (merged PRs, releases) instead. When work happens elsewhere before release, say "2.0 development happens in a private staging repository and publishes here at release".

Tooling:
- Set up with `uv sync --extra dev`. The test and lint tools live in the `dev` extra, so plain `uv sync` has no pytest. `uv.lock` is committed.
- Run `uv run pre-commit install` once per clone. The hooks then run ruff-format, ruff `--fix` and mypy (`uv run mypy src/pyfia/`) on every commit.
- CI (`.github/workflows/tests.yml`) runs `pytest tests/unit`, mypy and `ruff check` on Python 3.11–3.14 for every pull request, including stacked ones. It doesn't check formatting, so the pre-commit hook is the only format gate.
- DB-backed tests use a committed three-county Alabama FIADB subset (`tests/fixtures/fiadb_al/`, fixtures `fiadb_fixture_path` and `fiadb_fixture` in `tests/conftest.py`), so they run in CI. `tests/validation/` needs full state DBs (`PYFIA_DATABASE_PATH`) and the EVALIDator API.

The `Makefile` targets:

- `make test`: the default `-m "not slow and not network"`.
- `make validate`: the EVALIDator comparisons under `tests/validation/`.
- `make lint`, `make format`, `make typecheck`.

## FIA data facts worth keeping resident

- **Column types come from `pyfia.constants.fiadb_schema`, generated from DataMart's SQLite export** by `scripts/gen_fiadb_schema.py` (#178). Regenerate it when DataMart publishes a new FIADB release; `download()` warns when a database's release differs from the schema's.
  - CNs are text (some have 23 digits). DataMart writes some integer columns with a decimal point (`TREE.SPCD` as `693.0`), and DuckDB's CSV reader would round `693.5` into an integer column, so integers are cast only after a whole-number check.
  - Some DataMart CSVs repeat their header mid-file (`RI_SOILS_LAB.csv`, 4 times); the importer drops those rows.
  - Databases built before 1.6.0 guessed types per file, so the same column can be text in one state and a number in another. The builders cast what they read (`pyfia.core.fiadb_types`); estimators read the stored types.

- **FIADB populates `CARBON_AG`/`CARBON_BG` for dead trees only when `STANDING_DEAD_CD = 1`.**
  - Dead trees with `STANDING_DEAD_CD = 0` (fallen since the last measurement) carry zero or null tree carbon. So do those with `NULL` (older inventories).
  - That fallen material is counted in `COND.CARBON_DOWN_DEAD` instead.
  - So a dead-tree carbon path that filters only `STATUSCD == 2` doesn't double-count downed wood in current data. Keep the explicit `STANDING_DEAD_CD` filter anyway, as a defense (`src/pyfia/carbon/standing_dead.py`).
- **EVALIDator groups growth, removals and mortality by the condition of the tree's time-2 record** (`TREE.CONDID`), not by its previous condition. `attach_tree_conditions()` in `src/pyfia/estimation/grm.py` does the same (#138).
- **EVALIDator's per-acre value is the attribute total divided by the full domain area** (all in-domain conditions, with adjustment factors), not by the area of conditions that hold a qualifying tree. pyFIA's shared two-stage aggregation doesn't do this yet (#146).
- **EVALIDator returns the SQL behind every estimate**, in `metadata.sql` of the fullreport response. Diff against it before calling a gap a methodology difference.
  - Per-period estimates reproduce exactly from a local database of the same FIADB release. Annual ones (÷ `REMPER`) can drift about 0.05% when EVALIDator serves a newer release, so validate exactness on the per-period snums.
- **Area change counts one `SUBP_COND_CHNG_MTRX` row per subplot**: `SUBPTYP` 1 when `COND.PROP_BASIS` is `SUBP`, 3 when it is `MACR`, with the matching adjustment factor, and `COND_NONSAMPLE_REASN_CD` 0 at both times (#151).
  - FIADB's COND has no `PREVCOND` (1.9.4 and 1.9.5), so the change matrix is the only reliable time-1 ↔ time-2 condition link. Plots outside it (periodic inventories) fall back to the same CONDID (`condition_intervals`).
- **All-live GRM estimates (trees at least 1 inch) use the `MICR_*_AL_*` columns** of `TREE_GRM_COMPONENT`, whose `SUBPTYP_GRM` 2 rows are microplot saplings. `SUBP_*_AL_*` is EVALIDator's at-least-5-inch population; growing stock and sawtimber use `SUBP_*` (#167).
- **A domain's standard error needs every plot in the evaluation**, zero-filled where the plot has nothing in the domain, with the exact Bechtold & Patterson formula (`s²_h`, not `s²_h / n_h`). #147, #149 and #159 each broke this.

## Regression guardrails

Estimator output isn't byte-stable from run to run.
- `*_SE` and `*_VARIANCE` columns jitter at the ULP level, because polars sums floats in parallel.
- Point estimates are stable to 8+ significant figures.
- So never hash an output frame to prove a refactor is behavior-preserving.
- Canonicalize both sides first: sort rows by all columns, drop `*_SE`/`*_VARIANCE`, round the remaining floats to 8 significant figures, then diff. Counts (`N_PLOTS`, `N_TREES`) and point estimates should then match exactly.

## Docs (Mintlify)

- The public docs are Mintlify: `docs/docs.json` plus MDX pages under `docs/`. The site is pyfia.mintlify.app, deployed on push by the Mintlify GitHub App.
- The API reference in `docs/api/*.mdx` is generated from the NumPy docstrings by `./scripts/gen_api_docs.sh` (mdxify). Mintlify serves the committed MDX, so re-run the script and commit the output whenever the public API or its docstrings change.
- If you add or remove an API page, update the "API Reference" tab in `docs/docs.json`.
- The NSVB carbon module (`pyfia.carbon`) has shipped in the wheel since 1.4.3, but it stays out of the public docs until a later release, as `scripts/gen_api_docs.sh` states. The script already strips carbon-named sections that leak in through class pages (`FIA.carbon_flux`, `EVALIDatorClient.get_carbon`). Don't hand-add carbon pages.
