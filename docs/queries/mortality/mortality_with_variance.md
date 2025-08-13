# Mortality of Merchantable Volume with Variance by Evaluation Group

This query implements a stratified variance estimator at the estimation-unit level and aggregates to evaluation-group totals, following FIA EVALIDator methodology.

- **Attribute**: 574157 - Average annual mortality of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land
- **Tables**: `POP_EVAL_GRP`, `POP_EVAL_TYP`, `POP_EVAL`, `POP_ESTN_UNIT`, `POP_STRATUM`, `POP_PLOT_STRATUM_ASSGN`, `PLOT`, `PLOTGEOM`, `COND`, `TREE`, `TREE_GRM_BEGIN`, `TREE_GRM_MIDPT`, `TREE_GRM_COMPONENT`, `REF_SPECIES`
- **Compatibility**: DuckDB (as loaded by pyfia)

## Usage Notes

- Filter to GRM mortality evaluations with `PET.eval_typ = 'EXPMORT'`.
- Optionally filter to specific evaluation groups using `PEG.eval_grp IN (...)`.
- Uses growing-stock forest components and multiplies by `TRE_MIDPT.VOLCFSND` to obtain merchantable bole volume in cubic feet.

## SQL

```sql
-- See file for full query
-- Download link below provides the complete SQL.
```

## Download

<a href="mortality_with_variance.sql" download class="md-button md-button--primary">
  :material-download: Download SQL File
</a>
