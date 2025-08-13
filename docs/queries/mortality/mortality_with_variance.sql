-- Mortality of Merchantable Volume with Stratified Variance by Evaluation Group
-- Attribute 574157: Average annual mortality of sound bole wood volume of trees
-- (timber species at least 5 inches d.b.h.), in cubic feet, on forest land
--
-- Notes for pyfia/DuckDB usage:
-- - This query is adapted from FIA handbook-style variance estimation using
--   stratified estimators at the estimation-unit level, then aggregated to
--   evaluation-group totals with SE and variance.
-- - Remove schema prefixes (uses raw FIADB table names as in pyfia DuckDB loads).
-- - Set the filter on evaluation group(s) as needed. Example below keeps all groups;
--   uncomment and set PEG.eval_grp IN (...) to target specific groups.

SELECT
  eval_grp,
  eval_grp_descr,
  574157 AS attribute_nbr,
  'Average annual mortality of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land' AS attribute_descr,
  SUM(estimate_by_estn_unit.estimate) AS estimate,
  CASE WHEN SUM(estimate_by_estn_unit.estimate) <> 0
       THEN ABS(SQRT(SUM(estimate_by_estn_unit.var_of_estimate)) / SUM(estimate_by_estn_unit.estimate) * 100)
       ELSE 0 END AS se_of_estimate_pct,
  SQRT(SUM(estimate_by_estn_unit.var_of_estimate)) AS se_of_estimate,
  SUM(estimate_by_estn_unit.var_of_estimate) AS var_of_estimate,
  SUM(estimate_by_estn_unit.total_plots) AS total_plots,
  SUM(estimate_by_estn_unit.non_zero_plots) AS non_zero_plots,
  SUM(estimate_by_estn_unit.tot_pop_area_acres) AS tot_pop_area_acres
FROM (
  SELECT
    phase_1_summary.pop_eval_grp_cn,
    phase_1_summary.eval_grp,
    phase_1_summary.eval_grp_descr,
    SUM(COALESCE(ysum_hd, 0) * phase_1_summary.expns) AS estimate,
    phase_1_summary.n AS total_plots,
    SUM(phase_summary.number_plots_in_domain) AS domain_plots,
    SUM(phase_summary.non_zero_plots) AS non_zero_plots,
    total_area * total_area / phase_1_summary.n * (
      (
        SUM(
          w_h * phase_1_summary.n_h * (
            (
              COALESCE(ysum_hd_sqr, 0) / phase_1_summary.n_h
              - (
                (COALESCE(ysum_hd, 0) / phase_1_summary.n_h)
                * (COALESCE(ysum_hd, 0) / phase_1_summary.n_h)
              )
            ) / NULLIF(phase_1_summary.n_h - 1, 0)
          )
        )
      )
      + (1.0 / phase_1_summary.n) * (
        SUM(
          (1 - w_h) * phase_1_summary.n_h * (
            (
              COALESCE(ysum_hd_sqr, 0) / phase_1_summary.n_h
              - (
                (COALESCE(ysum_hd, 0) / phase_1_summary.n_h)
                * (COALESCE(ysum_hd, 0) / phase_1_summary.n_h)
              )
            ) / NULLIF(phase_1_summary.n_h - 1, 0)
          )
        )
      )
    ) AS var_of_estimate,
    total_area AS tot_pop_area_acres
  FROM (
    SELECT
      PEV.cn AS eval_cn,
      PEG.eval_grp,
      PEG.eval_grp_descr,
      PEG.cn AS pop_eval_grp_cn,
      POP_STRATUM.ESTN_UNIT_CN,
      POP_STRATUM.expns,
      POP_STRATUM.cn AS POP_STRATUM_cn,
      p1pointcnt / (
        SELECT SUM(str.p1pointcnt)
        FROM POP_STRATUM str
        WHERE str.ESTN_UNIT_CN = POP_STRATUM.ESTN_UNIT_CN
      ) AS w_h,
      (
        SELECT SUM(eu_s.area_used)
        FROM POP_ESTN_UNIT eu_s
        WHERE eu_s.cn = POP_STRATUM.ESTN_UNIT_CN
      ) AS total_area,
      (
        SELECT SUM(str.p2pointcnt)
        FROM POP_STRATUM str
        WHERE str.ESTN_UNIT_CN = POP_STRATUM.ESTN_UNIT_CN
      ) AS n,
      POP_STRATUM.p2pointcnt AS n_h
    FROM POP_EVAL_GRP PEG
    JOIN POP_EVAL_TYP PET ON PET.EVAL_GRP_CN = PEG.CN
    JOIN POP_EVAL PEV ON PEV.CN = PET.EVAL_CN
    JOIN POP_ESTN_UNIT PEU ON PEV.CN = PEU.EVAL_CN
    JOIN POP_STRATUM POP_STRATUM ON PEU.CN = POP_STRATUM.ESTN_UNIT_CN
    WHERE PET.eval_typ = 'EXPMORT'
      -- AND PEG.eval_grp IN (/* set eval grp(s) here, e.g. 132303 */)
  ) phase_1_summary
  LEFT JOIN (
    SELECT
      POP_STRATUM_cn,
      ESTN_UNIT_CN,
      eval_cn,
      SUM(y_hid_adjusted) AS ysum_hd,
      SUM(y_hid_adjusted * y_hid_adjusted) AS ysum_hd_sqr,
      COUNT(*) AS number_plots_in_domain,
      SUM(CASE WHEN y_hid_adjusted IS NULL THEN 0 WHEN y_hid_adjusted = 0 THEN 0 ELSE 1 END) AS non_zero_plots
    FROM (
      SELECT
        574157 AS Attribute_nbr,
        'Average annual mortality of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land' AS Attribute_descr,
        SUM(
          GRM.TPAMORT_UNADJ
          * CASE
              WHEN COALESCE(GRM.SUBPTYP_GRM, 0) = 0 THEN 0
              WHEN GRM.SUBPTYP_GRM = 1 THEN POP_STRATUM.ADJ_FACTOR_SUBP
              WHEN GRM.SUBPTYP_GRM = 2 THEN POP_STRATUM.ADJ_FACTOR_MICR
              WHEN GRM.SUBPTYP_GRM = 3 THEN POP_STRATUM.ADJ_FACTOR_MACR
              ELSE 0
            END
          * CASE WHEN GRM.COMPONENT LIKE 'MORTALITY%' THEN TRE_MIDPT.VOLCFSND ELSE 0 END
        ) AS y_hid_adjusted,
        peu.cn AS ESTN_UNIT_CN,
        pev.cn AS eval_cn,
        POP_STRATUM.cn AS POP_STRATUM_cn,
        plot.cn AS plt_cn
      FROM POP_EVAL_GRP PEG
      JOIN POP_EVAL_TYP PET ON PET.EVAL_GRP_CN = PEG.CN
      JOIN POP_EVAL PEV ON PEV.CN = PET.EVAL_CN
      JOIN POP_ESTN_UNIT PEU ON PEV.CN = PEU.EVAL_CN
      JOIN POP_STRATUM POP_STRATUM ON PEU.CN = POP_STRATUM.ESTN_UNIT_CN
      JOIN POP_PLOT_STRATUM_ASSGN POP_PLOT_STRATUM_ASSGN ON POP_STRATUM.CN = POP_PLOT_STRATUM_ASSGN.STRATUM_CN
      JOIN PLOT PLOT ON POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN
      JOIN PLOTGEOM ON PLOT.CN = PLOTGEOM.CN
      JOIN COND COND ON PLOT.CN = COND.PLT_CN
      JOIN (
        SELECT P.PREV_PLT_CN, T.*
        FROM PLOT P
        JOIN TREE T ON P.CN = T.PLT_CN
      ) TREE ON (TREE.CONDID = COND.CONDID AND TREE.PLT_CN = COND.PLT_CN)
      LEFT JOIN PLOT PPLOT ON PLOT.PREV_PLT_CN = PPLOT.CN
      LEFT JOIN COND PCOND ON (TREE.PREVCOND = PCOND.CONDID AND TREE.PREV_PLT_CN = PCOND.PLT_CN)
      LEFT JOIN TREE PTREE ON TREE.PREV_TRE_CN = PTREE.CN
      LEFT JOIN TREE_GRM_BEGIN TRE_BEGIN ON TREE.CN = TRE_BEGIN.TRE_CN
      LEFT JOIN TREE_GRM_MIDPT TRE_MIDPT ON TREE.CN = TRE_MIDPT.TRE_CN
      LEFT JOIN (
        SELECT
          TRE_CN,
          DIA_BEGIN,
          DIA_MIDPT,
          DIA_END,
          SUBP_COMPONENT_GS_FOREST AS COMPONENT,
          SUBP_SUBPTYP_GRM_GS_FOREST AS SUBPTYP_GRM,
          SUBP_TPAMORT_UNADJ_GS_FOREST AS TPAMORT_UNADJ
        FROM TREE_GRM_COMPONENT
      ) GRM ON TREE.CN = GRM.TRE_CN
      LEFT JOIN REF_SPECIES ON TREE.SPCD = REF_SPECIES.SPCD
      WHERE REF_SPECIES.WOODLAND = 'N'
        AND PET.EVAL_TYP = 'EXPMORT'
        -- AND PEG.EVAL_GRP IN (/* set eval grp(s) here, e.g. 132303 */)
      GROUP BY
        peu.cn,
        pev.cn,
        POP_STRATUM.cn,
        plot.cn
    ) plot_summary
    GROUP BY POP_STRATUM_cn, ESTN_UNIT_CN, eval_cn
  ) phase_summary
  ON (
    phase_1_summary.POP_STRATUM_cn = phase_summary.POP_STRATUM_cn
    AND phase_1_summary.eval_cn = phase_summary.eval_cn
    AND phase_1_summary.ESTN_UNIT_CN = phase_summary.ESTN_UNIT_CN
  )
  GROUP BY
    phase_1_summary.pop_eval_grp_cn,
    phase_1_summary.eval_grp,
    phase_1_summary.eval_grp_descr,
    phase_1_summary.ESTN_UNIT_CN,
    phase_1_summary.total_area,
    phase_1_summary.n
) estimate_by_estn_unit
WHERE non_zero_plots IS NOT NULL
GROUP BY pop_eval_grp_cn, eval_grp, eval_grp_descr
ORDER BY eval_grp, eval_grp_descr;


