select eval_grp,
       eval_grp_descr,
       owngrpcd,
       574157 attribute_nbr,
       'Average annual mortality of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land' attribute_descr,
       sum(estimate_by_estn_unit.estimate) estimate,
       CAST(CASE
         when sum(estimate_by_estn_unit.estimate) <> 0 then
          abs(sqrt(sum(estimate_by_estn_unit.var_of_estimate)) /
              sum(estimate_by_estn_unit.estimate) * 100)
         else
          0
       END AS REAL) as se_of_estimate_pct,
       sqrt(sum(estimate_by_estn_unit.var_of_estimate)) se_of_estimate,
       sum(estimate_by_estn_unit.var_of_estimate) var_of_estimate,
       sum(estimate_by_estn_unit.total_plots) total_plots,
       sum(estimate_by_estn_unit.non_zero_plots) non_zero_plots,
       sum(estimate_by_estn_unit.tot_pop_area_acres) tot_pop_ac
  from (select pop_eval_grp_cn,
               eval_grp,
               eval_grp_descr,
               owngrpcd,
               SUM(IFNULL(ysum_hd, 0) * phase_1_summary.expns) estimate,
               phase_1_summary.n total_plots,
               SUM(phase_summary.number_plots_in_domain) domain_plots,
               SUM(phase_summary.non_zero_plots) non_zero_plots,
               CAST(total_area * total_area / phase_1_summary.n *
               ((SUM(w_h * phase_1_summary.n_h *
                     (((IFNULL(ysum_hd_sqr, 0) / phase_1_summary.n_h) -
                     ((IFNULL(ysum_hd, 0) / phase_1_summary.n_h) *
                     (IFNULL(ysum_hd, 0) / phase_1_summary.n_h))) /
                     (phase_1_summary.n_h - 1)))) +
               1.0 / phase_1_summary.n *
               (SUM((1 - w_h) * phase_1_summary.n_h *
                     (((IFNULL(ysum_hd_sqr, 0) / phase_1_summary.n_h) -
                     ((IFNULL(ysum_hd, 0) / phase_1_summary.n_h) *
                     (IFNULL(ysum_hd, 0) / phase_1_summary.n_h))) /
                     (phase_1_summary.n_h - 1))))) AS REAL) var_of_estimate,
               total_area tot_pop_area_acres
          from (select PEV.cn eval_cn,
                       PEG.eval_grp,
                       PEG.eval_grp_descr,
                       PEG.cn pop_eval_grp_cn,
                       POP_STRATUM.ESTN_UNIT_CN,
                       CAST(POP_STRATUM.expns AS REAL) expns,
                       POP_STRATUM.cn POP_STRATUM_cn,
                       CAST(p1pointcnt AS REAL) /
                       CAST((select sum(str.p1pointcnt)
                          from POP_STRATUM str
                         where str.ESTN_UNIT_CN = POP_STRATUM.ESTN_UNIT_CN) AS REAL) w_h,
                       (select sum(str.p1pointcnt)
                          from POP_STRATUM str
                         where str.ESTN_UNIT_CN = POP_STRATUM.ESTN_UNIT_CN) n_prime,
                       p1pointcnt n_prime_h,
                       CAST((select sum(eu_s.area_used)
                          from POP_ESTN_UNIT eu_s
                         where eu_s.cn = POP_STRATUM.ESTN_UNIT_CN) AS REAL) total_area,
                       (select sum(str.p2pointcnt)
                          from POP_STRATUM str
                         where str.ESTN_UNIT_CN = POP_STRATUM.ESTN_UNIT_CN) n,
                       POP_STRATUM.p2pointcnt n_h
                  FROM POP_EVAL_GRP PEG
                  JOIN POP_EVAL_TYP PET
                    ON PET.EVAL_GRP_CN = PEG.CN
                  JOIN POP_EVAL PEV
                    ON PEV.CN = PET.EVAL_CN
                  JOIN POP_ESTN_UNIT PEU
                    ON PEV.CN = PEU.EVAL_CN
                  JOIN POP_STRATUM POP_STRATUM
                    ON PEU.CN = POP_STRATUM.ESTN_UNIT_CN
                 where PEG.eval_grp = 132021
                   and PET.eval_typ = 'EXPMORT') phase_1_summary
          left outer join (select POP_STRATUM_cn,
                                 ESTN_UNIT_CN,
                                 eval_cn,
                                 owngrpcd,
                                 CAST(sum(y_hid_adjusted) AS REAL) ysum_hd,
                                 CAST(sum(y_hid_adjusted * y_hid_adjusted) AS REAL) ysum_hd_sqr,
                                 count(*) number_plots_in_domain,
                                 SUM(case
                                       when y_hid_adjusted is NULL then
                                        0
                                       when y_hid_adjusted = 0 then
                                        0
                                       else
                                        1
                                     end) non_zero_plots
                            from (SELECT 574157 Attribute_nbr,
                                         'Average annual mortality of sound bole wood volume of trees (timber species at least 5 inches d.b.h.), in cubic feet, on forest land' Attribute_descr,
                                         CAST(SUM((CAST(GRM.TPAMORT_UNADJ AS REAL) * (CASE
                                               WHEN IFNULL(GRM.SUBPTYP_GRM, 0) = 0 THEN
                                                (0)
                                               WHEN GRM.SUBPTYP_GRM = 1 THEN
                                                CAST(POP_STRATUM.ADJ_FACTOR_SUBP AS REAL)
                                               WHEN GRM.SUBPTYP_GRM = 2 THEN
                                                CAST(POP_STRATUM.ADJ_FACTOR_MICR AS REAL)
                                               WHEN GRM.SUBPTYP_GRM = 3 THEN
                                                CAST(POP_STRATUM.ADJ_FACTOR_MACR AS REAL)
                                               ELSE
                                                (0)
                                             END) * (CASE
                                               WHEN GRM.COMPONENT LIKE
                                                    'MORTALITY%' THEN
                                                CAST(TRE_MIDPT.VOLCFSND AS REAL)
                                               ELSE
                                                (0)
                                             END))) AS REAL) AS y_hid_adjusted,
                                         peu.cn ESTN_UNIT_CN,
                                         pev.cn eval_cn,
                                         POP_STRATUM.cn POP_STRATUM_cn,
                                         plot.cn plt_cn,
                                         COND.OWNGRPCD
                                    FROM POP_EVAL_GRP PEG
                                    JOIN POP_EVAL_TYP PET
                                      ON PET.EVAL_GRP_CN = PEG.CN
                                    JOIN POP_EVAL PEV
                                      ON PEV.CN = PET.EVAL_CN
                                    JOIN POP_ESTN_UNIT PEU
                                      ON PEV.CN = PEU.EVAL_CN
                                    JOIN POP_STRATUM POP_STRATUM
                                      ON PEU.CN = POP_STRATUM.ESTN_UNIT_CN
                                    JOIN POP_PLOT_STRATUM_ASSGN POP_PLOT_STRATUM_ASSGN
                                      ON POP_STRATUM.CN = POP_PLOT_STRATUM_ASSGN.STRATUM_CN
                                    JOIN PLOT PLOT
                                      ON POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN
                                    JOIN PLOTGEOM 
                                      ON PLOT.CN = PLOTGEOM.CN
                                    JOIN COND COND
                                      ON PLOT.CN = COND.PLT_CN
                                    JOIN (SELECT P.PREV_PLT_CN, T.*
                                           FROM PLOT P
                                           JOIN TREE T
                                             ON P.CN = T.PLT_CN) TREE
                                      ON (TREE.CONDID = COND.CONDID AND
                                         TREE.PLT_CN = COND.PLT_CN)
                                    LEFT OUTER JOIN PLOT PPLOT
                                      ON PLOT.PREV_PLT_CN = PPLOT.CN
                                    LEFT OUTER JOIN COND PCOND
                                      ON (TREE.PREVCOND = PCOND.CONDID AND
                                         TREE.PREV_PLT_CN = PCOND.PLT_CN)
                                    LEFT OUTER JOIN TREE PTREE
                                      ON TREE.PREV_TRE_CN = PTREE.CN
                                    LEFT OUTER JOIN TREE_GRM_BEGIN TRE_BEGIN
                                      ON TREE.CN = TRE_BEGIN.TRE_CN
                                    LEFT OUTER JOIN TREE_GRM_MIDPT TRE_MIDPT
                                      ON TREE.CN = TRE_MIDPT.TRE_CN
                                    LEFT OUTER JOIN (SELECT TRE_CN,
                                                           DIA_BEGIN,
                                                           DIA_MIDPT,
                                                           DIA_END,
                                                           SUBP_COMPONENT_AL_FOREST     AS COMPONENT,
                                                           SUBP_SUBPTYP_GRM_AL_FOREST   AS SUBPTYP_GRM,
                                                           SUBP_TPAMORT_UNADJ_AL_FOREST AS TPAMORT_UNADJ
                                                      FROM TREE_GRM_COMPONENT) GRM
                                      ON TREE.CN = GRM.TRE_CN
                                    JOIN REF_SPECIES
                                      ON TREE.SPCD = REF_SPECIES.SPCD
                                   WHERE 1 = 1
                                     AND REF_SPECIES.WOODLAND = 'N'
                                     AND PET.EVAL_TYP = 'EXPMORT'
                                     AND PEG.EVAL_GRP = 132021
                                     AND 1 = 1 
                                   group by peu.cn,
                                            pev.cn,
                                            POP_STRATUM.cn,
                                            plot.cn,
                                            COND.OWNGRPCD) plot_summary
                           group by POP_STRATUM_cn,
                                    ESTN_UNIT_CN,
                                    eval_cn,
                                    owngrpcd) phase_summary
            on (phase_1_summary.POP_STRATUM_cn = phase_summary.POP_STRATUM_cn and
               phase_1_summary.eval_cn = phase_summary.eval_cn and
               phase_1_summary.ESTN_UNIT_CN = phase_summary.ESTN_UNIT_CN)
         group by phase_1_summary.pop_eval_grp_cn,
                  phase_1_summary.eval_grp,
                  phase_1_summary.eval_grp_descr,
                  phase_1_summary.ESTN_UNIT_CN,
                  phase_1_summary.total_area,
                  phase_1_summary.n,
                  phase_summary.owngrpcd) estimate_by_estn_unit
 where non_zero_plots is not null
 group by pop_eval_grp_cn,
          eval_grp,
          eval_grp_descr,
          owngrpcd;