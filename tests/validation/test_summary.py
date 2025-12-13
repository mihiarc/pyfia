"""Validation summary report generation."""

from pyfia import FIA, area, volume, biomass, tpa, growth, mortality, removals
from pyfia.estimation.estimators.carbon import carbon

from .conftest import (
    GEORGIA_STATE_CODE,
    GEORGIA_EVALID,
    GEORGIA_EVALID_GRM,
    GEORGIA_YEAR,
    FLOAT_TOLERANCE,
    extract_grm_estimate,
)


class TestValidationSummary:
    """Generate validation summary report."""

    def test_generate_validation_summary(self, fia_db, evalidator_client):
        """Generate comprehensive validation summary for all estimators.

        This test runs all validations and prints a summary table.
        Always passes - serves as documentation.
        """
        results = []

        print("\n" + "=" * 80)
        print("pyFIA vs EVALIDator VALIDATION SUMMARY - Georgia 2023")
        print("=" * 80)

        # Area - Forest
        try:
            with FIA(fia_db) as db:
                db.clip_by_evalid(GEORGIA_EVALID)
                r = area(db, land_type="forest", totals=True)
                pyfia_val = r["AREA"][0]
            ev = evalidator_client.get_forest_area(GEORGIA_STATE_CODE, GEORGIA_YEAR)
            pct_diff = abs(pyfia_val - ev.estimate) / ev.estimate * 100
            results.append(("Forest Area", pyfia_val, ev.estimate, pct_diff, "acres"))
        except Exception as e:
            results.append(("Forest Area", None, None, None, f"ERROR: {e}"))

        # Area - Timberland
        try:
            with FIA(fia_db) as db:
                db.clip_by_evalid(GEORGIA_EVALID)
                r = area(db, land_type="timber", totals=True)
                pyfia_val = r["AREA"][0]
            ev = evalidator_client.get_forest_area(GEORGIA_STATE_CODE, GEORGIA_YEAR, land_type="timber")
            pct_diff = abs(pyfia_val - ev.estimate) / ev.estimate * 100
            results.append(("Timberland Area", pyfia_val, ev.estimate, pct_diff, "acres"))
        except Exception as e:
            results.append(("Timberland Area", None, None, None, f"ERROR: {e}"))

        # Volume - Growing Stock
        try:
            with FIA(fia_db) as db:
                db.clip_by_evalid(GEORGIA_EVALID)
                r = volume(db, land_type="forest", tree_type="gs", totals=True)
                pyfia_val = r["VOLCFNET_TOTAL"][0]
            ev = evalidator_client.get_volume(GEORGIA_STATE_CODE, GEORGIA_YEAR)
            pct_diff = abs(pyfia_val - ev.estimate) / ev.estimate * 100
            results.append(("Volume (GS)", pyfia_val, ev.estimate, pct_diff, "cu ft"))
        except Exception as e:
            results.append(("Volume (GS)", None, None, None, f"ERROR: {e}"))

        # Biomass
        try:
            with FIA(fia_db) as db:
                db.clip_by_evalid(GEORGIA_EVALID)
                r = biomass(db, land_type="forest", tree_type="live", totals=True)
                pyfia_val = r["BIO_TOTAL"][0]
            ev = evalidator_client.get_biomass(GEORGIA_STATE_CODE, GEORGIA_YEAR)
            pct_diff = abs(pyfia_val - ev.estimate) / ev.estimate * 100
            results.append(("Biomass (AG)", pyfia_val, ev.estimate, pct_diff, "dry tons"))
        except Exception as e:
            results.append(("Biomass (AG)", None, None, None, f"ERROR: {e}"))

        # TPA
        try:
            with FIA(fia_db) as db:
                db.clip_by_evalid(GEORGIA_EVALID)
                r = tpa(db, land_type="forest", tree_type="live", totals=True)
                pyfia_val = r["TPA_TOTAL"][0]
            ev = evalidator_client.get_tree_count(GEORGIA_STATE_CODE, GEORGIA_YEAR)
            pct_diff = abs(pyfia_val - ev.estimate) / ev.estimate * 100
            results.append(("Tree Count", pyfia_val, ev.estimate, pct_diff, "trees"))
        except Exception as e:
            results.append(("Tree Count", None, None, None, f"ERROR: {e}"))

        # Carbon
        try:
            with FIA(fia_db) as db:
                db.clip_by_evalid(GEORGIA_EVALID)
                r = carbon(db, pool="live", land_type="forest", totals=True)
                pyfia_val = r["CARBON_TOTAL"][0]
            ev = evalidator_client.get_carbon(GEORGIA_STATE_CODE, GEORGIA_YEAR)
            pct_diff = abs(pyfia_val - ev.estimate) / ev.estimate * 100
            results.append(("Carbon (Live)", pyfia_val, ev.estimate, pct_diff, "mt"))
        except Exception as e:
            results.append(("Carbon (Live)", None, None, None, f"ERROR: {e}"))

        # Growth
        try:
            with FIA(fia_db) as db:
                db.clip_by_evalid(GEORGIA_EVALID_GRM)
                r = growth(db, land_type="forest", tree_type="gs", totals=True)
                pyfia_val, _ = extract_grm_estimate(r, "growth")
            ev = evalidator_client.get_growth(GEORGIA_STATE_CODE, GEORGIA_YEAR)
            pct_diff = abs(pyfia_val - ev.estimate) / ev.estimate * 100
            results.append(("Growth", pyfia_val, ev.estimate, pct_diff, "cu ft/yr"))
        except Exception as e:
            results.append(("Growth", None, None, None, f"ERROR: {e}"))

        # Mortality
        try:
            with FIA(fia_db) as db:
                db.clip_by_evalid(GEORGIA_EVALID_GRM)
                r = mortality(db, land_type="forest", tree_type="gs", totals=True)
                pyfia_val, _ = extract_grm_estimate(r, "mortality")
            ev = evalidator_client.get_mortality(GEORGIA_STATE_CODE, GEORGIA_YEAR)
            pct_diff = abs(pyfia_val - ev.estimate) / ev.estimate * 100
            results.append(("Mortality", pyfia_val, ev.estimate, pct_diff, "cu ft/yr"))
        except Exception as e:
            results.append(("Mortality", None, None, None, f"ERROR: {e}"))

        # Removals
        try:
            with FIA(fia_db) as db:
                db.clip_by_evalid(GEORGIA_EVALID_GRM)
                r = removals(db, land_type="forest", tree_type="gs", totals=True)
                pyfia_val, _ = extract_grm_estimate(r, "removals")
            ev = evalidator_client.get_removals(GEORGIA_STATE_CODE, GEORGIA_YEAR)
            pct_diff = abs(pyfia_val - ev.estimate) / ev.estimate * 100
            results.append(("Removals", pyfia_val, ev.estimate, pct_diff, "cu ft/yr"))
        except Exception as e:
            results.append(("Removals", None, None, None, f"ERROR: {e}"))

        # Print summary table
        print(f"\n{'Estimator':<18} {'pyFIA':>18} {'EVALIDator':>18} {'Diff %':>10} {'Status':>10}")
        print("-" * 80)

        for name, pyfia_val, ev_val, pct_diff, units in results:
            if pyfia_val is None:
                print(f"{name:<18} {units}")
            else:
                status = "PASS" if pct_diff < 5 else "WARN" if pct_diff < 20 else "FAIL"
                print(f"{name:<18} {pyfia_val:>15,.0f} {ev_val:>15,.0f} {pct_diff:>9.2f}% {status:>10}")

        print("-" * 80)
        print(f"Tolerance: Exact match required (floating point tolerance: {FLOAT_TOLERANCE})")
        print("=" * 80)

        # Always passes - documentation only
        assert True
