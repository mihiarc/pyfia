#!/usr/bin/env bash
#
# Regenerate the Mintlify API reference (docs/api/*.mdx) from pyFIA's NumPy
# docstrings using mdxify. Mintlify serves the committed MDX, so re-run this
# whenever the public API or its docstrings change, then commit the result.
#
#   ./scripts/gen_api_docs.sh
#
# NOTE: the deferred NSVB carbon module (carbon / carbon_flux / carbon_pools)
# is intentionally NOT generated — it stays out of the public docs until 1.5.0.
# If you add/remove a page here, update the "API Reference" tab in docs/docs.json.
set -euo pipefail
cd "$(dirname "$0")/.."

uv run --with mdxify mdxify \
  pyfia.estimation.estimators.area \
  pyfia.estimation.estimators.area_change \
  pyfia.estimation.estimators.volume \
  pyfia.estimation.estimators.tpa \
  pyfia.estimation.estimators.biomass \
  pyfia.estimation.estimators.site_index \
  pyfia.estimation.estimators.mortality \
  pyfia.estimation.estimators.growth \
  pyfia.estimation.estimators.removals \
  pyfia.estimation.estimators.tree_metrics \
  pyfia.estimation.estimators.panel \
  pyfia.core.fia \
  pyfia.core.settings \
  pyfia.downloader \
  pyfia.utils.reference_tables \
  pyfia.evalidator \
  --output-dir docs/api --docstring-style numpy --format mdx --no-update-nav \
  --repo-url https://github.com/mihiarc/pyfia --branch main

# Drop low-level/internal pages we don't surface in the public nav.
rm -f \
  docs/api/pyfia-downloader-cache.mdx \
  docs/api/pyfia-downloader-client.mdx \
  docs/api/pyfia-downloader-exceptions.mdx \
  docs/api/pyfia-downloader-tables.mdx \
  docs/api/pyfia-evalidator-__init__.mdx \
  docs/api/pyfia-evalidator-estimate_types.mdx

# The downloader package page comes out titled "__init__"; give it a clean
# name/slug ("Data Download") so the nav and URL read well.
if [ -f docs/api/pyfia-downloader-__init__.mdx ]; then
  sed -e 's/^title: __init__/title: Data Download/' \
      -e 's/^sidebarTitle: __init__/sidebarTitle: Data Download/' \
      docs/api/pyfia-downloader-__init__.mdx > docs/api/pyfia-downloader.mdx
  rm -f docs/api/pyfia-downloader-__init__.mdx
fi

# Friendlier nav titles for the non-function reference pages (estimator pages
# keep their clean function-name titles, e.g. "mortality").
retitle() {  # $1 = page slug (no .mdx), $2 = display title
  local f="docs/api/$1.mdx"
  [ -f "$f" ] || return 0
  sed -i.bak -e "s/^title: .*/title: $2/" -e "s/^sidebarTitle: .*/sidebarTitle: $2/" "$f"
  rm -f "$f.bak"
}
retitle pyfia-core-fia           "FIA Database"
retitle pyfia-core-settings      "Settings"
retitle pyfia-utils-reference_tables "Reference Tables"
retitle pyfia-evalidator-client  "EVALIDator Client"
retitle pyfia-evalidator-validation "Validation"

echo "API reference regenerated in docs/api/. Keep docs.json navigation in sync."
