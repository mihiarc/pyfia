"""
Species groupings taken from FIADB ``REF_SPECIES``.

``tests/unit/test_code_constants.py`` checks each set against ``REF_SPECIES``.
"""

from __future__ import annotations

# Eastern species groups (REF_SPECIES.E_SPGRPCD) that make up the southern
# yellow pines: 1 "Longleaf and slash pines", 2 "Loblolly and shortleaf pines".
SOUTHERN_PINE_E_SPGRPCD: tuple[int, ...] = (1, 2)

# Their member species: shortleaf (110), slash (111), longleaf (121) and
# loblolly (131) pine.
SOUTHERN_PINE_SPCD: tuple[int, ...] = (110, 111, 121, 131)
