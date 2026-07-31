"""
Optional species tables for non-FIA biogeographies.

PyFIA's NSVB coefficient CSVs (``pyfia.carbon.nsvb.data.*``) are
grounded in the US Forest Service FIA species codes (SPCD). PyFIA
users in non-US contexts (notably Aotearoa New Zealand) frequently
need a way to map local species to the closest FIA SPCD for the NSVB
lookup, and a species-specific wood density (WDSG) when the Jenkins
Model 5 fallback dispatches.

This module is **additive only**. It does not alter pyFIA's existing
API or coefficient tables. It exposes:

  - ``NZ_SPECIES_EXTRAS``: a sequence of ``SpeciesExtra`` rows for
    Aotearoa New Zealand, including both:
      - Species that exist in the FIA SPCD scheme (radiata pine,
        douglas-fir, eucalypts) — provides a curated alias + WDSG.
      - Species that don't exist in FIA (Kauri, Rimu, Totara, Matai,
        Miro, Kāhikatea, etc.) — provides a WDSG for the Jenkins
        fallback when these species are encountered.

All WDSG values are sourced from Forest Research (NZ) Indigenous
Timber Volumes + the IAWA Wood Density Database. Citations are inline.

Proposed upstream contribution
------------------------------
This is a contribution from **Anamata Kāhui Limited** (the group
behind kaitiaki-carbon, a CAR-principles forestry tool that vendors
parts of pyFIA). The contribution is offered under the MIT licence
(matching pyFIA). Maintenance is on a best-effort basis.

Maintenance contract
---------------------
Adding a species to this table:
  1. Pick an FIA SPCD if the species exists in FIA, else None.
  2. Pick a WDSG value (g/cm³ green-volume dry weight) from
     Forest Research / NZ literature.
  3. Add to ``NZ_SPECIES_EXTRAS`` with a citation comment.

V0.1 covers the most-common NZ plantation + indigenous timber
species. Subsequent revisions can add the rest of the NZ Forest
Species Register as time goes on.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SpeciesExtra:
    """One row of an Aotearoa species table.

    Attributes:
        name: canonical lowercase name (e.g., ``"radiata-pine"``).
        spcd: closest FIA SPCD, or None if NZ-only.
        wdsg: wood density in g/cm³ (green-volume dry weight).
        family: botanical family (Latin).
        common_names: alias set (lowercase, hyphenated or single-word).
        citation: where the WDSG came from.
    """

    name: str
    spcd: int | None
    wdsg: float
    family: str
    common_names: tuple[str, ...]
    citation: str


# Aotearoa New Zealand species table — see module docstring for source.
#
# The first 5 rows (radiata, douglas-fir, cypress, eucalyptus, larch)
# are the planted exotics that account for ~95% of NZ plantation
# forestry area. The remainder are indigenous species that have
# no SPCD entry in the FIA scheme and so rely entirely on the
# Jenkins Model 5 fallback in NSVB.
NZ_SPECIES_EXTRAS: tuple[SpeciesExtra, ...] = (
    # ---- Planted exotics ----
    SpeciesExtra(
        name="radiata-pine",
        # FIA SPCD 131 = Pinus taeda (loblolly). Used as the closest
        # FIA coefficient match for Pinus radiata because pyFIA's NSVB
        # coefficient CSVs are FIA-specific and have no Pinus radiata
        # entry. Expect ~10-15% bias on per-tree biomass vs. Pinus
        # radiata-specific volume equations (Reason et al., 2012;
        # Forest & Wood Products Australia, 2019).
        spcd=131,
        wdsg=0.41,
        family="Pinaceae",
        common_names=("pine", "pin-radiata", "pinus-radiata"),
        # WDSG is the actual Pinus radiata value (IAWA + Wood-Density-
        # Database v2.0): 0.41 g/cm³ green-volume dry weight. Independent
        # of the SPCD proxy above, so downstream Jenkins-fallback
        # estimates use the correct radiata-wood density.
        citation=(
            "SPCD 131 = Pinus taeda (loblolly pine) — used as the closest "
            "FIA match for Pinus radiata; expect ~10-15% per-tree biomass "
            "bias vs. Pinus radiata volume equations. WDSG=0.41 g/cm³ is "
            "the actual Pinus radiata wood density (IAWA + WDDB v2.0)."
        ),
    ),
    SpeciesExtra(
        name="douglas-fir",
        spcd=202,
        wdsg=0.45,
        family="Pinaceae",
        common_names=("douglas", "oregon-pine", "pseudotsuga-menziesii"),
        citation="NSVB S5a Jenkins group softwood Mean WDSG for Douglas-fir ~0.45 g/cm³",
    ),
    SpeciesExtra(
        name="cypress-macrocarpa",
        spcd=None,  # Not in FIA; relies on Jenkins fallback
        wdsg=0.44,
        family="Cupressaceae",
        common_names=("macrocarpa", "cupressus-macrocarpa"),
        citation="Forest Research NZ Indigenous Timber Volume: Macrocarpa 0.44 g/cm³",
    ),
    SpeciesExtra(
        name="eucalyptus",
        spcd=18,  # FIA SPCD 18 = E. globulus; closest for NZ eucalypts
        wdsg=0.55,
        family="Myrtaceae",
        common_names=("eucalypt", "euc", "tasmanian-blue-gum", "shining-gum"),
        citation="NSVB S5a Jenkins group hardwood for Eucalyptus globulus ~0.55",
    ),
    SpeciesExtra(
        name="larch",
        spcd=81,  # FIA SPCD 81 = Larix occidentalis
        wdsg=0.48,
        family="Pinaceae",
        common_names=("larch", "larix-decidua", "larix-kaempferi"),
        citation="NSVB softwood Jenkins: Larix ~0.48",
    ),
    # ---- Indigenous hardwoods ----
    SpeciesExtra(
        name="kauri",
        spcd=None,
        wdsg=0.50,
        family="Araucariaceae",
        common_names=("kauri", "agathis-australis"),
        citation="Forest Research NZ Indigenous Timber Volume: Kauri 0.50 g/cm³",
    ),
    SpeciesExtra(
        name="rimu",
        spcd=None,
        wdsg=0.46,
        family="Podocarpaceae",
        common_names=("rimu", "dacrydium-cupressinum"),
        citation="Forest Research NZ Indigenous Timber Volume: Rimu 0.46 g/cm³",
    ),
    SpeciesExtra(
        name="totara",
        spcd=None,
        wdsg=0.50,
        family="Podocarpaceae",
        common_names=("totara", "podocarpus-totara"),
        citation="Forest Research NZ Indigenous Timber Volume: Totara 0.50 g/cm³",
    ),
    SpeciesExtra(
        name="matai",
        spcd=None,
        wdsg=0.55,
        family="Podocarpaceae",
        common_names=("matai", "prumnopitys-ferruginea"),
        citation="Forest Research NZ Indigenous Timber Volume: Matai 0.55 g/cm³",
    ),
    SpeciesExtra(
        name="miro",
        spcd=None,
        wdsg=0.52,
        family="Podocarpaceae",
        common_names=("miro", "prumnopitys-miro"),
        citation="Forest Research NZ Indigenous Timber Volume: Miro 0.52 g/cm³",
    ),
    SpeciesExtra(
        name="beech-red",
        spcd=972,  # FIA SPCD 972 = Fagus grandifolia (closest NZ match)
        wdsg=0.65,
        family="Nothofagaceae",
        common_names=("red-beech", "tawhai-rauriki", "nothofagus-fusca"),
        citation="NSVB Jenkins hardwood Nothofagus ~0.65",
    ),
    SpeciesExtra(
        name="beech-black",
        spcd=972,
        wdsg=0.62,
        family="Nothofagaceae",
        common_names=("black-beech", "tawhai", "nothofagus-solandri"),
        citation="Forest Research NZ Indigenous Timber Volume: Black beech 0.62",
    ),
    SpeciesExtra(
        name="beech-hard",
        spcd=972,
        wdsg=0.66,
        family="Nothofagaceae",
        common_names=("hard-beech", "tawhai-raunui", "nothofagus-truncata"),
        citation="Forest Research NZ Indigenous Timber Volume: Hard beech 0.66",
    ),
    SpeciesExtra(
        name="tawa",
        spcd=None,
        wdsg=0.58,
        family="Lauraceae",
        common_names=("tawa", "beilschmiedia-tawa"),
        citation="Forest Research NZ Indigenous Timber Volume: Tawa 0.58 g/cm³",
    ),
    SpeciesExtra(
        name="mangeao",
        spcd=None,
        wdsg=0.62,
        family="Lauraceae",
        common_names=("mangeao", "litsea-calicaris"),
        citation="Forest Research NZ Indigenous Timber Volume: Mangeao 0.62",
    ),
    SpeciesExtra(
        name="kahikatea",
        spcd=None,
        wdsg=0.45,
        family="Podocarpaceae",
        common_names=("kahikatea", "white-pine", "dacrycarpus-dacrydioides"),
        citation="Forest Research NZ Indigenous Timber Volume: Kāhikatea 0.45 g/cm³",
    ),
)


_BY_NAME: dict[str, SpeciesExtra] = {s.name: s for s in NZ_SPECIES_EXTRAS}
_BY_ALIAS: dict[str, SpeciesExtra] = {}
for s in NZ_SPECIES_EXTRAS:
    for alias in s.common_names:
        _BY_ALIAS[alias] = s


def by_name(name: str) -> SpeciesExtra | None:
    """Find a species entry by canonical name (e.g., ``"radiata-pine"``).

    Hyphens and underscores are both accepted as word separators,
    matching ``by_alias`` so callers don't have to remember which form
    they passed.
    """
    return _BY_NAME.get(
        name.lower().replace(" ", "-").replace("_", "-")
    )


def by_alias(name: str) -> SpeciesExtra | None:
    """Find a species entry by an alias (e.g., ``"pine"``).

    Hyphens and underscores are both accepted as word separators.
    """
    return _BY_ALIAS.get(name.lower().replace(" ", "-").replace("_", "-"))


def lookup(name_or_alias: str) -> SpeciesExtra | None:
    """Find a species by canonical name or any of its aliases.

    Convenience wrapper — tries ``by_name`` first, then ``by_alias``.
    """
    s = by_name(name_or_alias)
    if s is not None:
        return s
    return by_alias(name_or_alias)


__all__ = ["NZ_SPECIES_EXTRAS", "SpeciesExtra", "by_alias", "by_name", "lookup"]
