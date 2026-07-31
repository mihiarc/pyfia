"""
Te reo Māori macron aliases for the NZ species table.

A supplementary module to ``pyfia.constants.species_extra`` that
bridges te reo Māori name spellings — properly macrons-as-macrons
— into the canonical table.

Why this exists
---------------

``species_extra`` uses Latin / English canonical names (e.g.,
``"rimu"``, ``"kauri"``) for cross-border compatibility. NZ
foresters, kaitiaki, and iwi data collectors spell the same trees
with the proper te reo Māori macrons:

    rimu   →   rīmū
    kauri  →   kaurī
    matai  →   matāī
    totara →   tōtara
    ... etc.

This module maps those macroned spellings to the canonical ASCII
names. ASCII forms are *not* duplicated here — they resolve via
``pyfia.constants.species_extra.lookup()`` directly:

    >>> from pyfia.constants.species_macros import macron_to_canonical
    >>> from pyfia.constants.species_extra import lookup
    >>>
    >>> result = macron_to_canonical("rīmū")  # → ("rimu", "")
    >>> canonical, _ = result
    >>> entry = lookup(canonical)              # → SpeciesExtra(...)

Pure-data, additive; no changes to ``species_extra`` or pyFIA itself.


Maintenance contract
---------------------

Adding a new macron alias:

1. Strip macrons in your head, then check the stripped form is in
   ``NZ_SPECIES_EXTRAS.common_names`` or ``species_extra.NZ_SPECIES_EXTRAS``.
2. Add the macrons form to ``MACRON_ALIASES`` with a note about which
   species it maps to (canonical-name or common-alias).
3. Macrons are ā ē ī ō ū. ASCII forms (a e i o u with no macron)
   are unchanged.
"""

from __future__ import annotations

# Te reo Māori macron spellings → species_extra canonical names.
#
# Each entry is ``macroned_form: (canonical_name, note)``.
# The note can be "" (no special note) or a brief reference.
#
# Spelling convention: macrons ā ē ī ō ū; 'ng' digraph preserved
# (we do not split or merge). Whitespace as supplied.
MACRON_ALIASES: dict[str, tuple[str, str]] = {
    # ---- Planted exotics (less common in te reo Māori but still attested) ----
    "rātā": ("radiata-pine", "Māori loanword for radiata pine (Pinus radiata)"),
    "rāta": ("radiata-pine", "Common te reo spelling of radiata pine"),

    # ---- Indigenous hardwoods ----
    "rīmū": ("rimu", ""),
    "kaurī": ("kauri", ""),
    "tōtara": ("totara", ""),
    "matāī": ("matai", ""),
    "mīro": ("miro", ""),
    "tawhairaunui": ("beech-hard", "Compound: tawhai + raunui"),
    "tawhairauriki": ("beech-red", "Compound: tawhai + rauriki"),
    "tāwa": ("tawa", ""),
    "mangeāo": ("mangeao", ""),
    "kahikātea": ("kahikatea", ""),
    # Beech forms
    "tawhai": ("beech-hard", "Generic tawhai; beech-hard in absence of qualifier"),

}


def macron_to_canonical(macroned: str) -> tuple[str, str] | None:
    """Look up a macrons-only spelling in MACRON_ALIASES.

    Returns ``(canonical_name, note)`` or ``None`` if not in the
    aliases table.

    The note is preserved for callers that want to display provenance.
    """
    return MACRON_ALIASES.get(macroned.lower().strip())


__all__ = ["MACRON_ALIASES", "macron_to_canonical"]
