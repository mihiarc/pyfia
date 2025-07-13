"""
FIA Domain Knowledge Module.

This module provides comprehensive Forest Inventory Analysis domain knowledge
to enhance AI agents' understanding of FIA-specific terminology, relationships,
and query patterns.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Set


@dataclass
class FIAConcept:
    """Represents a FIA domain concept with synonyms and relationships."""

    name: str
    description: str
    synonyms: List[str]
    related_tables: List[str]
    related_columns: List[str]
    sql_patterns: List[str]
    category: str


class FIADomainKnowledge:
    """
    Comprehensive FIA domain knowledge base for AI agents.
    
    This class provides:
    - Term normalization and synonym mapping
    - Concept relationships and dependencies
    - Common query patterns
    - Statistical methodology guidance
    """

    def __init__(self):
        """Initialize the FIA domain knowledge base."""
        self.concepts = self._build_concept_database()
        self.synonym_map = self._build_synonym_map()
        self.query_patterns = self._build_query_patterns()
        self.statistical_rules = self._build_statistical_rules()

    def _build_concept_database(self) -> Dict[str, FIAConcept]:
        """Build comprehensive FIA concept database."""
        concepts = {}

        # Tree-related concepts
        concepts["trees_per_acre"] = FIAConcept(
            name="trees_per_acre",
            description="Number of trees per acre, fundamental FIA metric",
            synonyms=["tpa", "tree density", "stem density", "tree count per acre"],
            related_tables=["TREE", "POP_STRATUM"],
            related_columns=["TPA_UNADJ", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MICR", "ADJ_FACTOR_MACR"],
            sql_patterns=[
                "SUM(t.TPA_UNADJ * CASE WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR ELSE ps.ADJ_FACTOR_SUBP END)",
                "GROUP BY species for species-level TPA"
            ],
            category="tree_metrics"
        )

        concepts["basal_area"] = FIAConcept(
            name="basal_area",
            description="Cross-sectional area of trees at breast height per unit area",
            synonyms=["ba", "tree cross section", "stem area"],
            related_tables=["TREE"],
            related_columns=["DIA", "TPA_UNADJ"],
            sql_patterns=[
                "SUM(0.005454154 * POWER(t.DIA, 2) * t.TPA_UNADJ)",
                "-- Basal area in sq ft/acre from DBH in inches"
            ],
            category="tree_metrics"
        )

        concepts["biomass"] = FIAConcept(
            name="biomass",
            description="Total tree biomass including above and belowground components",
            synonyms=["tree weight", "carbon storage", "tree mass"],
            related_tables=["TREE"],
            related_columns=["DRYBIO_AG", "DRYBIO_BG", "DRYBIO_TOTAL", "CARBON_AG"],
            sql_patterns=[
                "SUM(t.DRYBIO_AG * t.TPA_UNADJ) for aboveground biomass",
                "Component options: 'AG', 'BG', 'TOTAL'"
            ],
            category="biomass_carbon"
        )

        concepts["volume"] = FIAConcept(
            name="volume",
            description="Tree stem volume measurements",
            synonyms=["timber volume", "wood volume", "merchantable volume", "cubic feet"],
            related_tables=["TREE"],
            related_columns=["VOLCFNET", "VOLCFGRS", "VOLCSNET", "VOLBFNET"],
            sql_patterns=[
                "SUM(t.VOLCFNET * t.TPA_UNADJ) for net cubic foot volume",
                "Volume types: NET (sound), GROSS (total), sawlog vs bole"
            ],
            category="volume"
        )

        # Forest condition concepts
        concepts["forest_area"] = FIAConcept(
            name="forest_area",
            description="Total forested area in acres",
            synonyms=["forested acres", "forest land", "timberland area", "woodland"],
            related_tables=["COND", "POP_STRATUM"],
            related_columns=["COND_STATUS_CD", "CONDPROP_UNADJ", "EXPNS"],
            sql_patterns=[
                "SUM(c.CONDPROP_UNADJ * ps.EXPNS) WHERE c.COND_STATUS_CD = 1",
                "Forest condition status = 1"
            ],
            category="area"
        )

        concepts["forest_type"] = FIAConcept(
            name="forest_type",
            description="Forest type classification based on dominant species",
            synonyms=["stand type", "forest composition", "vegetation type"],
            related_tables=["COND", "REF_FOREST_TYPE"],
            related_columns=["FORTYPCD", "MEANING", "TYPGRPCD"],
            sql_patterns=[
                "JOIN REF_FOREST_TYPE rft ON c.FORTYPCD = rft.FORTYPCD",
                "Common types: oak-hickory, loblolly-shortleaf pine"
            ],
            category="classification"
        )

        # Species concepts
        concepts["species"] = FIAConcept(
            name="species",
            description="Tree species identification",
            synonyms=["tree species", "tree type", "species code"],
            related_tables=["TREE", "REF_SPECIES"],
            related_columns=["SPCD", "COMMON_NAME", "SCIENTIFIC_NAME", "GENUS", "SPECIES"],
            sql_patterns=[
                "JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD",
                "Filter by COMMON_NAME or SPCD"
            ],
            category="classification"
        )

        # Growth and mortality
        concepts["mortality"] = FIAConcept(
            name="mortality",
            description="Tree mortality rates and volumes",
            synonyms=["tree death", "dead trees", "mortality rate", "tree loss"],
            related_tables=["TREE", "POP_EVAL"],
            related_columns=["STATUSCD", "PREV_STATUS_CD", "MORTALITY_YEAR"],
            sql_patterns=[
                "t.STATUSCD = 2 AND t.PREV_STATUS_CD = 1 for mortality trees",
                "Use GRM evaluation type for growth/removal/mortality"
            ],
            category="change"
        )

        concepts["growth"] = FIAConcept(
            name="growth",
            description="Tree and forest growth rates",
            synonyms=["increment", "growth rate", "annual growth", "accretion"],
            related_tables=["TREE", "TREE_GRM_COMPONENT"],
            related_columns=["DIA", "PREVDIA", "HT", "PREVHT"],
            sql_patterns=[
                "(t.DIA - t.PREVDIA) / years for diameter growth",
                "Requires GRM evaluation type"
            ],
            category="change"
        )

        # Evaluation and stratification
        concepts["evalid"] = FIAConcept(
            name="evalid",
            description="Evaluation identifier for statistically valid estimates",
            synonyms=["evaluation id", "evaluation", "eval"],
            related_tables=["POP_EVAL", "POP_PLOT_STRATUM_ASSGN"],
            related_columns=["EVALID", "EVAL_DESCR", "START_INVYR", "END_INVYR"],
            sql_patterns=[
                "JOIN POP_PLOT_STRATUM_ASSGN ppsa ON p.CN = ppsa.PLT_CN",
                "WHERE ppsa.EVALID = ? -- Always filter by EVALID"
            ],
            category="statistical"
        )

        concepts["stratum"] = FIAConcept(
            name="stratum",
            description="Stratification unit for post-stratified estimation",
            synonyms=["strata", "stratification", "estimation unit"],
            related_tables=["POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"],
            related_columns=["STRATUM_CN", "EXPNS", "ADJ_FACTOR_SUBP"],
            sql_patterns=[
                "JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN",
                "Expansion factor EXPNS converts to acres"
            ],
            category="statistical"
        )

        # Plot and measurement concepts
        concepts["plot"] = FIAConcept(
            name="plot",
            description="FIA inventory plot - basic sampling unit",
            synonyms=["sample plot", "inventory plot", "field plot"],
            related_tables=["PLOT", "SUBPLOT"],
            related_columns=["CN", "PLT_CN", "LAT", "LON", "INVYR"],
            sql_patterns=[
                "Plot CN is primary key (VARCHAR)",
                "Contains 4 subplots"
            ],
            category="sampling"
        )

        concepts["dbh"] = FIAConcept(
            name="dbh",
            description="Diameter at breast height (4.5 feet)",
            synonyms=["diameter", "dia", "tree diameter", "stem diameter"],
            related_tables=["TREE"],
            related_columns=["DIA", "PREVDIA"],
            sql_patterns=[
                "t.DIA >= 5.0 for subplot trees",
                "t.DIA BETWEEN 1.0 AND 4.9 for microplot trees"
            ],
            category="tree_metrics"
        )

        # Ownership and land use
        concepts["ownership"] = FIAConcept(
            name="ownership",
            description="Land ownership classification",
            synonyms=["owner", "land owner", "ownership type"],
            related_tables=["COND"],
            related_columns=["OWNCD", "OWNGRPCD"],
            sql_patterns=[
                "OWNGRPCD = 10 for National Forest",
                "OWNGRPCD = 40 for private land"
            ],
            category="classification"
        )

        # Tree status
        concepts["live_trees"] = FIAConcept(
            name="live_trees",
            description="Living trees in inventory",
            synonyms=["alive trees", "living stems", "live stems"],
            related_tables=["TREE"],
            related_columns=["STATUSCD"],
            sql_patterns=[
                "t.STATUSCD = 1 -- Live trees only",
                "Default for most analyses"
            ],
            category="tree_status"
        )

        concepts["dead_trees"] = FIAConcept(
            name="dead_trees",
            description="Standing dead trees",
            synonyms=["snags", "dead stems", "standing dead"],
            related_tables=["TREE"],
            related_columns=["STATUSCD", "STANDING_DEAD_CD"],
            sql_patterns=[
                "t.STATUSCD = 2 -- Dead trees",
                "Include for carbon/biomass pools"
            ],
            category="tree_status"
        )

        # Seedlings and regeneration
        concepts["seedlings"] = FIAConcept(
            name="seedlings",
            description="Small trees below measurement threshold",
            synonyms=["regeneration", "saplings", "young trees"],
            related_tables=["SEEDLING"],
            related_columns=["SPCD", "TREECOUNT"],
            sql_patterns=[
                "Trees < 1.0 inch DBH",
                "Counted on microplot"
            ],
            category="regeneration"
        )

        return concepts

    def _build_synonym_map(self) -> Dict[str, str]:
        """Build synonym to concept mapping."""
        synonym_map = {}

        for concept_name, concept in self.concepts.items():
            # Map concept name to itself
            synonym_map[concept_name.lower()] = concept_name

            # Map all synonyms
            for synonym in concept.synonyms:
                synonym_map[synonym.lower()] = concept_name

        # Add common abbreviations and variations
        additional_mappings = {
            "sq ft": "basal_area",
            "square feet": "basal_area",
            "cu ft": "volume",
            "cubic foot": "volume",
            "bd ft": "volume",
            "board feet": "volume",
            "tons": "biomass",
            "carbon": "biomass",
            "acres": "forest_area",
            "ha": "forest_area",
            "hectares": "forest_area",
            "pine": "species",
            "oak": "species",
            "maple": "species",
            "dead": "dead_trees",
            "alive": "live_trees",
            "living": "live_trees",
            "eval": "evalid",
            "evaluation": "evalid",
        }

        synonym_map.update(additional_mappings)

        return synonym_map

    def _build_query_patterns(self) -> Dict[str, List[Dict[str, str]]]:
        """Build common query patterns for different analysis types."""
        patterns = {
            "species_composition": [
                {
                    "description": "Trees per acre by species",
                    "pattern": """
                    SELECT 
                        t.SPCD,
                        rs.COMMON_NAME,
                        SUM(t.TPA_UNADJ * ps.ADJ_FACTOR_SUBP) as TPA
                    FROM TREE t
                    JOIN PLOT p ON t.PLT_CN = p.CN
                    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON p.CN = ppsa.PLT_CN
                    JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN
                    LEFT JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD
                    WHERE ppsa.EVALID = ? AND t.STATUSCD = 1
                    GROUP BY t.SPCD, rs.COMMON_NAME
                    ORDER BY TPA DESC
                    """
                },
                {
                    "description": "Biomass by species group",
                    "pattern": """
                    SELECT 
                        rs.MAJOR_SPGRPCD,
                        SUM(t.DRYBIO_AG * t.TPA_UNADJ * ps.ADJ_FACTOR_SUBP) as BIOMASS_AG
                    FROM TREE t
                    JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD
                    -- Additional joins...
                    GROUP BY rs.MAJOR_SPGRPCD
                    """
                }
            ],
            "area_estimation": [
                {
                    "description": "Forest area by forest type",
                    "pattern": """
                    SELECT 
                        c.FORTYPCD,
                        rft.MEANING,
                        SUM(c.CONDPROP_UNADJ * ps.EXPNS) as AREA_ACRES
                    FROM COND c
                    JOIN PLOT p ON c.PLT_CN = p.CN
                    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON p.CN = ppsa.PLT_CN
                    JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN
                    LEFT JOIN REF_FOREST_TYPE rft ON c.FORTYPCD = rft.FORTYPCD
                    WHERE ppsa.EVALID = ? AND c.COND_STATUS_CD = 1
                    GROUP BY c.FORTYPCD, rft.MEANING
                    """
                }
            ],
            "temporal_analysis": [
                {
                    "description": "Change over time using multiple evaluations",
                    "pattern": """
                    WITH eval_data AS (
                        SELECT 
                            pe.EVALID,
                            pe.END_INVYR,
                            -- metrics
                        FROM POP_EVAL pe
                        WHERE pe.STATECD = ? AND pe.EVAL_TYP = 'VOL'
                    )
                    SELECT * FROM eval_data ORDER BY END_INVYR
                    """
                }
            ]
        }

        return patterns

    def _build_statistical_rules(self) -> List[Dict[str, str]]:
        """Build FIA statistical methodology rules."""
        rules = [
            {
                "name": "Always filter by EVALID",
                "description": "Every query for population estimates must filter by EVALID to ensure statistical validity",
                "sql_hint": "JOIN POP_PLOT_STRATUM_ASSGN ppsa ON p.CN = ppsa.PLT_CN WHERE ppsa.EVALID = ?"
            },
            {
                "name": "Apply adjustment factors",
                "description": "Tree-level expansion requires both TPA_UNADJ and appropriate adjustment factor",
                "sql_hint": "t.TPA_UNADJ * ps.ADJ_FACTOR_[MICR|SUBP|MACR] based on tree size"
            },
            {
                "name": "Include expansion factors",
                "description": "Area estimates require EXPNS from POP_STRATUM table",
                "sql_hint": "c.CONDPROP_UNADJ * ps.EXPNS for area in acres"
            },
            {
                "name": "Stratified estimation",
                "description": "Results should be aggregated at stratum level before population totals",
                "sql_hint": "GROUP BY stratum first, then weight by stratum proportions"
            },
            {
                "name": "CN fields are VARCHAR",
                "description": "Control Number (CN) fields are strings, not integers",
                "sql_hint": "Use string comparisons for CN fields in joins"
            }
        ]

        return rules

    def normalize_term(self, term: str) -> Optional[str]:
        """Normalize a term to its canonical concept name."""
        return self.synonym_map.get(term.lower())

    def get_concept(self, term: str) -> Optional[FIAConcept]:
        """Get concept information for a term."""
        normalized = self.normalize_term(term)
        if normalized:
            return self.concepts.get(normalized)
        return None

    def extract_concepts(self, text: str) -> List[FIAConcept]:
        """Extract FIA concepts from natural language text."""
        text_lower = text.lower()
        found_concepts = []
        seen = set()

        # Check each synonym
        for synonym, concept_name in self.synonym_map.items():
            if synonym in text_lower and concept_name not in seen:
                concept = self.concepts.get(concept_name)
                if concept:
                    found_concepts.append(concept)
                    seen.add(concept_name)

        return found_concepts

    def suggest_tables(self, concepts: List[FIAConcept]) -> Set[str]:
        """Suggest relevant tables based on extracted concepts."""
        tables = set()
        for concept in concepts:
            tables.update(concept.related_tables)

        # Always include core tables for FIA queries
        if tables:
            tables.update(["PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"])

        return tables

    def suggest_columns(self, concepts: List[FIAConcept]) -> Set[str]:
        """Suggest relevant columns based on extracted concepts."""
        columns = set()
        for concept in concepts:
            columns.update(concept.related_columns)
        return columns

    def get_query_hints(self, query: str) -> List[str]:
        """Get SQL hints based on the natural language query."""
        hints = []
        concepts = self.extract_concepts(query)

        # Add concept-specific hints
        for concept in concepts:
            hints.extend(concept.sql_patterns)

        # Add general statistical rules
        hints.append("Remember to filter by EVALID for valid estimates")

        # Check for specific patterns
        if "by species" in query.lower():
            hints.append("GROUP BY t.SPCD, rs.COMMON_NAME")
            hints.append("JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD")

        if "by state" in query.lower():
            hints.append("GROUP BY p.STATECD")
            hints.append("Consider using REF_STATE for state names")

        if any(word in query.lower() for word in ["total", "sum", "all"]):
            hints.append("Use SUM() aggregation with proper expansion factors")

        if "average" in query.lower() or "mean" in query.lower():
            hints.append("For per-acre values, divide totals by area")

        return hints

    def validate_query_semantics(self, query: str, sql: str) -> List[str]:
        """Validate that generated SQL matches the semantic intent."""
        warnings = []
        concepts = self.extract_concepts(query)
        sql_upper = sql.upper()

        # Check for EVALID
        if "EVALID" not in sql_upper and any(c.category != "classification" for c in concepts):
            warnings.append("Query should filter by EVALID for statistical validity")

        # Check for live/dead trees
        if any(c.name == "live_trees" for c in concepts) and "STATUSCD = 1" not in sql:
            warnings.append("Query asks for live trees but doesn't filter STATUSCD = 1")

        # Check for proper joins
        if "TREE" in sql_upper and "POP_STRATUM" not in sql_upper:
            warnings.append("Tree queries usually need POP_STRATUM for expansion factors")

        # Check for species names
        if "species" in query.lower() and "REF_SPECIES" not in sql_upper:
            warnings.append("Consider joining REF_SPECIES for species names")

        return warnings

    def format_concept_help(self, concept_name: str) -> str:
        """Format help text for a specific concept."""
        concept = self.concepts.get(concept_name)
        if not concept:
            return f"No information found for concept: {concept_name}"

        help_text = f"""
**{concept.name.replace('_', ' ').title()}**

*Description:* {concept.description}

*Also known as:* {', '.join(concept.synonyms[:3])}

*Related tables:* {', '.join(concept.related_tables)}

*Key columns:* {', '.join(concept.related_columns[:5])}

*SQL patterns:*
"""
        for pattern in concept.sql_patterns[:2]:
            help_text += f"- {pattern}\n"

        return help_text


# Singleton instance for easy access
fia_knowledge = FIADomainKnowledge()
