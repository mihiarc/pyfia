"""
Advanced AI Agent for Forest Inventory Analysis using LangGraph.

This module implements a cutting-edge AI agent that specializes in querying
FIA (Forest Inventory Analysis) databases using natural language. It leverages
LangGraph for structured agent workflows and incorporates deep domain knowledge
about forest inventory data structures and statistical methodologies.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional, Union

# Load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # python-dotenv not available, skip loading
    pass

# LangChain/LangGraph imports
import polars as pl
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import Tool
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field

from .core import FIA
from .duckdb_query_interface import DuckDBQueryInterface
from .fia_domain_knowledge import fia_knowledge


# Pydantic models for structured outputs
class QueryRequest(BaseModel):
    """Structured query request from user."""

    natural_language_query: str = Field(
        description="User's natural language query about forest data"
    )
    query_type: str = Field(
        description="Type of query: 'data_retrieval', 'analysis', 'schema_info'"
    )
    specific_tables: Optional[List[str]] = Field(
        description="Specific tables mentioned or inferred"
    )
    temporal_scope: Optional[str] = Field(
        description="Time period mentioned (e.g., 'recent', '2020', 'latest')"
    )
    geographic_scope: Optional[str] = Field(
        description="Geographic area mentioned (state, region, etc.)"
    )


class QueryResult(BaseModel):
    """Structured query result."""

    sql_query: str = Field(description="Generated SQL query")
    explanation: str = Field(
        description="Human-readable explanation of what the query does"
    )
    results_summary: str = Field(description="Summary of query results")
    data_preview: Optional[str] = Field(description="Preview of returned data")
    warnings: Optional[List[str]] = Field(
        description="Any warnings about data interpretation"
    )


class AgentState(BaseModel):
    """State for the LangGraph agent."""

    model_config = {"arbitrary_types_allowed": True}

    messages: Annotated[
        List[Union[HumanMessage, AIMessage, SystemMessage]], add_messages
    ]
    query_request: Optional[QueryRequest] = None
    sql_query: Optional[str] = None
    query_results: Optional[pl.DataFrame] = None
    final_response: Optional[str] = None
    tools_used: List[str] = field(default_factory=list)


@dataclass
class FIAAgentConfig:
    """Configuration for the FIA AI Agent."""

    model_name: str = "gpt-4o"
    temperature: float = 0.1
    max_tokens: int = 2000
    result_limit: int = 100
    enable_query_validation: bool = True
    enable_safety_checks: bool = True
    verbose: bool = False


class FIAAgent:
    """
    Advanced AI Agent for Forest Inventory Analysis using LangGraph.

    This agent combines:
    - Deep FIA domain knowledge including new SURVEY, SUBPLOT, SUBP_COND, SEEDLING tables
    - Safe SQL query generation with forest science expertise
    - Statistical awareness for proper EVALID usage
    - Natural language interaction
    - Structured workflows via LangGraph
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        config: Optional[FIAAgentConfig] = None,
        api_key: Optional[str] = None,
    ):
        """
        Initialize the FIA AI Agent.

        Args:
            db_path: Path to FIA DuckDB database
            config: Agent configuration
            api_key: OpenAI API key (if not set in environment)
        """
        self.db_path = Path(db_path)
        self.config = config or FIAAgentConfig()

        # Initialize database interfaces
        self.query_interface = DuckDBQueryInterface(db_path)
        self.fia = FIA(db_path, engine="duckdb")
        
        # Enable debugging if verbose
        if self.config.verbose:
            print(f"[DEBUG] Initialized FIA Agent with database: {db_path}")

        # Initialize LLM
        llm_kwargs = {
            "model": self.config.model_name,
            "temperature": self.config.temperature,
        }

        # Set API key from parameter, environment, or .env file
        if api_key:
            llm_kwargs["api_key"] = api_key
        elif os.environ.get("OPENAI_API_KEY"):
            llm_kwargs["api_key"] = os.environ.get("OPENAI_API_KEY")

        self.llm = ChatOpenAI(**llm_kwargs)

        # Create tools
        self.tools = self._create_tools()

        # Build LangGraph workflow
        self.workflow = self._build_workflow()

        # Cache database schema for context
        self._schema_cache = None

    def _create_tools(self) -> List[Tool]:
        """Create specialized tools for FIA database interaction."""

        def execute_fia_query(query: str, limit: int = None) -> str:
            """Execute a SQL query against the FIA database safely."""
            if self.config.verbose:
                print(f"[DEBUG] Executing query: {query[:100]}...")
            
            try:
                limit = limit or self.config.result_limit
                result = self.query_interface.execute_query(query, limit=limit)
                
                if self.config.verbose:
                    print(f"[DEBUG] Query returned {len(result)} rows")

                # Format results for LLM
                formatted = self.query_interface.format_results_for_llm(result)
                return formatted
            except Exception as e:
                error_msg = f"Query execution failed: {str(e)}"
                if self.config.verbose:
                    print(f"[DEBUG] {error_msg}")
                return error_msg

        def get_database_schema() -> str:
            """Get database schema information for query generation."""
            if self._schema_cache is None:
                self._schema_cache = self.query_interface.get_natural_language_context()
            return self._schema_cache

        def get_evalid_info(state_code: int = None) -> str:
            """Get information about available EVALIDs for statistical queries."""
            try:
                evalid_df = self.query_interface.get_evalid_info()
                if state_code:
                    evalid_df = evalid_df.filter(pl.col("STATECD") == state_code)

                summary = f"Available EVALIDs: {len(evalid_df)} evaluations\n"
                if len(evalid_df) > 0:
                    # Show recent evaluations
                    recent = evalid_df.sort("END_INVYR", descending=True).head(10)
                    summary += "Recent evaluations:\n"
                    for row in recent.iter_rows(named=True):
                        summary += f"- EVALID {row['EVALID']}: {row['EVAL_DESCR']} (ends {row['END_INVYR']})\n"

                return summary
            except Exception as e:
                return f"Error getting EVALID info: {str(e)}"

        def find_species_codes(species_name: str) -> str:
            """Find species codes by common or scientific name."""
            try:
                # Clean the species name
                species_clean = species_name.replace("'", "''")  # Escape single quotes
                
                # Query to find species
                query = f"""
                SELECT SPCD, COMMON_NAME, SCIENTIFIC_NAME
                FROM REF_SPECIES
                WHERE LOWER(COMMON_NAME) LIKE LOWER('%{species_clean}%')
                   OR LOWER(SCIENTIFIC_NAME) LIKE LOWER('%{species_clean}%')
                ORDER BY 
                    CASE 
                        WHEN LOWER(COMMON_NAME) = LOWER('{species_clean}') THEN 1
                        WHEN LOWER(COMMON_NAME) LIKE LOWER('{species_clean}%') THEN 2
                        ELSE 3
                    END,
                    COMMON_NAME
                LIMIT 20
                """
                result = self.query_interface.execute_query(query)

                if len(result) == 0:
                    return f"No species found matching '{species_name}'"

                formatted = f"Species matches for '{species_name}':\n"
                count = 0
                for row in result.iter_rows(named=True):
                    formatted += f"- Code {row['SPCD']}: {row['COMMON_NAME']} ({row['SCIENTIFIC_NAME']})\n"
                    count += 1
                
                if count > 10:
                    formatted += f"\n(Showing first 10 of {len(result)} matches)"
                
                # Add hint for common searches
                if species_name.lower() == "oak":
                    formatted += "\nNote: 'Oak' includes many species. Common ones:\n"
                    formatted += "- White oak (802), Northern red oak (833), Chestnut oak (832)\n"
                    formatted += "- Live oak (838), Coast live oak (839), Canyon live oak (841)"
                elif species_name.lower() == "pine":
                    formatted += "\nNote: 'Pine' includes many species. Common ones:\n"
                    formatted += "- Loblolly pine (131), Longleaf pine (121), Ponderosa pine (122)"

                return formatted
            except Exception as e:
                return f"Error finding species: {str(e)}"

        def get_forest_type_info(fortypcd: int = None) -> str:
            """Get forest type information and descriptions."""
            try:
                if fortypcd:
                    query = f"""
                    SELECT FORTYPCD, MEANING, COMMON_NAME
                    FROM REF_FOREST_TYPE
                    WHERE FORTYPCD = {fortypcd}
                    """
                else:
                    query = """
                    SELECT FORTYPCD, MEANING, COMMON_NAME
                    FROM REF_FOREST_TYPE
                    ORDER BY FORTYPCD
                    LIMIT 20
                    """

                result = self.query_interface.execute_query(query)

                if len(result) == 0:
                    return (
                        f"No forest type found for code {fortypcd}"
                        if fortypcd
                        else "No forest types found"
                    )

                formatted = "Forest Type Information:\n"
                for row in result.iter_rows(named=True):
                    formatted += f"- Code {row['FORTYPCD']}: {row['MEANING']} ({row['COMMON_NAME']})\n"

                return formatted
            except Exception as e:
                return f"Error getting forest type info: {str(e)}"

        def get_survey_info(state_code: int = None) -> str:
            """Get survey and temporal information from SURVEY table."""
            try:
                if state_code:
                    query = f"""
                    SELECT
                        STATECD, STATENM, INVYR, CYCLE, SUBCYCLE,
                        ANN_INVENTORY, NOTES
                    FROM SURVEY
                    WHERE STATECD = {state_code}
                    ORDER BY INVYR DESC
                    LIMIT 10
                    """
                else:
                    query = """
                    SELECT
                        STATECD, STATENM,
                        MIN(INVYR) as earliest_year,
                        MAX(INVYR) as latest_year,
                        MAX(CYCLE) as latest_cycle,
                        COUNT(*) as survey_count
                    FROM SURVEY
                    GROUP BY STATECD, STATENM
                    ORDER BY latest_year DESC
                    LIMIT 15
                    """

                result = self.query_interface.execute_query(query)

                formatted = "Survey Information:\n"
                for row in result.iter_rows(named=True):
                    if state_code:
                        formatted += f"- {row['INVYR']}: Cycle {row['CYCLE']}.{row['SUBCYCLE']} - {row['ANN_INVENTORY']}\n"
                    else:
                        formatted += f"- {row['STATENM']}: {row['earliest_year']}-{row['latest_year']} (Cycle {row['latest_cycle']}, {row['survey_count']} surveys)\n"

                return formatted
            except Exception as e:
                return f"Error getting survey info: {str(e)}"

        def get_seedling_regeneration_info(species_name: str = None) -> str:
            """Get seedling/regeneration information from SEEDLING table."""
            try:
                if species_name:
                    query = f"""
                    SELECT
                        s.SPCD, rs.COMMON_NAME,
                        COUNT(*) as seedling_records,
                        SUM(s.TREECOUNT) as total_seedlings,
                        AVG(s.TREECOUNT) as avg_per_record
                    FROM SEEDLING s
                    LEFT JOIN REF_SPECIES rs ON s.SPCD = rs.SPCD
                    WHERE rs.COMMON_NAME LIKE '%{species_name}%'
                      AND s.TREECOUNT > 0
                    GROUP BY s.SPCD, rs.COMMON_NAME
                    ORDER BY total_seedlings DESC
                    LIMIT 10
                    """
                else:
                    query = """
                    SELECT
                        s.SPCD, rs.COMMON_NAME,
                        COUNT(*) as seedling_records,
                        SUM(s.TREECOUNT) as total_seedlings,
                        AVG(s.TREECOUNT) as avg_per_record
                    FROM SEEDLING s
                    LEFT JOIN REF_SPECIES rs ON s.SPCD = rs.SPCD
                    WHERE s.TREECOUNT > 0
                    GROUP BY s.SPCD, rs.COMMON_NAME
                    ORDER BY total_seedlings DESC
                    LIMIT 10
                    """

                result = self.query_interface.execute_query(query)

                formatted = "Seedling/Regeneration Information:\n"
                for row in result.iter_rows(named=True):
                    formatted += f"- {row['COMMON_NAME']}: {row['total_seedlings']:,.0f} seedlings ({row['seedling_records']:,} records)\n"

                return formatted
            except Exception as e:
                return f"Error getting seedling info: {str(e)}"

        def get_subplot_area_info(evalid: int = None) -> str:
            """Get subplot area information using SUBP_COND table."""
            try:
                if evalid:
                    query = f"""
                    SELECT
                        c.FORTYPCD,
                        COUNT(DISTINCT sc.PLT_CN) as plot_count,
                        SUM(sc.SUBPCOND_PROP) as total_subplot_area,
                        AVG(sc.SUBPCOND_PROP) as avg_subplot_proportion
                    FROM SUBP_COND sc
                    JOIN COND c ON sc.PLT_CN = c.PLT_CN AND sc.CONDID = c.CONDID
                    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON sc.PLT_CN = ppsa.PLT_CN
                    WHERE ppsa.EVALID = {evalid}
                      AND c.COND_STATUS_CD = 1
                      AND c.FORTYPCD IS NOT NULL
                    GROUP BY c.FORTYPCD
                    ORDER BY total_subplot_area DESC
                    LIMIT 10
                    """
                else:
                    query = """
                    SELECT
                        SUBP,
                        COUNT(DISTINCT PLT_CN) as plot_count,
                        COUNT(DISTINCT CONDID) as condition_count,
                        AVG(SUBPCOND_PROP) as avg_proportion
                    FROM SUBP_COND
                    GROUP BY SUBP
                    ORDER BY SUBP
                    """

                result = self.query_interface.execute_query(query)

                formatted = "Subplot Area Information:\n"
                for row in result.iter_rows(named=True):
                    if evalid:
                        formatted += f"- Forest Type {row['FORTYPCD']}: {row['plot_count']:,} plots, {row['total_subplot_area']:.1f} total area\n"
                    else:
                        formatted += f"- Subplot {row['SUBP']}: {row['plot_count']:,} plots, {row['condition_count']} conditions\n"

                return formatted
            except Exception as e:
                return f"Error getting subplot info: {str(e)}"

        def get_estimation_examples() -> str:
            """Get examples of common FIA estimation queries including new table capabilities."""
            examples = """
            Common FIA Query Patterns (Enhanced with New Tables):

            1. Trees per Acre by Species with EVALID:
            SELECT t.SPCD, rs.COMMON_NAME, SUM(t.TPA_UNADJ) as total_tpa
            FROM TREE t
            JOIN POP_PLOT_STRATUM_ASSGN psa ON t.PLT_CN = psa.PLT_CN
            LEFT JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD
            WHERE psa.EVALID = [EVALID] AND t.STATUSCD = 1
            GROUP BY t.SPCD, rs.COMMON_NAME

            2. Forest Area by Type using Subplot Conditions:
            SELECT c.FORTYPCD, SUM(sc.SUBPCOND_PROP * ps.EXPNS) as area_acres
            FROM SUBP_COND sc
            JOIN COND c ON sc.PLT_CN = c.PLT_CN AND sc.CONDID = c.CONDID
            JOIN POP_PLOT_STRATUM_ASSGN psa ON sc.PLT_CN = psa.PLT_CN
            JOIN POP_STRATUM ps ON psa.STRATUM_CN = ps.CN
            WHERE psa.EVALID = [EVALID] AND c.COND_STATUS_CD = 1
            GROUP BY c.FORTYPCD

            3. Regeneration Analysis with Seedlings:
            SELECT s.SPCD, rs.COMMON_NAME, SUM(s.TREECOUNT) as total_seedlings
            FROM SEEDLING s
            LEFT JOIN REF_SPECIES rs ON s.SPCD = rs.SPCD
            JOIN POP_PLOT_STRATUM_ASSGN psa ON s.PLT_CN = psa.PLT_CN
            WHERE psa.EVALID = [EVALID] AND s.TREECOUNT > 0
            GROUP BY s.SPCD, rs.COMMON_NAME

            4. Temporal Analysis with Survey Data:
            SELECT sv.INVYR, sv.CYCLE, COUNT(DISTINCT p.CN) as plot_count
            FROM SURVEY sv
            JOIN PLOT p ON sv.STATECD = p.STATECD
            WHERE sv.STATECD = [STATE] AND sv.INVYR >= 2010
            GROUP BY sv.INVYR, sv.CYCLE

            5. Subplot Status Analysis:
            SELECT sp.SUBP_STATUS_CD, COUNT(*) as subplot_count
            FROM SUBPLOT sp
            JOIN POP_PLOT_STRATUM_ASSGN psa ON sp.PLT_CN = psa.PLT_CN
            WHERE psa.EVALID = [EVALID]
            GROUP BY sp.SUBP_STATUS_CD

            Key Enhancements:
            - Use SUBP_COND for precise area calculations
            - Include SEEDLING data for regeneration analysis
            - Leverage SURVEY for temporal context
            - Use SUBPLOT for plot structure understanding
            - Always filter by EVALID for statistical validity
            """
            return examples

        def explain_fia_concept(concept_name: str) -> str:
            """Explain a specific FIA concept in detail."""
            # First try exact match
            concept = fia_knowledge.get_concept(concept_name)
            
            if not concept:
                # Try to find partial matches
                concepts = fia_knowledge.extract_concepts(concept_name)
                if concepts:
                    concept = concepts[0]
            
            if concept:
                return fia_knowledge.format_concept_help(concept.name)
            else:
                # Provide general help
                available_concepts = list(fia_knowledge.concepts.keys())
                return f"""
                Concept '{concept_name}' not found.
                
                Available FIA concepts include:
                {', '.join(available_concepts[:10])}...
                
                Try asking about specific terms like 'biomass', 'tpa', 'evalid', or 'forest type'.
                """

        return [
            Tool(
                name="execute_fia_query",
                description="Execute SQL query against FIA database. Use for data retrieval after query generation.",
                func=execute_fia_query,
            ),
            Tool(
                name="get_database_schema",
                description="Get FIA database schema and table information for query planning.",
                func=get_database_schema,
            ),
            Tool(
                name="get_evalid_info",
                description="Get information about available EVALIDs for statistical queries. Essential for proper FIA estimates.",
                func=get_evalid_info,
            ),
            Tool(
                name="find_species_codes",
                description="Find FIA species codes by common or scientific name.",
                func=find_species_codes,
            ),
            Tool(
                name="get_forest_type_info",
                description="Get forest type information and descriptions by FORTYPCD.",
                func=get_forest_type_info,
            ),
            Tool(
                name="get_survey_info",
                description="Get survey and temporal information from SURVEY table.",
                func=get_survey_info,
            ),
            Tool(
                name="get_seedling_regeneration_info",
                description="Get seedling/regeneration information from SEEDLING table.",
                func=get_seedling_regeneration_info,
            ),
            Tool(
                name="get_subplot_area_info",
                description="Get subplot area information using SUBP_COND table for precise area calculations.",
                func=get_subplot_area_info,
            ),
            Tool(
                name="get_estimation_examples",
                description="Get examples of common FIA estimation query patterns including new table capabilities.",
                func=get_estimation_examples,
            ),
            Tool(
                name="explain_fia_concept",
                description="Explain FIA forestry concepts like 'biomass', 'tpa', 'basal area', 'evalid', etc.",
                func=explain_fia_concept,
            ),
        ]

    def _build_workflow(self) -> StateGraph:
        """Build the LangGraph workflow for the agent."""

        # Define the enhanced system prompt with new table knowledge
        system_prompt = """You are an expert Forest Inventory Analysis (FIA) data scientist and SQL specialist.
        You help users query FIA databases using natural language by generating accurate, safe SQL queries.

        IMPORTANT: When users ask for counts or data (e.g., "how many trees"), you should:
        1. Generate the SQL query
        2. The system will execute it automatically
        3. Provide the actual numbers in your response
        
        For species queries:
        - "Oak" is not a single species - use COMMON_NAME LIKE '%oak%' to find all oaks
        - "Pine" is not a single species - use COMMON_NAME LIKE '%pine%' to find all pines
        - Always join with REF_SPECIES to get species names

        Enhanced FIA Knowledge (Now with Complete Table Set):
        - FIA data is organized around PLOTS, TREES, CONDITIONS, and POPULATION ESTIMATION tables
        - NEW: SURVEY table provides temporal context and inventory cycle information
        - NEW: SUBPLOT table contains subplot-level data and status information
        - NEW: SUBP_COND table links subplots to conditions for precise area calculations
        - NEW: SEEDLING table contains regeneration data for forest succession analysis
        - EVALID is crucial - it defines statistically valid plot groupings for estimates
        - Always use POP_PLOT_STRATUM_ASSGN to link plots to evaluations
        - Live trees have STATUSCD = 1, dead trees = 2
        - Forest conditions have COND_STATUS_CD = 1
        - Expansion factors (EXPNS) in POP_STRATUM are needed for area estimates
        - TPA_UNADJ provides trees per acre expansion for tree-level data
        - Species codes (SPCD) link to REF_SPECIES for names
        - Forest type codes (FORTYPCD) link to REF_FOREST_TYPE for descriptions
        - State code for California is 6

        Enhanced Analysis Capabilities:
        - Use SUBP_COND for more accurate area calculations than COND alone
        - Include SEEDLING data for regeneration and succession analysis
        - Leverage SURVEY data for temporal trends and cycle information
        - Use SUBPLOT for understanding plot structure and sampling design
        - Combine tables for comprehensive forest health assessments

        Safety Rules:
        - Only generate SELECT queries (no INSERT, UPDATE, DELETE)
        - Always include reasonable LIMIT clauses
        - Validate queries before execution
        - Explain statistical assumptions and limitations
        - Warn about small sample sizes

        When users ask about forest data, help them understand both the query and the forest science behind it.
        """

        def query_planner(state: AgentState) -> AgentState:
            """Plan the query based on user input."""
            last_message = state.messages[-1]
            query_text = last_message.content

            # Extract FIA concepts from the query
            concepts = fia_knowledge.extract_concepts(query_text)
            suggested_tables = fia_knowledge.suggest_tables(concepts)
            suggested_columns = fia_knowledge.suggest_columns(concepts)
            query_hints = fia_knowledge.get_query_hints(query_text)

            # Build enhanced context for LLM
            domain_context = "\n\nExtracted FIA Concepts:\n"
            for concept in concepts:
                domain_context += f"- {concept.name}: {concept.description}\n"
            
            if suggested_tables:
                domain_context += f"\nSuggested Tables: {', '.join(suggested_tables)}\n"
            
            if query_hints:
                domain_context += "\nQuery Hints:\n"
                for hint in query_hints[:3]:
                    domain_context += f"- {hint}\n"

            # Use LLM to understand the query intent
            planning_prompt = ChatPromptTemplate.from_messages(
                [
                    ("system", system_prompt + domain_context),
                    (
                        "human",
                        "Analyze this forest inventory query and create a plan: {query}",
                    ),
                ]
            )

            try:
                response = self.llm.invoke(
                    planning_prompt.format(query=last_message.content)
                )

                # Extract query intent with domain knowledge
                query_request = QueryRequest(
                    natural_language_query=last_message.content,
                    query_type="data_retrieval",  # Default
                    specific_tables=list(suggested_tables) if suggested_tables else None,
                    temporal_scope=None,
                    geographic_scope=None,
                )

                state.query_request = query_request
                state.messages.append(
                    AIMessage(content=f"Planning query: {response.content}\n{domain_context}")
                )

            except Exception as e:
                state.messages.append(
                    AIMessage(content=f"Error in query planning: {str(e)}")
                )

            return state

        def tool_user(state: AgentState) -> AgentState:
            """Use tools to gather information and execute queries."""
            last_message = state.messages[-1]
            query_lower = last_message.content.lower()

            # Extract concepts to help determine tools
            concepts = fia_knowledge.extract_concepts(last_message.content)
            concept_names = [c.name for c in concepts]

            # Determine which tools to use based on query
            tool_selections = []
            
            # Check for concept explanations
            if any(word in query_lower for word in ["what is", "explain", "define", "tell me about"]):
                if concepts:
                    tool_selections.append(("explain_fia_concept", concepts[0].name))
                else:
                    tool_selections.append(("get_database_schema", ""))
            
            # Check for specific information needs
            if "schema" in query_lower or "table" in query_lower:
                tool_selections.append(("get_database_schema", ""))
            
            if "species" in concept_names or "species" in query_lower:
                # Extract species name (simplified)
                tool_selections.append(("find_species_codes", last_message.content))
            
            if "evalid" in concept_names or "evalid" in query_lower:
                tool_selections.append(("get_evalid_info", ""))
            
            if "forest_type" in concept_names or "forest type" in query_lower:
                tool_selections.append(("get_forest_type_info", ""))
            
            if "seedlings" in concept_names or any(w in query_lower for w in ["seedling", "regeneration"]):
                tool_selections.append(("get_seedling_regeneration_info", ""))
            
            if any(w in query_lower for w in ["survey", "temporal", "time", "trend"]):
                tool_selections.append(("get_survey_info", ""))
            
            if "example" in query_lower or "pattern" in query_lower:
                tool_selections.append(("get_estimation_examples", ""))
            
            # Default to schema if no specific tools selected
            if not tool_selections:
                tool_selections.append(("get_database_schema", ""))

            # Execute selected tools (limit to first 2 to avoid overload)
            for tool_name, tool_input in tool_selections[:2]:
                try:
                    tool_result = None
                    for tool in self.tools:
                        if tool.name == tool_name:
                            tool_result = (
                                tool.func(tool_input) if tool_input else tool.func()
                            )
                            break

                    if tool_result:
                        state.tools_used.append(tool_name)
                        state.messages.append(
                            AIMessage(content=f"Tool {tool_name} result: {tool_result}")
                        )

                except Exception as e:
                    state.messages.append(
                        AIMessage(content=f"Error using tool {tool_name}: {str(e)}")
                    )

            return state

        def query_generator(state: AgentState) -> AgentState:
            """Generate SQL query based on gathered information."""
            # Combine all context
            context_parts = []
            for msg in state.messages:
                if isinstance(msg, AIMessage) and "Tool" in msg.content:
                    context_parts.append(msg.content)

            context = "\n".join(context_parts)
            user_query = (
                state.query_request.natural_language_query
                if state.query_request
                else state.messages[0].content
            )

            # Get domain-specific query hints
            domain_hints = fia_knowledge.get_query_hints(user_query)
            concepts = fia_knowledge.extract_concepts(user_query)
            
            # Build enhanced prompt with domain knowledge
            domain_guidance = "\n\nDomain-Specific Guidance:\n"
            if concepts:
                domain_guidance += "Identified concepts: " + ", ".join([c.name for c in concepts]) + "\n"
            if domain_hints:
                domain_guidance += "SQL Patterns:\n"
                for hint in domain_hints[:5]:
                    domain_guidance += f"- {hint}\n"

            query_prompt = ChatPromptTemplate.from_messages(
                [
                    ("system", system_prompt + "\n\nDatabase Context:\n" + context + domain_guidance),
                    (
                        "human",
                        """Generate a safe SQL query for this request: {query}

                Requirements:
                - Use proper FIA table relationships including new SURVEY, SUBPLOT, SUBP_COND, SEEDLING tables
                - Include appropriate EVALID filtering if needed for estimates
                - Add reasonable LIMIT clause
                - Apply proper expansion factors (TPA_UNADJ, EXPNS, adjustment factors)
                - Explain the query logic and any forest science concepts

                Format your response as:
                SQL: [your query]
                EXPLANATION: [explanation of what the query does and any limitations]
                """,
                    ),
                ]
            )

            try:
                response = self.llm.invoke(query_prompt.format(query=user_query))

                # Extract SQL (handle different formats)
                content = response.content
                sql_part = None
                explanation_part = ""
                
                # Try different SQL extraction patterns
                if "SQL:" in content:
                    sql_part = content.split("SQL:")[1].split("EXPLANATION:")[0].strip()
                    if "EXPLANATION:" in content:
                        explanation_part = content.split("EXPLANATION:")[1].strip()
                elif "```sql" in content.lower():
                    # Extract from markdown code block
                    import re
                    sql_match = re.search(r'```sql\s*(.*?)\s*```', content, re.DOTALL | re.IGNORECASE)
                    if sql_match:
                        sql_part = sql_match.group(1).strip()
                    # Get explanation after code block
                    parts = re.split(r'```sql.*?```', content, flags=re.DOTALL | re.IGNORECASE)
                    if len(parts) > 1:
                        explanation_part = parts[1].strip()
                elif "SELECT" in content.upper():
                    # Try to extract raw SQL
                    lines = content.split('\n')
                    sql_lines = []
                    in_sql = False
                    for line in lines:
                        if 'SELECT' in line.upper() and not in_sql:
                            in_sql = True
                        if in_sql:
                            if line.strip() and not line.startswith('*'):
                                sql_lines.append(line)
                            if ';' in line:
                                break
                    if sql_lines:
                        sql_part = '\n'.join(sql_lines)
                
                if sql_part:

                    # Validate the generated SQL semantically
                    warnings = fia_knowledge.validate_query_semantics(user_query, sql_part)
                    if warnings:
                        explanation_part += "\n\nWarnings:\n" + "\n".join(f"- {w}" for w in warnings)

                    state.sql_query = sql_part
                    state.messages.append(
                        AIMessage(
                            content=f"Generated SQL: {sql_part}\nExplanation: {explanation_part}"
                        )
                    )

            except Exception as e:
                state.messages.append(
                    AIMessage(content=f"Error generating query: {str(e)}")
                )

            return state

        def query_executor(state: AgentState) -> AgentState:
            """Execute the generated SQL query."""
            if self.config.verbose:
                print(f"[DEBUG] Query executor called. SQL query present: {bool(state.sql_query)}")
                
            if state.sql_query:
                try:
                    # Clean the SQL query
                    sql_query = state.sql_query.strip()
                    if sql_query.endswith('```'):
                        sql_query = sql_query[:-3].strip()
                    
                    if self.config.verbose:
                        print(f"[DEBUG] Executing SQL: {sql_query[:100]}...")
                    
                    # Execute using the query tool
                    tool_found = False
                    for tool in self.tools:
                        if tool.name == "execute_fia_query":
                            tool_found = True
                            if self.config.verbose:
                                print("[DEBUG] Found execute_fia_query tool")
                            result = tool.func(sql_query)
                            state.messages.append(
                                AIMessage(content=f"Query results: {result}")
                            )
                            break
                    
                    if not tool_found:
                        # Fallback: execute directly
                        if self.config.verbose:
                            print("[DEBUG] Tool not found, using direct execution")
                        result = self.query_interface.execute_query(sql_query, limit=self.config.result_limit)
                        formatted_result = self.query_interface.format_results_for_llm(result)
                        state.messages.append(
                            AIMessage(content=f"Query results: {formatted_result}")
                        )
                        
                except Exception as e:
                    error_msg = f"Error executing query: {str(e)}"
                    if self.config.verbose:
                        print(f"[DEBUG] {error_msg}")
                        import traceback
                        traceback.print_exc()
                    state.messages.append(
                        AIMessage(content=error_msg)
                    )
            else:
                if self.config.verbose:
                    print("[DEBUG] No SQL query to execute")

            return state

        def response_formatter(state: AgentState) -> AgentState:
            """Format the final response for the user."""
            # Combine all information into a comprehensive response
            final_response = "## Enhanced Forest Inventory Analysis Results\n\n"

            # Extract concepts from the original query
            user_query = state.messages[0].content if state.messages else ""
            concepts = fia_knowledge.extract_concepts(user_query)

            if state.sql_query:
                final_response += (
                    f"**Generated Query:**\n```sql\n{state.sql_query}\n```\n\n"
                )

            # Look for query results and errors in all messages
            query_results_found = False
            error_found = False
            
            for msg in reversed(state.messages):
                if isinstance(msg, AIMessage):
                    if "Query results:" in msg.content:
                        results_part = msg.content.split("Query results:")[1].strip()
                        final_response += f"**Results:**\n{results_part}\n\n"
                        query_results_found = True
                        break
                    elif "Error executing query:" in msg.content or "Query execution failed:" in msg.content:
                        final_response += f"**Error:** {msg.content}\n\n"
                        error_found = True
                        break
            
            # If no results but we have a query, check why
            if state.sql_query and not query_results_found and not error_found:
                final_response += "**Note:** Query generated but results not available. This might be a workflow issue.\n\n"
                
                # Debug info if verbose
                if self.config.verbose:
                    final_response += "**Debug Info:**\n"
                    final_response += f"- Total messages: {len(state.messages)}\n"
                    final_response += f"- Tools used: {state.tools_used}\n"
                    final_response += f"- SQL query length: {len(state.sql_query)}\n\n"

            # Add concept explanations if relevant
            if concepts and len(concepts) <= 3:
                final_response += "**Key Concepts Used:**\n"
                for concept in concepts:
                    final_response += f"- **{concept.name.replace('_', ' ').title()}**: {concept.description}\n"
                final_response += "\n"

            # Add enhanced context about new capabilities
            final_response += "**Enhanced Capabilities Note:** This analysis leverages the complete FIA database including "
            final_response += "SURVEY (temporal context), SUBPLOT (plot structure), SUBP_COND (precise area calculations), "
            final_response += "and SEEDLING (regeneration data) tables for comprehensive forest inventory analysis.\n\n"

            final_response += "**Statistical Note:** FIA data requires proper statistical interpretation. Consider consulting with a forest biometrician for population estimates."

            state.final_response = final_response
            state.messages.append(AIMessage(content=final_response))

            return state

        # Build the workflow graph
        workflow = StateGraph(AgentState)

        # Add nodes
        workflow.add_node("planner", query_planner)
        workflow.add_node("tool_user", tool_user)
        workflow.add_node("query_generator", query_generator)
        workflow.add_node("query_executor", query_executor)
        workflow.add_node("response_formatter", response_formatter)

        # Add edges
        workflow.set_entry_point("planner")
        workflow.add_edge("planner", "tool_user")
        workflow.add_edge("tool_user", "query_generator")
        workflow.add_edge("query_generator", "query_executor")
        workflow.add_edge("query_executor", "response_formatter")
        workflow.add_edge("response_formatter", END)

        return workflow.compile()

    def query(self, user_input: str) -> str:
        """
        Process a natural language query about forest inventory data.

        Args:
            user_input: Natural language query from user

        Returns:
            Formatted response with query results and explanations
        """
        # Initialize state
        initial_state = AgentState(
            messages=[HumanMessage(content=user_input)], tools_used=[]
        )

        try:
            # Run the workflow
            final_state = self.workflow.invoke(initial_state)
            
            # Handle different return types from langgraph
            if hasattr(final_state, 'final_response'):
                return final_state.final_response or "I encountered an error processing your query."
            elif isinstance(final_state, dict) and 'final_response' in final_state:
                return final_state['final_response'] or "I encountered an error processing your query."
            elif isinstance(final_state, dict) and 'messages' in final_state:
                # Extract the last AI message
                messages = final_state['messages']
                for msg in reversed(messages):
                    if isinstance(msg, AIMessage) and msg.content:
                        return msg.content
                return "Query processed but no response generated."
            else:
                return f"Unexpected response format: {type(final_state)}"
        except Exception as e:
            return f"Error processing query: {str(e)}"

    def get_available_evaluations(
        self, state_code: Optional[int] = None
    ) -> pl.DataFrame:
        """Get available evaluations for query planning."""
        return self.query_interface.get_evalid_info(state_code)

    def validate_query(self, sql_query: str) -> Dict[str, Any]:
        """Validate a SQL query before execution."""
        return self.query_interface.validate_query(sql_query)


# Convenience function for quick agent creation
def create_fia_agent(
    db_path: Union[str, Path], api_key: Optional[str] = None, **config_kwargs
) -> FIAAgent:
    """
    Create a FIA AI Agent with optional configuration.

    Args:
        db_path: Path to FIA DuckDB database
        api_key: OpenAI API key
        **config_kwargs: Configuration options for FIAAgentConfig

    Returns:
        Configured FIAAgent instance
    """
    config = FIAAgentConfig(**config_kwargs)
    return FIAAgent(db_path, config, api_key)
