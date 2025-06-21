"""
Advanced AI Agent for Forest Inventory Analysis using LangGraph.

This module implements a cutting-edge AI agent that specializes in querying
FIA (Forest Inventory Analysis) databases using natural language. It leverages
LangGraph for structured agent workflows and incorporates deep domain knowledge
about forest inventory data structures and statistical methodologies.
"""

from typing import Optional, Dict, Any, List, Union, Annotated
from dataclasses import dataclass, field
from pathlib import Path
import json
import re
import os

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not available, skip loading
    pass

# LangChain/LangGraph imports
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.tools import Tool
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

import polars as pl
from .duckdb_query_interface import DuckDBQueryInterface
from .core import FIA


# Pydantic models for structured outputs
class QueryRequest(BaseModel):
    """Structured query request from user."""
    natural_language_query: str = Field(description="User's natural language query about forest data")
    query_type: str = Field(description="Type of query: 'data_retrieval', 'analysis', 'schema_info'")
    specific_tables: Optional[List[str]] = Field(description="Specific tables mentioned or inferred")
    temporal_scope: Optional[str] = Field(description="Time period mentioned (e.g., 'recent', '2020', 'latest')")
    geographic_scope: Optional[str] = Field(description="Geographic area mentioned (state, region, etc.)")


class QueryResult(BaseModel):
    """Structured query result."""
    sql_query: str = Field(description="Generated SQL query")
    explanation: str = Field(description="Human-readable explanation of what the query does")
    results_summary: str = Field(description="Summary of query results")
    data_preview: Optional[str] = Field(description="Preview of returned data")
    warnings: Optional[List[str]] = Field(description="Any warnings about data interpretation")


class AgentState(BaseModel):
    """State for the LangGraph agent."""
    model_config = {"arbitrary_types_allowed": True}
    
    messages: Annotated[List[Union[HumanMessage, AIMessage, SystemMessage]], add_messages]
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
    
    def __init__(self, 
                 db_path: Union[str, Path],
                 config: Optional[FIAAgentConfig] = None,
                 api_key: Optional[str] = None):
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
        
        # Initialize LLM
        llm_kwargs = {
            "model": self.config.model_name, 
            "temperature": self.config.temperature
        }
        
        # Set API key from parameter, environment, or .env file
        if api_key:
            llm_kwargs["api_key"] = api_key
        elif os.environ.get('OPENAI_API_KEY'):
            llm_kwargs["api_key"] = os.environ.get('OPENAI_API_KEY')
        
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
            try:
                limit = limit or self.config.result_limit
                result = self.query_interface.execute_query(query, limit=limit)
                
                # Format results for LLM
                formatted = self.query_interface.format_results_for_llm(result)
                return formatted
            except Exception as e:
                return f"Query execution failed: {str(e)}"
        
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
                    evalid_df = evalid_df.filter(pl.col('STATECD') == state_code)
                
                summary = f"Available EVALIDs: {len(evalid_df)} evaluations\n"
                if len(evalid_df) > 0:
                    # Show recent evaluations
                    recent = evalid_df.sort('END_INVYR', descending=True).head(10)
                    summary += "Recent evaluations:\n"
                    for row in recent.iter_rows(named=True):
                        summary += f"- EVALID {row['EVALID']}: {row['EVAL_DESCR']} (ends {row['END_INVYR']})\n"
                
                return summary
            except Exception as e:
                return f"Error getting EVALID info: {str(e)}"
        
        def find_species_codes(species_name: str) -> str:
            """Find species codes by common or scientific name."""
            try:
                # Simple query to find species
                query = f"""
                SELECT SPCD, COMMON_NAME, SCIENTIFIC_NAME 
                FROM REF_SPECIES 
                WHERE LOWER(COMMON_NAME) LIKE LOWER('%{species_name}%')
                   OR LOWER(SCIENTIFIC_NAME) LIKE LOWER('%{species_name}%')
                LIMIT 10
                """
                result = self.query_interface.execute_query(query)
                
                if len(result) == 0:
                    return f"No species found matching '{species_name}'"
                
                formatted = "Species matches:\n"
                for row in result.iter_rows(named=True):
                    formatted += f"- Code {row['SPCD']}: {row['COMMON_NAME']} ({row['SCIENTIFIC_NAME']})\n"
                
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
                    return f"No forest type found for code {fortypcd}" if fortypcd else "No forest types found"
                
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
        
        return [
            Tool(
                name="execute_fia_query",
                description="Execute SQL query against FIA database. Use for data retrieval after query generation.",
                func=execute_fia_query
            ),
            Tool(
                name="get_database_schema", 
                description="Get FIA database schema and table information for query planning.",
                func=get_database_schema
            ),
            Tool(
                name="get_evalid_info",
                description="Get information about available EVALIDs for statistical queries. Essential for proper FIA estimates.",
                func=get_evalid_info
            ),
            Tool(
                name="find_species_codes",
                description="Find FIA species codes by common or scientific name.",
                func=find_species_codes
            ),
            Tool(
                name="get_forest_type_info",
                description="Get forest type information and descriptions by FORTYPCD.",
                func=get_forest_type_info
            ),
            Tool(
                name="get_survey_info",
                description="Get survey and temporal information from SURVEY table.",
                func=get_survey_info
            ),
            Tool(
                name="get_seedling_regeneration_info",
                description="Get seedling/regeneration information from SEEDLING table.",
                func=get_seedling_regeneration_info
            ),
            Tool(
                name="get_subplot_area_info",
                description="Get subplot area information using SUBP_COND table for precise area calculations.",
                func=get_subplot_area_info
            ),
            Tool(
                name="get_estimation_examples",
                description="Get examples of common FIA estimation query patterns including new table capabilities.",
                func=get_estimation_examples
            )
        ]
    
    def _build_workflow(self) -> StateGraph:
        """Build the LangGraph workflow for the agent."""
        
        # Define the enhanced system prompt with new table knowledge
        system_prompt = """You are an expert Forest Inventory Analysis (FIA) data scientist and SQL specialist. 
        You help users query FIA databases using natural language by generating accurate, safe SQL queries.

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
            
            # Use LLM to understand the query intent
            planning_prompt = ChatPromptTemplate.from_messages([
                ("system", system_prompt),
                ("human", "Analyze this forest inventory query and create a plan: {query}")
            ])
            
            try:
                response = self.llm.invoke(planning_prompt.format(query=last_message.content))
                
                # Extract query intent (simplified - could use structured output)
                query_request = QueryRequest(
                    natural_language_query=last_message.content,
                    query_type="data_retrieval",  # Default
                    specific_tables=None,
                    temporal_scope=None,
                    geographic_scope=None
                )
                
                state.query_request = query_request
                state.messages.append(AIMessage(content=f"Planning query: {response.content}"))
                
            except Exception as e:
                state.messages.append(AIMessage(content=f"Error in query planning: {str(e)}"))
            
            return state
        
        def tool_user(state: AgentState) -> AgentState:
            """Use tools to gather information and execute queries."""
            last_message = state.messages[-1]
            
            # Determine which tools to use based on query
            if "schema" in last_message.content.lower() or "table" in last_message.content.lower():
                tool_name = "get_database_schema"
                tool_input = ""
            elif "species" in last_message.content.lower():
                # Extract species name (simplified)
                tool_name = "find_species_codes"
                tool_input = last_message.content  # Could be more sophisticated
            elif "evalid" in last_message.content.lower():
                tool_name = "get_evalid_info"
                tool_input = ""
            elif "forest type" in last_message.content.lower():
                tool_name = "get_forest_type_info"
                tool_input = ""
            elif "seedling" in last_message.content.lower() or "regeneration" in last_message.content.lower():
                tool_name = "get_seedling_regeneration_info"
                tool_input = ""
            elif "survey" in last_message.content.lower() or "temporal" in last_message.content.lower():
                tool_name = "get_survey_info"
                tool_input = ""
            elif "subplot" in last_message.content.lower() or "area" in last_message.content.lower():
                tool_name = "get_subplot_area_info"
                tool_input = ""
            else:
                # Default to getting schema first
                tool_name = "get_database_schema"
                tool_input = ""
            
            # Execute tool
            tool_result = None
            try:
                for tool in self.tools:
                    if tool.name == tool_name:
                        tool_result = tool.func(tool_input) if tool_input else tool.func()
                        break
                
                if tool_result:
                    state.tools_used.append(tool_name)
                    state.messages.append(AIMessage(content=f"Tool {tool_name} result: {tool_result}"))
                    
            except Exception as e:
                state.messages.append(AIMessage(content=f"Error using tool {tool_name}: {str(e)}"))
            
            return state
        
        def query_generator(state: AgentState) -> AgentState:
            """Generate SQL query based on gathered information."""
            # Combine all context
            context_parts = []
            for msg in state.messages:
                if isinstance(msg, AIMessage) and "Tool" in msg.content:
                    context_parts.append(msg.content)
            
            context = "\n".join(context_parts)
            user_query = state.query_request.natural_language_query if state.query_request else state.messages[0].content
            
            query_prompt = ChatPromptTemplate.from_messages([
                ("system", system_prompt + "\n\nDatabase Context:\n" + context),
                ("human", """Generate a safe SQL query for this request: {query}
                
                Requirements:
                - Use proper FIA table relationships including new SURVEY, SUBPLOT, SUBP_COND, SEEDLING tables
                - Include appropriate EVALID filtering if needed for estimates
                - Add reasonable LIMIT clause
                - Explain the query logic and any forest science concepts
                
                Format your response as:
                SQL: [your query]
                EXPLANATION: [explanation of what the query does and any limitations]
                """)
            ])
            
            try:
                response = self.llm.invoke(query_prompt.format(query=user_query))
                
                # Extract SQL (simplified parsing)
                content = response.content
                if "SQL:" in content:
                    sql_part = content.split("SQL:")[1].split("EXPLANATION:")[0].strip()
                    explanation_part = content.split("EXPLANATION:")[1].strip() if "EXPLANATION:" in content else ""
                    
                    state.sql_query = sql_part
                    state.messages.append(AIMessage(content=f"Generated SQL: {sql_part}\nExplanation: {explanation_part}"))
                    
            except Exception as e:
                state.messages.append(AIMessage(content=f"Error generating query: {str(e)}"))
            
            return state
        
        def query_executor(state: AgentState) -> AgentState:
            """Execute the generated SQL query."""
            if state.sql_query:
                try:
                    # Execute using the query tool
                    for tool in self.tools:
                        if tool.name == "execute_fia_query":
                            result = tool.func(state.sql_query)
                            state.messages.append(AIMessage(content=f"Query results: {result}"))
                            break
                except Exception as e:
                    state.messages.append(AIMessage(content=f"Error executing query: {str(e)}"))
            
            return state
        
        def response_formatter(state: AgentState) -> AgentState:
            """Format the final response for the user."""
            # Combine all information into a comprehensive response
            final_response = "## Enhanced Forest Inventory Analysis Results\n\n"
            
            if state.sql_query:
                final_response += f"**Generated Query:**\n```sql\n{state.sql_query}\n```\n\n"
            
            # Add results from the last message
            if state.messages and isinstance(state.messages[-1], AIMessage):
                last_result = state.messages[-1].content
                if "Query results:" in last_result:
                    results_part = last_result.split("Query results:")[1].strip()
                    final_response += f"**Results:**\n{results_part}\n\n"
            
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
            messages=[HumanMessage(content=user_input)],
            tools_used=[]
        )
        
        try:
            # Run the workflow
            final_state = self.workflow.invoke(initial_state)
            return final_state.final_response or "I encountered an error processing your query."
        except Exception as e:
            return f"Error processing query: {str(e)}"
    
    def get_available_evaluations(self, state_code: Optional[int] = None) -> pl.DataFrame:
        """Get available evaluations for query planning."""
        return self.query_interface.get_evalid_info(state_code)
    
    def validate_query(self, sql_query: str) -> Dict[str, Any]:
        """Validate a SQL query before execution."""
        return self.query_interface.validate_query(sql_query)


# Convenience function for quick agent creation
def create_fia_agent(db_path: Union[str, Path], 
                     api_key: Optional[str] = None,
                     **config_kwargs) -> FIAAgent:
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