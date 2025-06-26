"""
AI Agent for Forest Inventory Analysis using 2025 LangChain patterns.

This module implements a streamlined FIA agent using LangGraph's create_react_agent
pattern with built-in memory, tool calling, and human-in-the-loop capabilities.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime

# LangChain imports
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
# Removed unused import

# Local imports
from ..database.query_interface import DuckDBQueryInterface
from ..core import FIA

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class FIAAgent:
    """
    AI Agent for Forest Inventory Analysis using LangGraph's create_react_agent pattern.
    
    This implementation follows 2025 best practices:
    - Simple tool functions with clear docstrings
    - Built-in memory and persistence
    - Human-in-the-loop capabilities
    - Streamlined architecture
    """
    
    def __init__(
        self,
        db_path: Union[str, Path],
        api_key: Optional[str] = None,
        model_name: str = "gpt-4-turbo-preview",
        temperature: float = 0,
        verbose: bool = False,
        enable_human_approval: bool = False,
        checkpoint_dir: Optional[str] = None,
    ):
        """
        Initialize the modern FIA agent.
        
        Args:
            db_path: Path to FIA DuckDB database
            api_key: OpenAI API key (uses env var if not provided)
            model_name: LLM model to use
            temperature: LLM temperature
            verbose: Enable debug output
            enable_human_approval: Require human approval for tool calls
            checkpoint_dir: Directory for conversation persistence
        """
        self.db_path = Path(db_path)
        self.verbose = verbose
        self.enable_human_approval = enable_human_approval
        
        # Initialize database interfaces
        self.query_interface = DuckDBQueryInterface(db_path)
        self.fia = FIA(str(db_path))
        
        # Initialize LLM
        api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key required")
            
        self.llm = ChatOpenAI(
            model=model_name,
            temperature=temperature,
            api_key=api_key,
        )
        
        # Create checkpointer for memory
        # Note: Using MemorySaver for now, can upgrade to persistent storage later
        self.checkpointer = MemorySaver()
        
        # Create the agent
        self.agent = self._create_agent()
        
    def _create_agent(self):
        """Create the ReAct agent with tools."""
        
        # Define tools as simple functions
        def execute_fia_query(query: str, limit: int = 1000) -> str:
            """
            Execute a SQL query against the FIA database.
            
            Args:
                query: SQL query to execute
                limit: Maximum number of results to return
                
            Returns:
                Formatted query results or error message
            """
            if self.verbose:
                print(f"[DEBUG] Executing query: {query[:100]}...")
                
            try:
                # Clean query if it has markdown formatting
                query = query.strip()
                if query.startswith('```sql'):
                    query = query[6:].strip()
                elif query.startswith('```'):
                    query = query[3:].strip()
                if query.endswith('```'):
                    query = query[:-3].strip()
                    
                result = self.query_interface.execute_query(query, limit=limit)
                
                if self.verbose:
                    print(f"[DEBUG] Query returned {len(result)} rows")
                    
                return self.query_interface.format_results_for_llm(result)
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                return f"""Query execution failed for: {query}

Error details:
{str(e)}

Full traceback:
{error_details}

This error occurred while trying to execute a SQL query against the FIA database. Please check the query syntax and database connection."""
        
        def get_database_schema(table_name: Optional[str] = None) -> str:
            """
            Get database schema information.
            
            Args:
                table_name: Specific table name, or None for all tables
                
            Returns:
                Schema information as formatted text
            """
            try:
                if table_name:
                    return self.query_interface.get_table_summary(table_name)
                else:
                    return self.query_interface.get_natural_language_context()
            except Exception as e:
                return f"Error getting schema: {str(e)}"
        
        def find_species_codes(species_name: str) -> str:
            """
            Find species codes by common or scientific name.
            
            Args:
                species_name: Common or scientific name to search
                
            Returns:
                Matching species with codes
            """
            try:
                species_clean = species_name.replace("'", "''")
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
                for row in result.head(10).iter_rows(named=True):
                    formatted += f"- Code {row['SPCD']}: {row['COMMON_NAME']} ({row['SCIENTIFIC_NAME']})\n"
                
                if len(result) > 10:
                    formatted += f"\n(Showing first 10 of {len(result)} matches)"
                    
                return formatted
            except Exception as e:
                return f"Error finding species: {str(e)}"
        
        def get_evalid_info(state_code: Optional[int] = None, eval_type: Optional[str] = None) -> str:
            """
            Get FIA evaluation information with intelligent prioritization.
            
            Args:
                state_code: Optional state FIPS code to filter by
                eval_type: Optional evaluation type (EXPVOL, EXPCURR, etc.)
                
            Returns:
                Evaluation information with IDs and descriptions, prioritizing statewide over regional evaluations
            """
            try:
                from ..filters.evalid import get_evalid_info as get_evalid_info_impl
                return get_evalid_info_impl(self.query_interface, state_code, eval_type)
            except Exception as e:
                return f"Error getting EVALID info: {str(e)}"
        
        def get_state_codes() -> str:
            """
            Get list of state codes and names in the database.
            
            Returns:
                List of states with their FIPS codes
            """
            try:
                query = """
                SELECT DISTINCT 
                    VALUE AS STATECD,
                    MEANING AS STATE_NAME
                FROM REF_STATECD
                ORDER BY STATE_NAME
                """
                result = self.query_interface.execute_query(query)
                
                formatted = "State Codes in Database:\n"
                for row in result.iter_rows(named=True):
                    formatted += f"- {row['STATE_NAME']}: {row['STATECD']}\n"
                
                return formatted
            except Exception as e:
                return f"Error getting state codes: {str(e)}"
        
        def parse_location_from_query(query_text: str) -> str:
            """
            Parse and resolve locations from natural language query text.
            
            Args:
                query_text: User's natural language query
            
            Returns:
                Location information and suggested area_domain filters
            """
            try:
                from ..locations import LocationParser, LocationResolver
                
                parser = LocationParser()
                resolver = LocationResolver()
                
                # Find primary location in the query
                primary_location = parser.find_primary_location(query_text)
                
                if not primary_location:
                    return "No locations detected in query. If you meant to specify a location, please be more explicit (e.g., 'in North Carolina', 'in Texas', etc.)"
                
                # Resolve the location to get identifiers
                resolved_location = resolver.resolve(primary_location)
                
                if resolved_location.state_code:
                    domain_filter = resolved_location.to_domain_filter()
                    state_name = resolver.get_state_name(resolved_location.state_code)
                    
                    return f"""Detected location: {resolved_location.raw_text}
Resolved to: {state_name} (FIPS Code: {resolved_location.state_code})
Confidence: {resolved_location.confidence:.1%}
Suggested area_domain: "{domain_filter}"

Use this area_domain parameter in your tree count or area estimation commands."""
                else:
                    return f"Could not resolve location '{resolved_location.raw_text}' to a valid FIPS code. Please check the spelling or try a different format."
                    
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                return f"Error parsing location from query: {str(e)}\n\nFull traceback:\n{error_details}"
        
        def get_recommended_evalid(state_code: int, analysis_type: str = "tree_count") -> str:
            """
            Get recommended EVALID for a specific state and analysis type.
            
            Args:
                state_code: FIPS state code (e.g., 48 for Texas)
                analysis_type: Type of analysis (tree_count, volume, biomass, etc.)
                
            Returns:
                Recommended EVALID with explanation
            """
            try:
                from ..filters.evalid import get_recommended_evalid as get_rec_evalid
                evalid, explanation = get_rec_evalid(self.query_interface, state_code, analysis_type)
                
                if evalid:
                    return f"Recommended EVALID: {evalid}\n\n{explanation}"
                else:
                    return explanation
            except Exception as e:
                return f"Error getting recommended EVALID: {str(e)}"
        
        def explain_domain_filters(
            tree_type: str = "all",
            land_type: str = "forest",
            tree_domain: Optional[str] = None,
            area_domain: Optional[str] = None,
            query_context: Optional[str] = None,
        ) -> str:
            """
            Explain what filtering assumptions are being made for transparency.
            
            Args:
                tree_type: Tree type filter (e.g., "live", "gs", "auto")
                land_type: Land type filter (e.g., "forest", "timber", "auto")
                tree_domain: Custom tree filter expression
                area_domain: Custom area filter expression
                query_context: Original user query for context
                
            Returns:
                Human-readable explanation of all filtering assumptions
            """
            try:
                from ..filters.domain import create_domain_explanation
                return create_domain_explanation(
                    tree_type, land_type, tree_domain, area_domain, query_context
                )
            except Exception as e:
                return f"Error explaining domain filters: {str(e)}"
        
        def suggest_domain_options(analysis_type: str = "general") -> str:
            """
            Suggest common domain filter options for different analysis types.
            
            Args:
                analysis_type: Type of analysis to suggest for ("volume", "biomass", "mortality", "area")
                
            Returns:
                Formatted suggestions for domain filters
            """
            try:
                from ..filters.domain import suggest_common_domains
                suggestions = suggest_common_domains(analysis_type)
                
                formatted = f"Common domain filter suggestions for {analysis_type} analysis:\n\n"
                
                if suggestions["tree_types"]:
                    formatted += f"Recommended tree types: {', '.join(suggestions['tree_types'])}\n"
                
                if suggestions["land_types"]:
                    formatted += f"Recommended land types: {', '.join(suggestions['land_types'])}\n"
                
                if suggestions["tree_domains"]:
                    formatted += "\nCommon tree domain filters:\n"
                    for domain in suggestions["tree_domains"]:
                        formatted += f"  • {domain}\n"
                
                if suggestions["area_domains"]:
                    formatted += "\nCommon area domain filters:\n"
                    for domain in suggestions["area_domains"]:
                        formatted += f"  • {domain}\n"
                
                return formatted
                
            except Exception as e:
                return f"Error getting domain suggestions: {str(e)}"

        def execute_area_command(command_args: str) -> str:
            """
            Execute area estimation commands using the estimation interface.
            
            Args:
                command_args: CLI-style arguments (e.g., "byLandType landType=forest totals=true stateCode=27")
                
            Returns:
                Formatted area estimation results with enhanced statistical context
            """
            try:
                # Parse CLI arguments into kwargs
                kwargs = {}
                state_code = None
                evalid = None
                
                # Handle boolean flags
                if "byLandType" in command_args:
                    kwargs["by_land_type"] = True
                if "totals" in command_args:
                    kwargs["totals"] = True
                if "variance" in command_args:
                    kwargs["variance"] = True
                
                # Parse key=value pairs
                import shlex
                try:
                    parts = shlex.split(command_args)
                except ValueError:
                    parts = command_args.split()
                
                for part in parts:
                    if "=" in part:
                        key, value = part.split("=", 1)
                        # Convert camelCase to snake_case for consistency
                        if key == "treeDomain":
                            key = "tree_domain"
                        elif key == "areaDomain":
                            key = "area_domain"
                        elif key == "landType":
                            key = "land_type"
                        elif key == "grpBy":
                            key = "grp_by"
                            # Handle list values
                            if value.startswith("[") and value.endswith("]"):
                                value = value[1:-1].split(",")
                                value = [v.strip().strip('"\'') for v in value]
                        elif key == "byLandType":
                            key = "by_land_type"
                            value = value.lower() in ('true', '1', 'yes', 'on')
                        elif key == "totals":
                            key = "totals"
                            value = value.lower() in ('true', '1', 'yes', 'on')
                        elif key == "variance":
                            key = "variance"
                            value = value.lower() in ('true', '1', 'yes', 'on')
                        elif key == "stateCode":
                            state_code = int(value)
                            continue  # Don't add to kwargs
                        elif key == "evalid":
                            evalid = int(value)
                            continue  # Don't add to kwargs
                        
                        # Handle quoted strings
                        if isinstance(value, str):
                            if value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            elif value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                            else:
                                # Try to convert to appropriate type
                                if value.lower() in ('true', 'false'):
                                    value = value.lower() == 'true'
                                else:
                                    try:
                                        value = int(value)
                                    except ValueError:
                                        try:
                                            value = float(value)
                                        except ValueError:
                                            pass  # Keep as string
                        kwargs[key] = value
                
                # Handle EVALID setup - get recommended EVALID if not provided
                if evalid is None and state_code is not None:
                    from ..filters.evalid import get_recommended_evalid as get_rec_evalid
                    recommended_evalid, explanation = get_rec_evalid(self.query_interface, state_code, "area")
                    if recommended_evalid:
                        evalid = recommended_evalid
                
                # Apply EVALID filter if specified
                if evalid is not None:
                    self.fia.clip_by_evalid(evalid)
                elif not self.fia.evalid:
                    return "Error: No EVALID specified and unable to determine appropriate EVALID. Please specify stateCode or evalid parameter."
                
                # Determine which area function to use based on complexity
                use_workflow = (
                    kwargs.get("variance", False) or  # Enhanced features
                    len(kwargs.get("grp_by", [])) > 3 or  # Complex grouping
                    (kwargs.get("tree_domain") and len(kwargs.get("tree_domain", "")) > 50) or  # Complex tree domain
                    (kwargs.get("area_domain") and len(kwargs.get("area_domain", "")) > 50)     # Complex area domain
                )
                
                if use_workflow:
                    # Use advanced workflow for complex queries
                    from ..estimation.area_workflow import area_workflow
                    result = area_workflow(self.fia, **kwargs)
                    calculation_method = "Advanced Workflow"
                else:
                    # Use basic area function for simple queries
                    from ..estimation.area import area
                    result = area(self.fia, **kwargs)
                    calculation_method = "Core Function"
                
                if len(result) == 0:
                    return "No area found matching the specified criteria."
                
                # Enhanced result formatting
                return self._format_area_results_enhanced(result, kwargs, calculation_method)
                
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                return f"""Error executing area command with arguments: {command_args}

Parsed parameters: {kwargs}
State code: {state_code}
EVALID: {evalid}

Error details:
{str(e)}

Full traceback:
{error_details}

This error occurred while trying to execute an area estimation query. Please check the parameters and database connection."""
        
        def execute_tree_command(command_args: str) -> str:
            """
            Execute tree count commands using the CLI interface.
            
            This function automatically handles EVALID selection by:
            1. Parsing area_domain for state codes
            2. Using get_recommended_evalid for proper EVALID selection
            3. Setting the EVALID on the FIA instance before calling tree_count
            
            Args:
                command_args: CLI-style arguments (e.g., "bySpecies treeType=live area_domain=\"STATECD == 37\"")
                
            Returns:
                Formatted tree count results with enhanced statistical context
            """
            try:
                # Parse CLI arguments into kwargs
                kwargs = {}
                
                # Handle boolean flags
                if "bySpecies" in command_args:
                    kwargs["by_species"] = True
                if "bySizeClass" in command_args:
                    kwargs["by_size_class"] = True
                
                # Parse key=value pairs
                import shlex
                try:
                    parts = shlex.split(command_args)
                except ValueError:
                    parts = command_args.split()
                
                for part in parts:
                    if "=" in part:
                        key, value = part.split("=", 1)
                        # Convert camelCase to snake_case for consistency
                        if key == "treeDomain":
                            key = "tree_domain"
                        elif key == "areaDomain":
                            key = "area_domain"
                        elif key == "treeType":
                            key = "tree_type"
                        elif key == "landType":
                            key = "land_type"
                        elif key == "grpBy":
                            key = "grp_by"
                        
                        # Handle quoted strings
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        else:
                            # Try to convert to appropriate type
                            try:
                                value = int(value)
                            except ValueError:
                                try:
                                    value = float(value)
                                except ValueError:
                                    pass  # Keep as string
                        kwargs[key] = value
                
                # Always include totals for population estimates
                kwargs["totals"] = True
                
                # Handle EVALID selection before calling tree_count
                if not self.fia.evalid and kwargs.get("area_domain"):
                    # Extract state code from area_domain
                    import re
                    area_domain = kwargs["area_domain"]
                    state_match = re.search(r'STATECD\s*==\s*(\d+)', area_domain)
                    if state_match:
                        state_code = int(state_match.group(1))
                        # Get recommended EVALID
                        from ..filters.evalid import get_recommended_evalid
                        recommended_evalid, explanation = get_recommended_evalid(
                            self.query_interface, state_code, "tree_count"
                        )
                        if recommended_evalid:
                            self.fia.clip_by_evalid(recommended_evalid)
                            if self.verbose:
                                print(f"[DEBUG] Auto-selected EVALID {recommended_evalid}: {explanation}")
                
                # Execute tree count through the estimation module
                from ..estimation.tree import tree_count
                result = tree_count(self.fia, **kwargs)
                
                if len(result) == 0:
                    return "No trees found matching the specified criteria."
                
                # Enhanced result formatting
                return self._format_tree_results_enhanced(result, kwargs)
                
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                return f"""Error executing tree command with arguments: {command_args}

Parsed parameters: {kwargs}

Error details:
{str(e)}

Full traceback:
{error_details}

This error occurred while trying to execute a tree count query. Please check the parameters and database connection."""
        
        def execute_mortality_command(command_args: str) -> str:
            """
            Execute mortality estimation commands using the estimation interface.
            
            This function automatically handles GRM EVALID selection by:
            1. Parsing area_domain for state codes
            2. Using get_recommended_evalid with "mortality" analysis type
            3. Setting the EVALID on the FIA instance before calling mortality
            
            Args:
                command_args: CLI-style arguments (e.g., "bySpecies treeClass=growing_stock area_domain=\"STATECD == 37\"")
                
            Returns:
                Formatted mortality estimation results with enhanced statistical context
            """
            try:
                # Parse CLI arguments into kwargs
                kwargs = {}
                
                # Handle boolean flags
                if "bySpecies" in command_args:
                    kwargs["by_species"] = True
                if "bySizeClass" in command_args:
                    kwargs["by_size_class"] = True
                if "totals" in command_args:
                    kwargs["totals"] = True
                if "variance" in command_args:
                    kwargs["variance"] = True
                
                # Parse key=value pairs
                import shlex
                try:
                    parts = shlex.split(command_args)
                except ValueError:
                    parts = command_args.split()
                
                for part in parts:
                    if "=" in part:
                        key, value = part.split("=", 1)
                        # Convert camelCase to snake_case for consistency
                        if key == "treeDomain":
                            key = "tree_domain"
                        elif key == "areaDomain":
                            key = "area_domain"
                        elif key == "treeType":
                            key = "tree_type"
                        elif key == "landType":
                            key = "land_type"
                        elif key == "treeClass":
                            key = "tree_class"
                        elif key == "grpBy":
                            key = "grp_by"
                        
                        # Handle quoted strings
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        else:
                            # Try to convert to appropriate type
                            if value.lower() in ('true', 'false'):
                                value = value.lower() == 'true'
                            else:
                                try:
                                    value = int(value)
                                except ValueError:
                                    try:
                                        value = float(value)
                                    except ValueError:
                                        pass  # Keep as string
                        kwargs[key] = value
                
                # Set defaults appropriate for mortality
                kwargs.setdefault("tree_type", "all")  # Mortality can include all tree types
                kwargs.setdefault("land_type", "forest")
                kwargs.setdefault("tree_class", "all")  # Can be "all" or "growing_stock"
                kwargs["totals"] = True  # Always include totals for population estimates
                
                # Handle GRM EVALID selection before calling mortality
                if not self.fia.evalid and kwargs.get("area_domain"):
                    # Extract state code from area_domain
                    import re
                    area_domain = kwargs["area_domain"]
                    state_match = re.search(r'STATECD\s*==\s*(\d+)', area_domain)
                    if state_match:
                        state_code = int(state_match.group(1))
                        # Get recommended GRM EVALID for mortality
                        from ..filters.evalid import get_recommended_evalid
                        recommended_evalid, explanation = get_recommended_evalid(
                            self.query_interface, state_code, "mortality"
                        )
                        if recommended_evalid:
                            self.fia.evalid = recommended_evalid
                            if self.verbose:
                                print(f"[DEBUG] Auto-selected GRM EVALID {recommended_evalid}: {explanation}")
                
                # Check if we have a valid GRM EVALID
                if not self.fia.evalid:
                    return "Error: Mortality estimation requires GRM (Growth/Removal/Mortality) evaluation. Please specify area_domain with state code or use mr=true for most recent GRM evaluation."
                
                # Execute mortality estimation through the estimation module
                from ..estimation.mortality import mortality
                result = mortality(self.fia, **kwargs)
                
                if len(result) == 0:
                    return "No mortality data found matching the specified criteria."
                
                # Enhanced result formatting
                return self._format_mortality_results_enhanced(result, kwargs)
                
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                return f"""Error executing mortality command with arguments: {command_args}

Parsed parameters: {kwargs}

Error details:
{str(e)}

Full traceback:
{error_details}

This error occurred while trying to execute a mortality estimation query. Please check the parameters and database connection. Note that mortality estimation requires GRM (Growth/Removal/Mortality) evaluations, not standard volume evaluations."""
        
        # Create system prompt
        system_prompt = """You are an expert Forest Inventory Analysis (FIA) assistant.

Your role is to help users query and analyze FIA data using natural language.

Key concepts:
- EVALID: Evaluation ID groups statistically valid plot measurements
- Always use EVALID for population estimates, not raw year filtering
- EVALID selection prioritizes statewide evaluations over regional ones
- Species are identified by SPCD (species code)
- States are identified by STATECD (FIPS code)
- Mortality requires GRM (Growth/Removal/Mortality) evaluations with remeasurement data

When users ask questions:
1. First identify what they're looking for (species, location, metric)
2. If user mentions any geographic location, use parse_location_from_query to extract and resolve it
3. Find appropriate EVALIDs using get_evalid_info (prioritizes statewide evaluations)
4. Look up species codes with find_species_codes if needed
5. Use execute_tree_command for tree counts, execute_area_command for area estimates, execute_mortality_command for mortality analysis, or execute_fia_query for complex queries
6. ALWAYS include the area_domain filter based on parsed location information
7. ALWAYS explain your filtering assumptions using explain_domain_filters
8. Provide clear, concise answers with appropriate context

ERROR HANDLING AND TRANSPARENCY:
When any tool fails or returns an error:
- ALWAYS share the specific error message with the user
- Explain what operation was being attempted
- Suggest alternative approaches or troubleshooting steps
- Never hide technical details - users need to understand what went wrong
- If an error occurs, immediately provide the full error details and context

Domain Filter Intelligence:
- Use explain_domain_filters to transparently communicate what assumptions you're making
- Use suggest_domain_options to help users understand common filter options for their analysis type
- When users don't specify filters, use intelligent defaults and explain them clearly
- Examples: "live trees" vs "all trees", "forest land" vs "timberland"

Tree counting examples:
- "bySpecies treeType=live area_domain=\"STATECD == 37\"" - Live trees by species in North Carolina
- "bySizeClass landType=timber area_domain=\"STATECD == 48\"" - Trees by size class on timber land in Texas
- "treeDomain=\"SPCD == 131\" treeType=live area_domain=\"STATECD == 37\"" - Live loblolly pine trees in North Carolina

Mortality estimation examples:
- "totals landType=forest area_domain=\"STATECD == 37\"" - Total annual mortality in North Carolina
- "bySpecies treeClass=growing_stock area_domain=\"STATECD == 48\"" - Growing stock mortality by species in Texas
- "treeDomain=\"SPCD == 131\" treeClass=all area_domain=\"STATECD == 37\"" - Loblolly pine mortality in North Carolina

CRITICAL: Always include area_domain with STATECD when the user specifies a location. This is required for proper EVALID selection.

Location Resolution Workflow:
- For ANY query mentioning a location, use parse_location_from_query first
- Extract the suggested area_domain from the response
- Include that area_domain in your command parameters
- Example: "loblolly trees in north carolina" → parse_location_from_query → area_domain="STATECD == 37"
- Example: "oak trees in Texas" → parse_location_from_query → area_domain="STATECD == 48"

Area estimation examples:
- "landType=forest totals=true stateCode=27" - Total forest area for Minnesota
- "byLandType totals=true evalid=272201" - Area by land type for specific EVALID
- "landType=timber areaDomain=\"FORTYPCD == 182\" stateCode=48" - Loblolly pine timber area in Texas
- "treeDomain=\"SPCD == 131\" landType=forest variance=true stateCode=37" - Forest area with loblolly pine trees in North Carolina

IMPORTANT EVALID Workflow for Area Estimation:
1. Use get_recommended_evalid to find the appropriate EVALID for area analysis
2. For area estimates, prefer "area" analysis type which maps to EXPCURR evaluation type
3. Always include stateCode or evalid parameter in execute_area_command
4. The system automatically chooses between basic area estimation (fast) and advanced workflow (enhanced validation and metadata) based on query complexity

TRANSPARENCY REQUIREMENT:
Always explain what filters and assumptions you're using! Use explain_domain_filters after any estimation to show users:
- What tree types are included (live, dead, growing stock, etc.)
- What land types are included (forest, timber, all land)
- Any custom filters that were applied
- What defaults were chosen and why

This helps users understand your analysis and adjust filters if needed."""
        
        # Define interrupt points if human approval is enabled
        interrupt_before = ["execute_fia_query", "count_trees_by_criteria"] if self.enable_human_approval else None
        
        # Create the agent
        tools = [
            execute_fia_query,
            get_database_schema,
            find_species_codes,
            get_evalid_info,
            get_recommended_evalid,
            get_state_codes,
            parse_location_from_query,
            execute_tree_command,
            execute_area_command,
            execute_mortality_command,
            explain_domain_filters,
            suggest_domain_options,
        ]
        
        agent = create_react_agent(
            model=self.llm,
            tools=tools,
            checkpointer=self.checkpointer,
            prompt=system_prompt,
            interrupt_before=interrupt_before,
        )
        
        return agent
    
    def _format_tree_results_enhanced(self, result: 'pl.DataFrame', query_params: dict) -> str:
        """
        Enhanced formatting for tree count results using the new result formatter.
        
        Args:
            result: Polars DataFrame with tree count results
            query_params: Original query parameters for context
            
        Returns:
            Formatted string with comprehensive result presentation
        """
        try:
            from .result_formatter import create_result_formatter
            
            # Create formatter instance
            formatter = create_result_formatter("enhanced")
            
            # Gather EVALID info if available
            evalid_info = None
            if hasattr(self.fia, 'evalid') and self.fia.evalid:
                evalid = self.fia.evalid if isinstance(self.fia.evalid, (int, str)) else self.fia.evalid[0]
                evalid_info = {
                    'evalid': evalid,
                    'description': f"FIA Evaluation {evalid}"
                }
            
            # Use the enhanced formatter
            return formatter.format_tree_count_results(result, query_params, evalid_info)
            
        except ImportError:
            # Fallback to simple formatting if formatter not available
            return self._format_tree_results_simple(result, query_params)
    
    def _format_area_results_enhanced(self, result: 'pl.DataFrame', query_params: dict, calculation_method: str) -> str:
        """
        Enhanced formatting for area estimation results using the result formatter.
        
        Args:
            result: Polars DataFrame with area estimation results
            query_params: Original query parameters for context
            calculation_method: Which calculation method was used
            
        Returns:
            Formatted string with comprehensive result presentation
        """
        try:
            from .result_formatter import create_result_formatter
            
            # Create formatter instance
            formatter = create_result_formatter("enhanced")
            
            # Gather EVALID info if available
            evalid_info = None
            if hasattr(self.fia, 'evalid') and self.fia.evalid:
                evalid = self.fia.evalid if isinstance(self.fia.evalid, (int, str)) else self.fia.evalid[0]
                evalid_info = {
                    'evalid': evalid,
                    'description': f"FIA Evaluation {evalid}"
                }
            
            # Use the enhanced formatter for area results
            return formatter.format_area_estimation_results(result, query_params, evalid_info, calculation_method)
            
        except ImportError:
            # Fallback to simple formatting if formatter not available
            return self._format_area_results_simple(result, query_params, calculation_method)
    
    def _format_area_results_simple(self, result: 'pl.DataFrame', query_params: dict, calculation_method: str) -> str:
        """
        Simple fallback formatting for area estimation results.
        
        Args:
            result: Polars DataFrame with area estimation results
            query_params: Original query parameters for context
            calculation_method: Which calculation method was used
            
        Returns:
            Basic formatted string
        """
        formatted = f"Area Estimation Results (using {calculation_method}):\n\n"
        
        for row in result.iter_rows(named=True):
            if 'LAND_TYPE' in row and row['LAND_TYPE']:
                formatted += f"Land Type: {row['LAND_TYPE']}\n"
            
            if 'AREA' in row:
                formatted += f"Total Area: {row['AREA']:,.0f} acres\n"
            
            if 'AREA_PERC' in row:
                formatted += f"Area Percentage: {row['AREA_PERC']:.2f}%\n"
            
            if 'AREA_SE' in row and row['AREA_SE']:
                formatted += f"Standard Error: ±{row['AREA_SE']:,.0f} acres\n"
                
            if 'N_PLOTS' in row:
                formatted += f"Sample Plots: {row['N_PLOTS']:,}\n"
            
            formatted += "\n"
        
        formatted += f"(Statistically valid area estimate using FIA methodology - {calculation_method})\n"
        return formatted
    
    def _format_tree_results_simple(self, result: 'pl.DataFrame', query_params: dict) -> str:
        """
        Simple fallback formatting for tree count results.
        
        Args:
            result: Polars DataFrame with tree count results
            query_params: Original query parameters for context
            
        Returns:
            Basic formatted string
        """
        formatted = "Tree Count Results:\n\n"
        
        for row in result.iter_rows(named=True):
            if 'COMMON_NAME' in row and row['COMMON_NAME']:
                formatted += f"Species: {row['COMMON_NAME']}"
                if 'SCIENTIFIC_NAME' in row and row['SCIENTIFIC_NAME']:
                    formatted += f" ({row['SCIENTIFIC_NAME']})"
                formatted += "\n"
            
            if 'SIZE_CLASS' in row and row['SIZE_CLASS']:
                formatted += f"Size Class: {row['SIZE_CLASS']}\n"
            
            if 'TREE_COUNT' in row:
                formatted += f"Total Population: {row['TREE_COUNT']:,.0f} trees\n"
            
            if 'SE' in row and row['SE']:
                formatted += f"Standard Error: {row['SE']:,.0f}\n"
                
            if 'SE_PERCENT' in row:
                formatted += f"Standard Error %: {row['SE_PERCENT']:.1f}%\n"
            
            formatted += "\n"
        
        formatted += "(Statistically valid population estimate using FIA methodology)\n"
        return formatted

    def _format_mortality_results_enhanced(self, result: 'pl.DataFrame', query_params: dict) -> str:
        """
        Enhanced formatting for mortality estimation results using the result formatter.
        
        Args:
            result: Polars DataFrame with mortality estimation results
            query_params: Original query parameters for context
            
        Returns:
            Formatted string with comprehensive result presentation
        """
        try:
            from .result_formatter import create_result_formatter
            
            # Create formatter instance
            formatter = create_result_formatter("enhanced")
            
            # Gather EVALID info if available
            evalid_info = None
            if hasattr(self.fia, 'evalid') and self.fia.evalid:
                evalid = self.fia.evalid if isinstance(self.fia.evalid, (int, str)) else self.fia.evalid[0]
                evalid_info = {
                    'evalid': evalid,
                    'description': f"FIA GRM Evaluation {evalid}"
                }
            
            # Use the enhanced formatter for mortality results
            return formatter.format_mortality_estimation_results(result, query_params, evalid_info)
            
        except ImportError:
            # Fallback to simple formatting if formatter not available
            return self._format_mortality_results_simple(result, query_params)
    
    def _format_mortality_results_simple(self, result: 'pl.DataFrame', query_params: dict) -> str:
        """
        Simple fallback formatting for mortality estimation results.
        
        Args:
            result: Polars DataFrame with mortality estimation results
            query_params: Original query parameters for context
            
        Returns:
            Basic formatted string
        """
        formatted = "Mortality Estimation Results:\n\n"
        
        for row in result.iter_rows(named=True):
            if 'COMMON_NAME' in row and row['COMMON_NAME']:
                formatted += f"Species: {row['COMMON_NAME']}"
                if 'SCIENTIFIC_NAME' in row and row['SCIENTIFIC_NAME']:
                    formatted += f" ({row['SCIENTIFIC_NAME']})"
                formatted += "\n"
            
            if 'TREE_CLASS' in row and row['TREE_CLASS']:
                formatted += f"Tree Class: {row['TREE_CLASS']}\n"
            
            # Tree mortality
            if 'MORT_TPA_TOTAL' in row:
                formatted += f"Annual Tree Mortality: {row['MORT_TPA_TOTAL']:,.0f} trees/year\n"
            elif 'MORT_TPA_AC' in row:
                formatted += f"Tree Mortality Rate: {row['MORT_TPA_AC']:.3f} trees/acre/year\n"
            
            # Volume mortality  
            if 'MORT_VOL_TOTAL' in row:
                formatted += f"Volume Mortality: {row['MORT_VOL_TOTAL']:,.0f} cu.ft./year\n"
            elif 'MORT_VOL_AC' in row:
                formatted += f"Volume Mortality Rate: {row['MORT_VOL_AC']:.2f} cu.ft./acre/year\n"
            
            # Biomass mortality
            if 'MORT_BIO_TOTAL' in row:
                formatted += f"Biomass Mortality: {row['MORT_BIO_TOTAL']:,.0f} tons/year\n"
            elif 'MORT_BIO_AC' in row:
                formatted += f"Biomass Mortality Rate: {row['MORT_BIO_AC']:.2f} tons/acre/year\n"
            
            # Standard errors
            if 'MORT_TPA_SE' in row and row['MORT_TPA_SE']:
                formatted += f"Standard Error: ±{row['MORT_TPA_SE']:,.0f}\n"
            if 'MORT_TPA_CV' in row and row['MORT_TPA_CV']:
                formatted += f"Coefficient of Variation: {row['MORT_TPA_CV']:.1f}%\n"
                
            if 'nPlots' in row:
                formatted += f"Sample Plots: {row['nPlots']:,}\n"
            
            formatted += "\n"
        
        formatted += "(Annual mortality estimates using FIA GRM methodology)\n"
        return formatted

    def query(
        self,
        question: str,
        thread_id: Optional[str] = None,
        config: Optional[Dict] = None,
    ) -> str:
        """
        Query the FIA database with a natural language question.
        
        Args:
            question: Natural language query
            thread_id: Conversation thread ID for memory
            config: Additional configuration options
            
        Returns:
            Agent's response
        """
        if not thread_id:
            thread_id = f"default_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create config with thread
        run_config = {
            "configurable": {"thread_id": thread_id}
        }
        if config:
            run_config.update(config)
        
        try:
            # Invoke the agent
            response = self.agent.invoke(
                {"messages": [HumanMessage(content=question)]},
                config=run_config,
            )
            
            # Extract the final message
            final_message = response["messages"][-1].content
            return final_message
            
        except Exception as e:
            return f"Error processing query: {str(e)}"
    
    def get_conversation_history(self, thread_id: str) -> List[BaseMessage]:
        """
        Get conversation history for a thread.
        
        Args:
            thread_id: Thread ID to retrieve
            
        Returns:
            List of messages in the conversation
        """
        state = self.agent.get_state({"configurable": {"thread_id": thread_id}})
        return state.values.get("messages", [])
    
    def clear_memory(self, thread_id: Optional[str] = None):
        """
        Clear conversation memory.
        
        Args:
            thread_id: Specific thread to clear, or None for all
        """
        if isinstance(self.checkpointer, MemorySaver):
            # For in-memory checkpointer, we need to clear it
            self.checkpointer = MemorySaver()
            self.agent = self._create_agent()


# Convenience functions for backward compatibility and ease of use

def create_fia_agent(
    db_path: Union[str, Path], 
    api_key: Optional[str] = None, 
    **kwargs
) -> FIAAgent:
    """
    Create a FIA AI Agent with optional configuration.
    
    Args:
        db_path: Path to FIA DuckDB database
        api_key: OpenAI API key
        **kwargs: Additional configuration options
        
    Returns:
        Configured FIAAgent instance
    """
    return FIAAgent(db_path, api_key=api_key, **kwargs)


# Aliases for backward compatibility
FIAAgentModern = FIAAgent  # Alias for old imports